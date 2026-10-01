package axruntime

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"
)

type executionProbe struct {
	ax.UnimplementedTaskExecutionServiceServer
	calls     chan *ax.ResourceRef
	cancelled chan struct{}
	headers   chan metadata.MD
}

func (s *executionProbe) record(ctx context.Context, ref *ax.ResourceRef) {
	s.calls <- ref
	md, _ := metadata.FromIncomingContext(ctx)
	s.headers <- md
}
func (s *executionProbe) StartProcess(ctx context.Context, req *ax.StartProcessRequest) (*ax.StartProcessResponse, error) {
	s.record(ctx, req.TaskRef)
	return &ax.StartProcessResponse{ProcessId: "process"}, nil
}
func (s *executionProbe) GetProcess(ctx context.Context, req *ax.GetProcessRequest) (*ax.Process, error) {
	s.record(ctx, req.TaskRef)
	return &ax.Process{ProcessId: req.ProcessId}, nil
}
func (s *executionProbe) KillProcess(ctx context.Context, req *ax.KillProcessRequest) (*ax.KillProcessResponse, error) {
	s.record(ctx, req.TaskRef)
	return &ax.KillProcessResponse{ExitCode: 137}, nil
}
func (s *executionProbe) StreamProcessOutputs(req *ax.StreamProcessOutputsRequest, stream grpc.ServerStreamingServer[ax.OutputChunk]) error {
	s.record(stream.Context(), req.TaskRef)
	if err := stream.Send(&ax.OutputChunk{Data: []byte("output")}); err != nil {
		return err
	}
	<-stream.Context().Done()
	close(s.cancelled)
	return stream.Context().Err()
}
func (s *executionProbe) ReadFile(req *ax.ReadFileRequest, stream grpc.ServerStreamingServer[ax.FileChunk]) error {
	s.record(stream.Context(), req.TaskRef)
	return stream.Send(&ax.FileChunk{Data: []byte{0, 255}})
}
func (s *executionProbe) WriteFile(stream grpc.ClientStreamingServer[ax.WriteFileRequest, ax.WriteFileResponse]) error {
	var size int64
	for {
		req, err := stream.Recv()
		if err == io.EOF {
			return stream.SendAndClose(&ax.WriteFileResponse{BytesWritten: size})
		}
		if err != nil {
			return err
		}
		s.record(stream.Context(), req.TaskRef)
		size += int64(len(req.Chunk))
	}
}
func TestExecutionPinsAllSixMethodsAndCancelsStream(t *testing.T) {
	listener := bufconn.Listen(1 << 20)
	probe := &executionProbe{calls: make(chan *ax.ResourceRef, 10), headers: make(chan metadata.MD, 10), cancelled: make(chan struct{})}
	server := grpc.NewServer()
	ax.RegisterTaskExecutionServiceServer(server, probe)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///ax", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	pinned := &ax.ResourceRef{Atespace: "team", Name: "sandbox", Uid: "task-uid"}
	expected := proto.CloneOf(pinned)
	client, err := BindExecution(ax.NewTaskExecutionServiceClient(conn), pinned)
	require.NoError(t, err)
	pinned.Uid = "changed-after-binding"
	attacker := &ax.ResourceRef{Atespace: "other", Name: "admin", Uid: "other-uid"}
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	ctx = metadata.NewOutgoingContext(ctx, metadata.Pairs("authorization", "Bearer attacker", "ate-target-actor", "other/admin", ax.TargetTaskHeader, "other/admin", ax.TargetTaskUIDHeader, "other-uid", "traceparent", "trace"))
	check := func() {
		t.Helper()
		select {
		case ref := <-probe.calls:
			require.True(t, proto.Equal(expected, ref))
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
		md := <-probe.headers
		for _, key := range []string{"authorization", "ate-target-actor", ax.TargetTaskHeader, ax.TargetTaskUIDHeader} {
			require.Empty(t, md.Get(key))
		}
		require.Equal(t, []string{"trace"}, md.Get("traceparent"))
	}
	request := &ax.StartProcessRequest{TaskRef: attacker, Command: []string{"true"}}
	_, err = client.StartProcess(ctx, request)
	require.NoError(t, err)
	check()
	require.True(t, proto.Equal(attacker, request.TaskRef))
	_, err = client.GetProcess(ctx, &ax.GetProcessRequest{TaskRef: attacker})
	require.NoError(t, err)
	check()
	_, err = client.KillProcess(ctx, &ax.KillProcessRequest{TaskRef: attacker})
	require.NoError(t, err)
	check()
	read, err := client.ReadFile(ctx, &ax.ReadFileRequest{TaskRef: attacker, Path: "data"})
	require.NoError(t, err)
	chunk, err := read.Recv()
	require.NoError(t, err)
	require.Equal(t, []byte{0, 255}, chunk.Data)
	check()
	_, err = read.Recv()
	require.ErrorIs(t, err, io.EOF)
	write, err := client.WriteFile(ctx)
	require.NoError(t, err)
	req := &ax.WriteFileRequest{TaskRef: attacker, Path: "data", Chunk: []byte{0, 255}}
	require.NoError(t, write.Send(req))
	require.NoError(t, write.SendMsg(req))
	result, err := write.CloseAndRecv()
	require.NoError(t, err)
	require.EqualValues(t, 4, result.BytesWritten)
	check()
	check()
	require.True(t, proto.Equal(attacker, req.TaskRef))
	streamCtx, stop := context.WithCancel(ctx)
	defer stop()
	stream, err := client.StreamProcessOutputs(streamCtx, &ax.StreamProcessOutputsRequest{TaskRef: attacker, Follow: true})
	require.NoError(t, err)
	_, err = stream.Recv()
	require.NoError(t, err)
	check()
	stop()
	select {
	case <-probe.cancelled:
	case <-ctx.Done():
		t.Fatal("stream cancellation did not reach AX")
	}
}
