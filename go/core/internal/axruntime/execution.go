package axruntime

import (
	"context"
	"fmt"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

// Execution pins every guest operation to a server-resolved, persisted Task.
// Caller messages and routing metadata cannot select another runtime. The
// underlying client must use the AX controller identity, never a user's token.
func (c *Client) ForTask(ref *ax.ResourceRef) (ax.TaskExecutionServiceClient, error) {
	return BindExecution(c.Execution, ref)
}

func BindExecution(client ax.TaskExecutionServiceClient, ref *ax.ResourceRef) (ax.TaskExecutionServiceClient, error) {
	if client == nil {
		return nil, fmt.Errorf("AX execution client is required")
	}
	if err := ax.ValidateRef(ref, true); err != nil {
		return nil, err
	}
	return &boundExecution{client: client, ref: proto.CloneOf(ref)}, nil
}

type boundExecution struct {
	client ax.TaskExecutionServiceClient
	ref    *ax.ResourceRef
}

// Explicitly drop caller credentials and transport addressing. The TLS client
// identity is installed on the connection; AX derives backend routing itself.
func executionContext(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	clean := metadata.MD{}
	for _, key := range []string{"traceparent", "tracestate"} {
		if values := md.Get(key); len(values) == 1 {
			clean.Set(key, values[0])
		}
	}
	return metadata.NewOutgoingContext(ctx, clean)
}
func (b *boundExecution) StartProcess(ctx context.Context, req *ax.StartProcessRequest, opts ...grpc.CallOption) (*ax.StartProcessResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("process request is required")
	}
	copy := proto.CloneOf(req)
	copy.TaskRef = proto.CloneOf(b.ref)
	return b.client.StartProcess(executionContext(ctx), copy, opts...)
}
func (b *boundExecution) GetProcess(ctx context.Context, req *ax.GetProcessRequest, opts ...grpc.CallOption) (*ax.Process, error) {
	if req == nil {
		return nil, fmt.Errorf("process request is required")
	}
	copy := proto.CloneOf(req)
	copy.TaskRef = proto.CloneOf(b.ref)
	return b.client.GetProcess(executionContext(ctx), copy, opts...)
}
func (b *boundExecution) KillProcess(ctx context.Context, req *ax.KillProcessRequest, opts ...grpc.CallOption) (*ax.KillProcessResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("process request is required")
	}
	copy := proto.CloneOf(req)
	copy.TaskRef = proto.CloneOf(b.ref)
	return b.client.KillProcess(executionContext(ctx), copy, opts...)
}
func (b *boundExecution) StreamProcessOutputs(ctx context.Context, req *ax.StreamProcessOutputsRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ax.OutputChunk], error) {
	if req == nil {
		return nil, fmt.Errorf("output request is required")
	}
	copy := proto.CloneOf(req)
	copy.TaskRef = proto.CloneOf(b.ref)
	return b.client.StreamProcessOutputs(executionContext(ctx), copy, opts...)
}
func (b *boundExecution) ReadFile(ctx context.Context, req *ax.ReadFileRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[ax.FileChunk], error) {
	if req == nil {
		return nil, fmt.Errorf("file request is required")
	}
	copy := proto.CloneOf(req)
	copy.TaskRef = proto.CloneOf(b.ref)
	return b.client.ReadFile(executionContext(ctx), copy, opts...)
}
func (b *boundExecution) WriteFile(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[ax.WriteFileRequest, ax.WriteFileResponse], error) {
	stream, err := b.client.WriteFile(executionContext(ctx), opts...)
	if err != nil {
		return nil, err
	}
	return &boundWriter{ClientStreamingClient: stream, ref: proto.CloneOf(b.ref)}, nil
}

type boundWriter struct {
	grpc.ClientStreamingClient[ax.WriteFileRequest, ax.WriteFileResponse]
	ref *ax.ResourceRef
}

func (w *boundWriter) Send(req *ax.WriteFileRequest) error {
	if req == nil {
		return fmt.Errorf("file chunk is required")
	}
	copy := proto.CloneOf(req)
	copy.TaskRef = proto.CloneOf(w.ref)
	return w.ClientStreamingClient.Send(copy)
}

// Generated streams expose SendMsg as well as Send. Keep the identity invariant
// even when a transport adapter forwards messages through grpc.ClientStream.
func (w *boundWriter) SendMsg(message any) error {
	req, ok := message.(*ax.WriteFileRequest)
	if !ok {
		return fmt.Errorf("expected AX WriteFileRequest")
	}
	return w.Send(req)
}
