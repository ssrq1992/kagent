package sandbox

import (
	"context"
	"errors"
	"io"
	"time"

	guestpb "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"google.golang.org/grpc"
)

const maxFileBytes = 64 << 20

// GuestDialer uses the shared authenticated AX connection. Lifecycle and data
// calls share the controller's mTLS identity, never the caller's user token.
type GuestDialer struct {
	execution guestpb.TaskExecutionServiceClient
}

func NewGuestDialer(connection grpc.ClientConnInterface) *GuestDialer {
	return &GuestDialer{execution: guestpb.NewTaskExecutionServiceClient(connection)}
}

func (s *Service) guestAccess(ctx context.Context, id string, verb auth.Verb) (context.Context, context.CancelFunc, guestpb.TaskExecutionServiceClient, error) {
	if err := uuid.Validate(id); err != nil {
		return nil, nil, nil, serviceerrors.NewInvalidArgument("Sandbox ID must be a UUID", err)
	}
	instance, err := s.authorized(ctx, id, verb)
	if err != nil {
		return nil, nil, nil, err
	}
	if instance.State != apiv1alpha1.RuntimeState_RUNTIME_STATE_READY || !time.Now().Before(instance.ExpiresAt.AsTime()) {
		return nil, nil, nil, serviceerrors.NewFailedPrecondition("Sandbox is not ready or has expired", database.ErrFailedPrecondition)
	}
	binding, err := s.config.Store.GetAXBinding(ctx, id)
	if err != nil {
		return nil, nil, nil, serviceerrors.NewUnavailable("Cannot resolve sandbox AX binding", err)
	}
	execution, err := axruntime.BindExecution(s.config.Guests.execution, binding.Task)
	if err != nil {
		return nil, nil, nil, err
	}
	guestCtx, cancel := context.WithDeadline(ctx, instance.ExpiresAt.AsTime())
	return guestCtx, cancel, execution, nil
}

func (s *Service) StartProcess(ctx context.Context, sandboxID string, request *guestpb.StartProcessRequest) (*guestpb.StartProcessResponse, error) {
	guestCtx, cancelGuest, execution, err := s.guestAccess(ctx, sandboxID, auth.VerbUpdate)
	if err != nil {
		return nil, err
	}
	defer cancelGuest()
	callCtx, cancel := context.WithTimeout(guestCtx, 30*time.Second)
	defer cancel()
	return execution.StartProcess(callCtx, request)
}

func (s *Service) GetProcess(ctx context.Context, sandboxID string, request *guestpb.GetProcessRequest) (*guestpb.Process, error) {
	guestCtx, cancelGuest, execution, err := s.guestAccess(ctx, sandboxID, auth.VerbGet)
	if err != nil {
		return nil, err
	}
	defer cancelGuest()
	callCtx, cancel := context.WithTimeout(guestCtx, 30*time.Second)
	defer cancel()
	return execution.GetProcess(callCtx, request)
}

func (s *Service) KillProcess(ctx context.Context, sandboxID string, request *guestpb.KillProcessRequest) (*guestpb.KillProcessResponse, error) {
	guestCtx, cancelGuest, execution, err := s.guestAccess(ctx, sandboxID, auth.VerbUpdate)
	if err != nil {
		return nil, err
	}
	defer cancelGuest()
	callCtx, cancel := context.WithTimeout(guestCtx, 30*time.Second)
	defer cancel()
	return execution.KillProcess(callCtx, request)
}

func (s *Service) StreamProcessOutputs(ctx context.Context, sandboxID string, request *guestpb.StreamProcessOutputsRequest, send func(*guestpb.OutputChunk) error) error {
	guestCtx, cancelGuest, execution, err := s.guestAccess(ctx, sandboxID, auth.VerbGet)
	if err != nil {
		return err
	}
	defer cancelGuest()
	stream, err := execution.StreamProcessOutputs(guestCtx, request)
	if err != nil {
		return err
	}
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		if err := send(chunk); err != nil {
			return err
		}
	}
}

func (s *Service) ReadFile(ctx context.Context, sandboxID string, request *guestpb.ReadFileRequest, send func(*guestpb.FileChunk) error) error {
	guestCtx, cancelGuest, execution, err := s.guestAccess(ctx, sandboxID, auth.VerbGet)
	if err != nil {
		return err
	}
	defer cancelGuest()
	stream, err := execution.ReadFile(guestCtx, request)
	if err != nil {
		return err
	}
	var total int64
	for {
		chunk, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return err
		}
		total += int64(len(chunk.Data))
		if total > maxFileBytes {
			return serviceerrors.NewResourceExhausted("File exceeds 64 MiB transfer limit", nil)
		}
		if err := send(chunk); err != nil {
			return err
		}
	}
}

func (s *Service) WriteFile(ctx context.Context, sandboxID string, recv func() (*guestpb.WriteFileRequest, error)) (*guestpb.WriteFileResponse, error) {
	guestCtx, cancelGuest, execution, err := s.guestAccess(ctx, sandboxID, auth.VerbUpdate)
	if err != nil {
		return nil, err
	}
	defer cancelGuest()
	stream, err := execution.WriteFile(guestCtx)
	if err != nil {
		return nil, err
	}
	var total int64
	for {
		chunk, err := recv()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return nil, err
		}
		total += int64(len(chunk.GetChunk()))
		if total > maxFileBytes {
			return nil, serviceerrors.NewResourceExhausted("File exceeds 64 MiB transfer limit", nil)
		}
		if err := stream.Send(chunk); errors.Is(err, io.EOF) {
			// Receive the guest's status when it rejects the write early.
			return stream.CloseAndRecv()
		} else if err != nil {
			return nil, err
		}
	}
	return stream.CloseAndRecv()
}
