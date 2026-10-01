// Package taskstore connects the upstream A2A SDK to kagent's private gRPC
// persistence service. It keeps no local copy of public conversation history.
package taskstore

import (
	"context"
	"fmt"
	"time"

	"github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2apb/v1/pbconv"
	sdktaskstore "github.com/a2aproject/a2a-go/v2/a2asrv/taskstore"
	"github.com/kagent-dev/kagent/go/adk/pkg/controllerclient"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type Store struct{ client *controllerclient.Client }

var _ sdktaskstore.Store = (*Store)(nil)

func New(client *controllerclient.Client) *Store { return &Store{client: client} }
func (s *Store) sessionID(ctx context.Context) (string, error) {
	ctx, cancel, err := s.callContext(ctx)
	if err != nil {
		return "", err
	}
	defer cancel()
	response, err := s.client.TaskStoreService().ResolveSession(ctx, &apiv1alpha1.TaskStoreServiceResolveSessionRequest{})
	if err != nil {
		return "", fmt.Errorf("resolve AX runtime session: %w", err)
	}
	if response.SessionId == "" {
		return "", fmt.Errorf("empty runtime session identity")
	}
	return response.SessionId, nil
}
func (s *Store) callContext(ctx context.Context) (context.Context, context.CancelFunc, error) {
	ctx, cancel := s.client.CallContext(ctx, "")
	incoming, _ := metadata.FromOutgoingContext(ctx)
	md := metadata.MD{}
	for _, key := range []string{"traceparent", "tracestate"} {
		if values := incoming.Get(key); len(values) > 0 {
			md.Set(key, values...)
		}
	}
	// AX injects the credential over HTTPS; guest code never reads or sends it.
	return metadata.NewOutgoingContext(ctx, md), cancel, nil
}

// Create persists the SDK's initial task and returns its committed version.
func (s *Store) Create(ctx context.Context, task *a2a.Task) (sdktaskstore.TaskVersion, error) {
	wire, err := pbconv.ToProtoTask(task)
	if err != nil {
		return 0, err
	}
	id, err := s.sessionID(ctx)
	if err != nil {
		return 0, err
	}
	if executionFailed(ctx) {
		return 0, fmt.Errorf("previous task persistence failed")
	}
	request := &apiv1alpha1.TaskStoreServiceCreateTaskRequest{SessionId: id, Task: wire}
	if state, ok := ctx.Value(executionKey{}).(*execution); ok {
		request.DispatchId = state.dispatchID
	}
	var response *apiv1alpha1.TaskStoreServiceCreateTaskResponse
	err = s.retry(ctx, func(ctx context.Context) error {
		var err error
		response, err = s.client.TaskStoreService().CreateTask(ctx, request)
		return err
	})
	if err != nil {
		recordSaveFailure(ctx)
		return 0, sdkError(err)
	}
	recordSave(ctx, task, response.Version)
	return sdktaskstore.TaskVersion(response.Version), nil
}

func (s *Store) Update(ctx context.Context, update *sdktaskstore.UpdateRequest) (sdktaskstore.TaskVersion, error) {
	if executionFailed(ctx) {
		return 0, fmt.Errorf("previous task persistence failed")
	}
	task, err := pbconv.ToProtoTask(update.Task)
	if err != nil {
		return 0, err
	}
	event, err := pbconv.ToProtoStreamResponse(update.Event)
	if err != nil {
		return 0, err
	}
	id, err := s.sessionID(ctx)
	if err != nil {
		return 0, err
	}
	request := &apiv1alpha1.TaskStoreServiceUpdateTaskRequest{SessionId: id, Task: task, Event: event, ExpectedVersion: int64(update.PrevVersion)}
	if state, ok := ctx.Value(executionKey{}).(*execution); ok {
		request.DispatchId = state.dispatchID
	}
	var response *apiv1alpha1.TaskStoreServiceUpdateTaskResponse
	err = s.retry(ctx, func(ctx context.Context) error {
		var err error
		response, err = s.client.TaskStoreService().UpdateTask(ctx, request)
		return err
	})
	if err != nil {
		recordSaveFailure(ctx)
		return 0, sdkError(err)
	}
	recordSave(ctx, update.Task, response.Version)
	return sdktaskstore.TaskVersion(response.Version), nil
}

func (s *Store) Get(ctx context.Context, taskID a2a.TaskID) (*sdktaskstore.StoredTask, error) {
	id, err := s.sessionID(ctx)
	if err != nil {
		return nil, err
	}
	var response *apiv1alpha1.TaskStoreServiceGetTaskResponse
	err = s.retry(ctx, func(ctx context.Context) error {
		var err error
		response, err = s.client.TaskStoreService().GetTask(ctx, &apiv1alpha1.TaskStoreServiceGetTaskRequest{SessionId: id, TaskId: string(taskID)})
		return err
	})
	if err != nil {
		return nil, sdkError(err)
	}
	current, err := fromStored(response.Stored)
	if err != nil {
		return nil, err
	}
	return current, nil
}

func (s *Store) List(ctx context.Context, request *a2a.ListTasksRequest) (*a2a.ListTasksResponse, error) {
	wire, err := pbconv.ToProtoListTasksRequest(request)
	if err != nil {
		return nil, err
	}
	id, err := s.sessionID(ctx)
	if err != nil {
		return nil, err
	}
	var response *apiv1alpha1.TaskStoreServiceListTasksResponse
	err = s.retry(ctx, func(ctx context.Context) error {
		var err error
		response, err = s.client.TaskStoreService().ListTasks(ctx, &apiv1alpha1.TaskStoreServiceListTasksRequest{SessionId: id, Request: wire})
		return err
	})
	if err != nil {
		return nil, sdkError(err)
	}
	return pbconv.FromProtoListTasksResponse(response.Result)
}

// retry retains the same immutable mutation after a lost response. It never
// retries conflicts, advances a version locally, or invokes the agent again.
func (s *Store) retry(ctx context.Context, call func(context.Context) error) error {
	for attempt := range 4 {
		rpcCtx, cancel, err := s.callContext(ctx)
		if err != nil {
			return err
		}
		err = call(rpcCtx)
		cancel()
		if (status.Code(err) != codes.Unavailable && status.Code(err) != codes.DeadlineExceeded) || attempt == 3 {
			return err
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 100 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
	panic("unreachable")
}

func fromStored(stored *apiv1alpha1.StoredTask) (*sdktaskstore.StoredTask, error) {
	if stored == nil || stored.Task == nil || stored.Version <= 0 {
		return nil, fmt.Errorf("TaskStore returned an invalid stored task")
	}
	task, err := pbconv.FromProtoTask(stored.Task)
	if err != nil {
		return nil, err
	}
	return &sdktaskstore.StoredTask{Task: task, Version: sdktaskstore.TaskVersion(stored.Version)}, nil
}

func sdkError(err error) error {
	switch status.Code(err) {
	case codes.NotFound:
		return fmt.Errorf("load runtime task: %w", a2a.ErrTaskNotFound)
	case codes.AlreadyExists:
		return fmt.Errorf("create runtime task: %w", sdktaskstore.ErrTaskAlreadyExists)
	case codes.Aborted:
		return fmt.Errorf("save runtime task: %w", sdktaskstore.ErrConcurrentModification)
	case codes.PermissionDenied, codes.Unauthenticated:
		return a2a.ErrUnauthorized
	case codes.InvalidArgument, codes.FailedPrecondition:
		return a2a.NewError(a2a.ErrInvalidParams, status.Convert(err).Message())
	default:
		return err
	}
}
