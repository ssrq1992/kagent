package a2agateway

import (
	"context"
	"fmt"

	a2atype "github.com/a2aproject/a2a-go/v2/a2a"
	"github.com/a2aproject/a2a-go/v2/a2aclient"
	"github.com/a2aproject/a2a-go/v2/a2aext"
	a2agrpc "github.com/a2aproject/a2a-go/v2/a2agrpc/v1"
	a2apb "github.com/a2aproject/a2a-go/v2/a2apb/v1"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/axruntime"
	"github.com/kagent-dev/kagent/go/core/internal/database"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"
)

type runtimeBindingStore interface {
	GetAXBinding(context.Context, string) (database.AXBinding, error)
}

// RuntimeDialer uses the shared authenticated AX connection and persisted Task UID.
// Neither a public session field nor caller metadata can choose its destination.
type RuntimeDialer struct {
	connection grpc.ClientConnInterface
	store      runtimeBindingStore
}

func NewRuntimeDialer(connection grpc.ClientConnInterface, store runtimeBindingStore) (*RuntimeDialer, error) {
	if connection == nil || store == nil {
		return nil, fmt.Errorf("AX connection and runtime binding store are required")
	}
	return &RuntimeDialer{connection: connection, store: store}, nil
}
func (d *RuntimeDialer) Dial(ctx context.Context, session *apiv1alpha1.Session) (*a2aclient.Client, error) {
	if d.store == nil || d.connection == nil || session.GetId() == "" {
		return nil, fmt.Errorf("persisted AX runtime binding is required")
	}
	binding, err := d.store.GetAXBinding(ctx, session.Id)
	if err != nil {
		return nil, err
	}
	if err = ax.ValidateRef(binding.Task, true); err != nil {
		return nil, err
	}
	conn := &taskConnection{connection: d.connection, ref: proto.CloneOf(binding.Task)}
	return a2aclient.NewFromEndpoints(ctx, []*a2atype.AgentInterface{{URL: "ax-task-gateway", ProtocolBinding: a2atype.TransportProtocolGRPC, ProtocolVersion: a2atype.Version}},
		a2aclient.WithTransport(a2atype.TransportProtocolGRPC, a2aclient.TransportFactoryFn(func(context.Context, *a2atype.AgentCard, *a2atype.AgentInterface) (a2aclient.Transport, error) {
			return a2agrpc.NewGRPCTransportFromClient(a2apb.NewA2AServiceClient(conn)), nil
		})),
		a2aclient.WithCallInterceptors(a2aext.NewClientPropagator(nil)))
}

type taskConnection struct {
	connection grpc.ClientConnInterface
	ref        *ax.ResourceRef
}

func (c *taskConnection) Invoke(ctx context.Context, method string, args, reply any, opts ...grpc.CallOption) error {
	ctx, err := axruntime.TaskContext(ctx, c.ref)
	if err != nil {
		return err
	}
	return c.connection.Invoke(ctx, method, args, reply, opts...)
}
func (c *taskConnection) NewStream(ctx context.Context, desc *grpc.StreamDesc, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	ctx, err := axruntime.TaskContext(ctx, c.ref)
	if err != nil {
		return nil, err
	}
	return c.connection.NewStream(ctx, desc, method, opts...)
}
