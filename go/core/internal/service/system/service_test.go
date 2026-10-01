package system_test

import (
	"context"
	"errors"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc"
	"testing"

	authimpl "github.com/kagent-dev/kagent/go/core/internal/httpserver/auth"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/internal/service/system"
	pkgAuth "github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrlclient "sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

type systemDenyAuthorizer struct{}

func (systemDenyAuthorizer) Check(context.Context, pkgAuth.Principal, pkgAuth.Verb, pkgAuth.Resource) error {
	return errors.New("denied")
}

func TestCurrentUser(t *testing.T) {
	service := system.NewService(nil, nil, nil, nil)
	claims := map[string]any{"sub": "user-1", "groups": []any{"admins"}}
	ctx := pkgAuth.AuthSessionTo(t.Context(), &authimpl.SimpleSession{P: pkgAuth.Principal{
		User:   pkgAuth.User{ID: "user-1"},
		Claims: claims,
	}})

	result, err := service.GetCurrentUser(ctx)
	require.NoError(t, err)
	assert.Equal(t, claims, result)

	ctx = pkgAuth.AuthSessionTo(t.Context(), &authimpl.SimpleSession{P: pkgAuth.Principal{
		User: pkgAuth.User{ID: "fallback-user"},
	}})
	result, err = service.GetCurrentUser(ctx)
	require.NoError(t, err)
	assert.Equal(t, map[string]any{"sub": "fallback-user"}, result)

	_, err = service.GetCurrentUser(t.Context())
	assert.True(t, serviceerrors.IsCode(err, serviceerrors.CodeUnauthenticated), err)
}

func TestListNamespaces(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, corev1.AddToScheme(scheme))

	t.Run("lists all and sorts case insensitively", func(t *testing.T) {
		kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithObjects(
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "Zoo"}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceActive}},
			&corev1.Namespace{ObjectMeta: metav1.ObjectMeta{Name: "alpha"}, Status: corev1.NamespaceStatus{Phase: corev1.NamespaceTerminating}},
		).Build()
		service := system.NewService(kubeClient, nil, nil, nil)

		result, err := service.ListNamespaces(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []system.Namespace{
			{Name: "alpha", Status: "Terminating"},
			{Name: "Zoo", Status: "Active"},
		}, result)
	})

	t.Run("falls back to watched names when reads are forbidden", func(t *testing.T) {
		kubeClient := fake.NewClientBuilder().WithScheme(scheme).WithInterceptorFuncs(interceptor.Funcs{
			Get: func(context.Context, ctrlclient.WithWatch, ctrlclient.ObjectKey, ctrlclient.Object, ...ctrlclient.GetOption) error {
				return apierrors.NewForbidden(schema.GroupResource{Resource: "namespaces"}, "", nil)
			},
		}).Build()
		service := system.NewService(kubeClient, []string{"team-b", "team-a"}, nil, nil)

		result, err := service.ListNamespaces(t.Context())
		require.NoError(t, err)
		assert.Equal(t, []system.Namespace{{Name: "team-a"}, {Name: "team-b"}}, result)
	})
}

type groupsClient struct {
	ax.AXClient
	calls int
	space string
}

func (c *groupsClient) ListTaskGroups(_ context.Context, r *ax.ListTaskGroupsRequest, _ ...grpc.CallOption) (*ax.ListTaskGroupsResponse, error) {
	c.calls++
	c.space = r.Atespace
	return &ax.ListTaskGroupsResponse{NextPageToken: r.PageToken}, nil
}
func TestTaskGroupsRequireAuthorizedWatchedNamespace(t *testing.T) {
	runtime := &groupsClient{}
	ctx := pkgAuth.AuthSessionTo(t.Context(), &authimpl.SimpleSession{P: pkgAuth.Principal{User: pkgAuth.User{ID: "alice"}}})
	denied := system.NewService(nil, []string{"team"}, systemDenyAuthorizer{}, runtime)
	_, err := denied.ListTaskGroups(ctx, "team", 25, "")
	require.Error(t, err)
	require.Zero(t, runtime.calls)
	service := system.NewService(nil, []string{"team"}, &pkgAuth.NoopAuthorizer{}, runtime)
	_, err = service.ListTaskGroups(ctx, "other", 25, "")
	require.Error(t, err)
	require.Zero(t, runtime.calls)
	_, err = service.ListTaskGroups(t.Context(), "team", 25, "")
	require.Error(t, err)
	require.Zero(t, runtime.calls)
	result, err := service.ListTaskGroups(ctx, "team", 25, "next")
	require.NoError(t, err)
	require.Equal(t, "next", result.NextPageToken)
	require.Equal(t, "team", runtime.space)
}
