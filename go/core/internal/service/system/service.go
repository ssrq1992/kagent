package system

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	"github.com/kagent-dev/kagent/go/core/internal/version"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"github.com/kagent-dev/kagent/go/pkg/logging"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

type Version struct {
	KAgentVersion string
	GitCommit     string
	BuildDate     string
}

type Service struct {
	kubeClient         client.Client
	observedNamespaces []string
	authorizer         auth.Authorizer
	runtime            ax.AXClient
}

type Namespace struct {
	Name   string
	Status string
}

func NewService(
	kubeClient client.Client,
	observedNamespaces []string,
	authorizer auth.Authorizer,
	runtime ax.AXClient,
) *Service {
	return &Service{
		kubeClient:         kubeClient,
		observedNamespaces: slices.Clone(observedNamespaces),
		authorizer:         authorizer,
		runtime:            runtime,
	}
}

func (s *Service) GetVersion() Version {
	info := version.Get()
	return Version{
		KAgentVersion: info.Version,
		GitCommit:     info.GitCommit,
		BuildDate:     info.BuildDate,
	}
}

func (s *Service) GetCurrentUser(ctx context.Context) (map[string]any, error) {
	principal, err := authenticatedPrincipal(ctx)
	if err != nil {
		return nil, err
	}
	if principal.Claims != nil {
		return maps.Clone(principal.Claims), nil
	}
	return map[string]any{"sub": principal.User.ID}, nil
}

func (s *Service) ListNamespaces(ctx context.Context) ([]Namespace, error) {
	if len(s.observedNamespaces) == 0 {
		namespaceList := &corev1.NamespaceList{}
		if err := s.kubeClient.List(ctx, namespaceList); err != nil {
			return nil, serviceerrors.NewInternal("Failed to list namespaces", err)
		}

		namespaces := make([]Namespace, 0, len(namespaceList.Items))
		for _, namespace := range namespaceList.Items {
			namespaces = append(namespaces, Namespace{Name: namespace.Name, Status: string(namespace.Status.Phase)})
		}
		sortNamespaces(namespaces)
		return namespaces, nil
	}

	namespaces := make([]Namespace, 0, len(s.observedNamespaces))
	for _, observedNamespace := range s.observedNamespaces {
		namespace := &corev1.Namespace{}
		if err := s.kubeClient.Get(ctx, client.ObjectKey{Name: observedNamespace}, namespace); err != nil {
			if apierrors.IsForbidden(err) || apierrors.IsUnauthorized(err) {
				namespaces = namespacesFromNames(s.observedNamespaces)
				break
			}
			if apierrors.IsNotFound(err) {
				continue
			}
			logging.FromContext(ctx).ErrorContext(ctx, "failed to get namespace", "error", err, "namespace", observedNamespace)
			continue
		}
		namespaces = append(namespaces, Namespace{Name: namespace.Name, Status: string(namespace.Status.Phase)})
	}
	sortNamespaces(namespaces)
	return namespaces, nil
}

func (s *Service) authorize(ctx context.Context, verb auth.Verb, resource auth.Resource) error {
	principal, err := authenticatedPrincipal(ctx)
	if err != nil {
		return err
	}
	if s.authorizer == nil {
		return serviceerrors.NewInternal("Authorization is not configured", nil)
	}
	if err := s.authorizer.Check(ctx, principal, verb, resource); err != nil {
		return serviceerrors.NewPermissionDenied("Not authorized", err)
	}
	return nil
}

func authenticatedPrincipal(ctx context.Context) (auth.Principal, error) {
	session, ok := auth.AuthSessionFrom(ctx)
	if !ok || session == nil {
		return auth.Principal{}, serviceerrors.NewUnauthenticated("Failed to get authenticated principal", fmt.Errorf("no session found"))
	}
	return session.Principal(), nil
}

func sortNamespaces(namespaces []Namespace) {
	slices.SortStableFunc(namespaces, func(left, right Namespace) int {
		return strings.Compare(strings.ToLower(left.Name), strings.ToLower(right.Name))
	})
}

func namespacesFromNames(names []string) []Namespace {
	result := make([]Namespace, 0, len(names))
	for _, name := range names {
		result = append(result, Namespace{Name: name})
	}
	return result
}

// ListTaskGroups is a configuration query, not an infrastructure inventory API.
func (s *Service) ListTaskGroups(ctx context.Context, namespace string, size int32, token string) (*ax.ListTaskGroupsResponse, error) {
	if namespace == "" {
		return nil, serviceerrors.NewInvalidArgument("namespace is required", nil)
	}
	if len(s.observedNamespaces) > 0 && !slices.Contains(s.observedNamespaces, namespace) {
		return nil, serviceerrors.NewPermissionDenied("Namespace is outside the configured scope", nil)
	}
	if err := s.authorize(ctx, auth.VerbGet, auth.Resource{Type: "TaskGroup", Namespace: namespace}); err != nil {
		return nil, err
	}
	if s.runtime == nil {
		return nil, serviceerrors.NewUnavailable("AX is not configured", nil)
	}
	result, err := s.runtime.ListTaskGroups(ctx, &ax.ListTaskGroupsRequest{Atespace: namespace, PageSize: size, PageToken: token})
	if err != nil {
		return nil, serviceerrors.NewUnavailable("Failed to list AX TaskGroups", err)
	}
	for _, group := range result.Groups {
		if group.GetMetadata().GetAtespace() != namespace {
			return nil, serviceerrors.NewInternal("AX returned a group outside the requested namespace", nil)
		}
	}
	return result, nil
}
