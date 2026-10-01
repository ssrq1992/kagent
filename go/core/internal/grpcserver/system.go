package grpcserver

import (
	"context"

	apiv1alpha1 "github.com/kagent-dev/kagent/go/api/gen/kagent/api/v1alpha1"
	"github.com/kagent-dev/kagent/go/core/internal/service/serviceerrors"
	systemservice "github.com/kagent-dev/kagent/go/core/internal/service/system"
	"google.golang.org/protobuf/types/known/structpb"
)

type systemServer struct {
	apiv1alpha1.UnimplementedSystemServiceServer
	service         *systemservice.Service
	maxMessageBytes int
}

func newSystemServer(service *systemservice.Service, maxMessageBytes int) *systemServer {
	return &systemServer{service: service, maxMessageBytes: maxMessageBytes}
}

func (s *systemServer) GetVersion(context.Context, *apiv1alpha1.GetVersionRequest) (*apiv1alpha1.GetVersionResponse, error) {
	result := s.service.GetVersion()
	return &apiv1alpha1.GetVersionResponse{
		KagentVersion: result.KAgentVersion,
		GitCommit:     result.GitCommit,
		BuildDate:     result.BuildDate,
	}, nil
}

func (s *systemServer) GetCurrentUser(ctx context.Context, _ *apiv1alpha1.GetCurrentUserRequest) (*apiv1alpha1.GetCurrentUserResponse, error) {
	claims, err := s.service.GetCurrentUser(ctx)
	if err != nil {
		return nil, err
	}
	encodedClaims, err := structpb.NewStruct(claims)
	if err != nil {
		return nil, serviceerrors.NewInternal("Failed to encode current user claims", err)
	}
	return &apiv1alpha1.GetCurrentUserResponse{Claims: encodedClaims}, nil
}

func (s *systemServer) ListNamespaces(ctx context.Context, _ *apiv1alpha1.ListNamespacesRequest) (*apiv1alpha1.ListNamespacesResponse, error) {
	result, err := s.service.ListNamespaces(ctx)
	if err != nil {
		return nil, err
	}
	namespaces := make([]*apiv1alpha1.Namespace, 0, len(result))
	for _, namespace := range result {
		namespaces = append(namespaces, &apiv1alpha1.Namespace{
			Name:   namespace.Name,
			Status: namespace.Status,
		})
	}
	return &apiv1alpha1.ListNamespacesResponse{Namespaces: namespaces}, nil
}

func (s *systemServer) ListTaskGroups(ctx context.Context, request *apiv1alpha1.ListTaskGroupsRequest) (*apiv1alpha1.ListTaskGroupsResponse, error) {
	groups, err := s.service.ListTaskGroups(ctx, request.Namespace, request.GetPage().GetLimit(), request.GetPage().GetPageToken())
	if err != nil {
		return nil, err
	}
	response := &apiv1alpha1.ListTaskGroupsResponse{Page: &apiv1alpha1.PageResponse{NextPageToken: groups.NextPageToken}}
	for _, g := range groups.Groups {
		response.Groups = append(response.Groups, &apiv1alpha1.TaskGroupChoice{Ref: &apiv1alpha1.ResourceReference{Namespace: g.Metadata.Atespace, Name: g.Metadata.Name}, Uid: g.Metadata.Uid, Phase: g.GetStatus().GetPhase(), Replicas: g.GetSpec().GetReplicas()})
	}
	return response, nil
}
