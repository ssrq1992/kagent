package controller

import (
	"context"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"testing"
)

type groupRefreshClient struct {
	ax.AXClient
	group *ax.TaskGroup
	err   error
	calls int
}

func (c *groupRefreshClient) GetTaskGroup(_ context.Context, req *ax.GetTaskGroupRequest, _ ...grpc.CallOption) (*ax.TaskGroup, error) {
	c.calls++
	if req.Atespace != "team" || req.Name != "agents" {
		panic("unexpected group")
	}
	return c.group, c.err
}
func TestTaskGroupRefreshReplacementAndRecovery(t *testing.T) {
	client := &groupRefreshClient{}
	ref := &ax.ResourceRef{Atespace: "team", Name: "agents", Uid: "old"}
	cached := &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: "team", Name: "agents", Uid: "old"}}
	refresh := func() {
		refreshTaskGroups(t.Context(), client, []*ax.ResourceRef{ref, ref}, func(g *ax.TaskGroup) { cached = g }, func(key string) { require.Equal(t, "team/agents", key); cached = nil })
	}
	client.err = status.Error(codes.Unavailable, "offline")
	refresh()
	require.Equal(t, "old", cached.Metadata.Uid)
	require.Equal(t, 1, client.calls, "deduplicate references")
	client.err = status.Error(codes.NotFound, "deleted")
	refresh()
	require.Nil(t, cached)
	client.err = nil
	client.group = &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: "team", Name: "agents", Uid: "replacement"}}
	refresh()
	require.Equal(t, "replacement", cached.Metadata.Uid, "missing observations must be retried")
	client.group = &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: "other", Name: "agents", Uid: "wrong"}}
	refresh()
	require.Equal(t, "replacement", cached.Metadata.Uid, "reject a cross-namespace response")
	client.group = &ax.TaskGroup{Metadata: &ax.ObjectMeta{Atespace: "team", Name: "agents"}}
	refresh()
	require.Equal(t, "replacement", cached.Metadata.Uid, "reject an unbound observation")
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	before := client.calls
	refreshTaskGroups(ctx, client, []*ax.ResourceRef{ref}, func(*ax.TaskGroup) { t.Fatal("cancelled") }, func(string) { t.Fatal("cancelled") })
	require.Equal(t, before, client.calls)
}
