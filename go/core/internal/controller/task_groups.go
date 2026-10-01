package controller

import (
	"context"
	"time"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Refresh only groups referenced by configuration. Missing groups remain eligible
// for future reads; transient failures retain the last observation. No resource
// is created, and a replacement UID invalidates the compiled revision through KRT.
func refreshTaskGroups(ctx context.Context, client ax.AXClient, refs []*ax.ResourceRef, observe func(*ax.TaskGroup), remove func(string)) {
	seen := make(map[string]bool)
	for _, ref := range refs {
		if ref == nil || ref.Atespace == "" || ref.Name == "" {
			continue
		}
		key := ref.Atespace + "/" + ref.Name
		if seen[key] {
			continue
		}
		seen[key] = true
		if ctx.Err() != nil {
			return
		}
		callCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		group, err := client.GetTaskGroup(callCtx, &ax.GetTaskGroupRequest{Atespace: ref.Atespace, Name: ref.Name})
		cancel()
		if status.Code(err) == codes.NotFound {
			remove(key)
			continue
		}
		if err != nil {
			continue
		}
		actual := ax.Ref(group.GetMetadata())
		if ax.ValidateRef(actual, true) != nil || actual.Atespace != ref.Atespace || actual.Name != ref.Name {
			continue
		}
		observe(group)
	}
}
