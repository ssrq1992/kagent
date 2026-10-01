package taskstore

import (
	"context"
	"crypto/tls"
	"errors"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"net/http"
	"strings"
	"testing"
)

type identityRuntime struct {
	ax.AXClient
	ref   *ax.ResourceRef
	err   error
	calls int
}

func (r *identityRuntime) AuthenticateRuntime(_ context.Context, req *ax.AuthenticateRuntimeRequest, _ ...grpc.CallOption) (*ax.AuthenticateRuntimeResponse, error) {
	r.calls++
	if r.err != nil {
		return nil, r.err
	}
	return &ax.AuthenticateRuntimeResponse{TaskRef: r.ref, Scopes: []string{"taskstore"}}, nil
}
func TestAXRuntimeAuthentication(t *testing.T) {
	id := uuid.NewString()
	runtime := &identityRuntime{ref: &ax.ResourceRef{Atespace: "team-a", Name: "session-" + id, Uid: "task-uid"}}
	secure := peer.NewContext(t.Context(), &peer.Peer{AuthInfo: credentials.TLSInfo{State: tls.ConnectionState{HandshakeComplete: true}}})
	credential := "team-a:" + strings.Repeat("a", 43)
	for _, test := range []struct {
		name    string
		ctx     context.Context
		headers http.Header
		ok      bool
	}{
		{"verified", secure, http.Header{http.CanonicalHeaderKey(ax.RuntimeCredentialHeader): {credential}}, true},
		{"plaintext", t.Context(), http.Header{http.CanonicalHeaderKey(ax.RuntimeCredentialHeader): {credential}}, false},
		{"unsigned legacy identity", secure, http.Header{"X-Kagent-Insecure-Runtime-Identity": {"team-a/session-" + id + "/task-uid"}}, false},
		{"duplicate", secure, http.Header{http.CanonicalHeaderKey(ax.RuntimeCredentialHeader): {credential, credential}}, false},
		{"user token", secure, http.Header{"Authorization": {"Bearer user-token"}}, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			session, err := (&Authenticator{Runtime: runtime}).Authenticate(test.ctx, test.headers, nil)
			if !test.ok {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.Equal(t, runtimeSession{sessionID: id, atespace: "team-a", taskUID: "task-uid"}, session)
		})
	}
	runtime.err = errors.New("revoked")
	_, err := (&Authenticator{Runtime: runtime}).Authenticate(secure, http.Header{http.CanonicalHeaderKey(ax.RuntimeCredentialHeader): {credential}}, nil)
	require.Error(t, err)
}
