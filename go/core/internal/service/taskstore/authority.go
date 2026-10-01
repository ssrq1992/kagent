package taskstore

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"slices"
	"strings"

	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"github.com/google/uuid"
	"github.com/kagent-dev/kagent/go/core/pkg/auth"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
)

// Authenticator verifies the credential injected by AX's HTTPS egress policy.
// The namespace prefix routes verification; only AX's response grants identity.
type Authenticator struct{ Runtime ax.AXClient }

var _ auth.AuthProvider = (*Authenticator)(nil)

func (a *Authenticator) Authenticate(ctx context.Context, headers http.Header, _ url.Values) (auth.Session, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return nil, fmt.Errorf("runtime storage requires TLS")
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok || !tlsInfo.State.HandshakeComplete {
		return nil, fmt.Errorf("runtime storage requires TLS")
	}
	values := headers.Values(ax.RuntimeCredentialHeader)
	if len(values) != 1 || a.Runtime == nil {
		return nil, fmt.Errorf("one AX runtime credential is required")
	}
	space, err := ax.RuntimeCredentialAtespace(values[0])
	if err != nil {
		return nil, err
	}
	verified, err := a.Runtime.AuthenticateRuntime(ctx, &ax.AuthenticateRuntimeRequest{Atespace: space, Credential: values[0]})
	if err != nil {
		return nil, err
	}
	ref := verified.GetTaskRef()
	if err = ax.ValidateRef(ref, true); err != nil {
		return nil, err
	}
	if ref.Atespace != space || !slices.Contains(verified.Scopes, "taskstore") {
		return nil, fmt.Errorf("runtime has no TaskStore authority")
	}
	id, ok := strings.CutPrefix(ref.Name, "session-")
	if !ok {
		return nil, fmt.Errorf("runtime is not a Session")
	}
	if _, err = uuid.Parse(id); err != nil {
		return nil, fmt.Errorf("invalid AX session identity")
	}
	return runtimeSession{sessionID: id, atespace: ref.Atespace, taskUID: ref.Uid}, nil
}
func (*Authenticator) UpstreamAuth(*http.Request, auth.Session, auth.Principal) error {
	return fmt.Errorf("runtime authentication cannot forward public credentials")
}

type runtimeSession struct{ sessionID, atespace, taskUID string }

func (s runtimeSession) Principal() auth.Principal {
	return auth.Principal{Agent: auth.Agent{ID: s.sessionID}}
}
