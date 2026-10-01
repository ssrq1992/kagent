package egress

import (
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCanonicalCredentials(t *testing.T) {
	base := Credential{Hostname: "API.Example.com.", Header: "Authorization", Prefix: "Bearer ", SecretKeyRef: &ax.CredentialSecretRef{Namespace: "team", Name: "auth", Key: "token"}}
	got, err := CanonicalCredentials([]Credential{base, base})
	require.NoError(t, err)
	require.Equal(t, []Credential{{Hostname: "api.example.com", Header: "authorization", Prefix: "Bearer ", SecretKeyRef: base.SecretKeyRef}}, got)
	require.Equal(t, "API.Example.com.", base.Hostname)
	for _, test := range []struct {
		name   string
		change func(*Credential)
	}{
		{"wildcard", func(c *Credential) { c.Hostname = "*.example.com" }},
		{"IP", func(c *Credential) { c.Hostname = "192.0.2.1" }},
		{"header", func(c *Credential) { c.Header = "Authorization\r\nInjected" }},
		{"prefix", func(c *Credential) { c.Prefix = "Bearer\n" }},
		{"missing ref", func(c *Credential) { c.SecretKeyRef = nil }},
		{"missing key", func(c *Credential) { c.SecretKeyRef.Key = "" }},
		{"invalid namespace", func(c *Credential) { c.SecretKeyRef.Namespace = "../team" }},
		{"invalid secret", func(c *Credential) { c.SecretKeyRef.Name = "auth/other" }},
		{"invalid key", func(c *Credential) { c.SecretKeyRef.Key = "token/other" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			binding := base
			binding.SecretKeyRef = proto.CloneOf(base.SecretKeyRef)
			test.change(&binding)
			_, err := CanonicalCredentials([]Credential{binding})
			require.Error(t, err)
		})
	}
	other := base
	other.SecretKeyRef = &ax.CredentialSecretRef{Namespace: "team", Name: "other", Key: "token"}
	_, err = CanonicalCredentials([]Credential{base, other})
	require.ErrorContains(t, err, "conflicting credentials")
}
