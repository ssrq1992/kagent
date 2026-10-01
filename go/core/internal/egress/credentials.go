package egress

import (
	"fmt"
	ax "github.com/google/ax/pkg/apis/v1alpha1"
	"google.golang.org/protobuf/proto"
	"net/netip"
	"slices"
	"strings"

	"golang.org/x/net/http/httpguts"
	"k8s.io/apimachinery/pkg/util/validation"
)

// Credential binds a Secret reference to an exact destination and HTTP header.
// Values are fetched by the gateway and never enter a runtime revision.
type Credential struct {
	Hostname     string                  `json:"hostname"`
	Header       string                  `json:"header"`
	Prefix       string                  `json:"prefix,omitempty"`
	SecretKeyRef *ax.CredentialSecretRef `json:"secretKeyRef"`
}

// CanonicalCredentials validates and orders bindings. AX matches exact
// hostnames, so two credentials for the same host and header cannot coexist.
func CanonicalCredentials(bindings []Credential) ([]Credential, error) {
	result := slices.Clone(bindings)
	for i := range result {
		c := &result[i]
		c.Hostname = strings.TrimSuffix(strings.ToLower(c.Hostname), ".")
		c.Header = strings.ToLower(c.Header)
		_, ipErr := netip.ParseAddr(c.Hostname)
		if ipErr == nil || len(validation.IsDNS1123Subdomain(c.Hostname)) != 0 {
			return nil, fmt.Errorf("credential injection requires an exact DNS hostname, got %q", c.Hostname)
		}
		if !httpguts.ValidHeaderFieldName(c.Header) || !httpguts.ValidHeaderFieldValue(c.Prefix) {
			return nil, fmt.Errorf("invalid credential injection header %q", c.Header)
		}
		ref := c.SecretKeyRef
		if ref == nil || len(validation.IsDNS1123Label(ref.Namespace)) != 0 || len(validation.IsDNS1123Subdomain(ref.Name)) != 0 || ref.Key == "" || len(validation.IsConfigMapKey(ref.Key)) != 0 {
			return nil, fmt.Errorf("credential must identify a namespace, Secret and key")
		}
		c.SecretKeyRef = proto.CloneOf(ref)
	}
	slices.SortFunc(result, func(a, b Credential) int {
		return strings.Compare(a.Hostname+"\x00"+a.Header, b.Hostname+"\x00"+b.Header)
	})
	for i := 1; i < len(result); i++ {
		a, b := result[i-1], result[i]
		if a.Hostname == b.Hostname && a.Header == b.Header && !credentialsEqual(a, b) {
			return nil, fmt.Errorf("conflicting credentials for %s header %s", a.Hostname, a.Header)
		}
	}
	result = slices.CompactFunc(result, credentialsEqual)
	counts := map[string]int{}
	for _, c := range result {
		counts[c.Hostname]++
		if counts[c.Hostname] > 16 {
			return nil, fmt.Errorf("credential injection supports at most 16 headers per hostname")
		}
	}
	return result, nil
}

func credentialsEqual(a, b Credential) bool {
	return a.Hostname == b.Hostname && a.Header == b.Header && a.Prefix == b.Prefix && proto.Equal(a.SecretKeyRef, b.SecretKeyRef)
}

// CredentialsEqual ignores protobuf reflection caches.
func CredentialsEqual(a, b []Credential) bool { return slices.EqualFunc(a, b, credentialsEqual) }
