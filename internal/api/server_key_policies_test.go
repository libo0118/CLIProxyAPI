package api

import (
	"context"
	"fmt"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/keypolicy"
)

func TestKeyPolicyHTTPModelScopeAndTransport(t *testing.T) {
	s := newTestServer(t)
	s.cfg.KeyPolicyFile = filepath.Join(t.TempDir(), "policy.json")
	s.initializeKeyPolicies()
	if s.keyPolicies == nil {
		t.Fatal(s.keyPolicyError)
	}
	for _, suffix := range []string{"allowed", "forbidden"} {
		id := "key-policy-http-" + suffix
		if _, err := s.handlers.AuthManager.Register(context.Background(), &auth.Auth{ID: id, Index: id, Provider: "codex"}); err != nil {
			t.Fatal(err)
		}
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: id, OwnedBy: "test"}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	}
	resources := s.handlers.AuthManager.KeyPolicyResources()
	s.keyPolicies.SetResources(resources)
	id := keypolicy.KeyID("test-key")
	if err := s.keyPolicies.UpdatePolicy(id, s.keyPolicies.Snapshot(resources).Revision, keypolicy.Policy{Rules: []keypolicy.Rule{{ResourceID: "key-policy-http-allowed", Period: "month"}}}); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/v1/models", "/v1/models?client_version=0.153.4"} {
		r := httptest.NewRequest("GET", path, nil)
		r.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, r)
		if w.Code != 200 || !strings.Contains(w.Body.String(), "key-policy-http-allowed") || strings.Contains(w.Body.String(), "key-policy-http-forbidden") {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/v1/images/generations", "/v1/alpha/search", "/v1/realtime/client_secrets"} {
		r := httptest.NewRequest("POST", path, strings.NewReader(`{}`))
		r.Header.Set("Authorization", "Bearer test-key")
		w := httptest.NewRecorder()
		s.engine.ServeHTTP(w, r)
		if w.Code != 403 || !strings.Contains(w.Body.String(), "key_policy_transport_unsupported") {
			t.Fatalf("unsupported transport %s: %d %s", path, w.Code, w.Body.String())
		}
	}
	// Exercise strict policy input parsing without bypassing it through the SDK.
	for _, limit := range []string{"", `,"limit_usd":5`, `,"limit_usd":"-1"`} {
		body := fmt.Sprintf(`{"revision":%d,"allow_all":false,"rules":[{"resource_id":"key-policy-http-allowed","period":"month"%s}]}`, s.keyPolicies.Snapshot(resources).Revision, limit)
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("PUT", "/", strings.NewReader(body))
		c.Params = gin.Params{{Key: "key_id", Value: id}}
		s.putKeyPolicy(c)
		if w.Code != 400 {
			t.Fatalf("invalid limit accepted: %d %s", w.Code, w.Body.String())
		}
	}
	if err := s.keyPolicies.UpdatePolicy(id, s.keyPolicies.Snapshot(resources).Revision, keypolicy.Policy{Rules: []keypolicy.Rule{}}); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "/v1/models", nil)
	r.Header.Set("Authorization", "Bearer test-key")
	w := httptest.NewRecorder()
	s.engine.ServeHTTP(w, r)
	if w.Code != 200 || strings.Contains(w.Body.String(), "key-policy-http-") {
		t.Fatalf("revoked catalog leaked: %d %s", w.Code, w.Body.String())
	}
}
