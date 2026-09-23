package management

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
)

func TestAPICallCodexQuotaRecovery(t *testing.T) {
	const available = `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":0},"secondary_window":{"used_percent":20}}}`
	for _, tc := range []struct {
		name, body, host, token, account, hostOverride, errorType string
		status, errorStatus                                       int
		concurrent, disabled, wantRecovery                        bool
	}{
		{name: "official reset recovered", body: available, wantRecovery: true},
		{name: "still exhausted", body: strings.ReplaceAll(available, `"used_percent":0`, `"used_percent":100`)},
		{name: "denied", body: strings.ReplaceAll(available, `"allowed":true`, `"allowed":false`)},
		{name: "secondary exhausted", body: strings.ReplaceAll(available, `"used_percent":20`, `"used_percent":100`)},
		{name: "missing windows", body: `{"rate_limit":{"allowed":true}}`},
		{name: "invalid percentage", body: strings.ReplaceAll(available, `"used_percent":0`, `"used_percent":-1`)},
		{name: "ordinary rate limit", body: available, errorType: "rate_limit_exceeded"},
		{name: "authentication failure", body: available, errorStatus: 401},
		{name: "missing evidence", body: `{}`},
		{name: "malformed", body: `{"rate_limit":`},
		{name: "additional limit exhausted", body: `{"rate_limit":{"allowed":true,"primary_window":{"used_percent":0}},"additional_rate_limits":[{"rate_limit":{"allowed":false,"primary_window":{"used_percent":100}}}]}`},
		{name: "wrong host", body: available, host: "example.invalid"},
		{name: "different token", body: available, token: "unrelated-token"},
		{name: "different account", body: available, account: "another-account"},
		{name: "host override", body: available, hostOverride: "example.invalid"},
		{name: "upstream error", body: available, status: 429},
		{name: "newer failure", body: available, concurrent: true},
		{name: "disabled", body: available, disabled: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			manager := coreauth.NewManager(nil, nil, nil)
			ctx := context.Background()
			id := "codex-quota-recovery-test"
			reg := registry.GetGlobalRegistry()
			reg.RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "gpt-test"}, {ID: "network-test"}})
			defer reg.UnregisterClient(id)
			auth, err := manager.Register(ctx, &coreauth.Auth{ID: id, Provider: "codex", Status: coreauth.StatusActive, Disabled: tc.disabled, Metadata: map[string]any{"access_token": "test-token", "account_id": "test-account"}})
			if err != nil {
				t.Fatal(err)
			}
			retry := 97 * time.Hour
			errorStatus, errorType := tc.errorStatus, tc.errorType
			if errorStatus == 0 {
				errorStatus = 429
			}
			if errorType == "" {
				errorType = "usage_limit_reached"
			}
			fail := func() {
				manager.MarkResult(ctx, coreauth.Result{AuthID: id, Provider: "codex", Model: "gpt-test", Error: &coreauth.Error{HTTPStatus: errorStatus, Message: `{"error":{"type":"` + errorType + `"}}`}, RetryAfter: &retry, CredentialScope: errorType == "usage_limit_reached"})
			}
			fail()
			// A quota refresh must not clear an unrelated model's network failure.
			manager.MarkResult(ctx, coreauth.Result{AuthID: id, Provider: "codex", Model: "network-test", Error: &coreauth.Error{HTTPStatus: 503, Message: "unavailable"}})
			server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if tc.concurrent {
					fail()
				}
				if tc.status != 0 {
					w.WriteHeader(tc.status)
				}
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()
			transport := server.Client().Transport.(*http.Transport).Clone()
			transport.TLSClientConfig.ServerName = "127.0.0.1"
			transport.DialContext = func(ctx context.Context, network, _ string) (net.Conn, error) {
				return (&net.Dialer{}).DialContext(ctx, network, server.Listener.Addr().String())
			}
			original := http.DefaultTransport
			http.DefaultTransport = transport
			defer func() { http.DefaultTransport = original; transport.CloseIdleConnections() }()
			host, token := tc.host, tc.token
			if host == "" {
				host = "chatgpt.com"
			}
			if token == "" {
				token = "$TOKEN$"
			}
			account := tc.account
			if account == "" {
				account = "test-account"
			}
			body, _ := json.Marshal(map[string]any{"auth_index": auth.Index, "method": "GET", "url": "https://" + host + "/backend-api/wham/usage", "header": map[string]string{"Authorization": "Bearer " + token, "Chatgpt-Account-Id": account, "Host": tc.hostOverride}})
			h := &Handler{cfg: &config.Config{}, authManager: manager}
			router := gin.New()
			router.POST("/api-call", h.APICall)
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api-call", strings.NewReader(string(body))))
			if recorder.Code != 200 {
				t.Fatalf("status = %d: %s", recorder.Code, recorder.Body.String())
			}
			updated, _ := manager.GetByID(id)
			if got := updated.ModelStates["gpt-test"].LastError == nil; got != tc.wantRecovery {
				t.Fatalf("quota recovered = %v, want %v", got, tc.wantRecovery)
			}
			var response apiCallResponse
			if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
				t.Fatal(err)
			}
			if response.QuotaRecovered != tc.wantRecovery {
				t.Fatalf("recovery notification = %v, want %v", response.QuotaRecovered, tc.wantRecovery)
			}
			if updated.ModelStates["network-test"].LastError == nil || updated.ModelStates["network-test"].LastError.HTTPStatus != 503 {
				t.Fatal("unrelated failure was cleared")
			}
			if updated.Disabled != tc.disabled {
				t.Fatal("disabled state changed")
			}
			if tc.wantRecovery && reg.IsModelQuotaExceededForClient(id, "gpt-test") {
				t.Fatal("registry still blocks recovered quota")
			}
			if tc.wantRecovery && reg.IsModelSuspendedForClient(id, "gpt-test") {
				t.Fatal("registry still suspends recovered model")
			}
		})
	}
}
