package helps

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
)

func TestUpstreamDiagnosticsWithoutBodyLogging(t *testing.T) {
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	logging.SetGinRequestID(c, "turn-1")
	ctx := context.WithValue(context.Background(), "gin", c)
	cfg := &config.Config{}
	info := UpstreamRequestLog{URL: "wss://secret-user:secret-password@example.test/responses?key=secret-url", Method: "WEBSOCKET", Provider: "codex",
		Headers: http.Header{"Authorization": {"Bearer secret-key"}, "Content-Type": {"application/json"}},
		Body:    []byte(`{"model":"forwarded-model","reasoning":{"effort":"high"},"input":"secret-prompt","tools":[{"name":"secret-tool"}]}`)}
	RecordAPIWebsocketRequest(ctx, cfg, info)
	RecordAPIWebsocketHandshake(ctx, cfg, 101, http.Header{"X-Request-Id": {"up-1"}, "Set-Cookie": {"secret-cookie"}})
	AppendAPIWebsocketResponse(ctx, cfg, []byte(`{"type":"response.compaction.compacting","response":{"model":"actual-model","output":"secret-answer"}}`))
	RecordAPIWebsocketError(ctx, cfg, "read", &websocket.CloseError{Code: 1006, Text: "secret-error-body"})
	first := logging.UpstreamDiagnosticSnapshot(c)
	RecordAPIWebsocketRequest(ctx, cfg, info)
	RecordAPIWebsocketError(ctx, cfg, "read", &websocket.CloseError{Code: 1009, Text: "secret-rejection"})
	info.Method, info.URL = http.MethodPost, "https://example.test/responses?token=secret-url"
	RecordAPIRequest(ctx, cfg, info)
	RecordAPIResponseMetadata(ctx, cfg, 200, http.Header{"Content-Type": {"text/event-stream"}, "Authorization": {"secret-response-key"}})
	AppendAPIResponseChunk(ctx, cfg, []byte(`data: {"type":"response.completed","response":{"model":"actual-model","output":"secret-answer"}}`))
	fields := logging.UpstreamDiagnosticSnapshot(c)
	attempts := fields["upstream_attempts"].([]map[string]any)
	if len(attempts) != 3 {
		t.Fatalf("attempt count = %d", len(attempts))
	}
	ws := attempts[0]["response"].(map[string]any)
	if ws["close_code"] != 1006 || ws["handshake_observed"] != true || ws["terminal_received"] != false {
		t.Fatalf("WS evidence: %#v", ws)
	}
	if attempts[1]["response"].(map[string]any)["close_code"] != 1009 {
		t.Fatal("retry error lost")
	}
	final := attempts[2]["response"].(map[string]any)
	if final["terminal_event"] != "response.completed" || final["reported_model"] != "actual-model" || final["status_code"] != 200 {
		t.Fatalf("SSE evidence: %#v", final)
	}
	encoded, _ := json.Marshal(fields)
	if strings.Contains(string(encoded), "secret-") {
		t.Fatalf("private data leaked: %s", encoded)
	}
	if first["upstream_attempts_total"] != 1 {
		t.Fatal("previous snapshot mutated")
	}
	logging.SetGinRequestID(c, "turn-2")
	logging.ResetUpstreamDiagnostics(c)
	RecordAPIWebsocketRequest(ctx, cfg, info)
	if logging.UpstreamDiagnosticSnapshot(c)["upstream_attempts_total"] != 1 {
		t.Fatal("new turn inherited attempts")
	}
	if logging.UpstreamDiagnosticSnapshot(c)["upstream_response"].(map[string]any)["headers_received"] != false {
		t.Fatal("new turn inherited handshake")
	}
}

func TestUpstreamDiagnosticRetryBoundAndConcurrentSnapshot(t *testing.T) {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx := context.WithValue(context.Background(), "gin", c)
	cfg := &config.Config{}
	for i := 0; i < 20; i++ {
		RecordAPIRequest(ctx, cfg, UpstreamRequestLog{Method: "POST"})
		RecordAPIResponseError(ctx, cfg, io.ErrUnexpectedEOF)
	}
	fields := logging.UpstreamDiagnosticSnapshot(c)
	if fields["upstream_attempts_omitted"] != 4 || len(fields["upstream_attempts"].([]map[string]any)) != 16 {
		t.Fatal("attempt bound not applied")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			AppendAPIResponseChunk(ctx, cfg, []byte(`data: {"type":"response.in_progress"}`))
		}
	}()
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			_, _ = json.Marshal(logging.UpstreamDiagnosticSnapshot(c))
		}
	}()
	wg.Wait()
	RecordAPIResponseError(ctx, cfg, errors.New("secret-message"))
	encoded, _ := json.Marshal(logging.UpstreamDiagnosticSnapshot(c))
	if strings.Contains(string(encoded), "secret-message") {
		t.Fatal("raw error leaked")
	}
}

func TestUpstreamDiagnosticUpgradeRejectionIsOneAttempt(t *testing.T) {
	for _, fullLog := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		ctx := context.WithValue(context.Background(), "gin", c)
		cfg := &config.Config{}
		cfg.RequestLog = fullLog
		info := UpstreamRequestLog{Method: "WEBSOCKET", URL: "wss://example.test/responses"}
		RecordAPIWebsocketRequest(ctx, cfg, info)
		RecordAPIWebsocketUpgradeRejection(ctx, cfg, info, 403, http.Header{"Set-Cookie": {"secret-cookie"}}, []byte("secret-body"))
		fields := logging.UpstreamDiagnosticSnapshot(c)
		if fields["upstream_attempts_total"] != 1 || fields["upstream_response"].(map[string]any)["status_code"] != 403 {
			t.Fatalf("rejection missing or duplicated: %#v", fields)
		}
	}
}
