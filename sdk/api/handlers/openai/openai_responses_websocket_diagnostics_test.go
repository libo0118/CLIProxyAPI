package openai

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

type websocketDiagnosticExecutor struct {
	homeResponsesWebsocketExecutor
	ids chan string
}

func (e *websocketDiagnosticExecutor) ExecuteStream(ctx context.Context, auth *coreauth.Auth, request coreexecutor.Request, options coreexecutor.Options) (*coreexecutor.StreamResult, error) {
	e.ids <- logging.GetRequestID(ctx)
	cfg := &config.Config{}
	helps.RecordAPIWebsocketRequest(ctx, cfg, helps.UpstreamRequestLog{URL: "wss://upstream.test/responses", Method: "WEBSOCKET",
		Body: []byte(`{"model":"upstream-model","input":"private-ws-upstream-prompt"}`), Headers: http.Header{"Authorization": {"private-ws-upstream-key"}}})
	helps.RecordAPIWebsocketHandshake(ctx, cfg, 101, http.Header{"Set-Cookie": {"private-ws-upstream-cookie"}})
	helps.AppendAPIWebsocketResponse(ctx, cfg, []byte(`{"type":"response.completed","response":{"model":"upstream-reported-model","output":"private-ws-upstream-answer"}}`))
	return e.homeResponsesWebsocketExecutor.ExecuteStream(ctx, auth, request, options)
}

type websocketDiagnosticLogger struct {
	*logging.FileRequestLogger
	records chan map[string]any
}

func (l *websocketDiagnosticLogger) LogRequestDiagnostic(id string, fields map[string]any) error {
	if err := l.FileRequestLogger.LogRequestDiagnostic(id, fields); err != nil {
		return err
	}
	l.records <- fields
	return nil
}

func TestResponsesWebsocketDiagnosticsPersistEachTurnWhileConnected(t *testing.T) {
	gin.SetMode(gin.TestMode)
	executor := &websocketDiagnosticExecutor{ids: make(chan string, 2)}
	manager := coreauth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(executor)
	auth := &coreauth.Auth{ID: "websocket-diagnostic-auth", Provider: "codex", Status: coreauth.StatusActive, Attributes: map[string]string{"websockets": "true"}}
	if _, err := manager.Register(context.Background(), auth); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: "websocket-diagnostic-model"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
	logger := &websocketDiagnosticLogger{FileRequestLogger: logging.NewFileRequestLogger(false, t.TempDir(), "", 10), records: make(chan map[string]any, 2)}
	handler := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
	router := gin.New()
	router.Use(func(c *gin.Context) {
		logging.SetGinRequestID(c, "connection-id")
		c.Request = c.Request.WithContext(logging.WithRequestID(c.Request.Context(), "connection-id"))
		c.Next()
	})
	router.Use(middleware.RequestLoggingMiddleware(logger))
	router.GET("/v1/responses", handler.ResponsesWebsocket)
	server := httptest.NewServer(router)
	defer server.Close()
	headers := http.Header{"Authorization": {"Bearer private-ws-key"}, "Cookie": {"private-ws-cookie"}, "Session_id": {"ws-diagnostic-session"}}
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", headers)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	previousID := ""
	for turn := 0; turn < 2; turn++ {
		if err := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"websocket-diagnostic-model","input":[{"role":"user","content":"private-ws-body"}]}`)); err != nil {
			t.Fatal(err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, _, err := conn.ReadMessage(); err != nil {
			t.Fatal(err)
		}
		select {
		case fields := <-logger.records:
			id := fields["request_id"].(string)
			if id == "connection-id" || id == previousID || id != <-executor.ids {
				t.Fatalf("turn ID not isolated or correlated: %q", id)
			}
			previousID = id
			if fields["upstream_attempts_total"] != 1 {
				t.Fatal("upstream attempts missing or inherited from previous turn")
			}
			upstream := fields["upstream_response"].(map[string]any)
			if upstream["terminal_event"] != "response.completed" || upstream["reported_model"] != "upstream-reported-model" {
				t.Fatalf("upstream evidence missing from persisted diagnostic: %#v", upstream)
			}
			if fields["status"] != "succeeded" || fields["request_protocol"] != "ws" || fields["terminal_received"] != true || fields["terminal_event"] != "response.completed" {
				t.Fatalf("unexpected WS evidence: %#v", fields)
			}
			payload, _ := json.Marshal(fields)
			if strings.Contains(string(payload), "private-ws-") {
				t.Fatal("WS diagnostic leaked body or credentials")
			}
		case <-time.After(time.Second):
			t.Fatal("WS turn completed but diagnostic was not persisted while connection remains open")
		}
	}
}
