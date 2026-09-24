package openai

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/api/middleware"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestResponsesDiagnosticsWithBodyLoggingDisabled(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, test := range []struct{ model, status, terminal string }{
		{crossChunkMultilineResponsesModel, "succeeded", "response.completed"},
		{dataOnlyCleanCloseResponsesModel, "incomplete", ""},
		{prematureResponsesStreamModel, "failed", ""},
		{sensitiveInitialErrorResponsesModel, "failed", ""},
	} {
		t.Run(test.model, func(t *testing.T) {
			executor := &prematureResponsesStreamExecutor{}
			manager := coreauth.NewManager(nil, nil, nil)
			manager.RegisterExecutor(executor)
			auth := &coreauth.Auth{ID: "diagnostic-" + test.model, Provider: executor.Identifier(), Status: coreauth.StatusActive}
			if _, err := manager.Register(context.Background(), auth); err != nil {
				t.Fatal(err)
			}
			registry.GetGlobalRegistry().RegisterClient(auth.ID, auth.Provider, []*registry.ModelInfo{{ID: test.model}})
			t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(auth.ID) })
			h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&sdkconfig.SDKConfig{}, manager))
			var summary map[string]any
			logDir := t.TempDir()
			router := gin.New()
			router.Use(func(c *gin.Context) {
				logging.SetGinRequestID(c, "diag-test")
				c.Next()
				value, _ := c.Get(logging.RequestDiagnosticContextKey)
				summary, _ = value.(map[string]any)
			})
			router.Use(middleware.RequestLoggingMiddleware(logging.NewFileRequestLogger(false, logDir, "", 10)))
			router.POST("/v1/responses", h.Responses)
			request := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(fmt.Sprintf(`{"model":%q,"stream":true,"input":"private-prompt","reasoning":{"effort":"high"}}`, test.model)))
			request.Header.Set("User-Agent", "Codex Desktop/26.803.41515")
			request.Header.Set("Authorization", "Bearer private-key")
			request.Header.Set("Cookie", "secret-cookie")
			request.Header.Set("Session_id", "session-diagnostic")
			recorder := httptest.NewRecorder()
			router.ServeHTTP(recorder, request)
			if summary["status"] != test.status || summary["terminal_event"] != test.terminal {
				t.Fatalf("unexpected summary: %#v", summary)
			}
			if test.model == dataOnlyCleanCloseResponsesModel && (summary["status_code"] != 200 || summary["generated_terminal_event"] != "response.failed") {
				t.Fatalf("HTTP success must retain stream failure evidence: %#v", summary)
			}
			if summary["downstream_bytes_written"] != int64(recorder.Body.Len()) {
				t.Fatalf("written bytes = %v, body bytes = %d", summary["downstream_bytes_written"], recorder.Body.Len())
			}
			payload, err := json.Marshal(summary)
			if err != nil {
				t.Fatal(err)
			}
			paths, err := filepath.Glob(filepath.Join(logDir, "diagnostic-*-diag-test.log"))
			if err != nil || len(paths) != 1 {
				t.Fatalf("metadata artifact missing: %v %v", paths, err)
			}
			persisted, err := os.ReadFile(paths[0])
			if err != nil {
				t.Fatal(err)
			}
			for _, secret := range []string{"private-prompt", "private-key", "secret-cookie", "initial-message-secret", "initial-debug-secret", `"delta"`} {
				if strings.Contains(string(payload), secret) || strings.Contains(string(persisted), secret) {
					t.Fatalf("diagnostic leaked %q", secret)
				}
			}
			if summary["codex_session_id"] != "codex:session-diagnostic" {
				t.Fatalf("missing session correlation: %#v", summary)
			}
		})
	}
}

type diagnosticFailWriter struct {
	gin.ResponseWriter
	err error
}

func (w *diagnosticFailWriter) Write(p []byte) (int, error)       { return 1, w.err }
func (w *diagnosticFailWriter) WriteString(p string) (int, error) { return 1, w.err }

func TestResponsesDiagnosticsWriteFailureAndCancellation(t *testing.T) {
	for _, writeString := range []bool{false, true} {
		c, _ := gin.CreateTestContext(httptest.NewRecorder())
		c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
		d := &responsesStreamDiagnostics{ResponseWriter: &diagnosticFailWriter{ResponseWriter: c.Writer, err: io.ErrClosedPipe}, started: time.Now()}
		if writeString {
			_, _ = d.WriteString("test")
		} else {
			_, _ = d.Write([]byte("test"))
		}
		summary := d.summary(c, context.Background(), "test", 0, &responsesSSEFramer{terminalEvent: "response.completed"})
		if summary["status"] != "failed" || summary["completion_reason"] != "downstream_write_failed" || d.written != 1 || d.attempted != 4 {
			t.Fatalf("write failure lost: %#v", summary)
		}
	}
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	c.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil).WithContext(ctx)
	d := &responsesStreamDiagnostics{ResponseWriter: c.Writer, started: time.Now()}
	if got := d.summary(c, ctx, "test", 0, &responsesSSEFramer{}); got["status"] != "cancelled" {
		t.Fatalf("cancel classified as %#v", got)
	}
	d.recordWrite(4, 1, nil)
	if !errors.Is(d.writeErr, io.ErrShortWrite) {
		t.Fatal("short write not recorded")
	}
}

func TestResponsesDiagnosticsDistinguishesGeneratedTerminal(t *testing.T) {
	for _, test := range []struct{ payload, received string }{
		{`{"error":{"message":"failure"}}`, ""},
		{`{"type":"error","error":{"message":"failure"}}`, "error"},
	} {
		framer := &responsesSSEFramer{failureEvent: "response.failed"}
		var output bytes.Buffer
		framer.WriteChunk(&output, []byte("data: "+test.payload+"\n\n"))
		if framer.receivedTerminalEvent != test.received || framer.generatedTerminalEvent != "response.failed" {
			t.Fatalf("received=%q generated=%q", framer.receivedTerminalEvent, framer.generatedTerminalEvent)
		}
	}
}
