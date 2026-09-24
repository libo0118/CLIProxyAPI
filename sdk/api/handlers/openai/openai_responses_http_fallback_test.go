package openai_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	_ "github.com/router-for-me/CLIProxyAPI/v7/internal/translator"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers/openai"
	auth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	"github.com/tidwall/gjson"
)

// Exercise the real handler and both real transports on one client socket.
func TestResponsesWebsocketLargeRequestSwitchesUpstreamOnly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const model = "codex-http-size-fallback-test"
	var upgrades, wsTurns, httpTurns atomic.Int32
	var sequence atomic.Int32
	captured := make(chan []byte, 5)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	completion := func() []byte {
		n := sequence.Add(1)
		output := fmt.Sprintf(`[{"type":"message","id":"out-%d","role":"assistant","content":[{"type":"output_text","text":"ok-%d"}]}]`, n, n)
		if n == 1 || n == 3 {
			output = fmt.Sprintf(`[{"type":"function_call","id":"fc-%d","call_id":"call-%d","name":"lookup","arguments":"{}"}]`, n, n)
		}
		return []byte(fmt.Sprintf(`{"type":"response.completed","response":{"id":"resp-%d","status":"completed","model":%q,"output":%s,"usage":{"input_tokens":10,"output_tokens":2,"total_tokens":12}}}`, n, model, output))
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer test-secret" {
			t.Error("selected credential changed during transport switch")
		}
		if websocket.IsWebSocketUpgrade(r) {
			upgrades.Add(1)
			conn, err := upgrader.Upgrade(w, r, nil)
			if err != nil {
				t.Error(err)
				return
			}
			defer conn.Close()
			for {
				_, body, err := conn.ReadMessage()
				if err != nil {
					return
				}
				wsTurns.Add(1)
				captured <- body
				if err := conn.WriteMessage(websocket.TextMessage, completion()); err != nil {
					t.Error(err)
					return
				}
			}
		}
		httpTurns.Add(1)
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		captured <- body
		w.Header().Set("Content-Type", "text/event-stream")
		fmt.Fprintf(w, "data: %s\n\n", completion())
	}))
	defer upstream.Close()
	cfg := &config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}}
	manager := auth.NewManager(nil, nil, nil)
	manager.RegisterExecutor(runtimeexecutor.NewCodexAutoExecutor(cfg))
	credential := &auth.Auth{ID: model, Provider: "codex", Status: auth.StatusActive, Attributes: map[string]string{
		"api_key": "test-secret", "base_url": upstream.URL, "websockets": "true",
	}}
	if _, err := manager.Register(context.Background(), credential); err != nil {
		t.Fatal(err)
	}
	registry.GetGlobalRegistry().RegisterClient(credential.ID, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(credential.ID) })
	h := openai.NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.GET("/v1/responses", h.ResponsesWebsocket)
	proxy := httptest.NewServer(router)
	defer proxy.Close()
	client, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(proxy.URL, "http")+"/v1/responses", nil)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	roundTrip := func(body string) []byte {
		t.Helper()
		client.SetReadDeadline(time.Now().Add(30 * time.Second))
		if err := client.WriteMessage(websocket.TextMessage, []byte(body)); err != nil {
			t.Fatal(err)
		}
		for {
			kind, reply, err := client.ReadMessage()
			if err != nil {
				t.Fatalf("same downstream socket must stay open: %v", err)
			}
			if kind != websocket.TextMessage {
				t.Fatalf("unexpected frame kind %d", kind)
			}
			switch gjson.GetBytes(reply, "type").String() {
			case "error", "response.failed":
				t.Fatalf("request failed: %.500s", reply)
			case "response.completed":
				return <-captured
			}
		}
	}
	roundTrip(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"role":"user","content":"first-history"}]}`, model))
	smallDelta := roundTrip(`{"type":"response.create","previous_response_id":"resp-1","input":[{"type":"function_call_output","call_id":"call-1","output":"tool-result-one"}]}`)
	if gjson.GetBytes(smallDelta, "previous_response_id").String() != "resp-1" || len(gjson.GetBytes(smallDelta, "input").Array()) != 1 {
		t.Fatal("small websocket delta was expanded")
	}
	imageURL := "data:image/png;base64," + strings.Repeat("A", helps.CodexWebsocketHTTPThreshold)
	large := roundTrip(fmt.Sprintf(`{"type":"response.create","previous_response_id":"resp-2","input":[{"role":"user","content":[{"type":"input_image","image_url":%q}]}]}`, imageURL))
	if gjson.GetBytes(large, "previous_response_id").Exists() || !strings.Contains(string(large), "first-history") || !strings.Contains(string(large), "tool-result-one") || !strings.Contains(string(large), "ok-2") || !strings.Contains(string(large), imageURL) {
		t.Fatal("HTTP fallback lost or truncated history, tool output, or image")
	}
	followup := roundTrip(`{"type":"response.create","previous_response_id":"resp-3","input":[{"type":"function_call_output","call_id":"call-3","output":"tool-result-three"}]}`)
	if gjson.GetBytes(followup, "previous_response_id").Exists() || !strings.Contains(string(followup), "first-history") || !strings.Contains(string(followup), imageURL) || !strings.Contains(string(followup), "tool-result-three") {
		t.Fatal("turn after HTTP fallback lost its canonical history")
	}
	roundTrip(fmt.Sprintf(`{"type":"response.create","model":%q,"input":[{"type":"message","role":"assistant","content":"summary"},{"type":"message","role":"user","content":"small replacement"}]}`, model))
	if upgrades.Load() != 2 || wsTurns.Load() != 3 || httpTurns.Load() != 2 {
		t.Fatalf("transports upgrades=%d wsTurns=%d httpTurns=%d, want 2/3/2", upgrades.Load(), wsTurns.Load(), httpTurns.Load())
	}
}
