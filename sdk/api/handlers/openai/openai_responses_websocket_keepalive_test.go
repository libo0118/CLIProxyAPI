package openai

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/api/handlers"
	sdkconfig "github.com/router-for-me/CLIProxyAPI/v7/sdk/config"
)

func TestForwardResponsesWebsocketApplicationKeepalive(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, terminal := range []string{"completion", "cancellation", "error"} {
		t.Run(terminal, func(t *testing.T) {
			ctx, stop := context.WithCancel(context.Background())
			defer stop()
			data := make(chan []byte, 1)
			errs := make(chan *interfaces.ErrorMessage, 1)
			done := make(chan error, 1)
			completion := `{"type":"response.completed","response":{"id":"resp-heartbeat","output":[]}}`
			upstreamErr := errors.New("upstream failed")
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := responsesWebsocketUpgrader.Upgrade(w, r, nil)
				if err != nil {
					done <- err
					return
				}
				defer func() { _ = conn.Close() }()
				c, _ := gin.CreateTestContext(httptest.NewRecorder())
				c.Request = r.WithContext(ctx)
				cfg := &sdkconfig.SDKConfig{Streaming: sdkconfig.StreamingConfig{WebsocketApplicationKeepalive: true}}
				h := NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(cfg, nil))
				timeline := newInMemoryWebsocketTimelineLog()
				interval := 20 * time.Millisecond
				cancelled := false
				output, id, pending, errMsg, errForward := h.forwardResponsesWebsocket(
					c, newResponsesWebsocketWriter(conn), func(...interface{}) { cancelled = true }, data, errs, timeline, "keepalive-test",
					responsesWebsocketForwardOptions{keepAliveInterval: &interval, suppressError: func(*interfaces.ErrorMessage) bool { return true }},
				)
				if !cancelled || len(pending) != 0 || string(output) != "[]" || strings.Contains(timeline.builder.String(), "cpa.keepalive") {
					done <- fmt.Errorf("unexpected cleanup, output, pending calls or heartbeat timeline entry")
					return
				}
				switch terminal {
				case "completion":
					if id != "resp-heartbeat" || errMsg != nil || errForward != nil {
						done <- fmt.Errorf("completion changed: id=%q error=%v forward=%v", id, errMsg, errForward)
						return
					}
				case "cancellation":
					if !errors.Is(errForward, context.Canceled) {
						done <- fmt.Errorf("cancellation lost: %v", errForward)
						return
					}
				case "error":
					if errMsg == nil || !errors.Is(errMsg.Error, upstreamErr) || errForward != nil {
						done <- fmt.Errorf("upstream error lost: %v, %v", errMsg, errForward)
						return
					}
				}
				done <- nil
			}))
			defer server.Close()
			// Ensure fatal assertions also release the forwarding loop before server.Close.
			defer stop()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
			kind, payload, err := conn.ReadMessage()
			if err != nil || kind != websocket.TextMessage || string(payload) != `{"type":"cpa.keepalive"}` {
				t.Fatalf("expected private text heartbeat, got %d %s %v", kind, payload, err)
			}
			switch terminal {
			case "completion":
				data <- []byte(completion)
				for {
					_, payload, err = conn.ReadMessage()
					if err != nil {
						t.Fatal(err)
					}
					if string(payload) != `{"type":"cpa.keepalive"}` {
						break
					}
				}
				if string(payload) != completion {
					t.Fatalf("completion changed: %s", payload)
				}
				// Keep upstream open across several heartbeat intervals after completion.
				_ = conn.SetReadDeadline(time.Now().Add(120 * time.Millisecond))
				if _, payload, err = conn.ReadMessage(); err == nil {
					t.Fatalf("unexpected post-completion frame: %s", payload)
				} else if timeout, ok := err.(interface{ Timeout() bool }); !ok || !timeout.Timeout() {
					t.Fatalf("expected an idle, open connection after completion, got %v", err)
				}
				close(data)
			case "cancellation":
				stop()
			case "error":
				errs <- &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway, Error: upstreamErr}
			}
			select {
			case err = <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("forwarder did not exit after terminal signal")
			}
		})
	}
}
