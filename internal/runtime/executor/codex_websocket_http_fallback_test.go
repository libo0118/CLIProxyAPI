package executor

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

	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestCodexWebsocketPreparedSizeFallbackReportsUsageOnce(t *testing.T) {
	for _, stream := range []bool{false, true} {
		for _, status := range []int{http.StatusOK, http.StatusBadRequest} {
			t.Run(fmt.Sprintf("stream=%v/status=%d", stream, status), func(t *testing.T) {
				var calls atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					calls.Add(1)
					if r.Method != http.MethodPost || r.Header.Get("Upgrade") != "" || r.Header.Get("Authorization") != "Bearer fallback-test" {
						t.Error("expected one HTTP POST with the same credential, no websocket attempt")
					}
					io.Copy(io.Discard, r.Body)
					w.Header().Set("Content-Type", "text/event-stream")
					w.WriteHeader(status)
					if status != http.StatusOK {
						fmt.Fprint(w, `{"error":{"message":"test rejection"}}`)
						return
					}
					fmt.Fprint(w, "data: "+codexCompletedEventBody+"\n\n")
				}))
				defer server.Close()
				plugin := captureModelUsage{records: make(chan usage.Record, 4)}
				usage.RegisterNamedPlugin("size-fallback-usage-test", plugin)
				t.Cleanup(func() { usage.RegisterNamedPlugin("size-fallback-usage-test", noopClaudeUsagePlugin{}) })
				ctx := cliproxyexecutor.WithDownstreamWebsocket(usage.WithRequestedModelAlias(context.Background(), "model-provenance-test"))
				prefix := `{"model":"gpt-5.6-terra","input":[{"role":"user","content":"`
				suffix := `"}]}`
				// The client body is below the threshold; preparation pushes the wire body over it.
				body := []byte(prefix + strings.Repeat("x", helps.CodexWebsocketHTTPThreshold-1-len(prefix)-len(suffix)) + suffix)
				req := cliproxyexecutor.Request{Model: "gpt-5.6-terra", Payload: body}
				opts := cliproxyexecutor.Options{SourceFormat: sdktranslator.FromString("openai-response"), ResponseFormat: sdktranslator.FromString("openai-response")}
				exec := NewCodexAutoExecutor(&config.Config{SDKConfig: config.SDKConfig{DisableImageGeneration: config.DisableImageGenerationAll}})
				auth := &cliproxyauth.Auth{ID: "size-fallback-auth", Attributes: map[string]string{"api_key": "fallback-test", "base_url": server.URL, "websockets": "true"}}
				var err error
				if stream {
					var result *cliproxyexecutor.StreamResult
					result, err = exec.ExecuteStream(ctx, auth, req, opts)
					if err == nil {
						_, err = drainChunks(result)
					}
				} else {
					_, err = exec.Execute(ctx, auth, req, opts)
				}
				if (err != nil) != (status != http.StatusOK) {
					t.Fatalf("unexpected result: %v", err)
				}
				// A queue barrier proves that no second failure record was published by the WS wrapper.
				usage.PublishRecord(ctx, usage.Record{Provider: "codex", Alias: "model-provenance-test", Model: "barrier"})
				count := 0
				for {
					select {
					case record := <-plugin.records:
						if record.Model == "barrier" {
							if count != 1 || calls.Load() != 1 {
								t.Fatalf("usage=%d upstream calls=%d, want 1/1", count, calls.Load())
							}
							return
						}
						count++
						if record.AuthID != auth.ID || record.Failed != (status != http.StatusOK) {
							t.Fatalf("incorrect usage attribution: %+v", record)
						}
					case <-time.After(5 * time.Second):
						t.Fatal("usage queue did not reach barrier")
					}
				}
			})
		}
	}
}

func TestCodexHTTPFallbackRetainsScopeUntilClientSessionEnds(t *testing.T) {
	lifecycle := &trackedWebsocketLifecycle{}
	sess := &codexWebsocketSession{lifecycle: lifecycle}
	sess.detachUpstreamForHTTP()
	if lifecycle.ends.Load() != 0 || sess.lifecycle != lifecycle {
		t.Fatal("transport switch ended the active execution scope")
	}
	closeCodexWebsocketSession(sess, "client_closed")
	if lifecycle.ends.Load() != 1 {
		t.Fatal("client close leaked the retained execution scope")
	}
}
