package pluginhost

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/config"
	_ "github.com/router-for-me/CLIProxyAPI/v7/internal/translator/openai/openai/responses"
	coreexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
	sdktranslator "github.com/router-for-me/CLIProxyAPI/v7/sdk/translator"
)

func TestExecutorAdapterCodexMultiAgentTask(t *testing.T) {
	const task = "CPA_AGENT_TASK_MUST_SURVIVE"
	payload := []byte(`{"input":[{"type":"message","role":"user","content":[{"type":"input_text","text":"existing history"}]},{"type":"agent_message","author":"/root","recipient":"/root/worker","content":[{"type":"input_text","text":"CPA_AGENT_TASK_MUST_SURVIVE"}]}]}`)
	for _, tc := range []struct {
		name       string
		enabled    bool
		userAgent  string
		ginContext bool
		wantTask   bool
	}{
		{name: "codex_headers", enabled: true, userAgent: "Codex Desktop/0.155.0", wantTask: true},
		{name: "codex_context", enabled: true, userAgent: "Codex Desktop/0.155.0", ginContext: true, wantTask: true},
		{name: "disabled", userAgent: "Codex Desktop/0.155.0"},
		{name: "unrelated_client", enabled: true, userAgent: "unrelated-client"},
	} {
		for _, operation := range []string{"execute", "stream", "count_tokens"} {
			t.Run(tc.name+"/"+operation, func(t *testing.T) {
				var captured pluginapi.ExecutorRequest
				capture := func(_ context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorResponse, error) {
					captured = req
					return pluginapi.ExecutorResponse{Payload: []byte(`{}`)}, nil
				}
				host := New()
				host.runtimeConfig = &config.Config{Codex: config.CodexConfig{OptimizeMultiAgentV2: tc.enabled}}
				adapter := newCurrentExecutorAdapterForTest(host, "workbuddy", &fakeExecutor{
					execute:     capture,
					countTokens: capture,
					executeStream: func(_ context.Context, req pluginapi.ExecutorRequest) (pluginapi.ExecutorStreamResponse, error) {
						captured = req
						chunks := make(chan pluginapi.ExecutorStreamChunk)
						close(chunks)
						return pluginapi.ExecutorStreamResponse{Chunks: chunks}, nil
					},
				}, []sdktranslator.Format{sdktranslator.FormatOpenAI}, []sdktranslator.Format{sdktranslator.FormatOpenAI})
				ctx := context.Background()
				headers := http.Header{"User-Agent": []string{tc.userAgent}}
				if tc.ginContext {
					ginCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
					ginCtx.Request = httptest.NewRequest(http.MethodPost, "/v1/responses", nil)
					ginCtx.Request.Header = headers
					ctx = context.WithValue(ctx, "gin", ginCtx)
					headers = nil
				}
				req := coreexecutor.Request{Model: "workbuddy-model", Payload: bytes.Clone(payload)}
				opts := coreexecutor.Options{SourceFormat: sdktranslator.FormatOpenAIResponse, Headers: headers}
				var err error
				switch operation {
				case "execute":
					_, err = adapter.Execute(ctx, nil, req, opts)
				case "stream":
					opts.Stream = true
					var result *coreexecutor.StreamResult
					result, err = adapter.ExecuteStream(ctx, nil, req, opts)
					if err == nil {
						for range result.Chunks {
						}
					}
				case "count_tokens":
					_, err = adapter.CountTokens(ctx, nil, req, opts)
				}
				if err != nil {
					t.Fatal(err)
				}
				if got := bytes.Contains(captured.Payload, []byte(task)); got != tc.wantTask {
					t.Fatalf("task preserved = %v, want %v; plugin payload = %s", got, tc.wantTask, captured.Payload)
				}
				if !bytes.Contains(captured.Payload, []byte("existing history")) {
					t.Fatalf("ordinary message lost: %s", captured.Payload)
				}
				if !bytes.Equal(req.Payload, payload) {
					t.Fatal("adapter mutated the original request")
				}
			})
		}
	}
}
