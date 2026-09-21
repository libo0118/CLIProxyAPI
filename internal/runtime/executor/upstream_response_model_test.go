package executor

import (
	"context"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

type captureModelUsage struct{ records chan usage.Record }

func (p captureModelUsage) HandleUsage(_ context.Context, record usage.Record) {
	if record.Provider == "codex" && record.Alias == "model-provenance-test" {
		select {
		case p.records <- record:
		default:
		}
	}
}

func TestCodexUpstreamModelSurvivesNativeSSEAndWebSocket(t *testing.T) {
	const created = `{"type":"response.created","response":{"id":"resp_1","model":"reported-other-model"}}`
	for _, websocket := range []bool{false, true} {
		t.Run(map[bool]string{false: "SSE", true: "WebSocket"}[websocket], func(t *testing.T) {
			plugin := captureModelUsage{records: make(chan usage.Record, 2)}
			usage.RegisterNamedPlugin("model-provenance-test", plugin)
			t.Cleanup(func() { usage.RegisterNamedPlugin("model-provenance-test", noopClaudeUsagePlugin{}) })
			ctx := usage.WithRequestedModelAlias(context.Background(), "model-provenance-test")
			if websocket {
				server := codexWebsocketServer(t, created, codexOutputDeltaEvent, codexCompletedEventBody)
				defer server.Close()
				req, opts := codexWebsocketRequest()
				result, err := NewCodexWebsocketsExecutor(codexBufferingConfig(true)).ExecuteStream(ctx, codexTestAuth(server.URL), req, opts)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = drainChunks(result); err != nil {
					t.Fatal(err)
				}
			} else {
				server := codexSSEServer(created, codexOutputDeltaEvent, codexCompletedEventBody)
				defer server.Close()
				req, opts := codexTestRequest()
				result, err := NewCodexExecutor(codexBufferingConfig(true)).ExecuteStream(ctx, codexTestAuth(server.URL), req, opts)
				if err != nil {
					t.Fatal(err)
				}
				if _, err = drainChunks(result); err != nil {
					t.Fatal(err)
				}
			}
			select {
			case record := <-plugin.records:
				if record.Model != "gpt-5.6-terra" || record.UpstreamResponseModel != "reported-other-model" || record.Detail.TotalTokens != 2 || record.Failed {
					t.Fatalf("provenance or usage changed: %+v", record)
				}
			case <-time.After(3 * time.Second):
				t.Fatal("missing usage record")
			}
		})
	}
}
