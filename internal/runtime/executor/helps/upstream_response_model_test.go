package helps

import (
	"context"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
)

func TestUpstreamResponseModelPreservesUsageAndProvenance(t *testing.T) {
	for _, tt := range []struct{ payload, want string }{
		{`{"model":"reported","usage":{"total_tokens":8}}`, "reported"},
		{`{"response":{"model":"reported"}}`, "reported"},
		{`{"type":"message_start","message":{"model":"reported"}}`, "reported"},
		{`{"modelVersion":"reported"}`, "reported"},
		{`{"input":{"model":"echoed"},"tool":{"model":"argument"}}`, ""},
		{`{"model":23}`, ""},
		{`{"model":"bad\nname"}`, ""},
		{`{"model":"` + strings.Repeat("x", 513) + `"}`, ""},
	} {
		if got := responseModel([]byte(tt.payload)); got != tt.want {
			t.Fatalf("response model = %q, want %q", got, tt.want)
		}
	}
	buffer := &StreamUsageBuffer{}
	buffer.ObserveOpenAIStream([]byte(`data: {"model":"reported","choices":[]}`))
	buffer.ObserveOpenAIStream([]byte(`data: {"usage":{"prompt_tokens":5,"completion_tokens":3,"total_tokens":8}}`))
	buffer.Observe(usage.Detail{UpstreamResponseModel: "reported-final"}, true)
	detail, ok := buffer.Detail()
	if !ok || detail.TotalTokens != 8 || detail.UpstreamResponseModel != "reported-final" {
		t.Fatalf("metadata overwrote usage: %+v", detail)
	}
	reporter := NewUsageReporter(usage.WithRequestedModelAlias(context.Background(), "client-alias"), "openai", "sent-model", nil)
	record := reporter.buildRecord(detail, false)
	if record.Model != "sent-model" || record.Alias != "client-alias" || record.UpstreamResponseModel != "reported-final" {
		t.Fatalf("model provenance: %+v", record)
	}
	for _, protocol := range []string{"openai", "codex"} {
		detail := ParsePluginExecutorResponseUsage(protocol, []byte(`{"model":"synthesized","response":{"model":"synthesized"}}`))
		if detail.UpstreamResponseModel != "" {
			t.Fatal("plugin echoed model was trusted")
		}
	}
	// Model-only chunks must not trigger a once-only usage publication before tokens arrive.
	for _, parse := range []func([]byte) (usage.Detail, bool){ParseCodexUsage, ParseOpenAIStreamUsage, ParseClaudeStreamUsage, ParseGeminiStreamUsage, ParseAntigravityStreamUsage, ParseInteractionsStreamUsage} {
		detail, ok := parse([]byte(`{"model":"reported","modelVersion":"reported","response":{"model":"reported"}}`))
		if ok || detail.UpstreamResponseModel != "reported" {
			t.Fatalf("metadata changed usage availability: ok=%v detail=%+v", ok, detail)
		}
	}
	pluginBuffer := &StreamUsageBuffer{}
	ObservePluginExecutorStreamUsage("openai", []byte("data: {\"model\":\"synthesized\"}\n\n"), pluginBuffer)
	if detail, _ := pluginBuffer.Detail(); detail.UpstreamResponseModel != "" {
		t.Fatal("stream plugin echoed model was trusted")
	}
	ObservePluginExecutorStreamUsage("openai", []byte("data: {\"upstream_response_model\":\"provider-report\",\"usage\":{\"total_tokens\":8}}\n\n"), pluginBuffer)
	if detail, _ := pluginBuffer.Detail(); detail.UpstreamResponseModel != "provider-report" || detail.TotalTokens != 8 {
		t.Fatalf("explicit plugin metadata lost: %+v", detail)
	}
}

func TestUpstreamResponseModelFragmentedSSEAndWebSocketEvents(t *testing.T) {
	for _, step := range []int{1, 3, 64} {
		reporter := NewUsageReporter(context.Background(), "openai", "sent", nil)
		observer := &ResponseModelStream{Reporter: reporter}
		stream := []byte(": keepalive\r\nevent: response.created\r\ndata: {\"response\":\r\ndata: {\"model\":\"reported\"}}\r\n\r\ndata: [DONE]\n\n")
		for start := 0; start < len(stream); start += step {
			end := min(start+step, len(stream))
			observer.Observe(stream[start:end])
		}
		observer.Flush()
		if got := reporter.buildRecord(usage.Detail{}, false).UpstreamResponseModel; got != "reported" {
			t.Fatalf("fragment size %d: %q", step, got)
		}
	}
	reporter := NewUsageReporter(context.Background(), "codex", "sent", nil)
	reporter.ObserveUpstreamResponseModel([]byte(`{"type":"response.created","response":{"model":"ws-reported"}}`))
	detail, _ := ParseCodexUsage([]byte(`{"type":"response.completed","response":{"usage":{"input_tokens":2,"output_tokens":1,"total_tokens":3}}}`))
	if got := reporter.buildRecord(detail, false); got.UpstreamResponseModel != "ws-reported" || got.Detail.TotalTokens != 3 {
		t.Fatalf("WS metadata lost: %+v", got)
	}
	if got := reporter.buildRecordForModel("image-tool-model", usage.Detail{TotalTokens: 2}, false, usage.Failure{}).UpstreamResponseModel; got != "" {
		t.Fatalf("parent response model leaked into tool usage: %q", got)
	}
	reporter = NewUsageReporter(context.Background(), "openai", "sent", nil)
	observer := &ResponseModelStream{Reporter: reporter}
	observer.Observe([]byte("data: " + strings.Repeat("x", responseModelEventLimit+1)))
	observer.Observe([]byte("\ndata: {\"model\":\"must-skip\"}\n\n"))
	observer.Observe([]byte("data: {\"model\":\"next-event\"}\n\n"))
	observer.Flush()
	if got := reporter.buildRecord(usage.Detail{}, false).UpstreamResponseModel; got != "next-event" {
		t.Fatalf("overflow recovery: %q", got)
	}
	if len(observer.line)+len(observer.data) > responseModelEventLimit {
		t.Fatal("unbounded metadata buffering")
	}
	if got := NewUsageReporter(context.Background(), "openai", "sent", nil).buildRecord(usage.Detail{}, false).UpstreamResponseModel; got != "" {
		t.Fatalf("missing response model was guessed: %q", got)
	}
}
