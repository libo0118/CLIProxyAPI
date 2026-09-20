package helps

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestQoderCreditsOriginalPayload(t *testing.T) {
	payload := []byte(`{"usage":{"prompt_tokens":13159,"completion_tokens":16,"credits":0.11793750000000001,"original_credits":0.11793750000000001,"billable":false}}`)
	detail := ParsePluginExecutorResponseUsage("openai", payload)
	if detail.QoderCredits == nil || detail.QoderCredits.Billable == nil || *detail.QoderCredits.Billable || detail.QoderCredits.Credits.String() != "0.11793750000000001" {
		t.Fatalf("billing lost: %+v", detail.QoderCredits)
	}
	var stream StreamUsageBuffer
	ObservePluginExecutorStreamUsage("openai", append([]byte("data: "), payload...), &stream)
	ObservePluginExecutorStreamUsage("openai", []byte("data: [DONE]"), &stream)
	got, ok := stream.Detail()
	if !ok || got.QoderCredits == nil || got.QoderCredits.Credits != detail.QoderCredits.Credits {
		t.Fatal("stream billing lost")
	}
	// A failed attempt can still carry billed usage.
	reporter := NewUsageReporter(context.Background(), "qoder", "lite", nil)
	record := reporter.buildRecord(got, true)
	encoded, err := json.Marshal(record.Detail.QoderCredits)
	if err != nil || !strings.Contains(string(encoded), `"billable":false`) {
		t.Fatalf("failure billing lost: %s %v", encoded, err)
	}
	for _, raw := range []string{`{}`, `{"usage":{}}`, `{"usage":{"credits":-1,"billable":"false"}}`, `{"usage":{"credits":1e999}}`} {
		if parseQoderCredits([]byte(raw)) != nil {
			t.Fatalf("invalid billing accepted: %s", raw)
		}
	}
	zero := parseQoderCredits([]byte(`{"usage":{"credits":0,"billable":true}}`))
	if zero == nil || zero.Credits != "0" || zero.Billable == nil || !*zero.Billable {
		t.Fatal("valid charged zero lost")
	}
}
