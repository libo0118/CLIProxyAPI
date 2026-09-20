package helps

import "testing"

func TestWorkBuddyCreditsStream(t *testing.T) {
	for _, amount := range []string{"0", "0.11793750000000001"} {
		payload := []byte(`{"usage":{"prompt_tokens":15,"completion_tokens":32,"credit":` + amount + `}}`)
		detail := ParsePluginExecutorResponseUsage("openai", payload)
		if detail.WorkBuddyCredits == nil || string(detail.WorkBuddyCredits.Credits) != amount {
			t.Fatal("raw amount lost")
		}
		var buffer StreamUsageBuffer
		ObservePluginExecutorStreamUsage("openai", append([]byte("data: "), payload...), &buffer)
		ObservePluginExecutorStreamUsage("openai", []byte("data: [DONE]"), &buffer)
		got, _ := buffer.Detail()
		if got.WorkBuddyCredits == nil || got.WorkBuddyCredits.Credits != detail.WorkBuddyCredits.Credits {
			t.Fatal("stream amount lost")
		}
	}
	for _, s := range []string{`{}`, `{"usage":{"credit":null}}`, `{"usage":{"credit":-1}}`, `{"usage":{"credit":"0"}}`, `{"usage":{"credit":1e999}}`} {
		if parseWorkBuddyCredits([]byte(s)) != nil {
			t.Fatal("invalid billing accepted")
		}
	}
}
