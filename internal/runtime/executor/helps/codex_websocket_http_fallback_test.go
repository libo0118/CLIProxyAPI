package helps

import (
	"bytes"
	"context"
	"errors"
	"testing"

	executor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
)

func TestCodexWebsocketHTTPFallbackBoundaryAndHistory(t *testing.T) {
	full := []byte(`{"model":"gpt-5-codex","input":[{"role":"user","content":"complete history"}]}`)
	delta := []byte(`{"previous_response_id":"resp-1","input":[]}`)
	for _, size := range []int{CodexWebsocketHTTPThreshold - 1, CodexWebsocketHTTPThreshold, CodexWebsocketHTTPThreshold + 1} {
		called := false
		ctx := executor.WithWebsocketHTTPFallback(executor.WithRequiredUpstreamWebsocket(context.Background()), full, func() { called = true })
		req, opts, switched, err := CodexWebsocketHTTPFallback(ctx, make([]byte, size), executor.Request{Payload: delta}, executor.Options{})
		want := size >= 15*1024*1024
		if err != nil || switched != want || called != want {
			t.Fatalf("size %d: switched=%v callback=%v err=%v", size, switched, called, err)
		}
		if want && (!bytes.Equal(req.Payload, full) || !bytes.Equal(opts.OriginalRequest, full)) {
			t.Fatal("incremental fallback did not use full history")
		}
		if !want && !bytes.Equal(req.Payload, delta) {
			t.Fatal("small native delta was changed")
		}
	}
	_, _, switched, err := CodexWebsocketHTTPFallback(context.Background(), make([]byte, CodexWebsocketHTTPThreshold), executor.Request{Payload: delta}, executor.Options{})
	var scoped executor.RequestScopedError
	if switched || !errors.As(err, &scoped) || !scoped.IsRequestScoped() {
		t.Fatalf("missing history must fail before send, without credential rotation: %v", err)
	}
}
