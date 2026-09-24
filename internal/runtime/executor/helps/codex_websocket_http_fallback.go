package helps

import (
	"context"
	"net/http"
	"strings"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/tidwall/gjson"
)

// CodexWebsocketHTTPThreshold applies to the serialized upstream message, not tokens.
const CodexWebsocketHTTPThreshold = 15 * 1024 * 1024

// CodexWebsocketHTTPFallback selects HTTP before any upstream write. Incremental
// input must be replaced with the handler's complete, connection-local transcript.
func CodexWebsocketHTTPFallback(ctx context.Context, wirePayload []byte, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Request, cliproxyexecutor.Options, bool, error) {
	if len(wirePayload) < CodexWebsocketHTTPThreshold {
		return req, opts, false, nil
	}
	if cliproxyexecutor.RequiredUpstreamWebsocket(ctx) || strings.TrimSpace(gjson.GetBytes(req.Payload, "previous_response_id").String()) != "" || gjson.GetBytes(req.Payload, "type").String() == "response.append" {
		full := cliproxyexecutor.WebsocketHTTPFallbackPayload(ctx)
		if len(full) == 0 || strings.TrimSpace(gjson.GetBytes(full, "previous_response_id").String()) != "" {
			return req, opts, false, codexHTTPFallbackHistoryError{}
		}
		req.Payload = full
		opts.OriginalRequest = full
	}
	cliproxyexecutor.MarkWebsocketHTTPFallback(ctx)
	LogWithRequestID(ctx).Infof("codex: switching upstream to HTTP/SSE before send (websocket_bytes=%d threshold_bytes=%d)", len(wirePayload), CodexWebsocketHTTPThreshold)
	return req, opts, true, nil
}

type codexHTTPFallbackHistoryError struct{}

func (codexHTTPFallbackHistoryError) Error() string {
	return `{"error":{"type":"invalid_request_error","code":"http_fallback_history_unavailable","message":"Cannot switch an oversized incremental request to HTTP without its complete local history"}}`
}
func (codexHTTPFallbackHistoryError) StatusCode() int       { return http.StatusBadRequest }
func (codexHTTPFallbackHistoryError) IsRequestScoped() bool { return true }
