package logging

import (
	"bytes"
	"errors"
	"maps"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

const upstreamDiagnosticKey = "UPSTREAM_DIAGNOSTIC_ATTEMPTS"
const maxUpstreamDiagnosticAttempts = 16

type upstreamDiagnosticAttempt struct {
	index             int
	started, ended    time.Time
	request, response map[string]any
}

// Only allowlisted metadata is retained. Snapshots never share mutable maps with writers.
type upstreamDiagnostics struct {
	mu        sync.Mutex
	requestID string
	total     int
	attempts  []*upstreamDiagnosticAttempt
}

func DiagnosticRequestParameters(body []byte) map[string]any {
	parameters := make(map[string]any)
	for _, key := range []string{"model", "stream", "reasoning.effort", "service_tier", "max_output_tokens", "temperature", "top_p"} {
		value := gjson.GetBytes(body, key)
		if value.Exists() && !value.IsObject() && !value.IsArray() {
			if value.Type == gjson.String {
				parameters[key] = SafeDiagnosticForLog(value.String())
			} else {
				parameters[key] = value.Value()
			}
		}
	}
	return parameters
}

func ResetUpstreamDiagnostics(c *gin.Context) {
	c.Set(upstreamDiagnosticKey, nil)
	c.Set(UpstreamRequestDiagnosticContextKey, nil)
	c.Set(UpstreamResponseDiagnosticContextKey, nil)
}

func upstreamDiagnosticState(c *gin.Context) *upstreamDiagnostics {
	if c == nil {
		return nil
	}
	value, _ := c.Get(upstreamDiagnosticKey)
	d, _ := value.(*upstreamDiagnostics)
	if d != nil && d.requestID != GetGinRequestID(c) {
		return nil
	}
	return d
}

func StartUpstreamDiagnostic(c *gin.Context, rawURL, method, provider string, headers http.Header, body []byte) {
	if c == nil {
		return
	}
	d := upstreamDiagnosticState(c)
	if d == nil {
		d = &upstreamDiagnostics{requestID: GetGinRequestID(c)}
		c.Set(upstreamDiagnosticKey, d)
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if len(d.attempts) > 0 {
		previous := d.attempts[len(d.attempts)-1]
		if previous.ended.IsZero() {
			previous.ended = now
		}
	}
	d.total++
	request := DiagnosticUpstreamRequest(rawURL, method, provider, headers)
	request["parameters"] = DiagnosticRequestParameters(body)
	request["request_bytes"], request["body_capture"] = len(body), "omitted"
	request["timestamp_unix_ms"] = now.UnixMilli()
	response := map[string]any{"body_capture": "omitted", "headers": map[string]string{}, "headers_received": false,
		"terminal_received": false, "response_chunks": int64(0), "response_bytes_observed": int64(0),
		"byte_count_kind": "observed_payload_bytes"}
	// Connection reuse does not have a new handshake; never invent a 101 or its headers.
	if method == "WEBSOCKET" {
		response["handshake_observed"] = false
	}
	d.attempts = append(d.attempts, &upstreamDiagnosticAttempt{index: d.total, started: now, request: request, response: response})
	if len(d.attempts) > maxUpstreamDiagnosticAttempts {
		d.attempts[0] = nil
		d.attempts = d.attempts[1:]
	}
}

func updateUpstreamDiagnostic(c *gin.Context, update func(*upstreamDiagnosticAttempt, time.Time)) {
	d := upstreamDiagnosticState(c)
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.attempts) > 0 {
		update(d.attempts[len(d.attempts)-1], time.Now())
	}
}

func ObserveUpstreamHeaders(c *gin.Context, status int, headers http.Header, handshake bool) {
	updateUpstreamDiagnostic(c, func(a *upstreamDiagnosticAttempt, now time.Time) {
		a.response["status_code"] = status
		a.response["headers"] = RedactedDiagnosticHeaders(headers)
		a.response["headers_received"] = true
		a.response["headers_received_ms"] = now.Sub(a.started).Milliseconds()
		if handshake {
			a.response["handshake_observed"] = true
		}
	})
}

func ObserveUpstreamError(c *gin.Context, stage string, err error) {
	if err == nil {
		return
	}
	updateUpstreamDiagnostic(c, func(a *upstreamDiagnosticAttempt, now time.Time) {
		a.response["error_kind"] = SafeErrorDiagnostic(err)
		a.response["error_stage"] = SafeDiagnosticForLog(stage)
		a.response["error_observed_ms"] = now.Sub(a.started).Milliseconds()
		var closeErr *websocket.CloseError
		if errors.As(err, &closeErr) {
			a.response["close_code"] = closeErr.Code
		}
		var mappedClose interface{ WebsocketCloseCode() int }
		if errors.As(err, &mappedClose) {
			a.response["close_code"] = mappedClose.WebsocketCloseCode()
		}
		var statusError interface{ StatusCode() int }
		if errors.As(err, &statusError) {
			a.response["error_status_code"] = statusError.StatusCode()
		}
		a.ended = now
	})
}

func ObserveUpstreamChunk(c *gin.Context, chunk []byte) {
	if len(chunk) == 0 {
		return
	}
	updateUpstreamDiagnostic(c, func(a *upstreamDiagnosticAttempt, now time.Time) {
		count := a.response["response_chunks"].(int64)
		a.response["response_chunks"] = count + 1
		a.response["response_bytes_observed"] = a.response["response_bytes_observed"].(int64) + int64(len(chunk))
		if count == 0 {
			a.response["first_chunk_ms"] = now.Sub(a.started).Milliseconds()
		}
		a.response["last_chunk_ms"] = now.Sub(a.started).Milliseconds()
		// Callers supply a JSON message, an SSE line, or a complete body. Never retain it.
		data := bytes.TrimSpace(chunk)
		data = bytes.TrimSpace(bytes.TrimPrefix(data, []byte("data:")))
		if !gjson.ValidBytes(data) {
			return
		}
		event := gjson.GetBytes(data, "type").String()
		if event != "" {
			a.response["last_event"] = SafeDiagnosticForLog(event)
		}
		switch event {
		case "response.completed", "response.done", "response.failed", "response.incomplete", "error":
			a.response["terminal_event"], a.response["terminal_received"] = event, true
			a.ended = now
		}
		model := gjson.GetBytes(data, "response.model")
		if !model.Exists() {
			model = gjson.GetBytes(data, "model")
		}
		if model.Type == gjson.String {
			a.response["reported_model"] = SafeDiagnosticForLog(model.String())
		}
		for _, item := range []struct{ field, path string }{
			{"error_code", "response.error.code"}, {"error_type", "response.error.type"},
			{"error_code", "error.code"}, {"error_type", "error.type"},
			{"response_status", "response.status"}, {"incomplete_reason", "response.incomplete_details.reason"},
		} {
			value := gjson.GetBytes(data, item.path)
			if value.Type == gjson.String {
				a.response[item.field] = SafeDiagnosticForLog(value.String())
			}
		}
	})
}

func UpstreamDiagnosticSnapshot(c *gin.Context) map[string]any {
	d := upstreamDiagnosticState(c)
	if d == nil {
		return nil
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	attempts := make([]map[string]any, 0, len(d.attempts))
	for _, a := range d.attempts {
		response := maps.Clone(a.response)
		end := a.ended
		if end.IsZero() {
			end = time.Now()
		}
		response["observation_duration_ms"] = end.Sub(a.started).Milliseconds()
		attempts = append(attempts, map[string]any{"attempt": a.index, "request": maps.Clone(a.request), "response": response})
	}
	fields := map[string]any{"upstream_attempts": attempts, "upstream_attempts_total": d.total,
		"upstream_attempts_omitted": d.total - len(attempts)}
	if len(attempts) > 0 {
		last := attempts[len(attempts)-1]
		fields["upstream_request"], fields["upstream_response"] = last["request"], last["response"]
	}
	return fields
}
