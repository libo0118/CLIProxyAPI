package openai

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	log "github.com/sirupsen/logrus"
)

// responsesStreamDiagnostics observes writes without changing forwarding behavior.
// Written bytes are accepted by the local writer, not acknowledged by the client.
// Gin's Flush has no error result, so flush failures are not observable here.
type responsesStreamDiagnostics struct {
	gin.ResponseWriter
	started    time.Time
	firstWrite time.Time
	lastWrite  time.Time
	attempted  int64
	written    int64
	writeErr   error
	cancelErr  error
	parameters map[string]any
}

func responsesDiagnosticParameters(body []byte) map[string]any {
	return logging.DiagnosticRequestParameters(body)
}

func (d *responsesStreamDiagnostics) Write(p []byte) (int, error) {
	n, err := d.ResponseWriter.Write(p)
	d.recordWrite(len(p), n, err)
	return n, err
}

func (d *responsesStreamDiagnostics) WriteString(s string) (int, error) {
	n, err := d.ResponseWriter.WriteString(s)
	d.recordWrite(len(s), n, err)
	return n, err
}

func (d *responsesStreamDiagnostics) recordWrite(attempted, n int, err error) {
	d.attempted += int64(attempted)
	d.written += int64(n)
	if n > 0 {
		d.lastWrite = time.Now()
		if d.firstWrite.IsZero() {
			d.firstWrite = d.lastWrite
		}
	}
	if d.writeErr == nil {
		if err != nil {
			d.writeErr = err
		} else if n < attempted {
			d.writeErr = io.ErrShortWrite
		}
	}
}

func responsesDiagnosticErrorKind(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, io.ErrShortWrite):
		return "short_write"
	case errors.Is(err, syscall.EPIPE):
		return "broken_pipe"
	case errors.Is(err, syscall.ECONNRESET):
		return "connection_reset"
	}
	return logging.SafeErrorDiagnostic(err)
}

func (d *responsesStreamDiagnostics) summary(c *gin.Context, ctx context.Context, model string, requestBytes int, framer *responsesSSEFramer) map[string]any {
	status, reason := "incomplete", "missing_terminal_event"
	switch {
	case d.writeErr != nil:
		status, reason = "failed", "downstream_write_failed"
	case c.Request.Context().Err() != nil:
		status, reason = "cancelled", "downstream_cancelled"
	case framer.missingTerminal:
		status, reason = "incomplete", "upstream_closed_without_terminal"
	case framer.terminalError != nil:
		status, reason = "failed", "upstream_response_failed"
	case d.cancelErr != nil:
		status, reason = "failed", "upstream_stream_error"
	case framer.terminalEvent == "response.completed":
		status, reason = "succeeded", "response_complete"
	case framer.terminalEvent == "response.incomplete":
		status, reason = "incomplete", "upstream_response_incomplete"
	case framer.terminalEvent != "":
		status, reason = "failed", "upstream_response_failed"
	}
	fields := map[string]any{
		"diagnostic_version":                1,
		"body_capture":                      "omitted",
		"request_parameters":                d.parameters,
		"request_headers":                   logging.RedactedDiagnosticHeaders(c.Request.Header),
		"response_headers":                  logging.RedactedDiagnosticHeaders(d.Header()),
		"request_id":                        sanitizeResponsesStreamEventName(logging.GetGinRequestID(c)),
		"trace_id":                          sanitizeResponsesStreamEventName(logging.GetGinCPATraceID(c)),
		"timestamp_unix_ms":                 d.started.UnixMilli(),
		"codex_session_id":                  sanitizeResponsesStreamEventName(logging.GetClientRequestMetadata(ctx).SessionID),
		"requested_model":                   sanitizeResponsesStreamEventName(model),
		"request_protocol":                  "sse",
		"request_kind":                      "responses",
		"status":                            status,
		"status_code":                       d.Status(),
		"completion_reason":                 reason,
		"total_duration_ms":                 time.Since(d.started).Milliseconds(),
		"request_bytes":                     requestBytes,
		"last_event":                        framer.lastEvent,
		"terminal_event":                    framer.receivedTerminalEvent,
		"terminal_received":                 framer.receivedTerminalEvent != "",
		"generated_terminal_event":          framer.generatedTerminalEvent,
		"data_frames":                       framer.dataFrames,
		"response_started":                  d.Written(),
		"downstream_bytes_attempted":        d.attempted,
		"downstream_bytes_written":          d.written,
		"downstream_flush_error_observable": false,
		"write_error_kind":                  responsesDiagnosticErrorKind(d.writeErr),
		"cancel_error_kind":                 responsesDiagnosticErrorKind(d.cancelErr),
		"request_context_error_kind":        responsesDiagnosticErrorKind(c.Request.Context().Err()),
	}
	for _, item := range []struct{ key, field string }{
		{logging.UpstreamRequestDiagnosticContextKey, "upstream_request"},
		{logging.UpstreamResponseDiagnosticContextKey, "upstream_response"},
	} {
		if value, exists := c.Get(item.key); exists {
			fields[item.field] = value
		}
	}
	if !d.firstWrite.IsZero() {
		fields["first_downstream_write_ms"] = d.firstWrite.Sub(d.started).Milliseconds()
		fields["last_downstream_write_ms"] = d.lastWrite.Sub(d.started).Milliseconds()
	}
	for key, value := range logging.UpstreamDiagnosticSnapshot(c) {
		fields[key] = value
	}
	return fields
}

func (d *responsesStreamDiagnostics) finish(c *gin.Context, ctx context.Context, model string, requestBytes int, framer *responsesSSEFramer) {
	defer func() { c.Writer = d.ResponseWriter }()
	fields := d.summary(c, ctx, model, requestBytes, framer)
	c.Set(logging.RequestDiagnosticContextKey, fields)
	// CPA's text formatter drops unknown logrus fields. Keep the bounded metadata
	// JSON in the message so this evidence survives with request body logging off.
	payload, err := json.Marshal(fields)
	if err != nil {
		return
	}
	entry := log.WithField("request_id", fields["request_id"])
	if fields["status"] == "succeeded" {
		entry.Infof("responses_stream_summary %s", payload)
	} else {
		entry.Warnf("responses_stream_summary %s", payload)
	}
}
