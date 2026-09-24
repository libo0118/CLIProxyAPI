package openai

import (
	"context"
	"errors"
	"net/http"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/interfaces"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/logging"
	coresession "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/session"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

// A disconnect callback can write concurrently with the forwarding goroutine.
// The observer lock never covers socket I/O or persistence.
type responsesWebsocketDiagnostics struct {
	mu                             sync.Mutex
	base                           responsesStreamDiagnostics
	framer                         responsesSSEFramer
	ctx                            context.Context
	requestID, connectionID, model string
	requestBytes                   int
	upstreamCloseCode              int
}

func newResponsesWebsocketDiagnostics(c *gin.Context, connectionID, model string, payload []byte) *responsesWebsocketDiagnostics {
	id := logging.GenerateRequestID()
	logging.SetGinRequestID(c, id)
	logging.ResetUpstreamDiagnostics(c)
	info, _ := coresession.ExtractSessionInfo(c.Request.Header, nil, nil)
	ctx := logging.WithClientRequestMetadata(c.Request.Context(), logging.ClientRequestMetadata{SessionID: info.SessionID})
	return &responsesWebsocketDiagnostics{
		base: responsesStreamDiagnostics{ResponseWriter: c.Writer, started: time.Now(), parameters: responsesDiagnosticParameters(payload)},
		ctx:  ctx, requestID: id, connectionID: connectionID, model: model, requestBytes: len(payload),
	}
}

func (d *responsesWebsocketDiagnostics) observe(payload []byte) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	event := gjson.GetBytes(payload, "type").String()
	d.framer.dataFrames++
	d.framer.lastEvent = sanitizeResponsesStreamEventName(event)
	if responsesSSETerminalEvent(event) {
		d.framer.terminalEvent, d.framer.receivedTerminalEvent = event, event
	}
	if responsesSSEErrorEvent(event) {
		d.framer.terminalError = &interfaces.ErrorMessage{StatusCode: http.StatusBadGateway}
	}
}

func (d *responsesWebsocketDiagnostics) recordWrite(payload []byte, err error) {
	if d == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	n := 0
	if err == nil {
		n = len(payload)
	}
	d.base.recordWrite(len(payload), n, err)
	event := gjson.GetBytes(payload, "type").String()
	if responsesSSETerminalEvent(event) && event != d.framer.receivedTerminalEvent {
		d.framer.generatedTerminalEvent = event
	}
}

func (d *responsesWebsocketDiagnostics) recordError(err error, upstreamDisconnect bool) {
	if d == nil || err == nil {
		return
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if upstreamDisconnect {
		var closeErr *websocket.CloseError
		if errors.As(err, &closeErr) {
			d.upstreamCloseCode = closeErr.Code
		}
		if isResponsesWebsocketCompletionEvent(d.framer.receivedTerminalEvent) {
			return
		}
	}
	d.base.cancelErr = err
}

func (d *responsesWebsocketDiagnostics) missingTerminal() {
	if d == nil {
		return
	}
	d.mu.Lock()
	d.framer.missingTerminal = d.framer.receivedTerminalEvent == ""
	d.mu.Unlock()
}

func (w *responsesWebsocketWriter) finishDiagnostic(c *gin.Context, err error) {
	d := w.diagnostic.Swap(nil)
	if d == nil {
		return
	}
	d.mu.Lock()
	if err != nil && !errors.Is(err, websocket.ErrCloseSent) && d.base.cancelErr == nil {
		d.base.cancelErr = err
	}
	fields := d.base.summary(c, d.ctx, d.model, d.requestBytes, &d.framer)
	fields["request_id"], fields["connection_request_id"] = d.requestID, d.connectionID
	fields["request_protocol"], fields["status_code"] = "ws", http.StatusSwitchingProtocols
	fields["write_byte_count_kind"] = "completed_websocket_message_payloads"
	delete(fields, "downstream_flush_error_observable")
	if d.upstreamCloseCode != 0 {
		fields["upstream_close_code"] = d.upstreamCloseCode
	}
	if d.base.writeErr == nil && c.Request.Context().Err() == nil {
		switch {
		case d.framer.receivedTerminalEvent == "response.incomplete":
			fields["status"], fields["completion_reason"] = "incomplete", "upstream_response_incomplete"
		case d.base.cancelErr == nil && d.framer.receivedTerminalEvent == "response.done":
			fields["status"], fields["completion_reason"] = "succeeded", "response_complete"
		case d.base.cancelErr == nil && d.framer.generatedTerminalEvent == "response.completed":
			fields["status"], fields["completion_reason"] = "succeeded", "local_prewarm_complete"
		case d.base.cancelErr == nil && responsesSSEErrorEvent(d.framer.generatedTerminalEvent):
			fields["status"], fields["completion_reason"] = "failed", "request_rejected"
		}
	}
	d.mu.Unlock()
	if value, ok := c.Get(logging.RequestDiagnosticWriterContextKey); ok {
		if persist, ok := value.(func(string, map[string]any) error); ok {
			if err := persist(d.requestID, fields); err != nil {
				log.Warnf("failed to persist websocket diagnostic: %s", logging.SafeErrorDiagnostic(err))
			}
		}
	}
	log.WithField("request_id", d.requestID).Infof("responses_websocket_summary status=%s completion_reason=%s", fields["status"], fields["completion_reason"])
}
