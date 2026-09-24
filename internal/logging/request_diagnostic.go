package logging

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

const RequestDiagnosticContextKey = "REQUEST_DIAGNOSTIC"
const RequestDiagnosticWriterContextKey = "REQUEST_DIAGNOSTIC_WRITER"
const UpstreamRequestDiagnosticContextKey = "UPSTREAM_REQUEST_DIAGNOSTIC"
const UpstreamResponseDiagnosticContextKey = "UPSTREAM_RESPONSE_DIAGNOSTIC"

// RedactedDiagnosticHeaders keeps transport metadata and fully hides credentials,
// cookies and unknown header values. Never retain a raw header snapshot.
func RedactedDiagnosticHeaders(headers http.Header) map[string]string {
	keys := make([]string, 0, len(headers))
	for key := range headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	if len(keys) > 64 {
		keys = keys[:64]
	}
	out := make(map[string]string, len(keys))
	for _, key := range keys {
		value := "[REDACTED]"
		lower := strings.ToLower(key)
		switch lower {
		case "content-type", "content-length", "content-encoding", "accept", "accept-encoding",
			"cache-control", "connection", "transfer-encoding", "date", "server", "retry-after",
			"user-agent", "x-request-id", "request-id", "x-cpa-trace-id", "session-id", "session_id",
			"x-session-id", "openai-version", "openai-processing-ms":
			value = SafeDiagnosticForLog(strings.Join(headers[key], ", "))
		}
		out[SafeDiagnosticForLog(key)] = value
	}
	return out
}

func DiagnosticUpstreamRequest(rawURL, method, provider string, headers http.Header) map[string]any {
	fields := map[string]any{
		"method": SafeDiagnosticForLog(method), "provider": SafeDiagnosticForLog(provider),
		"headers": RedactedDiagnosticHeaders(headers),
	}
	if parsed, err := url.Parse(rawURL); err == nil {
		// URL paths and query strings may contain credentials or request content.
		fields["authority"] = SafeDiagnosticForLog(parsed.Host)
		fields["scheme"] = SafeDiagnosticForLog(parsed.Scheme)
	}
	return fields
}

// LogRequestDiagnostic persists only the metadata produced by the Responses
// observer. It works with full body logging disabled and uses existing log-dir
// size retention. The management log-by-ID endpoint prefers this safe artifact.
func (l *FileRequestLogger) LogRequestDiagnostic(requestID string, fields map[string]any) error {
	if requestID == "" || strings.ContainsAny(requestID, "/\\\r\n") {
		return fmt.Errorf("invalid diagnostic request ID")
	}
	payload, err := json.MarshalIndent(fields, "", "  ")
	if err != nil {
		return err
	}
	if err = l.ensureLogsDir(); err != nil {
		return err
	}
	// A temporary file prevents the viewer from reading a partial JSON document.
	tmp, err := os.CreateTemp(l.logsDir, ".diagnostic-*.tmp")
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmp.Name()) }()
	_, err = fmt.Fprintf(tmp, "=== REQUEST DIAGNOSTICS ===\n%s\n", payload)
	closeErr := tmp.Close()
	if err != nil {
		return err
	}
	if closeErr != nil {
		return closeErr
	}
	return os.Rename(tmp.Name(), filepath.Join(l.logsDir, "diagnostic-"+l.generateFilename("responses", requestID)))
}
