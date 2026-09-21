package helps

import (
	"bytes"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/tidwall/gjson"
)

// ObserveUpstreamResponseModel captures the first protocol-reported model, including
// early WebSocket events when the terminal response omits it.
func (r *UsageReporter) ObserveUpstreamResponseModel(payload []byte) {
	if r == nil {
		return
	}
	r.ttftMu.RLock()
	observed := r.upstreamResponseModel != ""
	r.ttftMu.RUnlock()
	if observed {
		return
	}
	if model := responseModel(payload); model != "" {
		r.ttftMu.Lock()
		if r.upstreamResponseModel == "" {
			r.upstreamResponseModel = model
		}
		r.ttftMu.Unlock()
	}
}

// ResponseModelStream observes SSE metadata without altering forwarded bytes.
// Oversized events are skipped; memory is bounded independently of stream length.
type ResponseModelStream struct {
	Reporter     *UsageReporter
	line         []byte
	data         []byte
	skipping     bool
	lineNonEmpty bool
}

const responseModelEventLimit = 1024 * 1024

func (s *ResponseModelStream) Observe(chunk []byte) {
	for len(chunk) > 0 {
		end := bytes.IndexByte(chunk, '\n')
		part := chunk
		if end >= 0 {
			part = chunk[:end]
		}
		s.lineNonEmpty = s.lineNonEmpty || len(bytes.TrimSuffix(part, []byte{'\r'})) > 0
		if !s.skipping {
			if len(s.line)+len(s.data)+len(part) > responseModelEventLimit {
				s.line, s.data, s.skipping = nil, nil, true
			} else {
				s.line = append(s.line, part...)
			}
		}
		if end < 0 {
			return
		}
		if s.skipping {
			if !s.lineNonEmpty {
				s.skipping = false
			}
		} else {
			s.consumeLine()
		}
		s.lineNonEmpty = false
		chunk = chunk[end+1:]
	}
}

func (s *ResponseModelStream) consumeLine() {
	line := bytes.TrimSuffix(s.line, []byte{'\r'})
	if len(line) == 0 {
		s.Reporter.ObserveUpstreamResponseModel(s.data)
		s.data = s.data[:0]
	} else if bytes.HasPrefix(line, []byte("data:")) {
		value := bytes.TrimPrefix(line[5:], []byte{' '})
		s.data = append(s.data, value...)
		s.data = append(s.data, '\n')
	} else if len(s.data) == 0 && len(line) > 0 && line[0] == '{' {
		s.Reporter.ObserveUpstreamResponseModel(line)
	}
	s.line = s.line[:0]
}

func (s *ResponseModelStream) Flush() {
	if s.skipping {
		return
	}
	if len(s.line) > 0 {
		s.consumeLine()
	}
	s.Reporter.ObserveUpstreamResponseModel(s.data)
	s.data = nil
}

// responseModel reads only protocol metadata, not echoed inputs or tool arguments.
func responseModel(payload []byte) string {
	payload = jsonPayload(payload)
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return ""
	}
	for _, path := range []string{"response.model", "message.model", "model", "modelVersion", "response.modelVersion"} {
		value := gjson.GetBytes(payload, path)
		if value.Type == gjson.String {
			if model := validResponseModel(value.String()); model != "" {
				return model
			}
		}
	}
	return ""
}

func validResponseModel(model string) string {
	model = strings.TrimSpace(model)
	if len(model) == 0 || len(model) > 512 || !utf8.ValidString(model) {
		return ""
	}
	for _, char := range model {
		if unicode.IsControl(char) {
			return ""
		}
	}
	return model
}

// Plugins may synthesize model from their input. Only their explicit upstream
// metadata can attest to a model reported by the actual provider.
func pluginResponseModel(payload []byte) string {
	payload = jsonPayload(payload)
	if len(payload) == 0 || !gjson.ValidBytes(payload) {
		return ""
	}
	value := gjson.GetBytes(payload, "upstream_response_model")
	if value.Type != gjson.String {
		return ""
	}
	return validResponseModel(value.String())
}
