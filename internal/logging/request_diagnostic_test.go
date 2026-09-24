package logging

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRequestDiagnosticRedactsBeforePersistence(t *testing.T) {
	metadata := DiagnosticUpstreamRequest("https://user:password@example.test/secret-path?token=secret-query", "POST", "codex", http.Header{
		"Authorization": {"Bearer secret-key"}, "Cookie": {"secret-cookie"}, "Set-Cookie": {"secret-set-cookie"},
		"X-Private": {"secret-custom"}, "Content-Type": {"text/event-stream"},
	})
	logger := &FileRequestLogger{logsDir: t.TempDir()}
	if err := logger.LogRequestDiagnostic("req-test", metadata); err != nil {
		t.Fatal(err)
	}
	paths, err := filepath.Glob(filepath.Join(logger.logsDir, "diagnostic-*-req-test.log"))
	if err != nil || len(paths) != 1 {
		t.Fatalf("diagnostic not persisted: %v %v", paths, err)
	}
	body, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"secret-", "password", "user:"} {
		if strings.Contains(string(body), secret) {
			t.Fatalf("secret retained: %s", body)
		}
	}
	if !strings.Contains(string(body), "text/event-stream") || !strings.Contains(string(body), "example.test") {
		t.Fatalf("transport evidence missing: %s", body)
	}
	if err := logger.LogRequestDiagnostic("../escape", metadata); err == nil {
		t.Fatal("unsafe ID accepted")
	}
}
