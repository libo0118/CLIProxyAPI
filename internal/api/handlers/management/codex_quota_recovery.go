package management

import (
	"context"
	"net/http"
	"strings"

	coreauth "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func (h *Handler) recoverCodexQuota(ctx context.Context, auth *coreauth.Auth, req *http.Request, resp *http.Response, body []byte, token string) bool {
	if h.authManager == nil || auth == nil || auth.Provider != "codex" || token == "" || resp.StatusCode != http.StatusOK ||
		!isCodexUsageRequest(req) || !isCodexUsageRequest(resp.Request) || req.Header.Get("Authorization") != "Bearer "+token {
		return false
	}
	accountID, _ := auth.Metadata["account_id"].(string)
	if req.Header.Get("Chatgpt-Account-Id") != strings.TrimSpace(accountID) || !codexQuotaAvailable(body) {
		return false
	}
	updated, _, err := h.authManager.RecoverCodexQuota(ctx, auth.ID, auth.Generation)
	if err != nil {
		log.WithError(err).Warn("failed to persist Codex quota recovery")
		return false
	}
	return updated != nil
}

func isCodexUsageRequest(req *http.Request) bool {
	return req != nil && req.URL != nil && req.Method == http.MethodGet && req.URL.Scheme == "https" &&
		req.URL.Host == "chatgpt.com" && req.URL.Path == "/backend-api/wham/usage" &&
		(req.Host == "" || req.Host == "chatgpt.com")
}

func codexQuotaAvailable(body []byte) bool {
	if !gjson.ValidBytes(body) {
		return false
	}
	root := gjson.ParseBytes(body)
	if !codexLimitAvailable(root.Get("rate_limit")) {
		return false
	}
	additional := root.Get("additional_rate_limits")
	if additional.Exists() && additional.Type != gjson.Null {
		if !additional.IsArray() {
			return false
		}
		for _, limit := range additional.Array() {
			if !codexLimitAvailable(limit.Get("rate_limit")) {
				return false
			}
		}
	}
	return true
}

func codexLimitAvailable(limit gjson.Result) bool {
	if limit.Get("allowed").Type != gjson.True {
		return false
	}
	reached := limit.Get("limit_reached")
	if reached.Exists() && reached.Type != gjson.False {
		return false
	}
	windows := 0
	for _, key := range []string{"primary_window", "secondary_window"} {
		window := limit.Get(key)
		if !window.Exists() || window.Type == gjson.Null {
			continue
		}
		used := window.Get("used_percent")
		if used.Type != gjson.Number || used.Float() < 0 || used.Float() >= 100 {
			return false
		}
		windows++
	}
	return windows > 0
}
