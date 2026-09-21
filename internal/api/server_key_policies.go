package api

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/keypolicy"
	log "github.com/sirupsen/logrus"
)

func normalizedPolicyKeys(keys []string) []string {
	out := []string{}
	seen := map[string]bool{}
	for _, key := range keys {
		key = strings.TrimSpace(key)
		if key != "" && !seen[key] {
			seen[key] = true
			out = append(out, key)
		}
	}
	return out
}

func (s *Server) initializeKeyPolicies() {
	path := strings.TrimSpace(s.cfg.KeyPolicyFile)
	if path == "" {
		return
	}
	s.keyPolicyConfigured = true
	if !filepath.IsAbs(path) {
		path = filepath.Join(filepath.Dir(s.configFilePath), path)
	}
	store, err := keypolicy.NewStore(path, normalizedPolicyKeys(s.cfg.APIKeys))
	if err != nil {
		s.keyPolicyError = err
		log.Errorf("key policy initialization failed; client access is blocked: %v", err)
		return
	}
	s.keyPolicies = store
	store.SetResources(s.handlers.AuthManager.KeyPolicyResources())
}

func (s *Server) keyPolicyMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		if !s.keyPolicyConfigured {
			c.Next()
			return
		}
		if s.keyPolicies == nil || s.keyPolicyError != nil {
			c.AbortWithStatusJSON(503, gin.H{"error": "key_policy_storage_unavailable"})
			return
		}
		principal := c.GetString("userApiKey")
		if principal == "" {
			c.AbortWithStatusJSON(401, gin.H{"error": "key_policy_requires_api_key"})
			return
		}
		key := keypolicy.KeyID(principal)
		s.keyPolicies.SetResources(s.handlers.AuthManager.KeyPolicyResources())
		ctx := keypolicy.WithStore(c.Request.Context(), s.keyPolicies)
		ctx = keypolicy.WithKeyID(ctx, key)
		c.Request = c.Request.WithContext(ctx)
		// Auxiliary live/video/search transports bypass the ordinary executor
		// accounting pipeline. Restricted keys fail closed on those transports.
		if s.cfg.Home.Enabled || (!s.keyPolicies.Unrestricted(key) && !keyPolicySupportedPath(c.Request.Method, c.Request.URL.Path)) {
			c.AbortWithStatusJSON(403, gin.H{"error": gin.H{"code": "key_policy_transport_unsupported", "message": "This transport does not support scoped key authorization and budgeting."}})
			return
		}
		c.Next()
	}
}

func keyPolicySupportedPath(method, path string) bool {
	if method == http.MethodGet {
		return path == "/v1/models" || path == "/v1/responses" || path == "/backend-api/codex/responses" || path == "/v1beta/models" || strings.HasPrefix(path, "/v1beta/models/")
	}
	if method != http.MethodPost {
		return false
	}
	switch path {
	case "/v1/chat/completions", "/v1/completions", "/v1/responses", "/v1/responses/compact", "/backend-api/codex/responses", "/backend-api/codex/responses/compact", "/v1/messages", "/v1/messages/count_tokens", "/v1beta/interactions":
		return true
	}
	return strings.HasPrefix(path, "/v1beta/models/")
}

func (s *Server) requireKeyPolicyStore(c *gin.Context) *keypolicy.Store {
	if !s.keyPolicyConfigured {
		c.JSON(404, gin.H{"error": "key_policies_not_enabled"})
		return nil
	}
	if s.keyPolicies == nil || s.keyPolicyError != nil {
		c.JSON(503, gin.H{"error": "key_policy_storage_unavailable"})
		return nil
	}
	s.keyPolicies.SetResources(s.handlers.AuthManager.KeyPolicyResources())
	return s.keyPolicies
}

func (s *Server) getKeyPolicies(c *gin.Context) {
	if store := s.requireKeyPolicyStore(c); store != nil {
		c.JSON(200, store.Snapshot(s.handlers.AuthManager.KeyPolicyResources()))
	}
}

func readKeyPolicyJSON(c *gin.Context, out any) error {
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, 2<<20)
	d := json.NewDecoder(c.Request.Body)
	d.DisallowUnknownFields()
	if err := d.Decode(out); err != nil {
		return err
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return keypolicy.ErrInvalid
	}
	return nil
}

func writeKeyPolicyError(c *gin.Context, err error) {
	status := 400
	switch {
	case errors.Is(err, keypolicy.ErrStale):
		status = 409
	case errors.Is(err, keypolicy.ErrUnavailable):
		status = 503
	case errors.Is(err, keypolicy.ErrDenied):
		status = 403
	}
	// Errors contain policy validation state only, never keys or credentials.
	c.JSON(status, gin.H{"error": err.Error()})
}

func (s *Server) putKeyPolicy(c *gin.Context) {
	store := s.requireKeyPolicyStore(c)
	if store == nil {
		return
	}
	var body struct {
		Revision uint64 `json:"revision"`
		Label    string `json:"label"`
		AllowAll *bool  `json:"allow_all"`
		Rules    []struct {
			ResourceID string          `json:"resource_id"`
			Period     string          `json:"period"`
			Limit      json.RawMessage `json:"limit_usd"`
		} `json:"rules"`
	}
	if err := readKeyPolicyJSON(c, &body); err != nil {
		c.JSON(400, gin.H{"error": "invalid_key_policy_request"})
		return
	}
	if body.AllowAll == nil || body.Revision == 0 || body.Rules == nil || len(body.Rules) > 10000 || len(body.Label) > 256 {
		c.JSON(400, gin.H{"error": "invalid_key_policy_request"})
		return
	}
	p := keypolicy.Policy{Label: strings.TrimSpace(body.Label), AllowAll: *body.AllowAll, Rules: []keypolicy.Rule{}}
	for _, r := range body.Rules {
		if len(r.Limit) == 0 {
			c.JSON(400, gin.H{"error": "limit_usd_must_be_explicit_null_or_decimal_string"})
			return
		}
		var limit *string
		if string(r.Limit) != "null" {
			var amount string
			if json.Unmarshal(r.Limit, &amount) != nil {
				c.JSON(400, gin.H{"error": "limit_usd_must_be_decimal_string"})
				return
			}
			limit = &amount
		}
		p.Rules = append(p.Rules, keypolicy.Rule{ResourceID: r.ResourceID, Period: r.Period, LimitUSD: limit})
	}
	if err := store.UpdatePolicy(c.Param("key_id"), body.Revision, p); err != nil {
		writeKeyPolicyError(c, err)
		return
	}
	c.JSON(200, store.Snapshot(s.handlers.AuthManager.KeyPolicyResources()))
}

func (s *Server) syncKeyPolicyData(c *gin.Context) {
	store := s.requireKeyPolicyStore(c)
	if store == nil {
		return
	}
	var body struct {
		Prices []keypolicy.Price `json:"prices"`
		Cycles []keypolicy.Cycle `json:"cycles"`
	}
	if err := readKeyPolicyJSON(c, &body); err != nil {
		c.JSON(400, gin.H{"error": "invalid_key_policy_sync"})
		return
	}
	if len(body.Prices) > 20000 || len(body.Cycles) > 20000 {
		c.JSON(400, gin.H{"error": "key_policy_sync_too_large"})
		return
	}
	if err := store.Sync(body.Prices, body.Cycles); err != nil {
		writeKeyPolicyError(c, err)
		return
	}
	c.JSON(200, gin.H{"status": "ok"})
}
