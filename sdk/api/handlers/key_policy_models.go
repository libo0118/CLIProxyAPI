package handlers

import (
	"context"
	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
)

func (h *BaseAPIHandler) ModelsForContext(ctx context.Context, format string) []map[string]any {
	r := registry.GetGlobalRegistry()
	if h != nil && h.AuthManager != nil {
		if ids, scoped := h.AuthManager.KeyPolicyModelClients(ctx); scoped {
			return r.GetModelsForClients(ids, format)
		}
	}
	return r.GetAvailableModels(format)
}
