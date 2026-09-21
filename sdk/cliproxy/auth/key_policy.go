package auth

import (
	"context"
	"path/filepath"
	"sort"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/keypolicy"
	log "github.com/sirupsen/logrus"
	"github.com/tidwall/gjson"
)

func KeyPolicyResourceID(a *Auth) string {
	if a == nil {
		return ""
	}
	if a.Index != "" {
		return a.Index
	}
	return a.Clone().EnsureIndex()
}

// KeyPolicyResources includes each OAuth file and each configured upstream key.
// AccountInfo is intentionally not used: API-key accounts can return secrets.
func (m *Manager) KeyPolicyResources() []keypolicy.Resource {
	if m == nil {
		return nil
	}
	auths := m.List()
	out := make([]keypolicy.Resource, 0, len(auths))
	for _, a := range auths {
		if a == nil {
			continue
		}
		id := KeyPolicyResourceID(a)
		if id == "" {
			continue
		}
		kind, label := "oauth", a.Label
		if a.AuthKind() == AuthKindAPIKey {
			kind = "api_key"
			label = a.Attributes["compat_name"]
		}
		if label == "" && a.FileName != "" {
			label = filepath.Base(a.FileName)
		}
		if label == "" {
			label = a.Provider + " · " + id
		}
		models := []string{}
		for _, info := range registry.GetGlobalRegistry().GetModelsForClient(a.ID) {
			if info != nil {
				models = append(models, info.ID)
			}
		}
		sort.Strings(models)
		out = append(out, keypolicy.Resource{ResourceID: id, Label: label, Provider: a.Provider, Kind: kind, Disabled: a.Disabled, Models: models})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ResourceID < out[j].ResourceID })
	return out
}

func (m *Manager) KeyPolicyModelClients(ctx context.Context) ([]string, bool) {
	store := keypolicy.StoreFromContext(ctx)
	if store == nil {
		return nil, false
	}
	store.SetResources(m.KeyPolicyResources())
	key := keypolicy.KeyIDFromContext(ctx)
	if store.AllowsAll(key) {
		return nil, false
	}
	ids := []string{}
	for _, a := range m.List() {
		if a != nil && store.Authorize(key, KeyPolicyResourceID(a)) == nil {
			ids = append(ids, a.ID)
		}
	}
	sort.Strings(ids)
	return ids, true
}

func (m *Manager) prepareKeyPolicy(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, countOnly bool) error {
	store := keypolicy.StoreFromContext(ctx)
	if store == nil {
		return nil
	}
	if m.HomeEnabled() {
		return keypolicy.Wrap(keypolicy.ErrUnavailable)
	}
	store.SetResources(m.KeyPolicyResources())
	key := keypolicy.KeyIDFromContext(ctx)
	wanted := map[string]bool{}
	for _, provider := range m.normalizeProviders(providers) {
		wanted[provider] = true
	}
	pin := pinnedAuthIDFromMetadata(opts.Metadata)
	var budgetErr error
	allowed := false
	m.mu.RLock()
	defer m.mu.RUnlock()
	for _, a := range m.auths {
		if a == nil || a.Disabled || !wanted[executorKeyFromAuth(a)] || (pin != "" && pin != a.ID) {
			continue
		}
		if model != "" && !m.authSupportsRouteModel(registry.GetGlobalRegistry(), a, model) {
			continue
		}
		if err := store.Authorize(key, KeyPolicyResourceID(a)); err != nil {
			continue
		}
		allowed = true
		if countOnly {
			return nil
		}
		if err := store.CheckBudget(key, KeyPolicyResourceID(a), time.Now()); err != nil {
			budgetErr = err
			continue
		}
		return nil
	}
	if allowed && budgetErr != nil {
		return keypolicy.Wrap(budgetErr)
	}
	return keypolicy.Wrap(keypolicy.ErrDenied)
}

func beginKeyPolicy(ctx context.Context, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (context.Context, func(), error) {
	store := keypolicy.StoreFromContext(ctx)
	if store == nil {
		return ctx, func() {}, nil
	}
	key := keypolicy.KeyIDFromContext(ctx)
	rid := KeyPolicyResourceID(a)
	if err := store.Authorize(key, rid); err != nil {
		return ctx, nil, keypolicy.Wrap(err)
	}
	// A client-side WebSocket warmup does not generate output or consume budget.
	if value := gjson.GetBytes(req.Payload, "generate"); value.Exists() && value.Type == gjson.False {
		return ctx, func() {}, nil
	}
	model := req.Model
	alias := requestedModelAliasFromOptions(opts, req.Model)
	if !store.HasPrice(model) && store.HasPrice(alias) {
		model = alias
	}
	output := int64(8192)
	for _, field := range []string{"max_output_tokens", "max_completion_tokens", "max_tokens", "generationConfig.maxOutputTokens"} {
		if v := gjson.GetBytes(req.Payload, field); v.Exists() && v.Int() > 0 {
			output = v.Int()
			break
		}
	}
	// Byte length is a conservative text-input estimate. This is a soft cap;
	// actual provider tokenization and multimodal billing can differ.
	estimate := keypolicy.Tokens{Input: int64(len(req.Payload)), Output: output}
	reservation, err := store.Begin(key, rid, model, estimate, time.Now())
	if err != nil {
		return ctx, nil, keypolicy.Wrap(err)
	}
	ctx = keypolicy.WithReservation(ctx, reservation)
	finish := func() {
		if err := store.FinishUnknown(reservation.ID, false); err != nil {
			log.Errorf("key budget finalization failed: %v", err)
		}
	}
	return ctx, finish, nil
}

func executeWithKeyPolicy(ctx context.Context, executor ProviderExecutor, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	ctx, finish, err := beginKeyPolicy(ctx, a, req, opts)
	if err != nil {
		return cliproxyexecutor.Response{}, err
	}
	defer finish()
	return executor.Execute(ctx, a, req, opts)
}

func streamWithKeyPolicy(ctx context.Context, executor ProviderExecutor, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	ctx, finish, err := beginKeyPolicy(ctx, a, req, opts)
	if err != nil {
		return nil, err
	}
	result, err := executor.ExecuteStream(ctx, a, req, opts)
	if err != nil || result == nil || result.Chunks == nil {
		finish()
		return result, err
	}
	if keypolicy.ReservationFromContext(ctx) == nil {
		return result, nil
	}
	chunks := make(chan cliproxyexecutor.StreamChunk)
	go func() {
		defer close(chunks)
		defer finish()
		for {
			select {
			case <-ctx.Done():
				return
			case chunk, ok := <-result.Chunks:
				if !ok {
					return
				}
				select {
				case chunks <- chunk:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return &cliproxyexecutor.StreamResult{Headers: result.Headers, Chunks: chunks}, nil
}

func countWithKeyPolicy(ctx context.Context, executor ProviderExecutor, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	if store := keypolicy.StoreFromContext(ctx); store != nil {
		if err := store.Authorize(keypolicy.KeyIDFromContext(ctx), KeyPolicyResourceID(a)); err != nil {
			return cliproxyexecutor.Response{}, keypolicy.Wrap(err)
		}
	}
	return executor.CountTokens(ctx, a, req, opts)
}
