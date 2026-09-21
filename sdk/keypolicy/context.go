package keypolicy

import "context"

// WithStore binds authorization to this server instance, never client metadata.
func WithStore(ctx context.Context, store *Store) context.Context {
	return context.WithValue(ctx, contextKey(2), store)
}

func StoreFromContext(ctx context.Context) *Store {
	if ctx == nil {
		return nil
	}
	store, _ := ctx.Value(contextKey(2)).(*Store)
	return store
}

// CopyScope preserves the authenticated scope when a handler detaches cancellation.
func CopyScope(dst, src context.Context) context.Context {
	if store := StoreFromContext(src); store != nil {
		dst = WithStore(dst, store)
		dst = WithKeyID(dst, KeyIDFromContext(src))
	}
	return dst
}

func (s *Store) AllowsAll(keyID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.data.Keys[keyID]
	return s.failed == nil && ok && s.active[keyID] && k.AllowAll
}

// Unrestricted also excludes resource-specific financial restrictions.
func (s *Store) Unrestricted(keyID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	k, ok := s.data.Keys[keyID]
	if s.failed != nil || !ok || !s.active[keyID] || !k.AllowAll {
		return false
	}
	for _, rule := range k.Rules {
		if rule.LimitUSD != nil {
			return false
		}
	}
	return true
}

func (s *Store) HasPrice(model string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.data.Prices[model]
	return ok
}
