package usage

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v7/sdk/keypolicy"
)

func TestBudgetUsesCanonicalProviderTokenPartitions(t *testing.T) {
	for _, tc := range []struct{ provider, want string }{{"openai", "0.000100"}, {"anthropic", "0.000162"}, {"gemini", "0.000112"}} {
		t.Run(tc.provider, func(t *testing.T) {
			store, err := keypolicy.NewStore(filepath.Join(t.TempDir(), "ledger.json"), []string{"key"})
			if err != nil {
				t.Fatal(err)
			}
			resources := []keypolicy.Resource{{ResourceID: "r"}}
			store.SetResources(resources)
			if err = store.Sync([]keypolicy.Price{{Model: "m", InputPerMillion: "1", OutputPerMillion: "1", CacheReadPerMillion: "0.25", CacheWritePerMillion: "1"}}, nil); err != nil {
				t.Fatal(err)
			}
			r, err := store.Begin(keypolicy.KeyID("key"), "r", "m", keypolicy.Tokens{Input: 1000}, time.Now())
			if err != nil {
				t.Fatal(err)
			}
			ctx := keypolicy.WithReservation(keypolicy.WithStore(context.Background(), store), r)
			var manager *Manager
			manager.Publish(ctx, Record{Provider: tc.provider, Detail: Detail{InputTokens: 100, OutputTokens: 30, ReasoningTokens: 12, CacheReadTokens: 40, CacheCreationTokens: 10}})
			budgets := store.Snapshot(resources).Budgets
			if len(budgets) != 1 || budgets[0].UsedUSD != tc.want || budgets[0].ReservedUSD != "0.000000" {
				t.Fatalf("incorrect budget: %+v", budgets)
			}
		})
	}
}
