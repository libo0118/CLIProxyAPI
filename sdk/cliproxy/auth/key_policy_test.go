package auth

import (
	"context"
	"errors"
	"path/filepath"
	"testing"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/executor"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/cliproxy/usage"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/keypolicy"
	"github.com/router-for-me/CLIProxyAPI/v7/sdk/pluginapi"
)

type budgetTestExecutor struct {
	schedulerTestExecutor
	calls []string
}

func (e *budgetTestExecutor) Execute(ctx context.Context, a *Auth, req cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.calls = append(e.calls, a.ID)
	usage.PublishRecord(ctx, usage.Record{Provider: "openai", Model: req.Model, AuthID: a.ID, Detail: usage.Detail{InputTokens: 10000, OutputTokens: 100}})
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}

func (e *budgetTestExecutor) ExecuteStream(ctx context.Context, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	response, err := e.Execute(ctx, a, req, opts)
	if err != nil {
		return nil, err
	}
	ch := make(chan cliproxyexecutor.StreamChunk, 1)
	ch <- cliproxyexecutor.StreamChunk{Payload: response.Payload}
	close(ch)
	return &cliproxyexecutor.StreamResult{Chunks: ch}, nil
}

func TestKeyPolicyExecutionIsolationAndSettlement(t *testing.T) {
	manager := NewManager(nil, &RoundRobinSelector{}, nil)
	executor := &budgetTestExecutor{schedulerTestExecutor: schedulerTestExecutor{provider: "budget-test"}}
	manager.RegisterExecutor(executor)
	for _, id := range []string{"budget-auth-a", "budget-auth-b"} {
		if _, err := manager.Register(context.Background(), &Auth{ID: id, Provider: "budget-test", Index: id}); err != nil {
			t.Fatal(err)
		}
	}
	registerSchedulerModels(t, "budget-test", "budget-model", "budget-auth-a", "budget-auth-b")
	path := filepath.Join(t.TempDir(), "ledger.json")
	store, err := keypolicy.NewStore(path, []string{"key-a", "key-b"})
	if err != nil {
		t.Fatal(err)
	}
	resources := manager.KeyPolicyResources()
	store.SetResources(resources)
	price := keypolicy.Price{Model: "budget-model", InputPerMillion: "1", OutputPerMillion: "1", CacheReadPerMillion: "0", CacheWritePerMillion: "0"}
	if err = store.Sync([]keypolicy.Price{price}, nil); err != nil {
		t.Fatal(err)
	}
	limit := "1"
	policy := keypolicy.Policy{Rules: []keypolicy.Rule{{ResourceID: "budget-auth-a", Period: "month", LimitUSD: &limit}}}
	key := keypolicy.KeyID("key-a")
	if err = store.UpdatePolicy(key, store.Snapshot(resources).Revision, policy); err != nil {
		t.Fatal(err)
	}
	ctx := keypolicy.WithKeyID(keypolicy.WithStore(context.Background(), store), key)
	req := cliproxyexecutor.Request{Model: "budget-model", Payload: []byte(`{"model":"budget-model","max_tokens":100}`)}
	if _, err = manager.Execute(ctx, []string{"budget-test"}, req, cliproxyexecutor.Options{}); err != nil {
		t.Fatal(err)
	}
	if len(executor.calls) != 1 || executor.calls[0] != "budget-auth-a" {
		t.Fatalf("unauthorized selection: %v", executor.calls)
	}
	for _, budget := range store.Snapshot(resources).Budgets {
		if budget.KeyID == key && budget.ResourceID == "budget-auth-a" {
			if budget.UsedUSD != "0.010100" || budget.ReservedUSD != "0.000000" {
				t.Fatalf("settlement: %+v", budget)
			}
		}
	}
	// Pinning and a plugin scheduler cannot escape the same shared eligibility check.
	if _, err = manager.Execute(ctx, []string{"budget-test"}, req, cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.PinnedAuthMetadataKey: "budget-auth-b"}}); !errors.Is(err, keypolicy.ErrDenied) {
		t.Fatalf("pin: %v", err)
	}
	manager.SetPluginScheduler(&fakePluginScheduler{handled: true, resp: pluginapi.SchedulerPickResponse{Handled: true, AuthID: "budget-auth-b"}})
	_, _ = manager.Execute(ctx, []string{"budget-test"}, req, cliproxyexecutor.Options{})
	for _, id := range executor.calls {
		if id != "budget-auth-a" {
			t.Fatalf("scheduler bypass: %v", executor.calls)
		}
	}
	manager.SetPluginScheduler(nil)
	stream, err := manager.ExecuteStream(ctx, []string{"budget-test"}, req, cliproxyexecutor.Options{Stream: true})
	if err != nil {
		t.Fatal(err)
	}
	for range stream.Chunks {
	}
	limit = "0"
	policy.Rules[0].LimitUSD = &limit
	if err = store.UpdatePolicy(key, store.Snapshot(resources).Revision, policy); err != nil {
		t.Fatal(err)
	}
	before := len(executor.calls)
	if _, err = manager.Execute(ctx, []string{"budget-test"}, req, cliproxyexecutor.Options{}); !errors.Is(err, keypolicy.ErrBudget) {
		t.Fatalf("zero budget: %v", err)
	}
	if len(executor.calls) != before {
		t.Fatal("zero budget reached executor")
	}
	// Another key may still use the same account; exhaustion is not account cooldown.
	other := keypolicy.WithKeyID(ctx, keypolicy.KeyID("key-b"))
	if _, err = manager.Execute(other, []string{"budget-test"}, req, cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.PinnedAuthMetadataKey: "budget-auth-a"}}); err != nil {
		t.Fatalf("other key affected: %v", err)
	}
	reopened, err := keypolicy.NewStore(path, []string{"key-a", "key-b"})
	if err != nil {
		t.Fatal(err)
	}
	reopened.SetResources(resources)
	if err = reopened.CheckBudget(key, "budget-auth-a", time.Now()); !errors.Is(err, keypolicy.ErrBudget) {
		t.Fatalf("restart lost budget: %v", err)
	}
}
