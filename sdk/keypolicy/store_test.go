package keypolicy

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var testNow = time.Date(2030, 6, 17, 12, 0, 0, 0, time.UTC)
var testResources = []Resource{{ResourceID: "r1"}, {ResourceID: "r2"}}
var testPrice = Price{Model: "m", InputPerMillion: "1", OutputPerMillion: "2", CacheReadPerMillion: "0.1", CacheWritePerMillion: "1.25"}

func ptr(s string) *string { return &s }
func setup(t *testing.T) *Store {
	t.Helper()
	s, e := NewStore(filepath.Join(t.TempDir(), "policy.json"), []string{"old", "other"})
	if e != nil {
		t.Fatal(e)
	}
	s.SetResources(testResources)
	if e = s.Sync([]Price{testPrice}, nil); e != nil {
		t.Fatal(e)
	}
	return s
}
func policy(t *testing.T, s *Store, key string, p Policy) {
	t.Helper()
	if e := s.UpdatePolicy(KeyID(key), s.SnapshotAt(testResources, testNow).Revision, p); e != nil {
		t.Fatal(e)
	}
}
func TestMigrationAuthorizationAndValidation(t *testing.T) {
	s := setup(t)
	if e := s.Authorize(KeyID("old"), "r1"); e != nil {
		t.Fatal(e)
	}
	reopened, e := NewStore(s.path, []string{"old", "new"})
	if e != nil {
		t.Fatal(e)
	}
	reopened.SetResources(testResources)
	if e = reopened.Authorize(KeyID("new"), "r1"); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
	if e = reopened.Authorize(KeyID("other"), "r1"); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
	if e = reopened.Authorize(KeyID("old"), "missing"); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
	policy(t, reopened, "old", Policy{Rules: []Rule{{ResourceID: "r1", Period: "day"}}})
	if e = reopened.Authorize(KeyID("old"), "r2"); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
	if e = reopened.UpdatePolicy(KeyID("old"), 0, Policy{}); !errors.Is(e, ErrStale) {
		t.Fatal(e)
	}
	for _, rules := range [][]Rule{{{ResourceID: "r1", Period: "bogus"}}, {{ResourceID: "missing", Period: "day"}}, {{ResourceID: "r1", Period: "day", LimitUSD: ptr("-1")}}, {{ResourceID: "r1", Period: "day"}, {ResourceID: "r1", Period: "week"}}} {
		if e = reopened.UpdatePolicy(KeyID("old"), reopened.data.Revision, Policy{Rules: rules}); !errors.Is(e, ErrInvalid) {
			t.Fatal(e)
		}
	}
	b, e := os.ReadFile(s.path)
	if e != nil {
		t.Fatal(e)
	}
	if strings.Contains(string(b), `"old"`) || strings.Contains(string(b), `"other"`) {
		t.Fatal("plaintext key persisted")
	}
}
func TestMoneyAndTokenPartitions(t *testing.T) {
	for _, s := range []string{"", "-1", "+1", "1e3", "1.0000001", ".5", "1.", "9223372036854.775808"} {
		if _, e := ParseUSD(s); e == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	if v, e := ParseUSD("9223372036854.775807"); e != nil || v != 9223372036854775807 {
		t.Fatal(v, e)
	}
	if v, e := cost(testPrice, Tokens{Input: 1000000, CacheRead: 200000, CacheWrite: 100000, Output: 100000}); e != nil || v != 1045000 {
		t.Fatal(v, e)
	}
	if _, e := cost(testPrice, Tokens{Input: 1, CacheRead: 1, CacheWrite: 1}); e == nil {
		t.Fatal("overlapping cache tokens")
	}
	if v, e := cost(testPrice, Tokens{CacheRead: 1, Input: 1}); e != nil || v != 1 {
		t.Fatal(v, e)
	}
}

func TestPriceRemovalAndObservedManualReset(t *testing.T) {
	s := setup(t)
	policy(t, s, "old", Policy{Rules: []Rule{{ResourceID: "r1", Period: "codex_weekly", LimitUSD: ptr("1")}}})
	cycle := Cycle{CycleID: "before-reset", ResourceID: "r1", Period: "codex_weekly", StartsAt: testNow.Add(-time.Hour), ResetAt: testNow.Add(7 * 24 * time.Hour), ObservedAt: testNow}
	if err := s.Sync(nil, []Cycle{cycle}); err != nil {
		t.Fatal(err)
	}
	r, err := s.Begin(KeyID("old"), "r1", "m", Tokens{Input: 1000000}, testNow)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.Settle(r.ID, Tokens{Input: 1000000}, testNow); err != nil {
		t.Fatal(err)
	}
	cycle.CycleID = "after-reset"
	cycle.StartsAt = testNow.Add(time.Minute)
	cycle.ObservedAt = cycle.StartsAt
	cycle.ResetAt = cycle.ResetAt.Add(time.Hour)
	if err = s.Sync(nil, []Cycle{cycle}); err != nil {
		t.Fatal(err)
	}
	if err = s.CheckBudget(KeyID("old"), "r1", cycle.StartsAt); err != nil {
		t.Fatalf("confirmed reset did not open new cycle: %v", err)
	}
	if err = s.Sync([]Price{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Begin(KeyID("old"), "r1", "m", Tokens{Input: 1}, cycle.StartsAt); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("deleted price remained usable: %v", err)
	}
}
func TestUnlimitedZeroAndMissingData(t *testing.T) {
	s := setup(t)
	r, e := s.Begin(KeyID("old"), "r1", "unpriced", Tokens{Input: 1}, testNow)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Settle(r.ID, Tokens{Input: 1}, testNow); e != nil {
		t.Fatal(e)
	}
	policy(t, s, "old", Policy{AllowAll: true, Rules: []Rule{{ResourceID: "r1", Period: "month", LimitUSD: ptr("5")}, {ResourceID: "r2", Period: "day", LimitUSD: ptr("0")}}})
	if _, e = s.Begin(KeyID("old"), "r1", "m", Tokens{}, testNow); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if _, e = s.Begin(KeyID("old"), "r2", "m", Tokens{}, testNow); !errors.Is(e, ErrBudget) {
		t.Fatal(e)
	}
	policy(t, s, "other", Policy{Rules: []Rule{{ResourceID: "r1", Period: "codex_weekly", LimitUSD: ptr("5")}}})
	if _, e = s.Begin(KeyID("other"), "r1", "m", Tokens{Input: 1}, testNow); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}
func TestConcurrentReservationRestartAndReplay(t *testing.T) {
	s := setup(t)
	policy(t, s, "old", Policy{AllowAll: true, Rules: []Rule{{ResourceID: "r1", Period: "day", LimitUSD: ptr("1")}}})
	var admitted atomic.Int64
	var mu sync.Mutex
	ids := []string{}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, e := s.Begin(KeyID("old"), "r1", "m", Tokens{Input: 100000}, testNow)
			if e == nil {
				admitted.Add(1)
				mu.Lock()
				ids = append(ids, r.ID)
				mu.Unlock()
			} else if !errors.Is(e, ErrBudget) {
				t.Errorf("admission: %v", e)
			}
		}()
	}
	wg.Wait()
	if admitted.Load() != 10 {
		t.Fatal(admitted.Load())
	}
	if _, e := s.Begin(KeyID("other"), "r1", "m", Tokens{Input: 1000000}, testNow); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Begin(KeyID("old"), "r2", "m", Tokens{Input: 1000000}, testNow); e != nil {
		t.Fatal(e)
	}
	reopened, e := NewStore(s.path, []string{"old", "other"})
	if e != nil {
		t.Fatal(e)
	}
	reopened.SetResources(testResources)
	if e = reopened.CheckBudget(KeyID("old"), "r1", testNow); !errors.Is(e, ErrBudget) {
		t.Fatal(e)
	}
	if e = reopened.FinishUnknown(ids[0], false); e != nil {
		t.Fatal(e)
	}
	if e = reopened.Settle(ids[0], Tokens{Input: 200000}, testNow); e != nil {
		t.Fatal(e)
	}
	if e = reopened.Settle(ids[0], Tokens{Input: 999999}, testNow); e != nil {
		t.Fatal(e)
	}
	if e = reopened.FinishUnknown(ids[0], true); e != nil {
		t.Fatal(e)
	}
	for _, b := range reopened.SnapshotAt(testResources, testNow).Budgets {
		if b.KeyID == KeyID("old") && b.ResourceID == "r1" {
			if b.UsedUSD != "0.200000" || b.ReservedUSD != "0.900000" {
				t.Fatal(b)
			}
		}
	}
	if e = reopened.FinishUnknown(ids[1], true); e != nil {
		t.Fatal(e)
	}
	if len(reopened.data.Pending) != 10 {
		t.Fatal(len(reopened.data.Pending))
	}
}
func TestObservedCyclesAndFrozenPrices(t *testing.T) {
	s := setup(t)
	policy(t, s, "old", Policy{Rules: []Rule{{ResourceID: "r1", Period: "codex_weekly", LimitUSD: ptr("1")}}})
	cycle := Cycle{ResourceID: "r1", Period: "codex_weekly", StartsAt: testNow.Add(-time.Hour), ResetAt: testNow.Add(time.Hour), ObservedAt: testNow}
	if e := s.Sync(nil, []Cycle{cycle}); e != nil {
		t.Fatal(e)
	}
	rev := s.data.Revision
	r, e := s.Begin(KeyID("old"), "r1", "m", Tokens{Input: 500000}, testNow)
	if e != nil {
		t.Fatal(e)
	}
	changed := testPrice
	changed.InputPerMillion = "100"
	cycle.ObservedAt = testNow.Add(time.Minute)
	if e = s.Sync([]Price{changed}, []Cycle{cycle}); e != nil {
		t.Fatal(e)
	}
	if e = s.Settle(r.ID, Tokens{Input: 500000}, testNow); e != nil {
		t.Fatal(e)
	}
	if s.data.Revision != rev {
		t.Fatal("usage/sync changed policy revision")
	}
	b := s.SnapshotAt(testResources, testNow).Budgets[0]
	if b.UsedUSD != "0.500000" {
		t.Fatal(b)
	}
	if e = s.CheckBudget(KeyID("old"), "r1", cycle.ResetAt); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	cycle.StartsAt = cycle.ResetAt
	cycle.ResetAt = cycle.StartsAt.Add(time.Hour)
	cycle.ObservedAt = cycle.StartsAt
	if e = s.Sync(nil, []Cycle{cycle}); e != nil {
		t.Fatal(e)
	}
	if e = s.CheckBudget(KeyID("old"), "r1", cycle.StartsAt); e != nil {
		t.Fatal(e)
	}
}
func TestPolicyChangePreservesSpendAndRevocation(t *testing.T) {
	s := setup(t)
	r, e := s.Begin(KeyID("old"), "r1", "m", Tokens{Input: 1000000}, testNow)
	if e != nil {
		t.Fatal(e)
	}
	if e = s.Settle(r.ID, Tokens{Input: 1000000}, testNow); e != nil {
		t.Fatal(e)
	}
	policy(t, s, "old", Policy{Rules: []Rule{{ResourceID: "r1", Period: "month", LimitUSD: ptr("1")}}})
	if e = s.CheckBudget(KeyID("old"), "r1", testNow); !errors.Is(e, ErrBudget) {
		t.Fatal(e)
	}
	if e = s.UpdatePolicy(KeyID("old"), s.data.Revision, Policy{Rules: []Rule{{ResourceID: "r1", Period: "day", LimitUSD: ptr("1")}}}); !errors.Is(e, ErrInvalid) {
		t.Fatal(e)
	}
	policy(t, s, "old", Policy{})
	if e = s.Authorize(KeyID("old"), "r1"); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
}
func TestCorruptAndPersistenceFailure(t *testing.T) {
	p := filepath.Join(t.TempDir(), "broken.json")
	if e := os.WriteFile(p, []byte(`{"version":1}`), 0600); e != nil {
		t.Fatal(e)
	}
	if _, e := NewStore(p, []string{"old"}); e == nil {
		t.Fatal("corrupt state accepted")
	}
	s := setup(t)
	before := s.data.Sequence
	// Replacing the destination with a directory forces rename failure even as root.
	if e := os.Remove(s.path); e != nil {
		t.Fatal(e)
	}
	if e := os.Mkdir(s.path, 0700); e != nil {
		t.Fatal(e)
	}
	if _, e := s.Begin(KeyID("old"), "r1", "m", Tokens{Input: 1}, testNow); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
	if s.data.Sequence != before || len(s.data.Pending) != 0 {
		t.Fatal("failed save published mutation")
	}
	if e := s.Authorize(KeyID("old"), "r1"); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}
func TestDeletedLedgerAndDeletedKeyFailClosed(t *testing.T) {
	s := setup(t)
	if e := s.SyncKeys([]string{"other"}); e != nil {
		t.Fatal(e)
	}
	if e := s.Authorize(KeyID("old"), "r1"); !errors.Is(e, ErrDenied) {
		t.Fatal(e)
	}
	if e := os.Remove(s.path); e != nil {
		t.Fatal(e)
	}
	if _, e := NewStore(s.path, []string{"old"}); !errors.Is(e, ErrUnavailable) {
		t.Fatal(e)
	}
}
