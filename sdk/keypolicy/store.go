package keypolicy

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
)

type account struct {
	KeyID      string    `json:"key_id"`
	ResourceID string    `json:"resource_id"`
	Period     string    `json:"period"`
	Start      time.Time `json:"start"`
	End        time.Time `json:"end"`
	Used       int64     `json:"used"`
	Reserved   int64     `json:"reserved"`
	Unpriced   int64     `json:"unpriced"`
}
type pending struct {
	Reservation
	Account string `json:"account"`
}
type state struct {
	Version         int                `json:"version"`
	Revision        uint64             `json:"revision"`
	Sequence        uint64             `json:"sequence"`
	Keys            map[string]Key     `json:"keys"`
	Prices          map[string]Price   `json:"prices"`
	Cycles          map[string]Cycle   `json:"cycles"`
	Accounts        map[string]account `json:"accounts"`
	Pending         map[string]pending `json:"pending"`
	PricesUpdatedAt time.Time          `json:"prices_updated_at"`
}

// Store has a single process owner. Do not open the same file in two processes.
// ponytail: mutex and full snapshots suit one server; use transactional storage
// when historical window counts or concurrent admission throughput become large.
type Store struct {
	mu        sync.Mutex
	path      string
	data      state
	resources map[string]Resource
	active    map[string]bool
	failed    error
}

func NewStore(path string, initialKeys []string) (*Store, error) {
	if path == "" {
		return nil, ErrInvalid
	}
	s := &Store{path: path, resources: map[string]Resource{}, active: map[string]bool{}}
	b, e := os.ReadFile(path)
	first := errors.Is(e, os.ErrNotExist)
	if e != nil && !first {
		return nil, fmt.Errorf("read key policy: %w", e)
	}
	if first {
		if _, err := os.Stat(path + ".initialized"); err == nil {
			return nil, fmt.Errorf("%w: initialized ledger is missing", ErrUnavailable)
		} else if !errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
	}
	if first {
		s.data = state{Version: 1, Revision: 1, Keys: map[string]Key{}, Prices: map[string]Price{}, Cycles: map[string]Cycle{}, Accounts: map[string]account{}, Pending: map[string]pending{}}
	} else {
		if e = json.Unmarshal(b, &s.data); e != nil {
			return nil, fmt.Errorf("decode key policy: %w", e)
		}
		if e = s.validate(); e != nil {
			return nil, e
		}
	}
	for _, raw := range initialKeys {
		if raw == "" {
			return nil, ErrInvalid
		}
		id := KeyID(raw)
		s.active[id] = true
		if _, ok := s.data.Keys[id]; !ok {
			s.data.Keys[id] = Key{KeyID: id, KeyPreview: keyPreview(raw), Policy: Policy{AllowAll: first, Rules: []Rule{}}}
		}
	}
	if e = os.MkdirAll(filepath.Dir(path), 0700); e != nil {
		return nil, e
	}
	marker, err := os.OpenFile(path+".initialized", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err == nil {
		err = marker.Sync()
		ce := marker.Close()
		if err == nil {
			err = ce
		}
	} else if errors.Is(err, os.ErrExist) {
		err = nil
	}
	if err != nil {
		return nil, err
	}
	if e = s.save(s.data); e != nil {
		return nil, e
	}
	return s, nil
}
func (s *Store) validate() error {
	d := s.data
	if d.Version != 1 || d.Revision == 0 || d.Keys == nil || d.Prices == nil || d.Cycles == nil || d.Accounts == nil || d.Pending == nil {
		return ErrInvalid
	}
	for id, k := range d.Keys {
		if len(id) != 64 || k.KeyID != id || validatePolicy(k.Policy) != nil {
			return ErrInvalid
		}
	}
	for model, p := range d.Prices {
		if model == "" || model != p.Model {
			return ErrInvalid
		}
		if _, e := cost(p, Tokens{}); e != nil {
			return e
		}
	}
	for id, c := range d.Cycles {
		if id != cycleID(c.ResourceID, c.Period) || validateCycle(c) != nil {
			return ErrInvalid
		}
	}
	sums := map[string]int64{}
	for id, r := range d.Pending {
		n, e := strconv.ParseUint(id, 10, 64)
		if e != nil || n == 0 || n > d.Sequence || r.ID != id || r.Reserved < 0 || (r.State != "pending" && r.State != "unknown") {
			return ErrInvalid
		}
		a, ok := d.Accounts[r.Account]
		if !ok || a.KeyID != r.KeyID || a.ResourceID != r.ResourceID {
			return ErrInvalid
		}
		if sums[r.Account] > math.MaxInt64-r.Reserved {
			return ErrInvalid
		}
		sums[r.Account] += r.Reserved
		if r.Price != nil {
			if _, e = cost(*r.Price, Tokens{}); e != nil {
				return e
			}
		}
	}
	for id, a := range d.Accounts {
		if a.Used < 0 || a.Reserved < 0 || a.Unpriced < 0 || a.Reserved != sums[id] || !validPeriod(a.Period) {
			return ErrInvalid
		}
		if _, ok := d.Keys[a.KeyID]; !ok {
			return ErrInvalid
		}
	}
	return nil
}
func (s *Store) save(d state) error {
	b, e := json.Marshal(d)
	if e != nil {
		return e
	}
	dir := filepath.Dir(s.path)
	if e = os.MkdirAll(dir, 0700); e != nil {
		return e
	}
	f, e := os.CreateTemp(dir, ".keypolicy-*")
	if e != nil {
		return e
	}
	name := f.Name()
	defer os.Remove(name)
	if e = f.Chmod(0600); e == nil {
		_, e = f.Write(b)
	}
	if e == nil {
		e = f.Sync()
	}
	closeErr := f.Close()
	if e == nil {
		e = closeErr
	}
	if e != nil {
		return e
	}
	if e = os.Rename(name, s.path); e != nil {
		return e
	}
	if runtime.GOOS != "windows" {
		df, err := os.Open(dir)
		if err != nil {
			return err
		}
		err = df.Sync()
		ce := df.Close()
		if err != nil {
			return err
		}
		if ce != nil {
			return ce
		}
	}
	return nil
}
func (s *Store) change(fn func(*state) error) error {
	if s.failed != nil {
		return fmt.Errorf("%w: persistence failed", ErrUnavailable)
	}
	b, _ := json.Marshal(s.data)
	var next state
	if e := json.Unmarshal(b, &next); e != nil {
		return e
	}
	if e := fn(&next); e != nil {
		return e
	}
	b, e := json.Marshal(next)
	if e != nil {
		return e
	}
	var isolated state
	if e = json.Unmarshal(b, &isolated); e != nil {
		return e
	}
	if e = s.save(isolated); e != nil {
		s.failed = e
		return fmt.Errorf("%w: persist state: %v", ErrUnavailable, e)
	}
	s.data = isolated
	return nil
}
func (s *Store) SyncKeys(keys []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	active := map[string]bool{}
	previews := map[string]string{}
	for _, raw := range keys {
		if raw == "" {
			return ErrInvalid
		}
		active[KeyID(raw)] = true
		previews[KeyID(raw)] = keyPreview(raw)
	}
	e := s.change(func(d *state) error {
		changed := len(active) != len(s.active)
		for id := range active {
			if !s.active[id] {
				changed = true
			}
			if _, ok := d.Keys[id]; !ok {
				d.Keys[id] = Key{KeyID: id, KeyPreview: previews[id], Policy: Policy{Rules: []Rule{}}}
				changed = true
			}
		}
		if changed {
			d.Revision++
		}
		return nil
	})
	if e == nil {
		s.active = active
	}
	return e
}
func (s *Store) SetResources(resources []Resource) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setResources(resources)
}
func (s *Store) setResources(resources []Resource) {
	s.resources = map[string]Resource{}
	for _, r := range resources {
		r.Models = append([]string(nil), r.Models...)
		s.resources[r.ResourceID] = r
	}
}
func ruleFor(k Key, rid string) (Rule, bool) {
	for _, r := range k.Rules {
		if r.ResourceID == rid {
			return r, true
		}
	}
	return Rule{ResourceID: rid, Period: "month"}, k.AllowAll
}
func (s *Store) authorize(id, rid string) (Rule, error) {
	if s.failed != nil {
		return Rule{}, ErrUnavailable
	}
	k, ok := s.data.Keys[id]
	if !ok || !s.active[id] {
		return Rule{}, ErrDenied
	}
	r, ok := s.resources[rid]
	if !ok || r.Disabled {
		return Rule{}, ErrDenied
	}
	rule, ok := ruleFor(k, rid)
	if !ok {
		return Rule{}, ErrDenied
	}
	return rule, nil
}
func (s *Store) Authorize(id, rid string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, e := s.authorize(id, rid)
	return e
}
func (s *Store) UpdatePolicy(id string, revision uint64, p Policy) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e := validatePolicy(p); e != nil {
		return e
	}
	for _, r := range p.Rules {
		if _, ok := s.resources[r.ResourceID]; !ok {
			// Existing tombstones remain visible/editable, but cannot execute.
			known := false
			for _, oldRule := range s.data.Keys[id].Rules {
				if oldRule.ResourceID == r.ResourceID {
					known = true
					break
				}
			}
			if !known {
				return ErrInvalid
			}
		}
	}
	return s.change(func(d *state) error {
		if revision != d.Revision {
			return ErrStale
		}
		k, ok := d.Keys[id]
		if !ok || !s.active[id] {
			return ErrInvalid
		}
		// Revocation is always allowed, but overlapping spending cannot be reset by
		// changing periods (including an expired day inside the new monthly window).
		now := time.Now().UTC()
		for _, a := range d.Accounts {
			if a.KeyID != id {
				continue
			}
			next, allowed := ruleFor(Key{Policy: p}, a.ResourceID)
			if !allowed || next.Period == a.Period || (a.Used == 0 && a.Reserved == 0 && a.Unpriced == 0) {
				continue
			}
			start, end, err := window(*d, next, now)
			oldEnd := a.End
			if c, ok := d.Cycles[cycleID(a.ResourceID, a.Period)]; ok && c.StartsAt.Equal(a.Start) && c.ResetAt.After(oldEnd) {
				oldEnd = c.ResetAt
			}
			if err != nil || oldEnd.IsZero() || now.Before(oldEnd) || a.Reserved > 0 || (a.Start.Before(end) && oldEnd.After(start)) {
				return fmt.Errorf("%w: overlapping spending window cannot change period", ErrInvalid)
			}
		}
		k.Policy = p
		d.Keys[id] = k
		d.Revision++
		return nil
	})
}
func cycleID(r, p string) string { return r + "\x00" + p }
func validateCycle(c Cycle) error {
	if c.ResourceID == "" || (c.Period != "codex_primary" && c.Period != "codex_weekly") || c.StartsAt.IsZero() || !c.ResetAt.After(c.StartsAt) || c.ObservedAt.Before(c.StartsAt) || !c.ObservedAt.Before(c.ResetAt) {
		return ErrInvalid
	}
	return nil
}
func (s *Store) Sync(prices []Price, cycles []Cycle) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	for _, p := range prices {
		if p.Model == "" || seen[p.Model] {
			return ErrInvalid
		}
		seen[p.Model] = true
		if p.Unavailable {
			continue
		}
		if _, e := cost(p, Tokens{}); e != nil {
			return e
		}
	}
	seen = map[string]bool{}
	for _, c := range cycles {
		if validateCycle(c) != nil || seen[cycleID(c.ResourceID, c.Period)] {
			return ErrInvalid
		}
		if _, ok := s.resources[c.ResourceID]; !ok {
			return ErrInvalid
		}
		seen[cycleID(c.ResourceID, c.Period)] = true
	}
	return s.change(func(d *state) error {
		// A supplied price array is the complete saved Keeper price catalog.
		// Deleted prices must not remain usable for new finite-budget requests.
		if prices != nil {
			d.Prices = map[string]Price{}
		}
		for _, p := range prices {
			if p.Unavailable {
				delete(d.Prices, p.Model)
				continue
			}
			d.Prices[p.Model] = p
		}
		if prices != nil {
			d.PricesUpdatedAt = time.Now().UTC()
		}
		for _, c := range cycles {
			id := cycleID(c.ResourceID, c.Period)
			old, ok := d.Cycles[id]
			if ok && c.ObservedAt.Before(old.ObservedAt) {
				continue
			}
			if ok && c.StartsAt.Before(old.ResetAt) && !c.StartsAt.Equal(old.StartsAt) {
				// Keeper supplies stable observed cycle IDs. A confirmed manual reset
				// can start a new cycle before the old window would have expired.
				if c.CycleID == "" || old.CycleID == "" || c.CycleID == old.CycleID || !c.StartsAt.After(old.StartsAt) || !c.ResetAt.After(old.ResetAt) {
					return fmt.Errorf("%w: overlapping cycle", ErrInvalid)
				}
			}
			d.Cycles[id] = c
		}
		return nil
	})
}
func window(d state, r Rule, now time.Time) (time.Time, time.Time, error) {
	now = now.UTC()
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, time.UTC)
	switch r.Period {
	case "day":
		return start, start.AddDate(0, 0, 1), nil
	case "week":
		start = start.AddDate(0, 0, -(int(start.Weekday())+6)%7)
		return start, start.AddDate(0, 0, 7), nil
	case "month":
		start = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, time.UTC)
		return start, start.AddDate(0, 1, 0), nil
	default:
		c, ok := d.Cycles[cycleID(r.ResourceID, r.Period)]
		if !ok || now.Before(c.StartsAt) || !now.Before(c.ResetAt) {
			return time.Time{}, time.Time{}, ErrUnavailable
		}
		return c.StartsAt.UTC(), c.ResetAt.UTC(), nil
	}
}
func accountID(id string, r Rule, start time.Time) string {
	return id + "\x00" + r.ResourceID + "\x00" + r.Period + "\x00" + start.Format(time.RFC3339Nano)
}
func current(d state, id string, r Rule, now time.Time) (string, account, error) {
	start, end, e := window(d, r, now)
	if e != nil {
		if r.LimitUSD != nil {
			return "", account{}, e
		}
		start = time.Time{}
		end = time.Time{}
	}
	if r.LimitUSD != nil && !start.IsZero() {
		unknown := d.Accounts[accountID(id, r, time.Time{})]
		if unknown.Used > 0 || unknown.Reserved > 0 || unknown.Unpriced > 0 {
			return "", account{}, ErrUnavailable
		}
	}
	aid := accountID(id, r, start)
	a, ok := d.Accounts[aid]
	if !ok {
		a = account{KeyID: id, ResourceID: r.ResourceID, Period: r.Period, Start: start, End: end}
	} else {
		a.End = end
	}
	return aid, a, nil
}
func budgetCheck(a account, r Rule, estimate int64) error {
	if r.LimitUSD == nil {
		return nil
	}
	limit, e := ParseUSD(*r.LimitUSD)
	if e != nil {
		return e
	}
	if a.Unpriced > 0 {
		return ErrUnavailable
	}
	if limit == 0 || a.Used >= limit || a.Reserved >= limit-a.Used || estimate > limit-a.Used-a.Reserved {
		return ErrBudget
	}
	return nil
}
func (s *Store) CheckBudget(id, rid string, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	r, e := s.authorize(id, rid)
	if e != nil {
		return e
	}
	_, a, e := current(s.data, id, r, now)
	if e != nil {
		return e
	}
	return budgetCheck(a, r, 0)
}
func (s *Store) Begin(id, rid, model string, estimate Tokens, now time.Time) (*Reservation, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validTokens(estimate) {
		return nil, ErrInvalid
	}
	r, e := s.authorize(id, rid)
	if e != nil {
		return nil, e
	}
	aid, a, e := current(s.data, id, r, now)
	if e != nil {
		return nil, e
	}
	var price *Price
	amount := int64(0)
	if p, ok := s.data.Prices[model]; ok {
		amount, e = cost(p, estimate)
		if e != nil {
			return nil, e
		}
		price = &p
	} else if r.LimitUSD != nil {
		return nil, ErrUnavailable
	}
	if e = budgetCheck(a, r, amount); e != nil {
		return nil, e
	}
	if a.Reserved > math.MaxInt64-amount || s.data.Sequence == math.MaxUint64 {
		return nil, ErrInvalid
	}
	var result Reservation
	e = s.change(func(d *state) error {
		d.Sequence++
		result = Reservation{ID: strconv.FormatUint(d.Sequence, 10), KeyID: id, ResourceID: rid, Model: model, StartedAt: now.UTC(), Price: price, Reserved: amount, State: "pending"}
		a.Reserved += amount
		if price == nil {
			a.Unpriced++
		}
		d.Accounts[aid] = a
		d.Pending[result.ID] = pending{Reservation: result, Account: aid}
		return nil
	})
	if e != nil {
		return nil, e
	}
	return &result, nil
}
func (s *Store) finished(id string) error {
	n, e := strconv.ParseUint(id, 10, 64)
	if e != nil || n == 0 || n > s.data.Sequence {
		return ErrInvalid
	}
	return nil
}
func (s *Store) Settle(id string, actual Tokens, now time.Time) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !validTokens(actual) {
		return ErrInvalid
	}
	p, ok := s.data.Pending[id]
	if !ok {
		return s.finished(id)
	}
	amount := int64(0)
	var e error
	if p.Price != nil {
		amount, e = cost(*p.Price, actual)
		if e != nil {
			return e
		}
	}
	return s.change(func(d *state) error {
		a := d.Accounts[p.Account]
		if a.Used > math.MaxInt64-amount {
			return ErrInvalid
		}
		a.Reserved -= p.Reserved
		a.Used += amount
		d.Accounts[p.Account] = a
		delete(d.Pending, id)
		return nil
	})
}
func (s *Store) FinishUnknown(id string, releaseKnownUnused bool) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	p, ok := s.data.Pending[id]
	if !ok {
		return s.finished(id)
	}
	if !releaseKnownUnused && p.State == "unknown" {
		return nil
	}
	return s.change(func(d *state) error {
		if releaseKnownUnused {
			a := d.Accounts[p.Account]
			a.Reserved -= p.Reserved
			if p.Price == nil {
				a.Unpriced--
			}
			d.Accounts[p.Account] = a
			delete(d.Pending, id)
		} else {
			p.State = "unknown"
			d.Pending[id] = p
		}
		return nil
	})
}
func (s *Store) Snapshot(resources []Resource) Report { return s.SnapshotAt(resources, time.Now()) }

// SnapshotAt supports deterministic window reporting and tests.
func (s *Store) SnapshotAt(resources []Resource, now time.Time) Report {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setResources(resources)
	out := Report{Revision: s.data.Revision, Keys: []Key{}, Resources: []Resource{}, Budgets: []Budget{}, PricesUpdatedAt: s.data.PricesUpdatedAt}
	for _, resource := range s.resources {
		out.Resources = append(out.Resources, resource)
	}
	sort.Slice(out.Resources, func(i, j int) bool { return out.Resources[i].ResourceID < out.Resources[j].ResourceID })
	for id, k := range s.data.Keys {
		if !s.active[id] {
			continue
		}
		out.Keys = append(out.Keys, k)
		for rid := range s.resources {
			r, ok := ruleFor(k, rid)
			if !ok {
				continue
			}
			_, a, e := current(s.data, id, r, now)
			b := Budget{KeyID: id, ResourceID: rid, Period: r.Period, LimitUSD: r.LimitUSD, UsedUSD: money(a.Used), ReservedUSD: money(a.Reserved), CycleStart: a.Start, ResetAt: a.End, Status: "ready", UnpricedRequests: a.Unpriced}
			if e != nil || s.failed != nil {
				b.Status = "unavailable"
			}
			if r.LimitUSD != nil {
				limit, _ := ParseUSD(*r.LimitUSD)
				remaining := limit - a.Used
				if a.Reserved >= remaining {
					remaining = 0
				} else {
					remaining -= a.Reserved
				}
				v := money(remaining)
				b.RemainingUSD = &v
				if budgetCheck(a, r, 0) == ErrBudget {
					b.Status = "exhausted"
				}
				if a.Unpriced > 0 {
					b.Status = "unpriced"
				}
			}
			out.Budgets = append(out.Budgets, b)
		}
	}
	sort.Slice(out.Keys, func(i, j int) bool { return out.Keys[i].KeyID < out.Keys[j].KeyID })
	sort.Slice(out.Budgets, func(i, j int) bool {
		a, b := out.Budgets[i], out.Budgets[j]
		if a.KeyID != b.KeyID {
			return a.KeyID < b.KeyID
		}
		return a.ResourceID < b.ResourceID
	})
	b, _ := json.Marshal(out)
	var clone Report
	_ = json.Unmarshal(b, &clone)
	return clone
}
