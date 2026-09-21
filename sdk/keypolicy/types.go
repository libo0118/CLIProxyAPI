// Package keypolicy enforces per-key resource access and durable soft spending caps.
package keypolicy

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"time"
)

var (
	ErrDenied      = errors.New("key resource access denied")
	ErrBudget      = errors.New("key resource budget exhausted")
	ErrStale       = errors.New("stale policy revision")
	ErrInvalid     = errors.New("invalid key policy data")
	ErrUnavailable = errors.New("key budget data unavailable")
)

type Rule struct {
	ResourceID string  `json:"resource_id"`
	Period     string  `json:"period"`
	LimitUSD   *string `json:"limit_usd"`
}
type Policy struct {
	Label    string `json:"label"`
	AllowAll bool   `json:"allow_all"`
	Rules    []Rule `json:"rules"`
}
type Key struct {
	KeyID      string `json:"key_id"`
	KeyPreview string `json:"key_preview"`
	Policy
	Active bool `json:"-"`
}
type Resource struct {
	ResourceID string   `json:"resource_id"`
	Label      string   `json:"label"`
	FileName   string   `json:"file_name,omitempty"`
	Provider   string   `json:"provider"`
	Kind       string   `json:"kind"`
	Disabled   bool     `json:"disabled"`
	Models     []string `json:"models"`
}
type Price struct {
	Model                string `json:"model"`
	Unavailable          bool   `json:"unavailable,omitempty"`
	InputPerMillion      string `json:"input_per_million"`
	OutputPerMillion     string `json:"output_per_million"`
	CacheReadPerMillion  string `json:"cache_read_per_million"`
	CacheWritePerMillion string `json:"cache_write_per_million"`
}
type Cycle struct {
	CycleID    string    `json:"cycle_id,omitempty"`
	ResourceID string    `json:"resource_id"`
	Period     string    `json:"period"`
	StartsAt   time.Time `json:"starts_at"`
	ResetAt    time.Time `json:"reset_at"`
	ObservedAt time.Time `json:"observed_at"`
}
type Tokens struct {
	Input      int64 `json:"input"`
	Output     int64 `json:"output"`
	CacheRead  int64 `json:"cache_read"`
	CacheWrite int64 `json:"cache_write"`
}
type Reservation struct {
	ID         string    `json:"id"`
	KeyID      string    `json:"key_id"`
	ResourceID string    `json:"resource_id"`
	Model      string    `json:"model"`
	StartedAt  time.Time `json:"started_at"`
	Price      *Price    `json:"price"`
	Reserved   int64     `json:"reserved_microusd"`
	Used       int64     `json:"used_microusd"`
	Actual     Tokens    `json:"actual"`
	State      string    `json:"state"`
}
type Budget struct {
	KeyID            string    `json:"key_id"`
	ResourceID       string    `json:"resource_id"`
	Period           string    `json:"period"`
	LimitUSD         *string   `json:"limit_usd"`
	UsedUSD          string    `json:"used_usd"`
	ReservedUSD      string    `json:"reserved_usd"`
	RemainingUSD     *string   `json:"remaining_usd"`
	CycleStart       time.Time `json:"cycle_start"`
	ResetAt          time.Time `json:"reset_at"`
	Status           string    `json:"status"`
	UnpricedRequests int64     `json:"unpriced_requests"`
}
type Report struct {
	Revision        uint64     `json:"revision"`
	Keys            []Key      `json:"keys"`
	Resources       []Resource `json:"resources"`
	Budgets         []Budget   `json:"budgets"`
	PricesUpdatedAt time.Time  `json:"prices_updated_at"`
}

func KeyID(raw string) string { sum := sha256.Sum256([]byte(raw)); return hex.EncodeToString(sum[:]) }

func keyPreview(raw string) string {
	if len(raw) >= 12 {
		return "…" + raw[len(raw)-4:] + " · " + KeyID(raw)[:6]
	}
	return "…" + KeyID(raw)[:8]
}

type contextKey uint8

func WithKeyID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKey(0), id)
}
func KeyIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	v, _ := ctx.Value(contextKey(0)).(string)
	return v
}
func WithReservation(ctx context.Context, r *Reservation) context.Context {
	return context.WithValue(ctx, contextKey(1), r)
}
func ReservationFromContext(ctx context.Context) *Reservation {
	if ctx == nil {
		return nil
	}
	v, _ := ctx.Value(contextKey(1)).(*Reservation)
	return v
}

// ParseUSD converts an unsigned decimal dollar amount to exact microUSD.
func ParseUSD(s string) (int64, error) {
	parts := strings.Split(s, ".")
	if len(parts) > 2 || len(parts[0]) == 0 {
		return 0, ErrInvalid
	}
	for _, p := range parts {
		if p == "" {
			return 0, ErrInvalid
		}
		for _, c := range p {
			if c < '0' || c > '9' {
				return 0, ErrInvalid
			}
		}
	}
	frac := ""
	if len(parts) == 2 {
		frac = parts[1]
	}
	if len(frac) > 6 {
		return 0, ErrInvalid
	}
	v, ok := new(big.Int).SetString(parts[0]+frac+strings.Repeat("0", 6-len(frac)), 10)
	if !ok || !v.IsInt64() {
		return 0, ErrInvalid
	}
	return v.Int64(), nil
}
func money(v int64) string { return fmt.Sprintf("%d.%06d", v/1000000, v%1000000) }
func validTokens(t Tokens) bool {
	return t.Input >= 0 && t.Output >= 0 && t.CacheRead >= 0 && t.CacheWrite >= 0 && t.CacheRead <= t.Input && t.CacheWrite <= t.Input-t.CacheRead
}
func cost(p Price, t Tokens) (int64, error) {
	if !validTokens(t) {
		return 0, ErrInvalid
	}
	rates := []string{p.InputPerMillion, p.OutputPerMillion, p.CacheReadPerMillion, p.CacheWritePerMillion}
	counts := []int64{t.Input - t.CacheRead - t.CacheWrite, t.Output, t.CacheRead, t.CacheWrite}
	total := new(big.Int)
	for i, s := range rates {
		v, e := ParseUSD(s)
		if e != nil {
			return 0, e
		}
		total.Add(total, new(big.Int).Mul(big.NewInt(v), big.NewInt(counts[i])))
	}
	// Round upward once, only at the final microUSD accounting boundary.
	total.Add(total, big.NewInt(999999))
	total.Div(total, big.NewInt(1000000))
	if !total.IsInt64() {
		return 0, ErrInvalid
	}
	return total.Int64(), nil
}
func validPeriod(p string) bool {
	switch p {
	case "day", "week", "month", "codex_primary", "codex_weekly":
		return true
	}
	return false
}
func validatePolicy(p Policy) error {
	seen := map[string]bool{}
	for _, r := range p.Rules {
		if r.ResourceID == "" || seen[r.ResourceID] || !validPeriod(r.Period) {
			return ErrInvalid
		}
		seen[r.ResourceID] = true
		if r.LimitUSD != nil {
			if _, e := ParseUSD(*r.LimitUSD); e != nil {
				return e
			}
		}
	}
	return nil
}
