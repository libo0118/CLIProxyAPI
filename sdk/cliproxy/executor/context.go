package executor

import (
	"context"
	"sync/atomic"
)

type downstreamWebsocketContextKey struct{}
type requireUpstreamWebsocketContextKey struct{}
type upstreamAttemptTrackerContextKey struct{}
type websocketHTTPFallbackContextKey struct{}

type websocketHTTPFallback struct {
	payload    []byte
	onFallback func()
}

// WithWebsocketHTTPFallback supplies a complete transcript for a pre-send transport switch.
// The payload and callback belong to this execution attempt, never to another connection.
func WithWebsocketHTTPFallback(ctx context.Context, payload []byte, onFallback func()) context.Context {
	return context.WithValue(ctx, websocketHTTPFallbackContextKey{}, &websocketHTTPFallback{payload: payload, onFallback: onFallback})
}

// WebsocketHTTPFallbackPayload returns the canonical request retained by the downstream handler.
func WebsocketHTTPFallbackPayload(ctx context.Context) []byte {
	if ctx != nil {
		if fallback, ok := ctx.Value(websocketHTTPFallbackContextKey{}).(*websocketHTTPFallback); ok {
			return fallback.payload
		}
	}
	return nil
}

// MarkWebsocketHTTPFallback tells the handler to retain HTTP replay state for the next turn.
func MarkWebsocketHTTPFallback(ctx context.Context) {
	if ctx != nil {
		if fallback, ok := ctx.Value(websocketHTTPFallbackContextKey{}).(*websocketHTTPFallback); ok && fallback.onFallback != nil {
			fallback.onFallback()
		}
	}
}

type upstreamAttemptTracker struct {
	attempted atomic.Bool
}

// WithDownstreamWebsocket marks the current request as coming from a downstream websocket connection.
func WithDownstreamWebsocket(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, downstreamWebsocketContextKey{}, true)
}

// DownstreamWebsocket reports whether the current request originates from a downstream websocket connection.
func DownstreamWebsocket(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw := ctx.Value(downstreamWebsocketContextKey{})
	enabled, ok := raw.(bool)
	return ok && enabled
}

// WithRequiredUpstreamWebsocket marks a request whose incremental context is valid only on the current upstream websocket.
func WithRequiredUpstreamWebsocket(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, requireUpstreamWebsocketContextKey{}, true)
}

// RequiredUpstreamWebsocket reports whether falling back to an HTTP upstream would lose request context.
func RequiredUpstreamWebsocket(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	raw := ctx.Value(requireUpstreamWebsocketContextKey{})
	enabled, ok := raw.(bool)
	return ok && enabled
}

// WithUpstreamAttemptTracker installs a fresh tracker for one provider execution attempt.
func WithUpstreamAttemptTracker(ctx context.Context) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, upstreamAttemptTrackerContextKey{}, &upstreamAttemptTracker{})
}

// MarkUpstreamAttempt records that the provider execution reached an upstream transport boundary.
func MarkUpstreamAttempt(ctx context.Context) {
	if ctx == nil {
		return
	}
	tracker, ok := ctx.Value(upstreamAttemptTrackerContextKey{}).(*upstreamAttemptTracker)
	if !ok || tracker == nil {
		return
	}
	tracker.attempted.Store(true)
}

// UpstreamAttempted reports whether the tracked provider execution reached an upstream transport boundary.
func UpstreamAttempted(ctx context.Context) bool {
	if ctx == nil {
		return false
	}
	tracker, ok := ctx.Value(upstreamAttemptTrackerContextKey{}).(*upstreamAttemptTracker)
	return ok && tracker != nil && tracker.attempted.Load()
}
