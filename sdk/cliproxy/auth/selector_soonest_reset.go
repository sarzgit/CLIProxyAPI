package auth

import (
	"context"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

// SoonestResetFirstSelector prefers the available credential whose longest quota
// window (the weekly window on Claude and Codex subscriptions) resets soonest.
// Quota left in a window when it resets is lost, so draining the account that
// resets first wastes the least.
//
// Reset times come from the quota signals recorded on each credential's most
// recent upstream response. A credential with no known future reset sorts first
// so one request can observe its window. Credentials whose resets fall in the
// same minute rotate in round-robin order. Availability, cooldowns, disabled
// credentials and priority tiers are handled exactly like the other selectors.
type SoonestResetFirstSelector struct {
	mu         sync.Mutex
	lastPicked map[string]string
	nowFunc    func() time.Time
}

// Pick selects the available auth whose quota window resets soonest.
func (s *SoonestResetFirstSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	_ = opts
	now := s.now()
	available, err := getSelectorAvailableAuths(ctx, auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	available = preferCodexWebsocketAuths(ctx, provider, available)
	if len(available) == 1 {
		return available[0], nil
	}

	// available is ID-sorted, and filtering keeps that order, which successorIndex needs.
	soonest := soonestResetAuths(available, now)

	key := provider + ":" + canonicalModelLookupKey(model)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.lastPicked == nil || (len(s.lastPicked) >= 4096 && s.lastPicked[key] == "") {
		s.lastPicked = make(map[string]string)
	}
	picked := soonest[successorIndex(soonest, s.lastPicked[key])]
	s.lastPicked[key] = picked.ID
	return picked, nil
}

func (s *SoonestResetFirstSelector) now() time.Time {
	if s.nowFunc != nil {
		return s.nowFunc()
	}
	return time.Now()
}

// soonestResetAuths returns the candidates that share the earliest reset minute,
// preserving input order. Unknown resets rank before every known reset.
func soonestResetAuths(auths []*Auth, now time.Time) []*Auth {
	var best []*Auth
	var bestReset time.Time
	bestKnown := true
	for _, auth := range auths {
		reset, known := quotaWindowResetAt(auth, now)
		if known {
			reset = reset.Truncate(time.Minute)
		}
		switch {
		case best == nil,
			!known && bestKnown,
			known && bestKnown && reset.Before(bestReset):
			best = []*Auth{auth}
			bestReset = reset
			bestKnown = known
		case known == bestKnown && (!known || reset.Equal(bestReset)):
			best = append(best, auth)
		}
	}
	return best
}

// quotaWindowResetAt reports when the credential's longest observed quota window
// resets. It returns false when no reset in the future is known.
func quotaWindowResetAt(auth *Auth, now time.Time) (time.Time, bool) {
	if auth == nil || len(auth.Quota.Signals) == 0 {
		return time.Time{}, false
	}
	signals := auth.Quota.Signals

	// Claude: the shared 7-day window.
	if reset, ok := parseResetTimestamp(quotaSignal(signals, "Anthropic-Ratelimit-Unified-7d-Reset")); ok && reset.After(now) {
		return reset, true
	}

	// Codex: the primary and secondary windows; the longer one is the weekly window.
	var longest time.Time
	longestMinutes := int64(-1)
	for _, prefix := range []string{"X-Codex-Primary-", "X-Codex-Secondary-"} {
		minutes, errMinutes := strconv.ParseInt(quotaSignal(signals, prefix+"Window-Minutes"), 10, 64)
		if errMinutes != nil || minutes <= 0 || minutes <= longestMinutes {
			continue
		}
		reset, ok := parseResetTimestamp(quotaSignal(signals, prefix+"Reset-At"))
		if !ok && !auth.Quota.ObservedAt.IsZero() {
			if seconds, errSeconds := strconv.ParseFloat(quotaSignal(signals, prefix+"Reset-After-Seconds"), 64); errSeconds == nil && seconds >= 0 {
				reset = auth.Quota.ObservedAt.Add(time.Duration(seconds * float64(time.Second)))
				ok = true
			}
		}
		if ok && reset.After(now) {
			longest = reset
			longestMinutes = minutes
		}
	}
	if longestMinutes > 0 {
		return longest, true
	}
	return time.Time{}, false
}

// quotaSignal looks up a recorded quota header regardless of its stored casing.
func quotaSignal(signals map[string]string, name string) string {
	if value, ok := signals[http.CanonicalHeaderKey(name)]; ok {
		return strings.TrimSpace(value)
	}
	for key, value := range signals {
		if strings.EqualFold(key, name) {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// parseResetTimestamp accepts Unix seconds (integer or fractional) or RFC 3339.
func parseResetTimestamp(raw string) (time.Time, bool) {
	if raw == "" {
		return time.Time{}, false
	}
	if seconds, errParse := strconv.ParseFloat(raw, 64); errParse == nil {
		if seconds <= 0 {
			return time.Time{}, false
		}
		whole := int64(seconds)
		return time.Unix(whole, int64((seconds-float64(whole))*1e9)), true
	}
	if parsed, errParse := time.Parse(time.RFC3339, raw); errParse == nil {
		return parsed, true
	}
	return time.Time{}, false
}
