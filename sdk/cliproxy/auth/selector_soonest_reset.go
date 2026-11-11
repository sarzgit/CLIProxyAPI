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

// unknownResetProbeInterval bounds how often a credential with no known reset is
// tried ahead of credentials whose resets are known. One response is normally
// enough to learn a window; a credential that never reports one must not keep
// taking new sessions from credentials whose quota is about to expire.
const unknownResetProbeInterval = 10 * time.Minute

const maxSoonestResetSelectorKeys = 4096

// SoonestResetFirstSelector prefers the available credential whose longest quota
// window (the weekly window on Claude and Codex subscriptions) resets soonest.
// Quota left in a window when it resets is lost, so draining the credential that
// resets first wastes the least.
//
// Reset times come from the quota signals recorded on each credential's most
// recent upstream response. Ranking, from first to last:
//  1. credentials with no known future reset that were not probed within
//     unknownResetProbeInterval, so one request can observe their window;
//  2. credentials with a known future reset, soonest first;
//  3. credentials with no known reset that were probed recently.
//
// Credentials in the same rank (and, for known resets, the same minute) rotate in
// round-robin order. Availability, cooldowns, disabled credentials, priority tiers
// and Codex websocket preference are handled like the other selectors.
type SoonestResetFirstSelector struct {
	mu         sync.Mutex
	lastPicked map[string]string
	probedAt   map[string]time.Time
	nowFunc    func() time.Time
	// maxProbes caps probe records; zero means maxSoonestResetSelectorKeys.
	maxProbes int
}

// Pick selects the available auth whose quota window resets soonest.
func (s *SoonestResetFirstSelector) Pick(ctx context.Context, provider, model string, opts cliproxyexecutor.Options, auths []*Auth) (*Auth, error) {
	_ = opts
	now := s.now()
	available, err := getSelectorAvailableAuths(ctx, auths, provider, model, now)
	if err != nil {
		return nil, err
	}
	available = preferCodexWebsocketAuths(ctx, websocketPreferenceProvider(provider, available), available)
	if len(available) == 1 {
		return available[0], nil
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// available is ID-sorted, and ranking keeps that order, which successorIndex needs.
	candidates, probing := s.soonestResetAuthsLocked(available, now)

	key := provider + ":" + canonicalModelLookupKey(model)
	if s.lastPicked == nil || (len(s.lastPicked) >= maxSoonestResetSelectorKeys && s.lastPicked[key] == "") {
		s.lastPicked = make(map[string]string)
	}
	picked := candidates[successorIndex(candidates, s.lastPicked[key])]
	s.lastPicked[key] = picked.ID
	if probing {
		s.recordProbeLocked(picked.ID, now)
	}
	return picked, nil
}

func (s *SoonestResetFirstSelector) now() time.Time {
	if s.nowFunc != nil {
		return s.nowFunc()
	}
	return time.Now()
}

// soonestResetAuthsLocked returns the best-ranked candidates in input order and
// whether they are unknown credentials being probed. Must be called with s.mu held.
func (s *SoonestResetFirstSelector) soonestResetAuthsLocked(auths []*Auth, now time.Time) ([]*Auth, bool) {
	// With probe storage full, unprobed credentials wait like recently probed ones
	// until records expire. Clearing history instead would restart probing for
	// every unknown credential and starve known resets.
	canProbe := s.pruneProbesLocked(now)
	var probe, known, recentlyProbed []*Auth
	var soonest time.Time
	for _, auth := range auths {
		reset, ok := quotaWindowResetAt(auth, now)
		if !ok {
			if last, probed := s.probedAt[auth.ID]; !canProbe || (probed && now.Sub(last) < unknownResetProbeInterval) {
				recentlyProbed = append(recentlyProbed, auth)
			} else {
				probe = append(probe, auth)
			}
			continue
		}
		reset = reset.Truncate(time.Minute)
		switch {
		case known == nil || reset.Before(soonest):
			known = []*Auth{auth}
			soonest = reset
		case reset.Equal(soonest):
			known = append(known, auth)
		}
	}
	switch {
	case len(probe) > 0:
		return probe, true
	case len(known) > 0:
		return known, false
	default:
		return recentlyProbed, false
	}
}

// pruneProbesLocked drops expired probe records once storage is full and reports
// whether another probe can be recorded. Must be called with s.mu held.
func (s *SoonestResetFirstSelector) pruneProbesLocked(now time.Time) bool {
	limit := s.maxProbes
	if limit <= 0 {
		limit = maxSoonestResetSelectorKeys
	}
	if len(s.probedAt) < limit {
		return true
	}
	for id, last := range s.probedAt {
		if now.Sub(last) >= unknownResetProbeInterval {
			delete(s.probedAt, id)
		}
	}
	return len(s.probedAt) < limit
}

// recordProbeLocked notes that an unknown credential was handed a request.
// Callers only record after pruneProbesLocked reported room. Must be called with s.mu held.
func (s *SoonestResetFirstSelector) recordProbeLocked(authID string, now time.Time) {
	if s.probedAt == nil {
		s.probedAt = make(map[string]time.Time)
	}
	s.probedAt[authID] = now
}

// websocketPreferenceProvider returns "codex" when a mixed-provider selection only
// contains Codex credentials, so Codex websocket preference still applies.
func websocketPreferenceProvider(provider string, auths []*Auth) string {
	if !strings.EqualFold(strings.TrimSpace(provider), "mixed") || len(auths) == 0 {
		return provider
	}
	for _, auth := range auths {
		if auth == nil || !strings.EqualFold(strings.TrimSpace(auth.Provider), "codex") {
			return provider
		}
	}
	return "codex"
}

// quotaWindowResetAt reports when the credential's longest observed quota window
// resets. It returns false when that reset is missing, unparseable or not in the
// future; a shorter window is never substituted for the longest one.
func quotaWindowResetAt(auth *Auth, now time.Time) (time.Time, bool) {
	if auth == nil || len(auth.Quota.Signals) == 0 {
		return time.Time{}, false
	}
	signals := auth.Quota.Signals

	// Claude: the shared 7-day window.
	if raw := quotaSignal(signals, "Anthropic-Ratelimit-Unified-7d-Reset"); raw != "" {
		reset, ok := parseResetTimestamp(raw)
		return reset, ok && reset.After(now)
	}

	// Codex: the longer of the primary and secondary windows is the weekly window.
	longestPrefix := ""
	longestMinutes := int64(0)
	for _, prefix := range []string{"X-Codex-Primary-", "X-Codex-Secondary-"} {
		minutes, errMinutes := strconv.ParseInt(quotaSignal(signals, prefix+"Window-Minutes"), 10, 64)
		if errMinutes == nil && minutes > longestMinutes {
			longestPrefix = prefix
			longestMinutes = minutes
		}
	}
	if longestPrefix == "" {
		return time.Time{}, false
	}
	reset, ok := parseResetTimestamp(quotaSignal(signals, longestPrefix+"Reset-At"))
	if !ok && !auth.Quota.ObservedAt.IsZero() {
		if seconds, errSeconds := strconv.ParseFloat(quotaSignal(signals, longestPrefix+"Reset-After-Seconds"), 64); errSeconds == nil && seconds >= 0 {
			reset = auth.Quota.ObservedAt.Add(time.Duration(seconds * float64(time.Second)))
			ok = true
		}
	}
	return reset, ok && reset.After(now)
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
