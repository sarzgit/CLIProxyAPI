package auth

import (
	"context"
	"net/http"
	"strconv"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

var soonestResetTestNow = time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

func newSoonestResetTestSelector() *SoonestResetFirstSelector {
	return &SoonestResetFirstSelector{nowFunc: func() time.Time { return soonestResetTestNow }}
}

func claudeAuthResettingIn(id string, in time.Duration) *Auth {
	return &Auth{ID: id, Quota: QuotaState{
		ObservedAt: soonestResetTestNow,
		Signals: map[string]string{
			"Anthropic-Ratelimit-Unified-7d-Reset": strconv.FormatInt(soonestResetTestNow.Add(in).Unix(), 10),
			"Anthropic-Ratelimit-Unified-5h-Reset": strconv.FormatInt(soonestResetTestNow.Add(time.Hour).Unix(), 10),
		},
	}}
}

func pickSoonest(t *testing.T, selector *SoonestResetFirstSelector, model string, auths []*Auth) string {
	t.Helper()
	got, err := selector.Pick(context.Background(), "claude", model, cliproxyexecutor.Options{}, auths)
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	return got.ID
}

func TestSoonestResetFirstSelector_PrefersEarliestClaudeWeeklyReset(t *testing.T) {
	t.Parallel()

	auths := []*Auth{
		claudeAuthResettingIn("a", 4*24*time.Hour),
		claudeAuthResettingIn("b", 25*time.Hour),
		claudeAuthResettingIn("c", 3*24*time.Hour),
	}
	selector := newSoonestResetTestSelector()
	for i := 0; i < 3; i++ {
		if got := pickSoonest(t, selector, "", auths); got != "b" {
			t.Fatalf("pick %d = %q, want b (resets soonest)", i, got)
		}
	}
}

func TestSoonestResetFirstSelector_UsesLongestCodexWindow(t *testing.T) {
	t.Parallel()

	// "short" has the sooner 5h reset but the later weekly reset.
	short := &Auth{ID: "short", Quota: QuotaState{ObservedAt: soonestResetTestNow, Signals: map[string]string{
		"X-Codex-Primary-Window-Minutes":        "300",
		"X-Codex-Primary-Reset-After-Seconds":   "600",
		"X-Codex-Secondary-Window-Minutes":      "10080",
		"X-Codex-Secondary-Reset-After-Seconds": strconv.Itoa(5 * 24 * 3600),
	}}}
	// "weekly" only reports reset-at timestamps.
	weekly := &Auth{ID: "weekly", Quota: QuotaState{ObservedAt: soonestResetTestNow, Signals: map[string]string{
		"X-Codex-Primary-Window-Minutes":   "300",
		"X-Codex-Primary-Reset-At":         strconv.FormatInt(soonestResetTestNow.Add(4*time.Hour).Unix(), 10),
		"X-Codex-Secondary-Window-Minutes": "10080",
		"X-Codex-Secondary-Reset-At":       strconv.FormatInt(soonestResetTestNow.Add(2*24*time.Hour).Unix(), 10),
	}}}

	got, err := newSoonestResetTestSelector().Pick(context.Background(), "codex", "", cliproxyexecutor.Options{}, []*Auth{short, weekly})
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if got.ID != "weekly" {
		t.Fatalf("Pick() = %q, want weekly (weekly window resets in 2d vs 5d)", got.ID)
	}
}

func TestSoonestResetFirstSelector_ResetAfterSecondsIsRelativeToObservation(t *testing.T) {
	t.Parallel()

	// Observed two days ago with five days left: resets in three days.
	stale := &Auth{ID: "stale", Quota: QuotaState{ObservedAt: soonestResetTestNow.Add(-2 * 24 * time.Hour), Signals: map[string]string{
		"X-Codex-Secondary-Window-Minutes":      "10080",
		"X-Codex-Secondary-Reset-After-Seconds": strconv.Itoa(5 * 24 * 3600),
	}}}
	// Observed now with four days left.
	fresh := &Auth{ID: "fresh", Quota: QuotaState{ObservedAt: soonestResetTestNow, Signals: map[string]string{
		"X-Codex-Secondary-Window-Minutes":      "10080",
		"X-Codex-Secondary-Reset-After-Seconds": strconv.Itoa(4 * 24 * 3600),
	}}}

	if got := pickSoonest(t, newSoonestResetTestSelector(), "", []*Auth{fresh, stale}); got != "stale" {
		t.Fatalf("Pick() = %q, want stale (resets in 3d vs 4d)", got)
	}
}

func TestSoonestResetFirstSelector_UnknownResetGoesFirst(t *testing.T) {
	t.Parallel()

	auths := []*Auth{
		claudeAuthResettingIn("known", time.Hour),
		{ID: "unknown"},
	}
	if got := pickSoonest(t, newSoonestResetTestSelector(), "", auths); got != "unknown" {
		t.Fatalf("Pick() = %q, want unknown so its window gets observed", got)
	}
}

func TestSoonestResetFirstSelector_PastResetCountsAsUnknown(t *testing.T) {
	t.Parallel()

	auths := []*Auth{
		claudeAuthResettingIn("future", time.Hour),
		claudeAuthResettingIn("expired", -time.Hour),
	}
	if got := pickSoonest(t, newSoonestResetTestSelector(), "", auths); got != "expired" {
		t.Fatalf("Pick() = %q, want expired (its window already reset, so its reset is unknown)", got)
	}
}

func TestSoonestResetFirstSelector_RotatesWithinSameMinute(t *testing.T) {
	t.Parallel()

	a := claudeAuthResettingIn("a", 24*time.Hour)
	b := claudeAuthResettingIn("b", 24*time.Hour+20*time.Second)
	later := claudeAuthResettingIn("later", 48*time.Hour)
	auths := []*Auth{later, b, a}
	selector := newSoonestResetTestSelector()

	want := []string{"a", "b", "a", "b"}
	for i, id := range want {
		if got := pickSoonest(t, selector, "", auths); got != id {
			t.Fatalf("pick %d = %q, want %q", i, got, id)
		}
	}
}

func TestSoonestResetFirstSelector_SkipsCooldownCredential(t *testing.T) {
	t.Parallel()

	model := "claude-test"
	soon := claudeAuthResettingIn("soon", time.Hour)
	soon.ModelStates = map[string]*ModelState{model: {
		Status:         StatusActive,
		Unavailable:    true,
		NextRetryAfter: soonestResetTestNow.Add(30 * time.Minute),
		Quota:          QuotaState{Exceeded: true},
	}}
	later := claudeAuthResettingIn("later", 3*24*time.Hour)

	if got := pickSoonest(t, newSoonestResetTestSelector(), model, []*Auth{soon, later}); got != "later" {
		t.Fatalf("Pick() = %q, want later (soon is cooling down)", got)
	}
}

func TestSoonestResetFirstSelector_RespectsPriorityTiers(t *testing.T) {
	t.Parallel()

	lowSoon := claudeAuthResettingIn("low-soon", time.Hour)
	lowSoon.Attributes = map[string]string{"priority": "0"}
	highLate := claudeAuthResettingIn("high-late", 5*24*time.Hour)
	highLate.Attributes = map[string]string{"priority": "10"}

	if got := pickSoonest(t, newSoonestResetTestSelector(), "", []*Auth{lowSoon, highLate}); got != "high-late" {
		t.Fatalf("Pick() = %q, want high-late (higher priority tier wins)", got)
	}
}

func TestSoonestResetFirstSelector_NoCandidates(t *testing.T) {
	t.Parallel()

	if _, err := newSoonestResetTestSelector().Pick(context.Background(), "claude", "", cliproxyexecutor.Options{}, nil); err == nil {
		t.Fatalf("Pick() error = nil, want error for empty candidates")
	}
}

func TestParseResetTimestamp(t *testing.T) {
	t.Parallel()

	cases := []struct {
		raw  string
		want time.Time
		ok   bool
	}{
		{raw: "1794398400", want: time.Unix(1794398400, 0), ok: true},
		{raw: "1794398400.5", want: time.Unix(1794398400, 5e8), ok: true},
		{raw: "2026-11-12T00:00:00Z", want: time.Date(2026, 11, 12, 0, 0, 0, 0, time.UTC), ok: true},
		{raw: "", ok: false},
		{raw: "0", ok: false},
		{raw: "soon", ok: false},
	}
	for _, tc := range cases {
		got, ok := parseResetTimestamp(tc.raw)
		if ok != tc.ok || (ok && !got.Equal(tc.want)) {
			t.Fatalf("parseResetTimestamp(%q) = %v, %v; want %v, %v", tc.raw, got, ok, tc.want, tc.ok)
		}
	}
}

func TestSoonestResetFirstSelector_SessionAffinityKeepsBoundAccount(t *testing.T) {
	t.Parallel()

	a := claudeAuthResettingIn("a", 3*24*time.Hour)
	b := claudeAuthResettingIn("b", 24*time.Hour)
	auths := []*Auth{a, b}
	selector := NewSessionAffinitySelectorWithConfig(SessionAffinityConfig{
		Fallback: newSoonestResetTestSelector(),
		TTL:      time.Hour,
	})
	defer selector.Stop()

	pick := func(session string) string {
		t.Helper()
		headers := http.Header{}
		headers.Set("X-Session-ID", session)
		got, err := selector.Pick(context.Background(), "claude", "", cliproxyexecutor.Options{Headers: headers}, auths)
		if err != nil {
			t.Fatalf("Pick(%s) error = %v", session, err)
		}
		return got.ID
	}

	if got := pick("session-one"); got != "b" {
		t.Fatalf("new session picked %q, want b (resets soonest)", got)
	}
	// b's window moves later; the bound session must stay put, new sessions follow the reset order.
	b.Quota.Signals["Anthropic-Ratelimit-Unified-7d-Reset"] = strconv.FormatInt(soonestResetTestNow.Add(6*24*time.Hour).Unix(), 10)
	if got := pick("session-one"); got != "b" {
		t.Fatalf("bound session moved to %q, want b (affinity keeps the cache warm)", got)
	}
	if got := pick("session-two"); got != "a" {
		t.Fatalf("new session picked %q, want a (now resets soonest)", got)
	}
}

func TestSoonestResetFirstSelector_BoundsUnknownProbing(t *testing.T) {
	t.Parallel()

	now := soonestResetTestNow
	selector := &SoonestResetFirstSelector{nowFunc: func() time.Time { return now }}
	// "silent" never reports a reset, so it stays unknown after every request.
	auths := []*Auth{claudeAuthResettingIn("known", 2*time.Hour), {ID: "silent"}}
	auths[0].Quota.Signals["Anthropic-Ratelimit-Unified-7d-Reset"] = strconv.FormatInt(now.Add(2*time.Hour).Unix(), 10)

	want := []string{"silent", "known", "known", "known"}
	for i, id := range want {
		if got := pickSoonest(t, selector, "", auths); got != id {
			t.Fatalf("pick %d = %q, want %q", i, got, id)
		}
	}
	now = now.Add(unknownResetProbeInterval)
	if got := pickSoonest(t, selector, "", auths); got != "silent" {
		t.Fatalf("after probe interval picked %q, want silent probed again", got)
	}
	if got := pickSoonest(t, selector, "", auths); got != "known" {
		t.Fatalf("after re-probe picked %q, want known", got)
	}
}

func TestSoonestResetFirstSelector_AllUnknownStillRotates(t *testing.T) {
	t.Parallel()

	selector := newSoonestResetTestSelector()
	auths := []*Auth{{ID: "a"}, {ID: "b"}}
	want := []string{"a", "b", "a", "b"}
	for i, id := range want {
		if got := pickSoonest(t, selector, "", auths); got != id {
			t.Fatalf("pick %d = %q, want %q", i, got, id)
		}
	}
}

func TestQuotaWindowResetAt_ExpiredLongestWindowIsUnknown(t *testing.T) {
	t.Parallel()

	auth := &Auth{ID: "codex", Quota: QuotaState{ObservedAt: soonestResetTestNow, Signals: map[string]string{
		"X-Codex-Primary-Window-Minutes":   "300",
		"X-Codex-Primary-Reset-At":         strconv.FormatInt(soonestResetTestNow.Add(2*time.Hour).Unix(), 10),
		"X-Codex-Secondary-Window-Minutes": "10080",
		"X-Codex-Secondary-Reset-At":       strconv.FormatInt(soonestResetTestNow.Add(-time.Hour).Unix(), 10),
	}}}
	if reset, ok := quotaWindowResetAt(auth, soonestResetTestNow); ok {
		t.Fatalf("quotaWindowResetAt() = %v, true; want unknown instead of the 5h window", reset)
	}
}

func TestSoonestResetFirstSelector_MixedCodexOnlyKeepsWebsocketPreference(t *testing.T) {
	t.Parallel()

	httpAuth := &Auth{ID: "a-http", Provider: "codex", Quota: QuotaState{Signals: map[string]string{
		"X-Codex-Secondary-Window-Minutes": "10080",
		"X-Codex-Secondary-Reset-At":       strconv.FormatInt(soonestResetTestNow.Add(time.Hour).Unix(), 10),
	}}}
	wsAuth := &Auth{ID: "b-ws", Provider: "codex", Attributes: map[string]string{"websockets": "true"}, Quota: QuotaState{Signals: map[string]string{
		"X-Codex-Secondary-Window-Minutes": "10080",
		"X-Codex-Secondary-Reset-At":       strconv.FormatInt(soonestResetTestNow.Add(48*time.Hour).Unix(), 10),
	}}}
	ctx := cliproxyexecutor.WithDownstreamWebsocket(context.Background())
	got, err := newSoonestResetTestSelector().Pick(ctx, "mixed", "", cliproxyexecutor.Options{}, []*Auth{httpAuth, wsAuth})
	if err != nil {
		t.Fatalf("Pick() error = %v", err)
	}
	if got.ID != "b-ws" {
		t.Fatalf("Pick() = %q, want b-ws (websocket preference applies before reset order)", got.ID)
	}
}

func TestSoonestResetFirstSelector_ManagerIgnoresStaleAliasCooldown(t *testing.T) {
	withQuotaCooldownEnabled(t)

	ctx := context.Background()
	const routeModel, targetModel = "srf-alias", "srf-target"
	manager := NewManager(nil, &SoonestResetFirstSelector{}, nil)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(schedulerTestExecutor{provider: "codex"})
	retryAfter := time.Hour
	for _, id := range []string{"srf-a-" + t.Name(), "srf-b-" + t.Name()} {
		registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: routeModel}, {ID: targetModel}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
		if _, errRegister := manager.Register(ctx, &Auth{ID: id, Provider: "codex", Status: StatusActive}); errRegister != nil {
			t.Fatal(errRegister)
		}
		// Cooldown recorded under the alias name before the alias pointed elsewhere.
		manager.MarkResult(ctx, Result{
			AuthID: id, Provider: "codex", Model: routeModel, RetryAfter: &retryAfter,
			Error: &Error{HTTPStatus: http.StatusTooManyRequests, Message: "model quota"},
		})
	}
	manager.SetOAuthModelAlias(map[string][]internalconfig.OAuthModelAlias{
		"codex": {{Name: targetModel, Alias: routeModel, Fork: true}},
	})

	if _, errSelect := manager.SelectAuth(ctx, "codex", routeModel, cliproxyexecutor.Options{Metadata: map[string]any{}}); errSelect != nil {
		t.Fatalf("SelectAuth() error = %v, want a healthy credential for the resolved target model", errSelect)
	}
}

func TestSoonestResetFirstSelector_FullProbeStorageDoesNotRestartProbing(t *testing.T) {
	t.Parallel()

	now := soonestResetTestNow
	selector := &SoonestResetFirstSelector{nowFunc: func() time.Time { return now }, maxProbes: 2}
	auths := []*Auth{
		claudeAuthResettingIn("known", 2*time.Hour),
		{ID: "unknown-1"}, {ID: "unknown-2"}, {ID: "unknown-3"},
	}

	// Two probes fill storage; the third unknown credential must then wait behind known.
	want := []string{"unknown-1", "unknown-2", "known", "known", "known"}
	for i, id := range want {
		if got := pickSoonest(t, selector, "", auths); got != id {
			t.Fatalf("pick %d = %q, want %q", i, got, id)
		}
	}
	if len(selector.probedAt) != 2 {
		t.Fatalf("probe records = %d, want 2 (history must not be cleared)", len(selector.probedAt))
	}
	// Once records expire, probing resumes.
	now = now.Add(unknownResetProbeInterval)
	if got := pickSoonest(t, selector, "", auths); got != "unknown-3" && got != "unknown-1" {
		t.Fatalf("after expiry picked %q, want an unknown credential probed", got)
	}
}
