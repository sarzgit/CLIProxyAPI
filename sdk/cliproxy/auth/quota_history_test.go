package auth

import (
	"context"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	internallogging "github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
)

func quotaHistoryAuth(id, provider string, observedAt time.Time, signals map[string]string) *Auth {
	return &Auth{
		ID:       id,
		Provider: provider,
		FileName: "/auths/" + id,
		Metadata: map[string]any{"email": id + "@example.com"},
		Quota:    QuotaState{ObservedAt: observedAt, Signals: signals},
	}
}

func newTestQuotaHistoryRecorder(path string, now *time.Time) *QuotaHistoryRecorder {
	recorder := NewQuotaHistoryRecorder(path)
	recorder.nowFunc = func() time.Time { return *now }
	return recorder
}

func TestQuotaHistorySampleClaude(t *testing.T) {
	observedAt := time.Unix(1_794_390_000, 0)
	for _, tc := range []struct {
		name          string
		utilization   string
		wantRemaining float64
	}{
		{"fraction", "0.52", 48},
		{"percent", "37.5", 62.5},
		{"over", "150", 0},
		{"negative", "-0.2", 100},
	} {
		t.Run(tc.name, func(t *testing.T) {
			auth := quotaHistoryAuth("claude-a.json", "claude", observedAt, map[string]string{
				"anthropic-ratelimit-unified-7d-utilization": tc.utilization,
				"Anthropic-Ratelimit-Unified-7d-Reset":       "1794500000",
			})
			sample, window, ok := quotaHistorySampleFromAuth(auth)
			if !ok {
				t.Fatal("expected a sample")
			}
			want := QuotaHistorySample{T: observedAt.Unix(), RemainingPercent: tc.wantRemaining, ResetAt: 1_794_500_000}
			if sample != want || window != 10080 {
				t.Fatalf("sample=%+v window=%d, want %+v window=10080", sample, window, want)
			}
		})
	}

	missing := quotaHistoryAuth("claude-b.json", "claude", observedAt, map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Reset": "1794500000",
	})
	if _, _, ok := quotaHistorySampleFromAuth(missing); ok {
		t.Fatal("expected no sample without utilization")
	}
}

func TestQuotaHistorySampleCodexUsesLongestWindow(t *testing.T) {
	observedAt := time.Unix(1_794_390_000, 0)
	auth := quotaHistoryAuth("codex-a.json", "codex", observedAt, map[string]string{
		"X-Codex-Primary-Used-Percent":     "80",
		"X-Codex-Primary-Window-Minutes":   "300",
		"X-Codex-Primary-Reset-At":         "1794391000",
		"x-codex-secondary-used-percent":   "25.5",
		"x-codex-secondary-window-minutes": "10080",
		"x-codex-secondary-reset-at":       "1794600000",
	})
	sample, window, ok := quotaHistorySampleFromAuth(auth)
	want := QuotaHistorySample{T: observedAt.Unix(), RemainingPercent: 74.5, ResetAt: 1_794_600_000}
	if !ok || sample != want || window != 10080 {
		t.Fatalf("sample=%+v window=%d ok=%v, want %+v", sample, window, ok, want)
	}
}

func TestQuotaHistorySampleCodexRelativeAndUnknownReset(t *testing.T) {
	observedAt := time.Unix(1_794_390_000, 0)
	relative := quotaHistoryAuth("codex-a.json", "codex", observedAt, map[string]string{
		"X-Codex-Primary-Used-Percent":        "10",
		"X-Codex-Primary-Window-Minutes":      "10080",
		"X-Codex-Primary-Reset-After-Seconds": "3600",
	})
	sample, _, ok := quotaHistorySampleFromAuth(relative)
	if !ok || sample.ResetAt != observedAt.Unix()+3600 || sample.RemainingPercent != 90 {
		t.Fatalf("relative sample=%+v ok=%v", sample, ok)
	}

	unknown := quotaHistoryAuth("codex-b.json", "codex", observedAt, map[string]string{
		"X-Codex-Primary-Used-Percent":   "10",
		"X-Codex-Primary-Window-Minutes": "10080",
	})
	sample, _, ok = quotaHistorySampleFromAuth(unknown)
	if !ok || sample.ResetAt != 0 {
		t.Fatalf("unknown reset sample=%+v ok=%v", sample, ok)
	}

	noWindow := quotaHistoryAuth("codex-c.json", "codex", observedAt, map[string]string{
		"X-Codex-Primary-Used-Percent": "10",
	})
	if _, _, ok = quotaHistorySampleFromAuth(noWindow); ok {
		t.Fatal("expected no sample without a window")
	}
	if _, _, ok = quotaHistorySampleFromAuth(quotaHistoryAuth("gem.json", "gemini", observedAt, map[string]string{"X": "1"})); ok {
		t.Fatal("expected no sample for unsupported provider")
	}
}

func TestQuotaHistoryRecordThresholds(t *testing.T) {
	now := time.Unix(1_794_390_000, 0)
	recorder := newTestQuotaHistoryRecorder("", &now)
	observe := func(offset time.Duration, utilization, reset string) {
		recorder.Observe(quotaHistoryAuth("claude-a.json", "claude", now.Add(offset), map[string]string{
			"Anthropic-Ratelimit-Unified-7d-Utilization": utilization,
			"Anthropic-Ratelimit-Unified-7d-Reset":       reset,
		}))
	}
	observe(0, "0.50", "1794500000")                 // first sample
	observe(time.Minute, "0.502", "1794500000")      // 0.2 points: skipped
	observe(2*time.Minute, "0.506", "1794500000")    // 0.6 points: recorded
	observe(3*time.Minute, "0.506", "1794500030")    // reset jitter within tolerance: skipped
	observe(4*time.Minute, "0.506", "1795100000")    // reset changed: recorded
	observe(5*time.Minute, "0.506", "1795100000")    // nothing changed: skipped
	observe(9*time.Minute, "0.506", "1795100000")    // 5 minutes since last: recorded
	observe(8*time.Minute, "0.10", "1795100000")     // older than last: skipped
	observe(9*time.Minute+time.Second, "0.506", "0") // reset became unknown: recorded

	entry := recorder.History(time.Unix(0, 0))["claude-a.json"]
	gotTimes := make([]int64, 0, len(entry.Samples))
	for _, sample := range entry.Samples {
		gotTimes = append(gotTimes, sample.T-now.Unix())
	}
	want := []int64{0, 120, 240, 540, 541}
	if len(gotTimes) != len(want) {
		t.Fatalf("sample offsets=%v, want %v", gotTimes, want)
	}
	for i := range want {
		if gotTimes[i] != want[i] {
			t.Fatalf("sample offsets=%v, want %v", gotTimes, want)
		}
	}
	if entry.Provider != "claude" || entry.Label != "claude-a.json@example.com" || entry.FileName != "claude-a.json" || entry.WindowMinutes != 10080 {
		t.Fatalf("entry metadata=%+v", entry)
	}
}

func TestQuotaHistoryRetentionAndCap(t *testing.T) {
	now := time.Unix(1_794_390_000, 0)
	recorder := newTestQuotaHistoryRecorder("", &now)
	cutoff := now.Add(-quotaHistoryRetention).Unix()

	samples := make([]QuotaHistorySample, 0, quotaHistoryMaxSamples+10)
	samples = append(samples, QuotaHistorySample{T: cutoff - 1, RemainingPercent: 1})
	for i := 0; i < quotaHistoryMaxSamples+9; i++ {
		samples = append(samples, QuotaHistorySample{T: cutoff + int64(i), RemainingPercent: 50})
	}
	pruned := pruneQuotaHistorySamples(samples, cutoff)
	if len(pruned) != quotaHistoryMaxSamples || pruned[0].T != cutoff+9 {
		t.Fatalf("pruned len=%d first=%d, want %d starting at %d", len(pruned), pruned[0].T, quotaHistoryMaxSamples, cutoff+9)
	}

	recorder.entries["old.json"] = &QuotaHistoryEntry{Provider: "codex", Samples: []QuotaHistorySample{{T: cutoff - 10}}}
	recorder.entries["new.json"] = &QuotaHistoryEntry{Provider: "codex", Samples: []QuotaHistorySample{{T: cutoff - 10}, {T: cutoff + 10}}}
	history := recorder.History(time.Unix(0, 0))
	if _, ok := history["old.json"]; ok {
		t.Fatal("expired entry should not be listed")
	}
	if got := history["new.json"].Samples; len(got) != 1 || got[0].T != cutoff+10 {
		t.Fatalf("new.json samples=%+v", got)
	}
}

func TestQuotaHistorySinceIncludesPreviousSample(t *testing.T) {
	now := time.Unix(1_794_390_000, 0)
	recorder := newTestQuotaHistoryRecorder("", &now)
	recorder.entries["a.json"] = &QuotaHistoryEntry{Provider: "codex", Samples: []QuotaHistorySample{
		{T: now.Unix() - 300}, {T: now.Unix() - 200}, {T: now.Unix() - 100}, {T: now.Unix()},
	}}
	got := recorder.History(time.Unix(now.Unix()-150, 0))["a.json"].Samples
	if len(got) != 3 || got[0].T != now.Unix()-200 || got[2].T != now.Unix() {
		t.Fatalf("samples=%+v", got)
	}
	got = recorder.History(now.Add(time.Hour))["a.json"].Samples
	if len(got) != 1 || got[0].T != now.Unix() {
		t.Fatalf("samples after all=%+v", got)
	}
}

func TestQuotaHistoryPersistenceRoundTrip(t *testing.T) {
	now := time.Unix(1_794_390_000, 0)
	path := filepath.Join(t.TempDir(), QuotaHistoryFileName)
	recorder := newTestQuotaHistoryRecorder(path, &now)
	recorder.Observe(quotaHistoryAuth("codex-a.json", "codex", now, map[string]string{
		"X-Codex-Secondary-Used-Percent":   "40",
		"X-Codex-Secondary-Window-Minutes": "10080",
		"X-Codex-Secondary-Reset-At":       "1794600000",
	}))
	if errClose := recorder.Close(); errClose != nil {
		t.Fatalf("Close() error = %v", errClose)
	}
	info, errStat := os.Stat(path)
	if errStat != nil {
		t.Fatalf("history file missing: %v", errStat)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Fatalf("history file perm=%o, want 600", perm)
	}
	leftovers, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "*.tmp"))
	if len(leftovers) != 0 {
		t.Fatalf("temp files left behind: %v", leftovers)
	}

	loaded := newTestQuotaHistoryRecorder(path, &now)
	if errLoad := loaded.Load(); errLoad != nil {
		t.Fatalf("Load() error = %v", errLoad)
	}
	entry, ok := loaded.History(time.Unix(0, 0))["codex-a.json"]
	want := QuotaHistorySample{T: now.Unix(), RemainingPercent: 60, ResetAt: 1_794_600_000}
	if !ok || len(entry.Samples) != 1 || entry.Samples[0] != want || entry.WindowMinutes != 10080 || entry.Provider != "codex" || entry.FileName != "codex-a.json" {
		t.Fatalf("loaded entry=%+v ok=%v", entry, ok)
	}

	// A clean recorder does not rewrite the file.
	if errRemove := os.Remove(path); errRemove != nil {
		t.Fatal(errRemove)
	}
	if errFlush := loaded.Flush(); errFlush != nil {
		t.Fatalf("Flush() error = %v", errFlush)
	}
	if _, errStat = os.Stat(path); !os.IsNotExist(errStat) {
		t.Fatalf("clean flush wrote the file: %v", errStat)
	}

	// Samples that expire while stored are dropped on load.
	later := now.Add(quotaHistoryRetention + time.Hour)
	if errWrite := writeQuotaHistoryFile(path, quotaHistoryFile{Version: 1, Accounts: map[string]*QuotaHistoryEntry{
		"codex-a.json": entry.clone(),
	}}); errWrite != nil {
		t.Fatal(errWrite)
	}
	expired := newTestQuotaHistoryRecorder(path, &later)
	if errLoad := expired.Load(); errLoad != nil {
		t.Fatalf("Load() error = %v", errLoad)
	}
	if got := expired.History(time.Unix(0, 0)); len(got) != 0 {
		t.Fatalf("expired history=%+v", got)
	}
}

func (e QuotaHistoryEntry) clone() *QuotaHistoryEntry {
	e.Samples = append([]QuotaHistorySample(nil), e.Samples...)
	return &e
}

func TestQuotaHistoryCorruptFileIsIgnored(t *testing.T) {
	now := time.Unix(1_794_390_000, 0)
	path := filepath.Join(t.TempDir(), QuotaHistoryFileName)
	if errWrite := os.WriteFile(path, []byte("{not json"), 0o600); errWrite != nil {
		t.Fatal(errWrite)
	}
	recorder := newTestQuotaHistoryRecorder(path, &now)
	if errLoad := recorder.Load(); errLoad != nil {
		t.Fatalf("Load() error = %v, want nil for corrupt file", errLoad)
	}
	if got := recorder.History(time.Unix(0, 0)); len(got) != 0 {
		t.Fatalf("history=%+v, want empty", got)
	}
	recorder.Observe(quotaHistoryAuth("claude-a.json", "claude", now, map[string]string{
		"Anthropic-Ratelimit-Unified-7d-Utilization": "0.1",
	}))
	if errFlush := recorder.Flush(); errFlush != nil {
		t.Fatalf("Flush() error = %v", errFlush)
	}
	reloaded := newTestQuotaHistoryRecorder(path, &now)
	if errLoad := reloaded.Load(); errLoad != nil {
		t.Fatal(errLoad)
	}
	if got := reloaded.History(time.Unix(0, 0))["claude-a.json"].Samples; len(got) != 1 {
		t.Fatalf("samples after overwrite=%+v", got)
	}
}

func TestQuotaHistoryPathForConfig(t *testing.T) {
	if got := QuotaHistoryPathForConfig(filepath.Join("home", "cfg", "config.yaml")); got != filepath.Join("home", "cfg", QuotaHistoryFileName) {
		t.Fatalf("path=%q", got)
	}
	if got := QuotaHistoryPathForConfig(" "); got != "" {
		t.Fatalf("empty config path=%q", got)
	}
}

func TestManagerMarkResultFeedsQuotaHistory(t *testing.T) {
	manager := NewManager(nil, nil, nil)
	recorder := NewQuotaHistoryRecorder("")
	manager.SetQuotaHistoryRecorder(recorder)
	auth, errRegister := manager.Register(context.Background(), &Auth{ID: "codex-history.json", Provider: "codex"})
	if errRegister != nil || auth == nil {
		t.Fatalf("Register() auth=%#v err=%v", auth, errRegister)
	}
	ctx := internallogging.WithResponseHeadersHolder(context.Background())
	internallogging.SetResponseHeaders(ctx, http.Header{
		"X-Codex-Primary-Used-Percent":   []string{"30"},
		"X-Codex-Primary-Window-Minutes": []string{"10080"},
		"X-Codex-Primary-Reset-At":       []string{"1782951970"},
	})
	manager.MarkResult(ctx, Result{AuthID: auth.ID, Provider: "codex", Model: "gpt-5.3-codex", Success: true})

	samples := recorder.History(time.Unix(0, 0))[auth.ID].Samples
	if len(samples) != 1 || samples[0].RemainingPercent != 70 || samples[0].ResetAt != 1_782_951_970 {
		t.Fatalf("recorded samples=%+v", samples)
	}

	// A result without quota headers records nothing new.
	manager.MarkResult(context.Background(), Result{AuthID: auth.ID, Provider: "codex", Model: "gpt-5.3-codex", Success: true})
	if got := recorder.History(time.Unix(0, 0))[auth.ID].Samples; len(got) != 1 {
		t.Fatalf("samples after header-less result=%+v", got)
	}
}
