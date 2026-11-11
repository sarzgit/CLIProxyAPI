package management

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestGetQuotaHistoryResponseShape(t *testing.T) {
	gin.SetMode(gin.TestMode)
	manager := coreauth.NewManager(nil, nil, nil)
	recorder := coreauth.NewQuotaHistoryRecorder("")
	manager.SetQuotaHistoryRecorder(recorder)

	now := time.Now().Truncate(time.Second)
	for _, auth := range []*coreauth.Auth{
		{ID: "claude-a.json", FileName: "claude-a.json", Provider: "claude", Metadata: map[string]any{"email": "a@example.com"}},
		{ID: "codex-b.json", FileName: "codex-b.json", Provider: "codex", Disabled: true, Status: coreauth.StatusDisabled},
		{ID: "gemini-c.json", FileName: "gemini-c.json", Provider: "gemini"},
	} {
		if _, errRegister := manager.Register(context.Background(), auth); errRegister != nil {
			t.Fatalf("Register(%s) error = %v", auth.ID, errRegister)
		}
	}
	claudeSignals := func(utilization string) map[string]string {
		return map[string]string{
			"Anthropic-Ratelimit-Unified-7d-Utilization": utilization,
			"Anthropic-Ratelimit-Unified-7d-Reset":       strconv.FormatInt(now.Add(72*time.Hour).Unix(), 10),
		}
	}
	claude := &coreauth.Auth{ID: "claude-a.json", FileName: "claude-a.json", Provider: "claude", Metadata: map[string]any{"email": "a@example.com"}}
	for _, point := range []struct {
		age         time.Duration
		utilization string
	}{{3 * time.Hour, "0.10"}, {2 * time.Hour, "0.20"}, {time.Hour, "0.30"}} {
		claude.Quota = coreauth.QuotaState{ObservedAt: now.Add(-point.age), Signals: claudeSignals(point.utilization)}
		recorder.Observe(claude)
	}
	recorder.Observe(&coreauth.Auth{
		ID: "codex-gone.json", FileName: "codex-gone.json", Provider: "codex", Label: "gone@example.com",
		Quota: coreauth.QuotaState{ObservedAt: now.Add(-time.Hour), Signals: map[string]string{
			"X-Codex-Primary-Used-Percent":   "5",
			"X-Codex-Primary-Window-Minutes": "10080",
		}},
	})

	recorder.Drain()

	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	since := now.Add(-90 * time.Minute).Unix()
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/quota/history?since="+strconv.FormatInt(since, 10), nil)
	h.GetQuotaHistory(ctx)
	if rec.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}

	var payload struct {
		GeneratedAt int64 `json:"generated_at"`
		Accounts    []struct {
			AuthID        string          `json:"auth_id"`
			Provider      string          `json:"provider"`
			Label         string          `json:"label"`
			FileName      string          `json:"file_name"`
			Disabled      bool            `json:"disabled"`
			WindowMinutes int64           `json:"window_minutes"`
			Samples       json.RawMessage `json:"samples"`
		} `json:"accounts"`
	}
	if errUnmarshal := json.Unmarshal(rec.Body.Bytes(), &payload); errUnmarshal != nil {
		t.Fatalf("unmarshal: %v body=%s", errUnmarshal, rec.Body.String())
	}
	if payload.GeneratedAt < now.Unix() {
		t.Fatalf("generated_at=%d, want >= %d", payload.GeneratedAt, now.Unix())
	}
	if len(payload.Accounts) != 3 {
		t.Fatalf("accounts=%s", rec.Body.String())
	}
	claudeAccount, codexAccount, goneAccount := payload.Accounts[0], payload.Accounts[1], payload.Accounts[2]
	if claudeAccount.AuthID != "claude-a.json" || claudeAccount.Provider != "claude" || claudeAccount.Label != "a@example.com" ||
		claudeAccount.FileName != "claude-a.json" || claudeAccount.Disabled || claudeAccount.WindowMinutes != 10080 {
		t.Fatalf("claude account=%+v", claudeAccount)
	}
	var claudeSamples []coreauth.QuotaHistorySample
	if errUnmarshal := json.Unmarshal(claudeAccount.Samples, &claudeSamples); errUnmarshal != nil {
		t.Fatal(errUnmarshal)
	}
	// Of the 3h and 2h samples before since, only the newest (2h) is kept.
	if len(claudeSamples) != 2 || claudeSamples[0].T != now.Add(-2*time.Hour).Unix() || claudeSamples[0].RemainingPercent != 80 ||
		claudeSamples[1].RemainingPercent != 70 || claudeSamples[1].ResetAt != now.Add(72*time.Hour).Unix() {
		t.Fatalf("claude samples=%+v", claudeSamples)
	}

	if codexAccount.AuthID != "codex-b.json" || !codexAccount.Disabled || string(codexAccount.Samples) != "[]" || codexAccount.Label != "codex-b.json" {
		t.Fatalf("codex account=%+v samples=%s", codexAccount, codexAccount.Samples)
	}
	if goneAccount.AuthID != "codex-gone.json" || !goneAccount.Disabled || goneAccount.Label != "gone@example.com" ||
		goneAccount.WindowMinutes != 10080 || goneAccount.FileName != "codex-gone.json" {
		t.Fatalf("historical account=%+v", goneAccount)
	}
}

func TestGetQuotaHistoryRejectsInvalidSince(t *testing.T) {
	gin.SetMode(gin.TestMode)
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, coreauth.NewManager(nil, nil, nil))
	rec := httptest.NewRecorder()
	ctx, _ := gin.CreateTestContext(rec)
	ctx.Request = httptest.NewRequest(http.MethodGet, "/v8/management/quota/history?since=yesterday", nil)
	h.GetQuotaHistory(ctx)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}
