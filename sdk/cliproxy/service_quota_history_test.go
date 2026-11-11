package cliproxy

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestServiceQuotaHistoryShutdownPersistsPendingSamples(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	service := &Service{
		configPath:  filepath.Join(t.TempDir(), "config.yaml"),
		coreManager: coreauth.NewManager(nil, nil, nil),
	}
	service.startQuotaHistory(ctx)
	r := service.coreManager.QuotaHistoryRecorder()
	if r == nil {
		t.Fatal("recorder not attached")
	}
	// Run cancellation happens before server stop and recorder close.
	cancel()
	r.Observe(&coreauth.Auth{ID: "shutdown", Provider: "claude", Quota: coreauth.QuotaState{
		ObservedAt: time.Now(), Signals: map[string]string{"Anthropic-Ratelimit-Unified-7d-Utilization": "0.25"},
	}})
	if errShutdown := service.Shutdown(context.Background()); errShutdown != nil {
		t.Fatal(errShutdown)
	}
	if service.coreManager.QuotaHistoryRecorder() != nil {
		t.Fatal("recorder still attached after shutdown")
	}
	loaded := coreauth.NewQuotaHistoryRecorder(coreauth.QuotaHistoryPathForConfig(service.configPath))
	if errLoad := loaded.Load(); errLoad != nil {
		t.Fatal(errLoad)
	}
	if samples := loaded.History(time.Unix(0, 0))["shutdown"].Samples; len(samples) != 1 || samples[0].RemainingPercent != 75 {
		t.Fatalf("shutdown lost pending quota history: %+v", samples)
	}
}
