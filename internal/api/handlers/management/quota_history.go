package management

import (
	"net/http"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

const defaultQuotaHistoryLookback = 14 * 24 * time.Hour

type quotaHistoryAccount struct {
	AuthID        string                        `json:"auth_id"`
	Provider      string                        `json:"provider"`
	Label         string                        `json:"label"`
	FileName      string                        `json:"file_name"`
	Disabled      bool                          `json:"disabled"`
	WindowMinutes int64                         `json:"window_minutes"`
	Samples       []coreauth.QuotaHistorySample `json:"samples"`
}

type quotaHistoryResponse struct {
	GeneratedAt int64                 `json:"generated_at"`
	Accounts    []quotaHistoryAccount `json:"accounts"`
}

// GetQuotaHistory returns recorded weekly quota samples for every Claude and
// Codex credential, plus credentials that no longer exist but still have
// history within retention.
func (h *Handler) GetQuotaHistory(c *gin.Context) {
	now := time.Now()
	since := now.Add(-defaultQuotaHistoryLookback)
	if raw := strings.TrimSpace(c.Query("since")); raw != "" {
		seconds, errParse := strconv.ParseInt(raw, 10, 64)
		if errParse != nil || seconds < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "since must be Unix seconds"})
			return
		}
		since = time.Unix(seconds, 0)
	}

	var history map[string]coreauth.QuotaHistoryEntry
	var auths []*coreauth.Auth
	if h.authManager != nil {
		history = h.authManager.QuotaHistoryRecorder().History(since)
		auths = h.authManager.List()
	}

	accounts := make([]quotaHistoryAccount, 0, len(auths)+len(history))
	seen := make(map[string]struct{}, len(auths))
	for _, auth := range auths {
		if auth == nil || !coreauth.QuotaHistorySupportsProvider(auth.Provider) {
			continue
		}
		seen[auth.ID] = struct{}{}
		account := quotaHistoryAccount{
			AuthID:   auth.ID,
			Provider: strings.ToLower(strings.TrimSpace(auth.Provider)),
			Label:    coreauth.QuotaHistoryLabel(auth),
			Disabled: auth.Disabled || auth.Status == coreauth.StatusDisabled,
			Samples:  []coreauth.QuotaHistorySample{},
		}
		if fileName := strings.TrimSpace(auth.FileName); fileName != "" {
			account.FileName = filepath.Base(fileName)
		}
		if entry, ok := history[auth.ID]; ok {
			account.WindowMinutes = entry.WindowMinutes
			if entry.Samples != nil {
				account.Samples = entry.Samples
			}
		}
		if account.WindowMinutes == 0 && account.Provider == "claude" {
			account.WindowMinutes = 7 * 24 * 60
		}
		accounts = append(accounts, account)
	}
	for id, entry := range history {
		if _, ok := seen[id]; ok {
			continue
		}
		samples := entry.Samples
		if samples == nil {
			samples = []coreauth.QuotaHistorySample{}
		}
		accounts = append(accounts, quotaHistoryAccount{
			AuthID:        id,
			Provider:      entry.Provider,
			Label:         entry.Label,
			FileName:      entry.FileName,
			Disabled:      true,
			WindowMinutes: entry.WindowMinutes,
			Samples:       samples,
		})
	}
	sort.Slice(accounts, func(i, j int) bool {
		if accounts[i].Provider != accounts[j].Provider {
			return accounts[i].Provider < accounts[j].Provider
		}
		return accounts[i].AuthID < accounts[j].AuthID
	})

	c.JSON(http.StatusOK, quotaHistoryResponse{GeneratedAt: now.Unix(), Accounts: accounts})
}
