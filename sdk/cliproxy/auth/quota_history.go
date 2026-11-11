package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

const (
	// QuotaHistoryFileName is the history file stored beside the config file.
	QuotaHistoryFileName = "quota-history.json"

	// claudeWeeklyWindowMinutes is the length of Claude's unified 7-day window.
	claudeWeeklyWindowMinutes = 7 * 24 * 60

	quotaHistoryRetention       = 30 * 24 * time.Hour
	quotaHistoryMaxSamples      = 20000
	quotaHistorySampleInterval  = 5 * time.Minute
	quotaHistoryRemainingDelta  = 0.5
	quotaHistoryResetTolerance  = 60 // seconds; absorbs jitter from relative resets
	quotaHistoryFlushInterval   = 60 * time.Second
	quotaHistoryFileVersion     = 1
	quotaHistoryPercentDecimals = 100 // round remaining_percent to two decimals
)

// QuotaHistorySample is one recorded point of a credential's weekly quota.
type QuotaHistorySample struct {
	// T is the observation time in Unix seconds.
	T int64 `json:"t"`
	// RemainingPercent is the share of the weekly window still unused, 0..100.
	RemainingPercent float64 `json:"remaining_percent"`
	// ResetAt is when the weekly window resets in Unix seconds, or 0 if unknown.
	ResetAt int64 `json:"reset_at"`
}

// QuotaHistoryEntry is the recorded history of one credential.
type QuotaHistoryEntry struct {
	AuthID        string               `json:"-"`
	Provider      string               `json:"provider"`
	Label         string               `json:"label"`
	FileName      string               `json:"file_name"`
	WindowMinutes int64                `json:"window_minutes"`
	Samples       []QuotaHistorySample `json:"samples"`
}

type quotaHistoryFile struct {
	Version  int                           `json:"version"`
	Accounts map[string]*QuotaHistoryEntry `json:"accounts"`
}

// QuotaHistoryRecorder keeps a bounded time series of each Claude and Codex
// credential's remaining weekly quota, built from passive response headers.
//
// Observe is called on the request path and only parses a few headers and
// appends under a mutex. Disk writes happen on a background loop (at most
// once per flush interval while dirty) and on Close.
type QuotaHistoryRecorder struct {
	path    string
	nowFunc func() time.Time

	mu      sync.Mutex
	entries map[string]*QuotaHistoryEntry
	dirty   bool

	writeMu sync.Mutex

	loopMu     sync.Mutex
	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

// NewQuotaHistoryRecorder creates a recorder that persists to path. An empty
// path keeps history in memory only.
func NewQuotaHistoryRecorder(path string) *QuotaHistoryRecorder {
	return &QuotaHistoryRecorder{
		path:    strings.TrimSpace(path),
		entries: make(map[string]*QuotaHistoryEntry),
	}
}

// QuotaHistoryPathForConfig returns the history file path for a config file path.
func QuotaHistoryPathForConfig(configPath string) string {
	configPath = strings.TrimSpace(configPath)
	if configPath == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(configPath), QuotaHistoryFileName)
}

func (r *QuotaHistoryRecorder) now() time.Time {
	if r.nowFunc != nil {
		return r.nowFunc()
	}
	return time.Now()
}

// Load replaces in-memory history with the persisted file. A missing file is
// not an error. A corrupt file is logged and ignored so startup continues with
// empty history; it is overwritten by the next flush.
func (r *QuotaHistoryRecorder) Load() error {
	if r == nil || r.path == "" {
		return nil
	}
	data, errRead := os.ReadFile(r.path)
	if errRead != nil {
		if errors.Is(errRead, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("read quota history %s: %w", r.path, errRead)
	}
	var file quotaHistoryFile
	if len(strings.TrimSpace(string(data))) > 0 {
		if errUnmarshal := json.Unmarshal(data, &file); errUnmarshal != nil {
			log.WithError(errUnmarshal).Warnf("ignoring corrupt quota history file %s", r.path)
			return nil
		}
	}
	cutoff := r.now().Add(-quotaHistoryRetention).Unix()
	entries := make(map[string]*QuotaHistoryEntry, len(file.Accounts))
	for id, entry := range file.Accounts {
		id = strings.TrimSpace(id)
		if id == "" || entry == nil {
			continue
		}
		entry.AuthID = id
		sort.SliceStable(entry.Samples, func(i, j int) bool { return entry.Samples[i].T < entry.Samples[j].T })
		entry.Samples = pruneQuotaHistorySamples(entry.Samples, cutoff)
		if len(entry.Samples) == 0 {
			continue
		}
		entries[id] = entry
	}
	r.mu.Lock()
	r.entries = entries
	r.dirty = false
	r.mu.Unlock()
	return nil
}

// Observe records a sample from the auth's latest quota observation when it
// differs enough from the previous sample. It never blocks on disk I/O.
func (r *QuotaHistoryRecorder) Observe(auth *Auth) {
	if r == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	sample, windowMinutes, ok := quotaHistorySampleFromAuth(auth)
	if !ok {
		return
	}
	provider := strings.ToLower(strings.TrimSpace(auth.Provider))
	label := QuotaHistoryLabel(auth)
	fileName := quotaHistoryFileName(auth)
	cutoff := r.now().Add(-quotaHistoryRetention).Unix()

	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[auth.ID]
	if entry == nil {
		entry = &QuotaHistoryEntry{AuthID: auth.ID}
		r.entries[auth.ID] = entry
	}
	if entry.Provider != provider || entry.Label != label || entry.FileName != fileName || entry.WindowMinutes != windowMinutes {
		entry.Provider, entry.Label, entry.FileName, entry.WindowMinutes = provider, label, fileName, windowMinutes
		r.dirty = true
	}
	if !shouldRecordQuotaHistorySample(entry.Samples, sample) {
		return
	}
	entry.Samples = append(entry.Samples, sample)
	entry.Samples = pruneQuotaHistorySamples(entry.Samples, cutoff)
	r.dirty = true
}

func shouldRecordQuotaHistorySample(samples []QuotaHistorySample, next QuotaHistorySample) bool {
	if len(samples) == 0 {
		return true
	}
	last := samples[len(samples)-1]
	if next.T < last.T {
		return false
	}
	if next.T-last.T >= int64(quotaHistorySampleInterval/time.Second) {
		return true
	}
	if math.Abs(next.RemainingPercent-last.RemainingPercent) >= quotaHistoryRemainingDelta {
		return true
	}
	return quotaHistoryResetChanged(last.ResetAt, next.ResetAt)
}

func quotaHistoryResetChanged(previous, next int64) bool {
	if (previous == 0) != (next == 0) {
		return true
	}
	diff := next - previous
	if diff < 0 {
		diff = -diff
	}
	return diff > quotaHistoryResetTolerance
}

// pruneQuotaHistorySamples drops samples older than cutoff and keeps at most
// quotaHistoryMaxSamples of the newest. samples must be sorted by T.
func pruneQuotaHistorySamples(samples []QuotaHistorySample, cutoff int64) []QuotaHistorySample {
	start := sort.Search(len(samples), func(i int) bool { return samples[i].T >= cutoff })
	if over := len(samples) - start - quotaHistoryMaxSamples; over > 0 {
		start += over
	}
	if start == 0 {
		return samples
	}
	return append(samples[:0], samples[start:]...)
}

// History returns a copy of every entry within retention. Each entry keeps
// samples with T >= since plus the newest sample before since, ascending.
func (r *QuotaHistoryRecorder) History(since time.Time) map[string]QuotaHistoryEntry {
	out := make(map[string]QuotaHistoryEntry)
	if r == nil {
		return out
	}
	sinceUnix := since.Unix()
	cutoff := r.now().Add(-quotaHistoryRetention).Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	for id, entry := range r.entries {
		if entry == nil {
			continue
		}
		start := sort.Search(len(entry.Samples), func(i int) bool { return entry.Samples[i].T >= sinceUnix })
		if start > 0 {
			start--
		}
		samples := make([]QuotaHistorySample, 0, len(entry.Samples)-start)
		for _, sample := range entry.Samples[start:] {
			if sample.T >= cutoff {
				samples = append(samples, sample)
			}
		}
		if len(samples) == 0 {
			// Nothing within retention: the entry is dropped on the next flush.
			continue
		}
		copied := *entry
		copied.AuthID = id
		copied.Samples = samples
		out[id] = copied
	}
	return out
}

// Start launches the debounced persistence loop. It stops when ctx ends or
// Close is called.
func (r *QuotaHistoryRecorder) Start(ctx context.Context) {
	if r == nil || r.path == "" {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.loopMu.Lock()
	defer r.loopMu.Unlock()
	if r.loopCancel != nil {
		return
	}
	loopCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	r.loopCancel = cancel
	r.loopDone = done
	go func() {
		defer close(done)
		ticker := time.NewTicker(quotaHistoryFlushInterval)
		defer ticker.Stop()
		for {
			select {
			case <-loopCtx.Done():
				return
			case <-ticker.C:
				if errFlush := r.Flush(); errFlush != nil {
					log.WithError(errFlush).Warn("failed to persist quota history")
				}
			}
		}
	}()
}

// Close stops the persistence loop and writes any pending history.
func (r *QuotaHistoryRecorder) Close() error {
	if r == nil {
		return nil
	}
	r.loopMu.Lock()
	cancel, done := r.loopCancel, r.loopDone
	r.loopCancel, r.loopDone = nil, nil
	r.loopMu.Unlock()
	if cancel != nil {
		cancel()
		<-done
	}
	return r.Flush()
}

// Flush writes history to disk if it changed since the last write.
func (r *QuotaHistoryRecorder) Flush() error {
	if r == nil || r.path == "" {
		return nil
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	cutoff := r.now().Add(-quotaHistoryRetention).Unix()
	r.mu.Lock()
	if !r.dirty {
		r.mu.Unlock()
		return nil
	}
	file := quotaHistoryFile{Version: quotaHistoryFileVersion, Accounts: make(map[string]*QuotaHistoryEntry, len(r.entries))}
	for id, entry := range r.entries {
		if entry == nil {
			delete(r.entries, id)
			continue
		}
		entry.Samples = pruneQuotaHistorySamples(entry.Samples, cutoff)
		if len(entry.Samples) == 0 {
			delete(r.entries, id)
			continue
		}
		copied := *entry
		copied.Samples = append([]QuotaHistorySample(nil), entry.Samples...)
		file.Accounts[id] = &copied
	}
	r.dirty = false
	r.mu.Unlock()

	if errWrite := writeQuotaHistoryFile(r.path, file); errWrite != nil {
		r.mu.Lock()
		r.dirty = true
		r.mu.Unlock()
		return errWrite
	}
	return nil
}

func writeQuotaHistoryFile(path string, file quotaHistoryFile) error {
	data, errMarshal := json.Marshal(file)
	if errMarshal != nil {
		return fmt.Errorf("marshal quota history: %w", errMarshal)
	}
	dir := filepath.Dir(path)
	if errMkdir := os.MkdirAll(dir, 0o700); errMkdir != nil {
		return fmt.Errorf("create quota history directory: %w", errMkdir)
	}
	tmpFile, errCreate := os.CreateTemp(dir, filepath.Base(path)+".*.tmp")
	if errCreate != nil {
		return fmt.Errorf("create quota history temp file: %w", errCreate)
	}
	tmp := tmpFile.Name()
	if errChmod := tmpFile.Chmod(0o600); errChmod != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("chmod quota history temp file: %w", errChmod)
	}
	if _, errWrite := tmpFile.Write(data); errWrite != nil {
		_ = tmpFile.Close()
		_ = os.Remove(tmp)
		return fmt.Errorf("write quota history temp file: %w", errWrite)
	}
	if errClose := tmpFile.Close(); errClose != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("close quota history temp file: %w", errClose)
	}
	if errRename := os.Rename(tmp, path); errRename != nil {
		_ = os.Remove(tmp)
		return fmt.Errorf("replace quota history file: %w", errRename)
	}
	return nil
}

// QuotaHistorySupportsProvider reports whether weekly quota history is recorded for provider.
func QuotaHistorySupportsProvider(provider string) bool {
	switch strings.ToLower(strings.TrimSpace(provider)) {
	case "claude", "codex":
		return true
	default:
		return false
	}
}

// quotaHistorySampleFromAuth extracts the weekly window sample from the auth's
// latest quota observation.
func quotaHistorySampleFromAuth(auth *Auth) (QuotaHistorySample, int64, bool) {
	if auth == nil || len(auth.Quota.Signals) == 0 || auth.Quota.ObservedAt.IsZero() {
		return QuotaHistorySample{}, 0, false
	}
	signals := auth.Quota.Signals
	observedAt := auth.Quota.ObservedAt
	switch strings.ToLower(strings.TrimSpace(auth.Provider)) {
	case "claude":
		utilization, errParse := strconv.ParseFloat(quotaSignal(signals, "Anthropic-Ratelimit-Unified-7d-Utilization"), 64)
		if errParse != nil || math.IsNaN(utilization) || math.IsInf(utilization, 0) {
			return QuotaHistorySample{}, 0, false
		}
		// The header is a 0..1 fraction; treat larger values as a percentage.
		usedPercent := utilization * 100
		if utilization > 1 {
			usedPercent = utilization
		}
		sample := QuotaHistorySample{
			T:                observedAt.Unix(),
			RemainingPercent: remainingFromUsedPercent(usedPercent),
		}
		if reset, ok := parseResetTimestamp(quotaSignal(signals, "Anthropic-Ratelimit-Unified-7d-Reset")); ok {
			sample.ResetAt = reset.Unix()
		}
		return sample, claudeWeeklyWindowMinutes, true
	case "codex":
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
			return QuotaHistorySample{}, 0, false
		}
		usedPercent, errUsed := strconv.ParseFloat(quotaSignal(signals, longestPrefix+"Used-Percent"), 64)
		if errUsed != nil || math.IsNaN(usedPercent) || math.IsInf(usedPercent, 0) {
			return QuotaHistorySample{}, 0, false
		}
		sample := QuotaHistorySample{
			T:                observedAt.Unix(),
			RemainingPercent: remainingFromUsedPercent(usedPercent),
		}
		if reset, ok := parseResetTimestamp(quotaSignal(signals, longestPrefix+"Reset-At")); ok {
			sample.ResetAt = reset.Unix()
		} else if seconds, errSeconds := strconv.ParseFloat(quotaSignal(signals, longestPrefix+"Reset-After-Seconds"), 64); errSeconds == nil && seconds >= 0 && !math.IsInf(seconds, 0) {
			sample.ResetAt = observedAt.Add(time.Duration(seconds * float64(time.Second))).Unix()
		}
		return sample, longestMinutes, true
	default:
		return QuotaHistorySample{}, 0, false
	}
}

func remainingFromUsedPercent(used float64) float64 {
	remaining := 100 - math.Max(0, math.Min(100, used))
	return math.Round(remaining*quotaHistoryPercentDecimals) / quotaHistoryPercentDecimals
}

// QuotaHistoryLabel returns a human label for a credential: its email or
// account label when present, otherwise its file name, otherwise its ID.
// API keys are never returned.
func QuotaHistoryLabel(auth *Auth) string {
	if auth == nil {
		return ""
	}
	if auth.Metadata != nil {
		if email, ok := auth.Metadata["email"].(string); ok && strings.TrimSpace(email) != "" {
			return strings.TrimSpace(email)
		}
	}
	for _, key := range []string{"email", "account_email"} {
		if value := strings.TrimSpace(auth.Attributes[key]); value != "" {
			return value
		}
	}
	if label := strings.TrimSpace(auth.Label); label != "" {
		return label
	}
	if fileName := quotaHistoryFileName(auth); fileName != "" {
		return fileName
	}
	return auth.ID
}

func quotaHistoryFileName(auth *Auth) string {
	if auth == nil {
		return ""
	}
	fileName := strings.TrimSpace(auth.FileName)
	if fileName == "" {
		return ""
	}
	return filepath.Base(fileName)
}

// SetQuotaHistoryRecorder installs the recorder fed by quota observations.
// Passing nil disables recording.
func (m *Manager) SetQuotaHistoryRecorder(recorder *QuotaHistoryRecorder) {
	if m == nil {
		return
	}
	m.quotaHistory.Store(recorder)
}

// QuotaHistoryRecorder returns the installed recorder, or nil.
func (m *Manager) QuotaHistoryRecorder() *QuotaHistoryRecorder {
	if m == nil {
		return nil
	}
	return m.quotaHistory.Load()
}
