package auth

import (
	"context"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/MoeForge/moego/ojson"
	log "github.com/sirupsen/logrus"
)

const (
	// QuotaHistoryFileName is the history file stored beside the config file.
	QuotaHistoryFileName = "quota-history.json"

	// weeklyWindowMinutes is the length of the Claude and Codex weekly windows.
	weeklyWindowMinutes = 7 * 24 * 60
	// weeklyWindowToleranceMinutes lets a Codex window slightly shorter than a
	// week still count as the weekly window.
	weeklyWindowToleranceMinutes = 60

	quotaHistoryRetention       = 30 * 24 * time.Hour
	quotaHistoryMaxSamples      = 20000
	quotaHistorySampleInterval  = 5 * time.Minute
	quotaHistoryRemainingDelta  = 0.5
	quotaHistoryResetTolerance  = 60 // seconds; absorbs jitter from relative resets
	quotaHistoryFlushInterval   = 60 * time.Second
	quotaHistoryFileVersion     = 1
	quotaHistoryPercentDecimals = 100 // round remaining_percent to two decimals
	quotaHistoryQueueSize       = 1024
)

// errQuotaHistoryWriteBlocked marks a flush skipped because the existing
// history file could not be read; writing would overwrite it.
var errQuotaHistoryWriteBlocked = errors.New("quota history file unreadable; writes paused")

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

// quotaHistoryObservation is the small, already parsed value handed from the
// request path to the recorder goroutine.
type quotaHistoryObservation struct {
	authID        string
	provider      string
	label         string
	fileName      string
	windowMinutes int64
	sample        QuotaHistorySample
}

// QuotaHistoryRecorder keeps a bounded time series of each Claude and Codex
// credential's remaining weekly quota, built from passive response headers.
//
// Observe runs on the request path: it parses a few headers and does a
// non-blocking send to a bounded queue, dropping the observation when the
// queue is full. A single goroutine started by Start applies queued
// observations and writes the file at most once per flush interval.
type QuotaHistoryRecorder struct {
	path    string
	nowFunc func() time.Time

	queue       chan quotaHistoryObservation
	dropped     atomic.Uint64
	wake        chan struct{}
	closed      atomic.Bool
	admissionMu sync.RWMutex
	drainMu     sync.Mutex

	mu           sync.Mutex
	entries      map[string]*QuotaHistoryEntry
	dirty        bool
	writeBlocked bool

	writeMu sync.Mutex

	loopMu     sync.Mutex
	loopCancel context.CancelFunc
	loopDone   chan struct{}
}

// NewQuotaHistoryRecorder creates a recorder that persists to path. An empty
// path keeps history in memory only.
func NewQuotaHistoryRecorder(path string) *QuotaHistoryRecorder {
	return &QuotaHistoryRecorder{
		path:         strings.TrimSpace(path),
		queue:        make(chan quotaHistoryObservation, quotaHistoryQueueSize),
		wake:         make(chan struct{}, 1),
		writeBlocked: true,
		entries:      make(map[string]*QuotaHistoryEntry),
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

// Load merges the persisted file into memory. A missing file is not an error.
// A corrupt file is moved aside to quota-history.json.corrupt-<timestamp>-* and
// history starts empty. Any other read failure is returned and pauses writes,
// so a file that could not be read is never overwritten; a later Load or
// Flush retries the read.
func (r *QuotaHistoryRecorder) Load() error {
	if r == nil || r.path == "" {
		return nil
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()
	return r.load()
}

// load requires writeMu, serializing recovery with atomic file replacement.
func (r *QuotaHistoryRecorder) load() error {
	persisted, errRead := r.readFile()
	r.mu.Lock()
	defer r.mu.Unlock()
	if errRead != nil {
		r.writeBlocked = true
		return errRead
	}
	r.writeBlocked = false
	cutoff := r.now().Add(-quotaHistoryRetention).Unix()
	for id, stored := range persisted {
		current := r.entries[id]
		if current == nil {
			r.entries[id] = stored
			continue
		}
		current.Samples = mergeQuotaHistorySamples(stored.Samples, current.Samples)
		r.dirty = true
	}
	if r.pruneLocked(cutoff) {
		r.dirty = true
	}
	return nil
}

// readFile reads and parses the history file. It returns nil entries for a
// missing or empty file and moves a corrupt file aside.
func (r *QuotaHistoryRecorder) readFile() (map[string]*QuotaHistoryEntry, error) {
	data, errRead := os.ReadFile(r.path)
	if errRead != nil {
		if errors.Is(errRead, os.ErrNotExist) {
			return nil, nil
		}
		return nil, fmt.Errorf("read quota history %s: %w", r.path, errRead)
	}
	if len(strings.TrimSpace(string(data))) == 0 {
		return nil, nil
	}
	file, errDecode := decodeQuotaHistoryFile(data)
	if errDecode != nil {
		backup, errCreate := os.CreateTemp(filepath.Dir(r.path), filepath.Base(r.path)+".corrupt-"+r.now().UTC().Format("20060102T150405Z")+"-*")
		if errCreate != nil {
			return nil, fmt.Errorf("reserve corrupt quota history backup: %w", errCreate)
		}
		aside := backup.Name()
		if errClose := backup.Close(); errClose != nil {
			_ = os.Remove(aside)
			return nil, fmt.Errorf("close corrupt quota history backup: %w", errClose)
		}
		if errRename := os.Rename(r.path, aside); errRename != nil {
			_ = os.Remove(aside)
			return nil, fmt.Errorf("move corrupt quota history %s aside: %w", r.path, errRename)
		}
		log.WithError(errDecode).Warnf("quota history file was corrupt; moved to %s and starting empty", aside)
		return nil, nil
	}
	entries := make(map[string]*QuotaHistoryEntry, len(file.Accounts))
	for id, entry := range file.Accounts {
		id = strings.TrimSpace(id)
		if id == "" || entry == nil {
			continue
		}
		entry.AuthID = id
		sort.SliceStable(entry.Samples, func(i, j int) bool { return entry.Samples[i].T < entry.Samples[j].T })
		entries[id] = entry
	}
	return entries, nil
}

// decodeQuotaHistoryFile validates persisted fields before recovery can enable writes.
func decodeQuotaHistoryFile(data []byte) (quotaHistoryFile, error) {
	file := quotaHistoryFile{Accounts: make(map[string]*QuotaHistoryEntry)}
	if !ojson.ValidBytes(data) {
		return file, errors.New("invalid quota history JSON")
	}
	root := ojson.ParseBytes(data)
	if !root.IsObject() {
		return file, errors.New("invalid quota history object")
	}
	var decodeErr error
	integer := func(value ojson.Result) int64 {
		if !value.Exists() || value.Type == ojson.Null {
			return 0
		}
		n, errParse := strconv.ParseInt(value.Raw, 10, 64)
		if value.Type != ojson.Number || errParse != nil {
			decodeErr = errors.New("invalid quota history integer")
		}
		return n
	}
	text := func(value ojson.Result) string {
		if value.Exists() && value.Type != ojson.Null && value.Type != ojson.String {
			decodeErr = errors.New("invalid quota history string")
		}
		return value.Str
	}
	file.Version = int(integer(root.Get("version")))
	accounts := root.Get("accounts")
	if accounts.Exists() && accounts.Type != ojson.Null && !accounts.IsObject() {
		return file, errors.New("invalid quota history accounts")
	}
	accounts.ForEach(func(key, value ojson.Result) bool {
		if value.Type == ojson.Null {
			return true
		}
		if !value.IsObject() {
			decodeErr = errors.New("invalid quota history entry")
			return false
		}
		entry := &QuotaHistoryEntry{
			Provider: text(value.Get("provider")), Label: text(value.Get("label")),
			FileName: text(value.Get("file_name")), WindowMinutes: integer(value.Get("window_minutes")),
		}
		samples := value.Get("samples")
		if samples.Exists() && samples.Type != ojson.Null && !samples.IsArray() {
			decodeErr = errors.New("invalid quota history samples")
			return false
		}
		if samples.Type == ojson.Null {
			file.Accounts[key.String()] = entry
			return decodeErr == nil
		}
		samples.ForEach(func(_, point ojson.Result) bool {
			remaining := point.Get("remaining_percent")
			if !point.IsObject() || (remaining.Exists() && remaining.Type != ojson.Null && remaining.Type != ojson.Number) || math.IsInf(remaining.Float(), 0) {
				decodeErr = errors.New("invalid quota history sample")
				return false
			}
			entry.Samples = append(entry.Samples, QuotaHistorySample{
				T: integer(point.Get("t")), RemainingPercent: remaining.Float(), ResetAt: integer(point.Get("reset_at")),
			})
			return decodeErr == nil
		})
		file.Accounts[key.String()] = entry
		return decodeErr == nil
	})
	return file, decodeErr
}

// mergeQuotaHistorySamples combines two sorted sample lists, keeping one
// sample per timestamp (the newer list wins).
func mergeQuotaHistorySamples(older, newer []QuotaHistorySample) []QuotaHistorySample {
	merged := make([]QuotaHistorySample, 0, len(older)+len(newer))
	merged = append(merged, older...)
	merged = append(merged, newer...)
	sort.SliceStable(merged, func(i, j int) bool { return merged[i].T < merged[j].T })
	out := merged[:0]
	for _, sample := range merged {
		if n := len(out); n > 0 && out[n-1].T == sample.T {
			out[n-1] = sample
			continue
		}
		out = append(out, sample)
	}
	return out
}

// Observe parses the auth's latest quota observation and queues it for the
// recorder goroutine. It never blocks: when the queue is full the observation
// is dropped and counted.
func (r *QuotaHistoryRecorder) Observe(auth *Auth) {
	if r == nil || auth == nil || strings.TrimSpace(auth.ID) == "" {
		return
	}
	sample, windowMinutes, ok := quotaHistorySampleFromAuth(auth)
	if !ok {
		return
	}
	observation := quotaHistoryObservation{
		authID:        auth.ID,
		provider:      strings.ToLower(strings.TrimSpace(auth.Provider)),
		label:         QuotaHistoryLabel(auth),
		fileName:      quotaHistoryFileName(auth),
		windowMinutes: windowMinutes,
		sample:        sample,
	}
	// Shutdown holds the exclusive gate only long enough to stop admission.
	// Never wait on a history reader, disk writer or shutdown on this path.
	if !r.admissionMu.TryRLock() {
		r.dropped.Add(1)
		return
	}
	defer r.admissionMu.RUnlock()
	if r.closed.Load() {
		return
	}
	select {
	case r.queue <- observation:
		select {
		case r.wake <- struct{}{}:
		default:
		}
	default:
		r.dropped.Add(1)
	}
}

// Dropped returns observations rejected by a full queue or the shutdown gate.
func (r *QuotaHistoryRecorder) Dropped() uint64 {
	if r == nil {
		return 0
	}
	return r.dropped.Load()
}

// Drain applies the queued batch synchronously in receive order. Concurrent
// producers cannot extend the batch indefinitely; later arrivals wake the worker.
func (r *QuotaHistoryRecorder) Drain() {
	if r == nil {
		return
	}
	r.drainMu.Lock()
	defer r.drainMu.Unlock()
	for pending := len(r.queue); pending > 0; pending-- {
		select {
		case observation := <-r.queue:
			r.apply(observation)
		default:
			return
		}
	}
}

func (r *QuotaHistoryRecorder) apply(observation quotaHistoryObservation) {
	cutoff := r.now().Add(-quotaHistoryRetention).Unix()
	r.mu.Lock()
	defer r.mu.Unlock()
	entry := r.entries[observation.authID]
	if entry == nil {
		entry = &QuotaHistoryEntry{AuthID: observation.authID}
		r.entries[observation.authID] = entry
	}
	if entry.Provider != observation.provider || entry.Label != observation.label ||
		entry.FileName != observation.fileName || entry.WindowMinutes != observation.windowMinutes {
		entry.Provider, entry.Label = observation.provider, observation.label
		entry.FileName, entry.WindowMinutes = observation.fileName, observation.windowMinutes
		r.dirty = true
	}
	if !shouldRecordQuotaHistorySample(entry.Samples, observation.sample) {
		return
	}
	entry.Samples = append(entry.Samples, observation.sample)
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

// pruneLocked applies retention to every entry, removes empty entries and
// reports whether anything was removed. Must be called with r.mu held.
func (r *QuotaHistoryRecorder) pruneLocked(cutoff int64) bool {
	changed := false
	for id, entry := range r.entries {
		if entry == nil {
			delete(r.entries, id)
			changed = true
			continue
		}
		before := len(entry.Samples)
		entry.Samples = pruneQuotaHistorySamples(entry.Samples, cutoff)
		if len(entry.Samples) != before {
			changed = true
		}
		if len(entry.Samples) == 0 {
			delete(r.entries, id)
			changed = true
		}
	}
	return changed
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

// Start launches the recorder goroutine, which applies queued observations
// and periodically flushes. It stops when ctx ends or Close is called.
func (r *QuotaHistoryRecorder) Start(ctx context.Context) {
	if r == nil {
		return
	}
	if ctx == nil {
		ctx = context.Background()
	}
	r.loopMu.Lock()
	defer r.loopMu.Unlock()
	if r.closed.Load() || r.loopCancel != nil {
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
			case <-r.wake:
				r.Drain()
			case <-ticker.C:
				r.flushAndLog()
			}
		}
	}()
}

func (r *QuotaHistoryRecorder) flushAndLog() {
	errFlush := r.Flush()
	switch {
	case errFlush == nil:
	case errors.Is(errFlush, errQuotaHistoryWriteBlocked):
		log.WithError(errFlush).Debug("quota history not persisted")
	default:
		log.WithError(errFlush).Warn("failed to persist quota history")
	}
}

// Close stops the recorder goroutine, applies anything still queued and
// writes pending history.
func (r *QuotaHistoryRecorder) Close() error {
	if r == nil {
		return nil
	}
	r.loopMu.Lock()
	defer r.loopMu.Unlock()
	r.admissionMu.Lock()
	r.closed.Store(true)
	r.admissionMu.Unlock()
	cancel, done := r.loopCancel, r.loopDone
	if cancel != nil {
		cancel()
		<-done
	}
	r.Drain()
	return r.Flush()
}

// Flush applies retention and writes history to disk if anything changed.
// While the existing file is unreadable it retries the read and writes
// nothing until that succeeds.
func (r *QuotaHistoryRecorder) Flush() error {
	if r == nil {
		return nil
	}
	r.Drain()
	if r.path == "" {
		return nil
	}
	r.writeMu.Lock()
	defer r.writeMu.Unlock()

	r.mu.Lock()
	blocked := r.writeBlocked
	r.mu.Unlock()
	if blocked {
		if errLoad := r.load(); errLoad != nil {
			return fmt.Errorf("%w: %w", errQuotaHistoryWriteBlocked, errLoad)
		}
	}

	cutoff := r.now().Add(-quotaHistoryRetention).Unix()
	r.mu.Lock()
	if r.pruneLocked(cutoff) {
		r.dirty = true
	}
	if !r.dirty {
		r.mu.Unlock()
		return nil
	}
	file := quotaHistoryFile{Version: quotaHistoryFileVersion, Accounts: make(map[string]*QuotaHistoryEntry, len(r.entries))}
	for id, entry := range r.entries {
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
	doc := ojson.NewDocString(`{}`)
	if errSet := doc.Set("version", file.Version); errSet != nil {
		return fmt.Errorf("marshal quota history version: %w", errSet)
	}
	if errSet := doc.Set("accounts", file.Accounts); errSet != nil {
		return fmt.Errorf("marshal quota history accounts: %w", errSet)
	}
	data := doc.Bytes()
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
		// The header is always a fraction; values above 1 mean over the limit.
		sample := QuotaHistorySample{
			T:                observedAt.Unix(),
			RemainingPercent: remainingFromUsedPercent(utilization * 100),
		}
		if reset, ok := parseResetTimestamp(quotaSignal(signals, "Anthropic-Ratelimit-Unified-7d-Reset")); ok {
			sample.ResetAt = reset.Unix()
		}
		return sample, weeklyWindowMinutes, true
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
		// Only the weekly window is recorded; a response carrying only the
		// short (e.g. 300-minute) window is skipped.
		if longestPrefix == "" || longestMinutes < weeklyWindowMinutes-weeklyWindowToleranceMinutes {
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
