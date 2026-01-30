// Package hooks provides extensibility points for the ingestion pipeline.
package hooks

import (
	"context"
	"sort"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// --- Hook types ---

// PreDecodeHook is called before decoding starts.
type PreDecodeHook func(ctx context.Context, source core.Source) (core.Source, error)

// PostBatchHook is called after each batch is decoded.
type PostBatchHook func(ctx context.Context, batch arrow.Record) (arrow.Record, error)

// PreWriteHook is called before writing to sink.
type PreWriteHook func(ctx context.Context, batch arrow.Record, metadata map[string]string) error

// ProgressHook is called to report progress.
type ProgressHook func(progress Progress)

// ErrorHook is called when errors occur.
type ErrorHook func(err error, context ErrorContext)

// --- Priority entry types ---

// preDecodeEntry wraps a PreDecodeHook with priority and an optional condition.
type preDecodeEntry struct {
	Priority  int
	Condition func(context.Context) bool
	Hook      PreDecodeHook
}

// postBatchEntry wraps a PostBatchHook with priority and an optional condition.
type postBatchEntry struct {
	Priority  int
	Condition func(context.Context) bool
	Hook      PostBatchHook
}

// preWriteEntry wraps a PreWriteHook with priority and an optional condition.
type preWriteEntry struct {
	Priority  int
	Condition func(context.Context) bool
	Hook      PreWriteHook
}

// progressEntry wraps a ProgressHook with priority.
type progressEntry struct {
	Priority int
	Hook     ProgressHook
}

// errorEntry wraps an ErrorHook with priority.
type errorEntry struct {
	Priority int
	Hook     ErrorHook
}

// defaultPriority is used when hooks are registered through the simple Register* methods.
const defaultPriority = 100

// --- Data types ---

// Progress contains progress information.
type Progress struct {
	SourceID       string
	BytesRead      int64
	BytesTotal     int64
	RowsRead       int64
	BatchesRead    int
	FilesProcessed int
	FilesTotal     int
	Percent        float64
}

// ErrorContext provides context for errors.
type ErrorContext struct {
	SourceID  string
	RowNumber int64
	Column    string
	Phase     string // "decode", "transform", "write"
}

// --- Manager ---

// Manager manages pipeline hooks.
type Manager struct {
	mu sync.RWMutex

	preDecode  []preDecodeEntry
	postBatch  []postBatchEntry
	preWrite   []preWriteEntry
	onProgress []progressEntry
	onError    []errorEntry
}

// NewManager creates a hook manager.
func NewManager() *Manager {
	return &Manager{}
}

// --- Registration: simple (default priority, no condition) ---

// RegisterPreDecode adds a pre-decode hook with default priority and no condition.
func (m *Manager) RegisterPreDecode(hook PreDecodeHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preDecode = append(m.preDecode, preDecodeEntry{
		Priority: defaultPriority,
		Hook:     hook,
	})
}

// RegisterPostBatch adds a post-batch hook with default priority and no condition.
func (m *Manager) RegisterPostBatch(hook PostBatchHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.postBatch = append(m.postBatch, postBatchEntry{
		Priority: defaultPriority,
		Hook:     hook,
	})
}

// RegisterPreWrite adds a pre-write hook with default priority and no condition.
func (m *Manager) RegisterPreWrite(hook PreWriteHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preWrite = append(m.preWrite, preWriteEntry{
		Priority: defaultPriority,
		Hook:     hook,
	})
}

// RegisterProgress adds a progress hook with default priority.
func (m *Manager) RegisterProgress(hook ProgressHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onProgress = append(m.onProgress, progressEntry{
		Priority: defaultPriority,
		Hook:     hook,
	})
}

// RegisterError adds an error hook with default priority.
func (m *Manager) RegisterError(hook ErrorHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onError = append(m.onError, errorEntry{
		Priority: defaultPriority,
		Hook:     hook,
	})
}

// --- Registration: with priority and optional condition ---

// RegisterPreDecodeWithPriority adds a pre-decode hook with explicit priority and condition.
// Lower priority values execute first. If condition is non-nil and returns false for the
// given context, the hook is skipped.
func (m *Manager) RegisterPreDecodeWithPriority(hook PreDecodeHook, priority int, condition func(context.Context) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preDecode = append(m.preDecode, preDecodeEntry{
		Priority:  priority,
		Condition: condition,
		Hook:      hook,
	})
}

// RegisterPostBatchWithPriority adds a post-batch hook with explicit priority and condition.
func (m *Manager) RegisterPostBatchWithPriority(hook PostBatchHook, priority int, condition func(context.Context) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.postBatch = append(m.postBatch, postBatchEntry{
		Priority:  priority,
		Condition: condition,
		Hook:      hook,
	})
}

// RegisterPreWriteWithPriority adds a pre-write hook with explicit priority and condition.
func (m *Manager) RegisterPreWriteWithPriority(hook PreWriteHook, priority int, condition func(context.Context) bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preWrite = append(m.preWrite, preWriteEntry{
		Priority:  priority,
		Condition: condition,
		Hook:      hook,
	})
}

// RegisterProgressWithPriority adds a progress hook with explicit priority.
func (m *Manager) RegisterProgressWithPriority(hook ProgressHook, priority int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onProgress = append(m.onProgress, progressEntry{
		Priority: priority,
		Hook:     hook,
	})
}

// RegisterErrorWithPriority adds an error hook with explicit priority.
func (m *Manager) RegisterErrorWithPriority(hook ErrorHook, priority int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onError = append(m.onError, errorEntry{
		Priority: priority,
		Hook:     hook,
	})
}

// --- Execution ---

// RunPreDecode runs all pre-decode hooks sorted by priority (ascending).
// Hooks whose condition returns false are skipped.
func (m *Manager) RunPreDecode(ctx context.Context, source core.Source) (core.Source, error) {
	m.mu.RLock()
	entries := make([]preDecodeEntry, len(m.preDecode))
	copy(entries, m.preDecode)
	m.mu.RUnlock()

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Priority < entries[j].Priority
	})

	var err error
	for _, entry := range entries {
		if entry.Condition != nil && !entry.Condition(ctx) {
			continue
		}
		source, err = entry.Hook(ctx, source)
		if err != nil {
			return nil, err
		}
	}
	return source, nil
}

// RunPostBatch runs all post-batch hooks sorted by priority (ascending).
// Hooks whose condition returns false are skipped.
func (m *Manager) RunPostBatch(ctx context.Context, batch arrow.Record) (arrow.Record, error) {
	m.mu.RLock()
	entries := make([]postBatchEntry, len(m.postBatch))
	copy(entries, m.postBatch)
	m.mu.RUnlock()

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Priority < entries[j].Priority
	})

	var err error
	for _, entry := range entries {
		if entry.Condition != nil && !entry.Condition(ctx) {
			continue
		}
		batch, err = entry.Hook(ctx, batch)
		if err != nil {
			return nil, err
		}
	}
	return batch, nil
}

// RunPreWrite runs all pre-write hooks sorted by priority (ascending).
// Hooks whose condition returns false are skipped.
func (m *Manager) RunPreWrite(ctx context.Context, batch arrow.Record, metadata map[string]string) error {
	m.mu.RLock()
	entries := make([]preWriteEntry, len(m.preWrite))
	copy(entries, m.preWrite)
	m.mu.RUnlock()

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Priority < entries[j].Priority
	})

	for _, entry := range entries {
		if entry.Condition != nil && !entry.Condition(ctx) {
			continue
		}
		if err := entry.Hook(ctx, batch, metadata); err != nil {
			return err
		}
	}
	return nil
}

// ReportProgress reports progress to all hooks sorted by priority (ascending).
func (m *Manager) ReportProgress(progress Progress) {
	m.mu.RLock()
	entries := make([]progressEntry, len(m.onProgress))
	copy(entries, m.onProgress)
	m.mu.RUnlock()

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Priority < entries[j].Priority
	})

	for _, entry := range entries {
		entry.Hook(progress)
	}
}

// ReportError reports an error to all hooks sorted by priority (ascending).
func (m *Manager) ReportError(err error, ctx ErrorContext) {
	m.mu.RLock()
	entries := make([]errorEntry, len(m.onError))
	copy(entries, m.onError)
	m.mu.RUnlock()

	sort.SliceStable(entries, func(i, j int) bool {
		return entries[i].Priority < entries[j].Priority
	})

	for _, entry := range entries {
		entry.Hook(err, ctx)
	}
}

// Common hooks

// LoggingHook logs progress to a function.
func LoggingHook(logFn func(string, ...interface{})) ProgressHook {
	return func(p Progress) {
		logFn("progress: %.1f%% (%d rows, %d bytes)",
			p.Percent, p.RowsRead, p.BytesRead)
	}
}

// FilterHook filters batches based on a predicate.
func FilterHook(predicate func(arrow.Record) bool) PostBatchHook {
	return func(ctx context.Context, batch arrow.Record) (arrow.Record, error) {
		if predicate(batch) {
			return batch, nil
		}
		// Return empty batch - actual filtering would be more complex
		return batch, nil
	}
}

// MetadataHook adds metadata before writing.
func MetadataHook(extra map[string]string) PreWriteHook {
	return func(ctx context.Context, batch arrow.Record, metadata map[string]string) error {
		for k, v := range extra {
			metadata[k] = v
		}
		return nil
	}
}
