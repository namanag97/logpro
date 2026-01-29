// Package hooks provides transformation and security hooks for the pipeline.
// Hooks allow injecting custom logic at various points in the data processing flow.
package hooks

import (
	"context"
	"io"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
)

// HookManager manages all registered hooks.
type HookManager struct {
	mu sync.RWMutex

	preDecodeHooks  []PreDecodeHook
	postBatchHooks  []PostBatchHook
	preWriteHooks   []PreWriteHook
	postWriteHooks  []PostWriteHook
	errorHooks      []ErrorHook
}

// NewHookManager creates a new hook manager.
func NewHookManager() *HookManager {
	return &HookManager{}
}

// PreDecodeHook is called before decoding data from a source.
// Use cases: authentication, decryption, decompression, validation.
type PreDecodeHook func(ctx context.Context, info *SourceInfo) (context.Context, error)

// SourceInfo contains information about the data source.
type SourceInfo struct {
	Path        string
	Format      string
	SizeBytes   int64
	ContentType string
	Metadata    map[string]string
	Reader      io.Reader // May be wrapped by the hook
}

// PostBatchHook is called after each batch is decoded but before writing.
// Use cases: filtering, PII masking, enrichment, validation.
type PostBatchHook func(ctx context.Context, batch arrow.Record) (arrow.Record, error)

// PreWriteHook is called before writing data to the sink.
// Use cases: adding metadata, partitioning decisions, encryption.
type PreWriteHook func(ctx context.Context, info *WriteInfo) error

// WriteInfo contains information about the write operation.
type WriteInfo struct {
	Path        string
	Format      string
	Metadata    map[string]string // Writable - hooks can add metadata
	Compression string
	RowCount    int64
}

// PostWriteHook is called after data is written.
// Use cases: logging, notification, catalog registration.
type PostWriteHook func(ctx context.Context, result *WriteResult) error

// WriteResult contains the outcome of a write operation.
type WriteResult struct {
	Path      string
	SizeBytes int64
	RowCount  int64
	Duration  int64 // nanoseconds
	Metadata  map[string]string
}

// ErrorHook is called when an error occurs.
// Use cases: alerting, logging, recovery.
type ErrorHook func(ctx context.Context, err error, phase string) error

// RegisterPreDecode adds a pre-decode hook.
func (m *HookManager) RegisterPreDecode(hook PreDecodeHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preDecodeHooks = append(m.preDecodeHooks, hook)
}

// RegisterPostBatch adds a post-batch hook.
func (m *HookManager) RegisterPostBatch(hook PostBatchHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.postBatchHooks = append(m.postBatchHooks, hook)
}

// RegisterPreWrite adds a pre-write hook.
func (m *HookManager) RegisterPreWrite(hook PreWriteHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preWriteHooks = append(m.preWriteHooks, hook)
}

// RegisterPostWrite adds a post-write hook.
func (m *HookManager) RegisterPostWrite(hook PostWriteHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.postWriteHooks = append(m.postWriteHooks, hook)
}

// RegisterError adds an error hook.
func (m *HookManager) RegisterError(hook ErrorHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.errorHooks = append(m.errorHooks, hook)
}

// RunPreDecode executes all pre-decode hooks.
func (m *HookManager) RunPreDecode(ctx context.Context, info *SourceInfo) (context.Context, error) {
	m.mu.RLock()
	hooks := m.preDecodeHooks
	m.mu.RUnlock()

	for _, hook := range hooks {
		var err error
		ctx, err = hook(ctx, info)
		if err != nil {
			return ctx, err
		}
	}
	return ctx, nil
}

// RunPostBatch executes all post-batch hooks.
func (m *HookManager) RunPostBatch(ctx context.Context, batch arrow.Record) (arrow.Record, error) {
	m.mu.RLock()
	hooks := m.postBatchHooks
	m.mu.RUnlock()

	var err error
	for _, hook := range hooks {
		batch, err = hook(ctx, batch)
		if err != nil {
			return nil, err
		}
	}
	return batch, nil
}

// RunPreWrite executes all pre-write hooks.
func (m *HookManager) RunPreWrite(ctx context.Context, info *WriteInfo) error {
	m.mu.RLock()
	hooks := m.preWriteHooks
	m.mu.RUnlock()

	for _, hook := range hooks {
		if err := hook(ctx, info); err != nil {
			return err
		}
	}
	return nil
}

// RunPostWrite executes all post-write hooks.
func (m *HookManager) RunPostWrite(ctx context.Context, result *WriteResult) error {
	m.mu.RLock()
	hooks := m.postWriteHooks
	m.mu.RUnlock()

	for _, hook := range hooks {
		if err := hook(ctx, result); err != nil {
			return err
		}
	}
	return nil
}

// RunError executes all error hooks.
func (m *HookManager) RunError(ctx context.Context, err error, phase string) error {
	m.mu.RLock()
	hooks := m.errorHooks
	m.mu.RUnlock()

	for _, hook := range hooks {
		if hookErr := hook(ctx, err, phase); hookErr != nil {
			return hookErr
		}
	}
	return err
}

// Clear removes all registered hooks.
func (m *HookManager) Clear() {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.preDecodeHooks = nil
	m.postBatchHooks = nil
	m.preWriteHooks = nil
	m.postWriteHooks = nil
	m.errorHooks = nil
}

// --- Built-in hooks ---

// PIIMaskingHook creates a hook that masks PII columns.
func PIIMaskingHook(columns []string, maskChar rune) PostBatchHook {
	return func(ctx context.Context, batch arrow.Record) (arrow.Record, error) {
		// This is a placeholder - full implementation would use Arrow compute
		// to replace column values with masked versions
		return batch, nil
	}
}

// ColumnFilterHook creates a hook that removes specified columns.
func ColumnFilterHook(columnsToRemove []string) PostBatchHook {
	removeSet := make(map[string]bool)
	for _, col := range columnsToRemove {
		removeSet[col] = true
	}

	return func(ctx context.Context, batch arrow.Record) (arrow.Record, error) {
		// This is a placeholder - full implementation would create
		// a new record with filtered columns
		return batch, nil
	}
}

// MetadataHook creates a hook that adds custom metadata to writes.
func MetadataHook(metadata map[string]string) PreWriteHook {
	return func(ctx context.Context, info *WriteInfo) error {
		if info.Metadata == nil {
			info.Metadata = make(map[string]string)
		}
		for k, v := range metadata {
			info.Metadata[k] = v
		}
		return nil
	}
}

// LoggingHook creates a hook that logs write operations.
func LoggingHook(logger func(format string, args ...interface{})) PostWriteHook {
	return func(ctx context.Context, result *WriteResult) error {
		logger("Wrote %d rows to %s (%d bytes)", result.RowCount, result.Path, result.SizeBytes)
		return nil
	}
}

// ValidationHook creates a hook that validates batches.
func ValidationHook(validator func(arrow.Record) error) PostBatchHook {
	return func(ctx context.Context, batch arrow.Record) (arrow.Record, error) {
		if err := validator(batch); err != nil {
			return nil, err
		}
		return batch, nil
	}
}

// --- Progress tracking ---

// Progress contains progress information.
type Progress struct {
	RowsRead     int64
	RowsWritten  int64
	BytesRead    int64
	BytesWritten int64
	FilesRead    int
	FilesWritten int
	Errors       int64
	StartTime    int64 // Unix nano
	CurrentFile  string
}

// ProgressHook is called periodically with progress updates.
type ProgressHook func(progress Progress)

// ProgressTracker tracks and reports progress.
type ProgressTracker struct {
	mu       sync.Mutex
	progress Progress
	hook     ProgressHook
	interval int64 // Report every N rows
	counter  int64
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker(interval int64, hook ProgressHook) *ProgressTracker {
	return &ProgressTracker{
		interval: interval,
		hook:     hook,
	}
}

// AddRows increments the row counter and optionally reports progress.
func (t *ProgressTracker) AddRows(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.progress.RowsRead += n
	t.counter += n

	if t.counter >= t.interval && t.hook != nil {
		t.hook(t.progress)
		t.counter = 0
	}
}

// AddBytes increments the byte counter.
func (t *ProgressTracker) AddBytes(n int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.BytesRead += n
}

// SetCurrentFile sets the current file being processed.
func (t *ProgressTracker) SetCurrentFile(path string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.progress.CurrentFile = path
}

// GetProgress returns a copy of the current progress.
func (t *ProgressTracker) GetProgress() Progress {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.progress
}
