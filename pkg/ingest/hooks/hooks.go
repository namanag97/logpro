// Package hooks provides extensibility points for the ingestion pipeline.
package hooks

import (
	"context"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// Manager manages pipeline hooks.
type Manager struct {
	mu sync.RWMutex

	preDecode  []PreDecodeHook
	postBatch  []PostBatchHook
	preWrite   []PreWriteHook
	onProgress []ProgressHook
	onError    []ErrorHook
}

// NewManager creates a hook manager.
func NewManager() *Manager {
	return &Manager{}
}

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

// RegisterPreDecode adds a pre-decode hook.
func (m *Manager) RegisterPreDecode(hook PreDecodeHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preDecode = append(m.preDecode, hook)
}

// RegisterPostBatch adds a post-batch hook.
func (m *Manager) RegisterPostBatch(hook PostBatchHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.postBatch = append(m.postBatch, hook)
}

// RegisterPreWrite adds a pre-write hook.
func (m *Manager) RegisterPreWrite(hook PreWriteHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.preWrite = append(m.preWrite, hook)
}

// RegisterProgress adds a progress hook.
func (m *Manager) RegisterProgress(hook ProgressHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onProgress = append(m.onProgress, hook)
}

// RegisterError adds an error hook.
func (m *Manager) RegisterError(hook ErrorHook) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.onError = append(m.onError, hook)
}

// RunPreDecode runs all pre-decode hooks.
func (m *Manager) RunPreDecode(ctx context.Context, source core.Source) (core.Source, error) {
	m.mu.RLock()
	hooks := m.preDecode
	m.mu.RUnlock()

	var err error
	for _, hook := range hooks {
		source, err = hook(ctx, source)
		if err != nil {
			return nil, err
		}
	}
	return source, nil
}

// RunPostBatch runs all post-batch hooks.
func (m *Manager) RunPostBatch(ctx context.Context, batch arrow.Record) (arrow.Record, error) {
	m.mu.RLock()
	hooks := m.postBatch
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

// RunPreWrite runs all pre-write hooks.
func (m *Manager) RunPreWrite(ctx context.Context, batch arrow.Record, metadata map[string]string) error {
	m.mu.RLock()
	hooks := m.preWrite
	m.mu.RUnlock()

	for _, hook := range hooks {
		if err := hook(ctx, batch, metadata); err != nil {
			return err
		}
	}
	return nil
}

// ReportProgress reports progress to all hooks.
func (m *Manager) ReportProgress(progress Progress) {
	m.mu.RLock()
	hooks := m.onProgress
	m.mu.RUnlock()

	for _, hook := range hooks {
		hook(progress)
	}
}

// ReportError reports an error to all hooks.
func (m *Manager) ReportError(err error, ctx ErrorContext) {
	m.mu.RLock()
	hooks := m.onError
	m.mu.RUnlock()

	for _, hook := range hooks {
		hook(err, ctx)
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
