package ingest

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/v14/arrow"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/hooks"
	"github.com/logflow/logflow/pkg/ingest/schema"
)

// StreamingPipeline orchestrates the Arrow-native streaming flow:
// Source → Decoder → Arrow RecordBatches → Sink
type StreamingPipeline struct {
	// Components
	registry      *decoders.Registry
	schemaManager *schema.MasterSchema
	hooks         *hooks.Manager

	// Config
	batchSize    int
	errorPolicy  core.ErrorPolicy
	schemaPolicy schema.Policy
	maxErrors    int

	// Metrics
	rowsProcessed int64
	batchesProcessed int64
	errorsEncountered int64
	bytesProcessed int64
}

// NewStreamingPipeline creates a new streaming pipeline.
func NewStreamingPipeline() *StreamingPipeline {
	return &StreamingPipeline{
		registry:      decoders.DefaultRegistry,
		schemaManager: schema.NewMasterSchema(schema.PolicyMergeNullable),
		hooks:         hooks.NewManager(),
		batchSize:     8192,
		errorPolicy:   core.ErrorPolicySkip,
		schemaPolicy:  schema.PolicyMergeNullable,
		maxErrors:     1000,
	}
}

// StreamingOptions configures the streaming pipeline.
type StreamingOptions struct {
	// Schema
	Schema       *arrow.Schema
	SchemaPolicy schema.Policy

	// Batching
	BatchSize int

	// Error handling
	ErrorPolicy core.ErrorPolicy
	MaxErrors   int

	// Callbacks
	OnProgress func(hooks.Progress)
	OnError    func(error, hooks.ErrorContext)
	OnBatch    func(arrow.Record) (arrow.Record, error)
}

// DefaultStreamingOptions returns sensible defaults.
func DefaultStreamingOptions() StreamingOptions {
	return StreamingOptions{
		BatchSize:    8192,
		SchemaPolicy: schema.PolicyMergeNullable,
		ErrorPolicy:  core.ErrorPolicySkip,
		MaxErrors:    1000,
	}
}

// Stream streams data from source to sink via Arrow batches.
func (p *StreamingPipeline) Stream(ctx context.Context, source core.Source, sink core.Sink, opts StreamingOptions) (*StreamResult, error) {
	startTime := time.Now()

	// Reset metrics
	atomic.StoreInt64(&p.rowsProcessed, 0)
	atomic.StoreInt64(&p.batchesProcessed, 0)
	atomic.StoreInt64(&p.errorsEncountered, 0)
	atomic.StoreInt64(&p.bytesProcessed, 0)

	// Get decoder for format
	decoder, err := p.registry.Get(source.Format())
	if err != nil {
		return nil, fmt.Errorf("no decoder for format %s: %w", source.Format(), err)
	}

	// Run pre-decode hooks
	source, err = p.hooks.RunPreDecode(ctx, source)
	if err != nil {
		return nil, fmt.Errorf("pre-decode hook failed: %w", err)
	}

	// Set up decode options
	decodeOpts := core.DecodeOptions{
		Schema:      opts.Schema,
		BatchSize:   opts.BatchSize,
		ErrorPolicy: opts.ErrorPolicy,
		MaxErrors:   opts.MaxErrors,
	}

	// Start decoding
	batches, err := decoder.Decode(ctx, source, decodeOpts)
	if err != nil {
		return nil, fmt.Errorf("decode failed: %w", err)
	}

	// Get schema from first batch
	var arrowSchema *arrow.Schema
	var firstBatch core.DecodedBatch
	var gotFirst bool

	for batch := range batches {
		if batch.Batch != nil {
			arrowSchema = batch.Batch.Schema()
			firstBatch = batch
			gotFirst = true
			break
		}
	}

	if !gotFirst || arrowSchema == nil {
		return nil, fmt.Errorf("no data or schema")
	}

	// Apply schema policy
	if opts.Schema != nil {
		arrowSchema, err = schema.Merge(opts.Schema, arrowSchema, opts.SchemaPolicy)
		if err != nil {
			return nil, fmt.Errorf("schema merge failed: %w", err)
		}
	}

	// Open sink
	sinkOpts := core.DefaultSinkOptions()
	if err := sink.Open(ctx, arrowSchema, sinkOpts); err != nil {
		return nil, fmt.Errorf("sink open failed: %w", err)
	}

	// Process first batch
	if err := p.processBatch(ctx, sink, firstBatch, opts); err != nil {
		sink.Close(ctx)
		return nil, err
	}

	// Process remaining batches
	for batch := range batches {
		select {
		case <-ctx.Done():
			sink.Close(ctx)
			return nil, ctx.Err()
		default:
		}

		if err := p.processBatch(ctx, sink, batch, opts); err != nil {
			if opts.ErrorPolicy == core.ErrorPolicyStrict {
				sink.Close(ctx)
				return nil, err
			}
			atomic.AddInt64(&p.errorsEncountered, 1)
		}

		// Check error limit
		if opts.MaxErrors > 0 && atomic.LoadInt64(&p.errorsEncountered) >= int64(opts.MaxErrors) {
			sink.Close(ctx)
			return nil, fmt.Errorf("max errors exceeded: %d", opts.MaxErrors)
		}
	}

	// Close sink
	sinkResult, err := sink.Close(ctx)
	if err != nil {
		return nil, fmt.Errorf("sink close failed: %w", err)
	}

	return &StreamResult{
		RowsProcessed:  atomic.LoadInt64(&p.rowsProcessed),
		BatchesProcessed: atomic.LoadInt64(&p.batchesProcessed),
		BytesProcessed:  atomic.LoadInt64(&p.bytesProcessed),
		ErrorsEncountered: atomic.LoadInt64(&p.errorsEncountered),
		RowsWritten:     sinkResult.RowsWritten,
		BytesWritten:    sinkResult.BytesWritten,
		FilesWritten:    sinkResult.FilesWritten,
		Duration:        time.Since(startTime),
	}, nil
}

func (p *StreamingPipeline) processBatch(ctx context.Context, sink core.Sink, decoded core.DecodedBatch, opts StreamingOptions) error {
	if decoded.Batch == nil {
		return nil
	}

	batch := decoded.Batch

	// Track errors from decoding
	atomic.AddInt64(&p.errorsEncountered, int64(len(decoded.Errors)))

	// Run post-batch hooks
	var err error
	batch, err = p.hooks.RunPostBatch(ctx, batch)
	if err != nil {
		return err
	}

	// Run custom callback
	if opts.OnBatch != nil {
		batch, err = opts.OnBatch(batch)
		if err != nil {
			return err
		}
	}

	// Write to sink
	if err := sink.Write(ctx, batch); err != nil {
		return err
	}

	// Update metrics
	atomic.AddInt64(&p.rowsProcessed, batch.NumRows())
	atomic.AddInt64(&p.batchesProcessed, 1)
	atomic.AddInt64(&p.bytesProcessed, decoded.BytesRead)

	// Report progress
	if opts.OnProgress != nil {
		opts.OnProgress(hooks.Progress{
			RowsRead:    atomic.LoadInt64(&p.rowsProcessed),
			BatchesRead: int(atomic.LoadInt64(&p.batchesProcessed)),
			BytesRead:   atomic.LoadInt64(&p.bytesProcessed),
		})
	}

	return nil
}

// Hooks returns the hook manager for registration.
func (p *StreamingPipeline) Hooks() *hooks.Manager {
	return p.hooks
}

// StreamResult contains streaming pipeline results.
type StreamResult struct {
	RowsProcessed     int64
	BatchesProcessed  int64
	BytesProcessed    int64
	ErrorsEncountered int64
	RowsWritten       int64
	BytesWritten      int64
	FilesWritten      int
	Duration          time.Duration
}

// Throughput returns rows per second.
func (r *StreamResult) Throughput() float64 {
	if r.Duration.Seconds() == 0 {
		return 0
	}
	return float64(r.RowsProcessed) / r.Duration.Seconds()
}

// StreamMultiple streams multiple sources to a single sink.
func (p *StreamingPipeline) StreamMultiple(ctx context.Context, sources []core.Source, sink core.Sink, opts StreamingOptions) (*StreamResult, error) {
	startTime := time.Now()

	var totalRows, totalBatches, totalBytes, totalErrors int64
	var mu sync.Mutex

	// Process sources (could be parallelized based on cost hints)
	for i, source := range sources {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		// Wrap progress callback to include file info
		wrappedOpts := opts
		if opts.OnProgress != nil {
			wrappedOpts.OnProgress = func(progress hooks.Progress) {
				progress.FilesProcessed = i + 1
				progress.FilesTotal = len(sources)
				opts.OnProgress(progress)
			}
		}

		result, err := p.Stream(ctx, source, sink, wrappedOpts)
		if err != nil {
			if opts.ErrorPolicy == core.ErrorPolicyStrict {
				return nil, err
			}
			mu.Lock()
			totalErrors++
			mu.Unlock()
			continue
		}

		mu.Lock()
		totalRows += result.RowsProcessed
		totalBatches += result.BatchesProcessed
		totalBytes += result.BytesProcessed
		totalErrors += result.ErrorsEncountered
		mu.Unlock()
	}

	return &StreamResult{
		RowsProcessed:     totalRows,
		BatchesProcessed:  totalBatches,
		BytesProcessed:    totalBytes,
		ErrorsEncountered: totalErrors,
		Duration:          time.Since(startTime),
	}, nil
}

// QuickStream is a convenience function for simple streaming.
func QuickStream(ctx context.Context, source core.Source, sink core.Sink) (*StreamResult, error) {
	pipeline := NewStreamingPipeline()
	return pipeline.Stream(ctx, source, sink, DefaultStreamingOptions())
}
