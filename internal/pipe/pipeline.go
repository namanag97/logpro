// Package pipe implements the Producer-Consumer pipeline for data ingestion.
package pipe

import (
	"context"
	"fmt"
	"io"
	"os"
	"sync/atomic"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/pkg/parser"
	"github.com/logflow/logflow/pkg/writer"
)

// Pipeline orchestrates the Reader -> Parser -> Writer data flow.
type Pipeline struct {
	parserCfg parser.Config
	writerCfg writer.Config

	// Channel buffer sizes
	eventBufferSize int

	// Statistics (atomic for lock-free access)
	eventsRead  atomic.Int64
	bytesRead   atomic.Int64
	rowsWritten atomic.Int64

	// Progress callback
	progressFn func(stats ProgressStats)
}

// ProgressStats provides real-time pipeline statistics.
type ProgressStats struct {
	EventsProcessed int64
	BytesRead       int64
	RowsWritten     int64
	EventsPerSecond float64
	BytesPerSecond  float64
	ElapsedTime     time.Duration
}

// Config holds pipeline configuration.
type Config struct {
	// Parser configuration
	ParserConfig parser.Config

	// Writer configuration
	WriterConfig writer.Config

	// EventBufferSize is the channel buffer size between stages.
	EventBufferSize int
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		ParserConfig:    parser.DefaultConfig(),
		WriterConfig:    writer.DefaultConfig(),
		EventBufferSize: 4096,
	}
}

// NewPipeline creates a new pipeline with the given configuration.
func NewPipeline(cfg Config) *Pipeline {
	if cfg.EventBufferSize <= 0 {
		cfg.EventBufferSize = 4096
	}
	return &Pipeline{
		parserCfg:       cfg.ParserConfig,
		writerCfg:       cfg.WriterConfig,
		eventBufferSize: cfg.EventBufferSize,
	}
}

// IngestResult contains the results of an ingestion operation.
type IngestResult struct {
	EventsProcessed int64
	BytesRead       int64
	RowsWritten     int64
}

// Ingest processes the input file and writes to the output file.
// This is the main entry point for the pipeline.
func (p *Pipeline) Ingest(ctx context.Context, inputPath, outputPath string, format parser.Format) (*IngestResult, error) {
	// Open input file
	inputFile, err := os.Open(inputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open input file %q: %w", inputPath, err)
	}
	defer inputFile.Close()

	// Get file size for progress tracking
	stat, err := inputFile.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to stat input file %q: %w", inputPath, err)
	}
	p.bytesRead.Store(stat.Size())

	// Create output file
	outputFile, err := os.Create(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create output file %q: %w", outputPath, err)
	}
	defer outputFile.Close()

	return p.IngestFromReaderToWriter(ctx, inputFile, outputFile, format)
}

// IngestFromReaderToWriter processes data from a reader to a writer.
// Uses errgroup for coordinated goroutine management and automatic cancellation on error.
func (p *Pipeline) IngestFromReaderToWriter(ctx context.Context, input io.Reader, output io.Writer, format parser.Format) (*IngestResult, error) {
	// Create parser
	inputParser, err := parser.NewParser(format, p.parserCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create parser: %w", err)
	}

	// Create writer
	p.writerCfg.Output = output
	parquetWriter, err := writer.NewParquetWriter(output, p.writerCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create writer: %w", err)
	}
	defer parquetWriter.Close()

	// Create event channel (buffered for performance)
	eventChan := make(chan *model.Event, p.eventBufferSize)

	// Use errgroup for coordinated shutdown - any error cancels all goroutines
	g, ctx := errgroup.WithContext(ctx)

	startTime := time.Now()

	// Parser goroutine (Reader + Parser)
	g.Go(func() error {
		defer close(eventChan)
		if err := inputParser.Parse(ctx, input, eventChan); err != nil {
			return fmt.Errorf("parser error at byte %d: %w", p.bytesRead.Load(), err)
		}
		return nil
	})

	// Writer goroutine with progress tracking
	g.Go(func() error {
		var count int64
		lastReport := time.Now()

		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event, ok := <-eventChan:
				if !ok {
					return nil
				}

				// Forward to writer via internal write
				if err := parquetWriter.WriteEvent(ctx, event); err != nil {
					return fmt.Errorf("writer error at event %d: %w", count, err)
				}

				count++
				p.eventsRead.Store(count)

				// Report progress periodically
				if p.progressFn != nil && time.Since(lastReport) > 100*time.Millisecond {
					elapsed := time.Since(startTime)
					p.progressFn(ProgressStats{
						EventsProcessed: count,
						BytesRead:       p.bytesRead.Load(),
						RowsWritten:     parquetWriter.RowsWritten(),
						EventsPerSecond: float64(count) / elapsed.Seconds(),
						BytesPerSecond:  float64(p.bytesRead.Load()) / elapsed.Seconds(),
						ElapsedTime:     elapsed,
					})
					lastReport = time.Now()
				}
			}
		}
	})

	// Wait for both goroutines - errgroup handles cancellation
	if err := g.Wait(); err != nil {
		return nil, err
	}

	// Final progress report
	if p.progressFn != nil {
		elapsed := time.Since(startTime)
		p.progressFn(ProgressStats{
			EventsProcessed: p.eventsRead.Load(),
			BytesRead:       p.bytesRead.Load(),
			RowsWritten:     parquetWriter.RowsWritten(),
			EventsPerSecond: float64(p.eventsRead.Load()) / elapsed.Seconds(),
			BytesPerSecond:  float64(p.bytesRead.Load()) / elapsed.Seconds(),
			ElapsedTime:     elapsed,
		})
	}

	return &IngestResult{
		EventsProcessed: p.eventsRead.Load(),
		BytesRead:       p.bytesRead.Load(),
		RowsWritten:     parquetWriter.RowsWritten(),
	}, nil
}

// SetProgressCallback sets a callback for progress updates.
func (p *Pipeline) SetProgressCallback(fn func(stats ProgressStats)) {
	p.progressFn = fn
}

// IngestWithProgress processes data with a legacy progress callback.
// Prefer using SetProgressCallback with Ingest for richer progress information.
func (p *Pipeline) IngestWithProgress(
	ctx context.Context,
	inputPath, outputPath string,
	format parser.Format,
	progressFn func(eventsProcessed int64),
) (*IngestResult, error) {
	// Wrap legacy callback
	if progressFn != nil {
		p.SetProgressCallback(func(stats ProgressStats) {
			progressFn(stats.EventsProcessed)
		})
	}
	return p.Ingest(ctx, inputPath, outputPath, format)
}
