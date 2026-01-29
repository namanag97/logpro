// +build ignore

// Test all XES files in the downloads folder
// Run: go run test_all_xes.go
package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/sinks"
)

const xesDir = "/Users/namanagarwal/Downloads/Process-Mining-Datasets"

func main() {
	fmt.Println()
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("       XES File Batch Test - All Files in Downloads")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println()

	ctx := context.Background()
	tmpDir, _ := os.MkdirTemp("", "xes-test-*")
	defer os.RemoveAll(tmpDir)

	// Find all XES files
	xesFiles, _ := filepath.Glob(filepath.Join(xesDir, "*.xes"))
	if len(xesFiles) == 0 {
		fmt.Println("No XES files found in:", xesDir)
		return
	}

	fmt.Printf("Found %d XES files\n\n", len(xesFiles))

	decoder := decoders.NewXESDecoder()
	var totalEvents int64
	var totalFiles int
	startAll := time.Now()

	for _, xesPath := range xesFiles {
		info, _ := os.Stat(xesPath)
		sizeMB := float64(info.Size()) / 1024 / 1024

		fmt.Printf("Processing: %s (%.1f MB)\n", filepath.Base(xesPath), sizeMB)

		start := time.Now()

		source := &fileSource{path: xesPath, format: core.FormatXES}
		opts := core.DefaultDecodeOptions()
		opts.BatchSize = 50000 // Large batches for big files

		batches, err := decoder.Decode(ctx, source, opts)
		if err != nil {
			fmt.Printf("  ERROR: %v\n\n", err)
			continue
		}

		var eventCount int64
		var batchCount int
		for b := range batches {
			if b.Batch != nil {
				eventCount += b.Batch.NumRows()
				batchCount++
				b.Batch.Release()
			}
		}

		elapsed := time.Since(start)
		eventsPerSec := float64(eventCount) / elapsed.Seconds()

		fmt.Printf("  Events:     %d\n", eventCount)
		fmt.Printf("  Batches:    %d\n", batchCount)
		fmt.Printf("  Duration:   %v\n", elapsed.Round(time.Millisecond))
		fmt.Printf("  Throughput: %.0f events/sec\n", eventsPerSec)

		// Write to Parquet to verify full pipeline
		if eventCount > 0 {
			parquetPath := filepath.Join(tmpDir, filepath.Base(xesPath)+".parquet")
			writeResult := writeToParquet(ctx, decoder, source, parquetPath)
			if writeResult != "" {
				fmt.Printf("  Parquet:    %s\n", writeResult)
			}
		}

		fmt.Println()

		totalEvents += eventCount
		totalFiles++
	}

	totalElapsed := time.Since(startAll)

	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Println("                         SUMMARY")
	fmt.Println("═══════════════════════════════════════════════════════════════")
	fmt.Printf("  Files processed:    %d\n", totalFiles)
	fmt.Printf("  Total events:       %d\n", totalEvents)
	fmt.Printf("  Total duration:     %v\n", totalElapsed.Round(time.Millisecond))
	fmt.Printf("  Overall throughput: %.0f events/sec\n", float64(totalEvents)/totalElapsed.Seconds())
	fmt.Println()
}

func writeToParquet(ctx context.Context, decoder *decoders.XESDecoder, source core.Source, outputPath string) string {
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return fmt.Sprintf("decode error: %v", err)
	}

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return "no batches"
	}

	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath
	sinkOpts.Compression = core.CompressionSnappy

	if err := sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts); err != nil {
		for _, b := range allBatches {
			b.Batch.Release()
		}
		return fmt.Sprintf("sink open error: %v", err)
	}

	var rows int64
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		rows += b.Batch.NumRows()
		b.Batch.Release()
	}

	result, _ := sink.Close(ctx)
	info, _ := os.Stat(outputPath)
	if info != nil {
		return fmt.Sprintf("%d rows, %d bytes", result.RowsWritten, info.Size())
	}
	return fmt.Sprintf("%d rows", result.RowsWritten)
}

type fileSource struct {
	path   string
	format core.Format
}

func (s *fileSource) ID() string                  { return s.path }
func (s *fileSource) Location() string            { return s.path }
func (s *fileSource) Format() core.Format         { return s.format }
func (s *fileSource) Size() int64                 { info, _ := os.Stat(s.path); if info != nil { return info.Size() }; return 0 }
func (s *fileSource) ModTime() time.Time          { return time.Now() }
func (s *fileSource) Metadata() map[string]string { return nil }
func (s *fileSource) Open(ctx context.Context) (io.ReadCloser, error) { return os.Open(s.path) }
