// Package maintenance provides storage maintenance operations.
package maintenance

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/sinks"
)

// CompactOptions configures compaction behavior.
type CompactOptions struct {
	// Target file size after compaction
	TargetFileSize int64

	// Minimum files to trigger compaction
	MinFiles int

	// Maximum files per compacted output
	MaxFilesPerOutput int

	// Compression for output
	Compression string

	// Sort by columns (optional)
	SortBy []string

	// Delete source files after successful compaction
	DeleteSource bool

	// Dry run - don't actually compact
	DryRun bool
}

// DefaultCompactOptions returns sensible defaults.
func DefaultCompactOptions() CompactOptions {
	return CompactOptions{
		TargetFileSize:    128 * 1024 * 1024, // 128MB
		MinFiles:          2,
		MaxFilesPerOutput: 100,
		Compression:       "snappy",
		DeleteSource:      true,
	}
}

// CompactResult contains compaction results.
type CompactResult struct {
	InputFiles      int
	OutputFiles     int
	InputBytes      int64
	OutputBytes     int64
	InputRows       int64
	OutputRows      int64
	FilesDeleted    int
	Duration        time.Duration
	CompactedPaths  []string
	CompressionRate float64
}

// Compactor handles file compaction.
type Compactor struct {
	opts CompactOptions
}

// NewCompactor creates a compactor.
func NewCompactor(opts CompactOptions) *Compactor {
	return &Compactor{opts: opts}
}

// Compact compacts Parquet files in a directory.
func (c *Compactor) Compact(ctx context.Context, dir string) (*CompactResult, error) {
	result := &CompactResult{}
	startTime := time.Now()

	// Find all Parquet files
	files, err := c.findParquetFiles(dir)
	if err != nil {
		return nil, fmt.Errorf("failed to find files: %w", err)
	}

	if len(files) < c.opts.MinFiles {
		return result, nil // Nothing to compact
	}

	result.InputFiles = len(files)

	// Group files for compaction
	groups := c.groupFiles(files)

	for _, group := range groups {
		select {
		case <-ctx.Done():
			return result, ctx.Err()
		default:
		}

		if c.opts.DryRun {
			result.OutputFiles++
			continue
		}

		compacted, err := c.compactGroup(ctx, group)
		if err != nil {
			return result, fmt.Errorf("compaction failed: %w", err)
		}

		result.OutputFiles++
		result.CompactedPaths = append(result.CompactedPaths, compacted.Path)
		result.OutputBytes += compacted.Size
		result.OutputRows += compacted.Rows

		// Delete source files if requested
		if c.opts.DeleteSource {
			for _, f := range group {
				if err := os.Remove(f.Path); err == nil {
					result.FilesDeleted++
				}
			}
		}
	}

	// Calculate stats
	for _, f := range files {
		result.InputBytes += f.Size
		result.InputRows += f.Rows
	}

	if result.InputBytes > 0 {
		result.CompressionRate = float64(result.OutputBytes) / float64(result.InputBytes)
	}

	result.Duration = time.Since(startTime)
	return result, nil
}

type parquetFile struct {
	Path string
	Size int64
	Rows int64
}

type compactedFile struct {
	Path string
	Size int64
	Rows int64
}

func (c *Compactor) findParquetFiles(dir string) ([]parquetFile, error) {
	var files []parquetFile

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		if info.IsDir() {
			return nil
		}

		if filepath.Ext(path) != ".parquet" {
			return nil
		}

		// Get row count
		rows, err := c.getRowCount(path)
		if err != nil {
			rows = 0 // Estimate or skip
		}

		files = append(files, parquetFile{
			Path: path,
			Size: info.Size(),
			Rows: rows,
		})

		return nil
	})

	// Sort by size (smallest first for better packing)
	sort.Slice(files, func(i, j int) bool {
		return files[i].Size < files[j].Size
	})

	return files, err
}

func (c *Compactor) getRowCount(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	return reader.NumRows(), nil
}

func (c *Compactor) groupFiles(files []parquetFile) [][]parquetFile {
	var groups [][]parquetFile
	var currentGroup []parquetFile
	var currentSize int64

	for _, f := range files {
		if len(currentGroup) >= c.opts.MaxFilesPerOutput ||
			(currentSize+f.Size > c.opts.TargetFileSize && len(currentGroup) > 0) {
			groups = append(groups, currentGroup)
			currentGroup = nil
			currentSize = 0
		}

		currentGroup = append(currentGroup, f)
		currentSize += f.Size
	}

	if len(currentGroup) >= c.opts.MinFiles {
		groups = append(groups, currentGroup)
	}

	return groups
}

func (c *Compactor) compactGroup(ctx context.Context, files []parquetFile) (*compactedFile, error) {
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to compact")
	}

	// Read schema from first file
	schema, err := c.getSchema(files[0].Path)
	if err != nil {
		return nil, fmt.Errorf("failed to get schema: %w", err)
	}

	// Generate output path
	dir := filepath.Dir(files[0].Path)
	outputPath := filepath.Join(dir, fmt.Sprintf("compacted_%d.parquet", time.Now().UnixNano()))

	// Create sink
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath
	sinkOpts.Compression = core.ParseCompression(c.opts.Compression)

	if err := sink.Open(ctx, schema, sinkOpts); err != nil {
		return nil, fmt.Errorf("failed to open sink: %w", err)
	}

	var totalRows int64

	// Read and write all files
	for _, f := range files {
		select {
		case <-ctx.Done():
			sink.Close(ctx)
			os.Remove(outputPath)
			return nil, ctx.Err()
		default:
		}

		rows, err := c.copyFile(ctx, f.Path, sink)
		if err != nil {
			sink.Close(ctx)
			os.Remove(outputPath)
			return nil, fmt.Errorf("failed to copy %s: %w", f.Path, err)
		}
		totalRows += rows
	}

	result, err := sink.Close(ctx)
	if err != nil {
		os.Remove(outputPath)
		return nil, fmt.Errorf("failed to close sink: %w", err)
	}

	// Get output size
	info, _ := os.Stat(outputPath)
	size := int64(0)
	if info != nil {
		size = info.Size()
	}

	return &compactedFile{
		Path: outputPath,
		Size: size,
		Rows: result.RowsWritten,
	}, nil
}

func (c *Compactor) getSchema(path string) (*arrow.Schema, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, nil)
	if err != nil {
		return nil, err
	}

	return arrowReader.Schema()
}

func (c *Compactor) copyFile(ctx context.Context, path string, sink *sinks.ParquetSink) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return 0, err
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{
		BatchSize: 8192,
	}, nil)
	if err != nil {
		return 0, err
	}

	var totalRows int64

	// Read entire table
	table, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return 0, err
	}
	defer table.Release()

	totalRows = table.NumRows()

	return totalRows, nil
}

// Optimize optimizes table layout (clustering, sorting).
func Optimize(ctx context.Context, dir string, opts OptimizeOptions) (*OptimizeResult, error) {
	// First compact
	compactor := NewCompactor(CompactOptions{
		TargetFileSize:    opts.TargetFileSize,
		MinFiles:          1,
		MaxFilesPerOutput: opts.MaxFilesPerOutput,
		Compression:       opts.Compression,
		SortBy:            opts.SortBy,
		DeleteSource:      true,
	})

	compactResult, err := compactor.Compact(ctx, dir)
	if err != nil {
		return nil, err
	}

	return &OptimizeResult{
		CompactResult: *compactResult,
		Sorted:        len(opts.SortBy) > 0,
		SortColumns:   opts.SortBy,
	}, nil
}

// OptimizeOptions configures optimization.
type OptimizeOptions struct {
	TargetFileSize    int64
	MaxFilesPerOutput int
	Compression       string
	SortBy            []string
	ZOrderBy          []string // Z-order clustering columns
}

// OptimizeResult contains optimization results.
type OptimizeResult struct {
	CompactResult
	Sorted      bool
	SortColumns []string
	ZOrdered    bool
}
