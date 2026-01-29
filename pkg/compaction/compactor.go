// Package compaction provides APIs for merging small Parquet files into larger ones.
// This addresses the "small file problem" in data lakes where many small files
// hurt query performance.
package compaction

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Config holds compaction configuration.
type Config struct {
	// TargetFileSizeMB is the target size for output files (default: 128MB)
	TargetFileSizeMB int64
	// MinInputFiles is minimum files needed to trigger compaction (default: 2)
	MinInputFiles int
	// RowGroupSizeMB is the target row group size (default: 64MB)
	RowGroupSizeMB int64
	// Compression codec (default: zstd)
	Compression string
	// PreservePartitions keeps partition directory structure
	PreservePartitions bool
	// SortColumns to sort output by (improves query performance)
	SortColumns []string
	// DryRun only reports what would be done
	DryRun bool
}

// DefaultConfig returns sensible default configuration.
func DefaultConfig() Config {
	return Config{
		TargetFileSizeMB:   128,
		MinInputFiles:      2,
		RowGroupSizeMB:     64,
		Compression:        "zstd",
		PreservePartitions: true,
	}
}

// Compactor merges small Parquet files.
type Compactor struct {
	db  *sql.DB
	cfg Config
}

// NewCompactor creates a new compactor.
func NewCompactor(cfg Config) (*Compactor, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize DuckDB: %w", err)
	}

	return &Compactor{
		db:  db,
		cfg: cfg,
	}, nil
}

// Close releases resources.
func (c *Compactor) Close() error {
	return c.db.Close()
}

// FileInfo holds information about a Parquet file.
type FileInfo struct {
	Path      string
	SizeBytes int64
	RowCount  int64
	ModTime   time.Time
	Partition string // Partition path component, if any
}

// CompactionPlan describes a planned compaction operation.
type CompactionPlan struct {
	InputFiles  []FileInfo
	OutputPath  string
	TotalBytes  int64
	TotalRows   int64
	Partition   string
	EstimatedOutputSizeMB int64
}

// CompactionResult contains the outcome of a compaction.
type CompactionResult struct {
	Plans           []CompactionPlan
	FilesProcessed  int
	FilesCreated    int
	BytesRead       int64
	BytesWritten    int64
	RowsProcessed   int64
	Duration        time.Duration
	SpaceSavedBytes int64
}

// Analyze scans a directory and returns a compaction plan.
func (c *Compactor) Analyze(ctx context.Context, inputDir string) ([]CompactionPlan, error) {
	// Find all Parquet files
	files, err := c.findParquetFiles(inputDir)
	if err != nil {
		return nil, err
	}

	if len(files) < c.cfg.MinInputFiles {
		return nil, nil // Not enough files to compact
	}

	// Get row counts and sort by partition
	for i := range files {
		rowCount, err := c.getRowCount(ctx, files[i].Path)
		if err != nil {
			return nil, err
		}
		files[i].RowCount = rowCount
	}

	// Group files by partition
	partitions := make(map[string][]FileInfo)
	for _, f := range files {
		partitions[f.Partition] = append(partitions[f.Partition], f)
	}

	// Create plans for each partition
	var plans []CompactionPlan
	targetBytes := c.cfg.TargetFileSizeMB * 1024 * 1024

	for partition, partFiles := range partitions {
		// Sort by size (ascending) to group small files first
		sort.Slice(partFiles, func(i, j int) bool {
			return partFiles[i].SizeBytes < partFiles[j].SizeBytes
		})

		// Group files into compaction batches
		var currentBatch []FileInfo
		var currentBytes int64

		for _, f := range partFiles {
			// If adding this file would exceed target, start new batch
			if currentBytes+f.SizeBytes > targetBytes && len(currentBatch) >= c.cfg.MinInputFiles {
				if len(currentBatch) >= c.cfg.MinInputFiles {
					plans = append(plans, c.createPlan(currentBatch, inputDir, partition))
				}
				currentBatch = nil
				currentBytes = 0
			}

			currentBatch = append(currentBatch, f)
			currentBytes += f.SizeBytes
		}

		// Handle remaining files
		if len(currentBatch) >= c.cfg.MinInputFiles {
			plans = append(plans, c.createPlan(currentBatch, inputDir, partition))
		}
	}

	return plans, nil
}

func (c *Compactor) createPlan(files []FileInfo, baseDir, partition string) CompactionPlan {
	var totalBytes, totalRows int64
	for _, f := range files {
		totalBytes += f.SizeBytes
		totalRows += f.RowCount
	}

	outputName := fmt.Sprintf("compacted_%d.parquet", time.Now().UnixNano())
	outputPath := baseDir
	if partition != "" {
		outputPath = filepath.Join(baseDir, partition)
	}
	outputPath = filepath.Join(outputPath, outputName)

	// Estimate output size (typically 70-90% of input due to better compression)
	estimatedOutput := totalBytes * 80 / 100 / (1024 * 1024)

	return CompactionPlan{
		InputFiles:            files,
		OutputPath:            outputPath,
		TotalBytes:            totalBytes,
		TotalRows:             totalRows,
		Partition:             partition,
		EstimatedOutputSizeMB: estimatedOutput,
	}
}

// Compact executes the compaction plans.
func (c *Compactor) Compact(ctx context.Context, inputDir string) (*CompactionResult, error) {
	start := time.Now()

	plans, err := c.Analyze(ctx, inputDir)
	if err != nil {
		return nil, err
	}

	if len(plans) == 0 {
		return &CompactionResult{
			Duration: time.Since(start),
		}, nil
	}

	result := &CompactionResult{
		Plans: plans,
	}

	for _, plan := range plans {
		if c.cfg.DryRun {
			result.FilesProcessed += len(plan.InputFiles)
			result.BytesRead += plan.TotalBytes
			result.RowsProcessed += plan.TotalRows
			continue
		}

		// Execute compaction
		err := c.executePlan(ctx, plan)
		if err != nil {
			return result, fmt.Errorf("compaction failed for %s: %w", plan.OutputPath, err)
		}

		// Get output file size
		outputInfo, err := os.Stat(plan.OutputPath)
		if err != nil {
			return result, err
		}

		result.FilesProcessed += len(plan.InputFiles)
		result.FilesCreated++
		result.BytesRead += plan.TotalBytes
		result.BytesWritten += outputInfo.Size()
		result.RowsProcessed += plan.TotalRows
		result.SpaceSavedBytes += plan.TotalBytes - outputInfo.Size()

		// Delete original files after successful compaction
		for _, f := range plan.InputFiles {
			if err := os.Remove(f.Path); err != nil {
				// Log but don't fail - compaction succeeded
				fmt.Printf("Warning: failed to remove %s: %v\n", f.Path, err)
			}
		}
	}

	result.Duration = time.Since(start)
	return result, nil
}

func (c *Compactor) executePlan(ctx context.Context, plan CompactionPlan) error {
	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(plan.OutputPath), 0755); err != nil {
		return err
	}

	// Build list of input files
	inputPaths := make([]string, len(plan.InputFiles))
	for i, f := range plan.InputFiles {
		inputPaths[i] = fmt.Sprintf("'%s'", escapePath(f.Path))
	}

	// Build query
	var query string
	if len(c.cfg.SortColumns) > 0 {
		query = fmt.Sprintf(`
			COPY (
				SELECT * FROM read_parquet([%s])
				ORDER BY %s
			)
			TO '%s'
			(FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
		`,
			strings.Join(inputPaths, ", "),
			strings.Join(c.cfg.SortColumns, ", "),
			escapePath(plan.OutputPath),
			c.cfg.Compression,
			c.cfg.RowGroupSizeMB*1024*1024/100, // Approximate rows for target size
		)
	} else {
		query = fmt.Sprintf(`
			COPY (SELECT * FROM read_parquet([%s]))
			TO '%s'
			(FORMAT PARQUET, COMPRESSION '%s')
		`,
			strings.Join(inputPaths, ", "),
			escapePath(plan.OutputPath),
			c.cfg.Compression,
		)
	}

	_, err := c.db.ExecContext(ctx, query)
	return err
}

func (c *Compactor) findParquetFiles(dir string) ([]FileInfo, error) {
	var files []FileInfo

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(path), ".parquet") {
			return nil
		}

		// Determine partition from path
		relPath, _ := filepath.Rel(dir, path)
		partition := filepath.Dir(relPath)
		if partition == "." {
			partition = ""
		}

		files = append(files, FileInfo{
			Path:      path,
			SizeBytes: info.Size(),
			ModTime:   info.ModTime(),
			Partition: partition,
		})
		return nil
	})

	return files, err
}

func (c *Compactor) getRowCount(ctx context.Context, path string) (int64, error) {
	var count int64
	query := fmt.Sprintf("SELECT COUNT(*) FROM read_parquet('%s')", escapePath(path))
	err := c.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

// BatchWriter writes data in batches, creating new files when target size is reached.
type BatchWriter struct {
	baseDir       string
	partitionFunc func(data interface{}) string
	targetSizeMB  int64
	compression   string
	fileCount     int
	currentFile   string
	currentSize   int64
	db            *sql.DB
}

// NewBatchWriter creates a batch writer for partition-aware file creation.
func NewBatchWriter(baseDir string, targetSizeMB int64, compression string) (*BatchWriter, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	return &BatchWriter{
		baseDir:      baseDir,
		targetSizeMB: targetSizeMB,
		compression:  compression,
		db:           db,
	}, nil
}

// WithPartitionFunc sets the partition function.
func (w *BatchWriter) WithPartitionFunc(fn func(data interface{}) string) *BatchWriter {
	w.partitionFunc = fn
	return w
}

// Close releases resources.
func (w *BatchWriter) Close() error {
	return w.db.Close()
}

// Optimize runs optimization on a directory, combining small files and sorting.
func Optimize(ctx context.Context, inputDir, outputDir string, cfg Config) (*CompactionResult, error) {
	compactor, err := NewCompactor(cfg)
	if err != nil {
		return nil, err
	}
	defer compactor.Close()

	// If outputDir is different, copy and compact
	if outputDir != "" && outputDir != inputDir {
		return compactor.CompactTo(ctx, inputDir, outputDir)
	}

	// In-place compaction
	return compactor.Compact(ctx, inputDir)
}

// CompactTo compacts files from input directory to output directory.
func (c *Compactor) CompactTo(ctx context.Context, inputDir, outputDir string) (*CompactionResult, error) {
	start := time.Now()

	// Find all Parquet files
	files, err := c.findParquetFiles(inputDir)
	if err != nil {
		return nil, err
	}

	if len(files) == 0 {
		return &CompactionResult{Duration: time.Since(start)}, nil
	}

	// Ensure output directory exists
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, err
	}

	result := &CompactionResult{}

	// Get row counts
	for i := range files {
		rowCount, err := c.getRowCount(ctx, files[i].Path)
		if err != nil {
			return nil, err
		}
		files[i].RowCount = rowCount
		result.RowsProcessed += rowCount
		result.BytesRead += files[i].SizeBytes
	}

	result.FilesProcessed = len(files)

	if c.cfg.DryRun {
		result.Duration = time.Since(start)
		return result, nil
	}

	// Build input list
	inputPaths := make([]string, len(files))
	for i, f := range files {
		inputPaths[i] = fmt.Sprintf("'%s'", escapePath(f.Path))
	}

	// Create single optimized output file
	outputPath := filepath.Join(outputDir, "optimized.parquet")

	var query string
	if len(c.cfg.SortColumns) > 0 {
		query = fmt.Sprintf(`
			COPY (
				SELECT * FROM read_parquet([%s])
				ORDER BY %s
			)
			TO '%s'
			(FORMAT PARQUET, COMPRESSION '%s')
		`,
			strings.Join(inputPaths, ", "),
			strings.Join(c.cfg.SortColumns, ", "),
			escapePath(outputPath),
			c.cfg.Compression,
		)
	} else {
		query = fmt.Sprintf(`
			COPY (SELECT * FROM read_parquet([%s]))
			TO '%s'
			(FORMAT PARQUET, COMPRESSION '%s')
		`,
			strings.Join(inputPaths, ", "),
			escapePath(outputPath),
			c.cfg.Compression,
		)
	}

	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return result, err
	}

	// Get output size
	outputInfo, err := os.Stat(outputPath)
	if err != nil {
		return result, err
	}

	result.FilesCreated = 1
	result.BytesWritten = outputInfo.Size()
	result.SpaceSavedBytes = result.BytesRead - result.BytesWritten
	result.Duration = time.Since(start)

	return result, nil
}

// Summary returns a human-readable summary of the result.
func (r *CompactionResult) Summary() string {
	if r.FilesProcessed == 0 {
		return "No files to compact"
	}

	compressionRatio := float64(r.BytesRead) / float64(r.BytesWritten)
	spaceSavedMB := float64(r.SpaceSavedBytes) / (1024 * 1024)

	return fmt.Sprintf(
		"Compacted %d files into %d files | %d rows | %.1f MB saved (%.1fx compression) | %v",
		r.FilesProcessed,
		r.FilesCreated,
		r.RowsProcessed,
		spaceSavedMB,
		compressionRatio,
		r.Duration.Round(time.Millisecond),
	)
}
