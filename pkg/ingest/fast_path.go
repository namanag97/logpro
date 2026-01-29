package ingest

import (
"context"
"database/sql"
"fmt"
"os"
"path/filepath"
"runtime"
"strings"

_ "github.com/marcboeker/go-duckdb"
)

// FastPath uses DuckDB native readers for maximum performance.
// This path is used for clean files where DuckDB can handle parsing.
type FastPath struct {
db *sql.DB
}

// NewFastPath creates a new DuckDB-based fast path.
func NewFastPath() (*FastPath, error) {
db, err := sql.Open("duckdb", "")
if err != nil {
return nil, fmt.Errorf("failed to initialize DuckDB: %w", err)
}

return &FastPath{db: db}, nil
}

// Close releases DuckDB resources.
func (f *FastPath) Close() error {
return f.db.Close()
}

// Process handles file conversion using DuckDB native readers.
func (f *FastPath) Process(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (*Result, error) {
// Configure DuckDB threads
threads := opts.DuckDBThreads
if threads <= 0 {
threads = runtime.NumCPU()
}
if _, err := f.db.ExecContext(ctx, fmt.Sprintf("SET threads=%d", threads)); err != nil {
// Non-fatal, continue with default
}

// For large files, disable insertion order preservation
if analysis.Size > 1024*1024*1024 { // 1GB
f.db.ExecContext(ctx, "SET preserve_insertion_order=false")
}

// Ensure output directory exists
if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
return nil, fmt.Errorf("failed to create output directory: %w", err)
}

var rowCount int64
var columnCount int
var err error

switch analysis.Format {
case FormatCSV, FormatTSV:
rowCount, columnCount, err = f.processCSV(ctx, inputPath, outputPath, analysis, opts)
case FormatJSON:
rowCount, columnCount, err = f.processJSON(ctx, inputPath, outputPath, false, opts)
case FormatJSONL:
rowCount, columnCount, err = f.processJSON(ctx, inputPath, outputPath, true, opts)
case FormatParquet:
rowCount, columnCount, err = f.processParquet(ctx, inputPath, outputPath, opts)
case FormatGzip:
rowCount, columnCount, err = f.processGzip(ctx, inputPath, outputPath, analysis, opts)
default:
return nil, fmt.Errorf("format %s not supported by fast path", analysis.Format)
}

if err != nil {
return nil, err
}

// Get output size
outputInfo, err := os.Stat(outputPath)
if err != nil {
return nil, fmt.Errorf("failed to stat output: %w", err)
}

result := &Result{
InputPath:       inputPath,
OutputPath:      outputPath,
Format:          analysis.Format,
Strategy:        StrategyFastDuckDB,
RowCount:        rowCount,
ColumnCount:     columnCount,
InputSize:       analysis.Size,
OutputSize:      outputInfo.Size(),
CompressionRate: float64(analysis.Size) / float64(outputInfo.Size()),
}

return result, nil
}

// processCSV converts CSV to Parquet using DuckDB read_csv_auto.
func (f *FastPath) processCSV(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (int64, int, error) {
// Build read_csv options based on analysis
csvOpts := f.buildCSVOptions(analysis, opts)

// Get row group size
rowGroupSize := opts.RowGroupSize
if rowGroupSize <= 0 {
rowGroupSize = 10000 // Optimized default
}

// Get compression
compression := opts.Compression
if compression == "" {
compression = "snappy"
}

// Build query
query := fmt.Sprintf(`
COPY (SELECT * FROM read_csv_auto('%s'%s))
TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
`, escapePath(inputPath), csvOpts, escapePath(outputPath), compression, rowGroupSize)

if _, err := f.db.ExecContext(ctx, query); err != nil {
return 0, 0, fmt.Errorf("CSV conversion failed: %w", err)
}

return f.getParquetStats(outputPath)
}

// buildCSVOptions constructs DuckDB read_csv options based on analysis.
func (f *FastPath) buildCSVOptions(analysis *FileAnalysis, opts Options) string {
var options []string

// Header
options = append(options, fmt.Sprintf("header=%v", analysis.HasHeader))

// Delimiter
delimStr := string(analysis.Delimiter)
if analysis.Delimiter == '\t' {
delimStr = "\\t"
}
options = append(options, fmt.Sprintf("delim='%s'", delimStr))

// Quote character
if analysis.HasQuotedFields {
options = append(options, fmt.Sprintf("quote='%c'", analysis.QuoteChar))
}

// Parallel reading for clean files
if analysis.IsClean && !analysis.HasEmbeddedNewlines {
options = append(options, "parallel=true")
}

// Type inference
options = append(options, "all_varchar=false")

// Null values
if len(analysis.NullValueStrings) > 0 {
nulls := make([]string, len(analysis.NullValueStrings))
for i, s := range analysis.NullValueStrings {
nulls[i] = fmt.Sprintf("'%s'", strings.ReplaceAll(s, "'", "''"))
}
options = append(options, fmt.Sprintf("null_padding=true"))
}

// Sample size for type inference
options = append(options, "sample_size=10000")

if len(options) == 0 {
return ""
}
return ", " + strings.Join(options, ", ")
}

// processJSON converts JSON/JSONL to Parquet.
func (f *FastPath) processJSON(ctx context.Context, inputPath, outputPath string, isLines bool, opts Options) (int64, int, error) {
format := "auto"
if isLines {
format = "newline_delimited"
}

rowGroupSize := opts.RowGroupSize
if rowGroupSize <= 0 {
rowGroupSize = 10000
}

compression := opts.Compression
if compression == "" {
compression = "snappy"
}

query := fmt.Sprintf(`
COPY (SELECT * FROM read_json_auto('%s', format='%s', maximum_object_size=33554432))
TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
`, escapePath(inputPath), format, escapePath(outputPath), compression, rowGroupSize)

if _, err := f.db.ExecContext(ctx, query); err != nil {
return 0, 0, fmt.Errorf("JSON conversion failed: %w", err)
}

return f.getParquetStats(outputPath)
}

// processParquet re-compresses or copies Parquet files.
func (f *FastPath) processParquet(ctx context.Context, inputPath, outputPath string, opts Options) (int64, int, error) {
rowGroupSize := opts.RowGroupSize
if rowGroupSize <= 0 {
rowGroupSize = 10000
}

compression := opts.Compression
if compression == "" {
compression = "snappy"
}

query := fmt.Sprintf(`
COPY (SELECT * FROM read_parquet('%s'))
TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
`, escapePath(inputPath), escapePath(outputPath), compression, rowGroupSize)

if _, err := f.db.ExecContext(ctx, query); err != nil {
return 0, 0, fmt.Errorf("Parquet conversion failed: %w", err)
}

return f.getParquetStats(outputPath)
}

// processGzip handles gzip-compressed files.
func (f *FastPath) processGzip(ctx context.Context, inputPath, outputPath string, analysis *FileAnalysis, opts Options) (int64, int, error) {
// DuckDB can read gzipped files directly
// Determine the underlying format from the filename
baseName := strings.TrimSuffix(inputPath, ".gz")
ext := strings.ToLower(filepath.Ext(baseName))

var format Format
switch ext {
case ".csv":
format = FormatCSV
case ".tsv":
format = FormatTSV
case ".json":
format = FormatJSON
case ".jsonl", ".ndjson":
format = FormatJSONL
default:
format = FormatCSV // Default to CSV
}

// Create a modified analysis for the underlying format
innerAnalysis := *analysis
innerAnalysis.Format = format

switch format {
case FormatCSV, FormatTSV:
return f.processCSV(ctx, inputPath, outputPath, &innerAnalysis, opts)
case FormatJSON:
return f.processJSON(ctx, inputPath, outputPath, false, opts)
case FormatJSONL:
return f.processJSON(ctx, inputPath, outputPath, true, opts)
default:
return f.processCSV(ctx, inputPath, outputPath, &innerAnalysis, opts)
}
}

// getParquetStats retrieves row count and column count from Parquet metadata.
func (f *FastPath) getParquetStats(path string) (int64, int, error) {
// Get row count from metadata (fast, doesn't scan data)
var rowCount int64
metaQuery := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_metadata('%s')`, escapePath(path))
if err := f.db.QueryRow(metaQuery).Scan(&rowCount); err != nil {
// Fallback to COUNT(*)
countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, escapePath(path))
if err := f.db.QueryRow(countQuery).Scan(&rowCount); err != nil {
return 0, 0, fmt.Errorf("failed to get row count: %w", err)
}
}

// Get column count from schema
var columnCount int
schemaQuery := fmt.Sprintf(`SELECT COUNT(*) FROM parquet_schema('%s') WHERE name IS NOT NULL`, escapePath(path))
if err := f.db.QueryRow(schemaQuery).Scan(&columnCount); err != nil {
// Fallback to DESCRIBE
descQuery := fmt.Sprintf(`DESCRIBE SELECT * FROM read_parquet('%s')`, escapePath(path))
rows, err := f.db.Query(descQuery)
if err != nil {
return rowCount, 0, nil
}
defer rows.Close()
for rows.Next() {
columnCount++
}
}

return rowCount, columnCount, nil
}

// BulkCSV processes multiple CSV files efficiently.
func (f *FastPath) BulkCSV(ctx context.Context, inputGlob, outputPath string, opts Options) (*Result, error) {
rowGroupSize := opts.RowGroupSize
if rowGroupSize <= 0 {
rowGroupSize = 10000
}

compression := opts.Compression
if compression == "" {
compression = "snappy"
}

// Use glob pattern for multiple files
query := fmt.Sprintf(`
COPY (SELECT * FROM read_csv_auto('%s', header=true, union_by_name=true))
TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
`, escapePath(inputGlob), escapePath(outputPath), compression, rowGroupSize)

if _, err := f.db.ExecContext(ctx, query); err != nil {
return nil, fmt.Errorf("bulk CSV conversion failed: %w", err)
}

rowCount, columnCount, err := f.getParquetStats(outputPath)
if err != nil {
return nil, err
}

outputInfo, _ := os.Stat(outputPath)
outputSize := int64(0)
if outputInfo != nil {
outputSize = outputInfo.Size()
}

return &Result{
InputPath:   inputGlob,
OutputPath:  outputPath,
Format:      FormatCSV,
Strategy:    StrategyFastDuckDB,
RowCount:    rowCount,
ColumnCount: columnCount,
OutputSize:  outputSize,
}, nil
}

// Query executes a custom DuckDB query for advanced use cases.
func (f *FastPath) Query(ctx context.Context, query string) (*sql.Rows, error) {
return f.db.QueryContext(ctx, query)
}

// Exec executes a custom DuckDB statement.
func (f *FastPath) Exec(ctx context.Context, query string) (sql.Result, error) {
return f.db.ExecContext(ctx, query)
}

// escapePath escapes a path for use in SQL.
func escapePath(path string) string {
return strings.ReplaceAll(path, "'", "''")
}
