// Package core provides the zero-config conversion pipeline.
// This is the "just works" path - no user input required.
package core

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/logflow/logflow/pkg/ingest/telemetry"
)

// ConversionResult holds the outcome of a conversion.
type ConversionResult struct {
	InputPath    string
	OutputPath   string
	InputFormat  string
	InputSize    int64
	OutputSize   int64
	RowCount     int64
	ColumnCount  int
	Columns      []ColumnInfo
	Duration     time.Duration
	Throughput   float64 // rows/sec
	Compression  float64 // ratio
}

// ColumnInfo describes a column in the output.
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
	NullPct  float64 // percentage of nulls
}

// ConversionOptions for customizing conversion (all optional).
type ConversionOptions struct {
	Compression string // snappy, zstd, gzip, lz4, none (default: snappy)
	OutputPath  string // auto-generated if empty
}

// DefaultOptions returns sensible defaults.
func DefaultOptions() ConversionOptions {
	return ConversionOptions{
		Compression: "snappy",
	}
}

// Converter handles format conversion with zero configuration.
type Converter struct {
	db *sql.DB
}

// NewConverter creates a new zero-config converter.
func NewConverter() (*Converter, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to initialize conversion engine: %w", err)
	}
	return &Converter{db: db}, nil
}

// Close releases resources.
func (c *Converter) Close() error {
	return c.db.Close()
}

// Convert performs zero-config conversion from any supported format to Parquet.
// No schema mapping required - all columns are preserved as-is.
func (c *Converter) Convert(ctx context.Context, inputPath string, opts ConversionOptions) (*ConversionResult, error) {
	start := time.Now()

	// Validate input exists
	inputInfo, err := os.Stat(inputPath)
	if err != nil {
		return nil, fmt.Errorf("input file not found: %w", err)
	}

	// Detect format
	format := detectFormat(inputPath)
	if format == "" {
		return nil, fmt.Errorf("unsupported file format: %s", filepath.Ext(inputPath))
	}

	// Generate output path if not specified
	outputPath := opts.OutputPath
	if outputPath == "" {
		outputPath = generateOutputPath(inputPath)
	}

	// Ensure output directory exists
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Get compression setting
	compression := opts.Compression
	if compression == "" {
		compression = "snappy"
	}

	// Perform conversion based on format
	var rowCount int64
	var columns []ColumnInfo

	switch format {
	case "csv":
		rowCount, columns, err = c.convertCSV(ctx, inputPath, outputPath, compression)
	case "json", "jsonl":
		rowCount, columns, err = c.convertJSON(ctx, inputPath, outputPath, compression, format == "jsonl")
	case "parquet":
		rowCount, columns, err = c.convertParquet(ctx, inputPath, outputPath, compression)
	case "xlsx":
		rowCount, columns, err = c.convertXLSX(ctx, inputPath, outputPath, compression)
	default:
		return nil, fmt.Errorf("conversion not implemented for format: %s", format)
	}

	if err != nil {
		return nil, err
	}

	// Get output size
	outputInfo, err := os.Stat(outputPath)
	if err != nil {
		return nil, fmt.Errorf("failed to stat output file: %w", err)
	}

	duration := time.Since(start)
	result := &ConversionResult{
		InputPath:   inputPath,
		OutputPath:  outputPath,
		InputFormat: format,
		InputSize:   inputInfo.Size(),
		OutputSize:  outputInfo.Size(),
		RowCount:    rowCount,
		ColumnCount: len(columns),
		Columns:     columns,
		Duration:    duration,
		Throughput:  float64(rowCount) / duration.Seconds(),
		Compression: float64(inputInfo.Size()) / float64(outputInfo.Size()),
	}

	// Record telemetry
	metrics := telemetry.Global()
	metrics.AddRowsRead(rowCount)
	metrics.AddRowsWritten(rowCount)
	metrics.AddBytesRead(inputInfo.Size())
	metrics.AddBytesWritten(outputInfo.Size())
	metrics.RecordFormat(format, rowCount, inputInfo.Size(), duration, 0)
	metrics.RecordPhase("conversion", duration)

	return result, nil
}

// convertCSV converts CSV to Parquet preserving all columns.
func (c *Converter) convertCSV(ctx context.Context, input, output, compression string) (int64, []ColumnInfo, error) {
	// Use DuckDB's read_csv_auto - completely automatic schema inference
	query := fmt.Sprintf(`
		COPY (SELECT * FROM read_csv_auto('%s', header=true, all_varchar=false))
		TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, escapePath(input), escapePath(output), compression)

	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return 0, nil, fmt.Errorf("CSV conversion failed: %w", err)
	}

	return c.getParquetInfo(output)
}

// convertJSON converts JSON/JSONL to Parquet.
func (c *Converter) convertJSON(ctx context.Context, input, output, compression string, isLines bool) (int64, []ColumnInfo, error) {
	format := "auto"
	if isLines {
		format = "newline_delimited"
	}

	query := fmt.Sprintf(`
		COPY (SELECT * FROM read_json_auto('%s', format='%s'))
		TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, escapePath(input), format, escapePath(output), compression)

	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return 0, nil, fmt.Errorf("JSON conversion failed: %w", err)
	}

	return c.getParquetInfo(output)
}

// convertParquet re-compresses or copies Parquet files.
func (c *Converter) convertParquet(ctx context.Context, input, output, compression string) (int64, []ColumnInfo, error) {
	query := fmt.Sprintf(`
		COPY (SELECT * FROM read_parquet('%s'))
		TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, escapePath(input), escapePath(output), compression)

	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return 0, nil, fmt.Errorf("Parquet conversion failed: %w", err)
	}

	return c.getParquetInfo(output)
}

// convertXLSX converts Excel files to Parquet.
func (c *Converter) convertXLSX(ctx context.Context, input, output, compression string) (int64, []ColumnInfo, error) {
	// DuckDB requires the spatial extension for Excel, but we can use a different approach
	// For now, return an error suggesting the user install the extension
	// In production, we'd use the Go xlsx library as fallback

	// Try with DuckDB spatial extension
	_, err := c.db.ExecContext(ctx, "INSTALL spatial; LOAD spatial;")
	if err != nil {
		return 0, nil, fmt.Errorf("Excel support requires DuckDB spatial extension: %w", err)
	}

	query := fmt.Sprintf(`
		COPY (SELECT * FROM st_read('%s'))
		TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, escapePath(input), escapePath(output), compression)

	if _, err := c.db.ExecContext(ctx, query); err != nil {
		return 0, nil, fmt.Errorf("Excel conversion failed: %w", err)
	}

	return c.getParquetInfo(output)
}

// getParquetInfo retrieves row count and schema from a Parquet file.
func (c *Converter) getParquetInfo(path string) (int64, []ColumnInfo, error) {
	// Get row count
	var rowCount int64
	countQuery := fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, escapePath(path))
	if err := c.db.QueryRow(countQuery).Scan(&rowCount); err != nil {
		return 0, nil, fmt.Errorf("failed to get row count: %w", err)
	}

	// Get schema
	schemaQuery := fmt.Sprintf(`DESCRIBE SELECT * FROM read_parquet('%s')`, escapePath(path))
	rows, err := c.db.Query(schemaQuery)
	if err != nil {
		return rowCount, nil, fmt.Errorf("failed to get schema: %w", err)
	}
	defer rows.Close()

	var columns []ColumnInfo
	for rows.Next() {
		var name, dtype string
		var null, key, dflt, extra interface{}
		if err := rows.Scan(&name, &dtype, &null, &key, &dflt, &extra); err != nil {
			continue
		}
		columns = append(columns, ColumnInfo{
			Name:     name,
			Type:     dtype,
			Nullable: null == "YES",
		})
	}

	return rowCount, columns, nil
}

// detectFormat determines file format from extension.
func detectFormat(path string) string {
	ext := strings.ToLower(filepath.Ext(path))

	// Handle .gz compression
	if ext == ".gz" {
		path = strings.TrimSuffix(path, ".gz")
		ext = strings.ToLower(filepath.Ext(path))
	}

	switch ext {
	case ".csv":
		return "csv"
	case ".json":
		return "json"
	case ".jsonl", ".ndjson":
		return "jsonl"
	case ".parquet":
		return "parquet"
	case ".xlsx", ".xls":
		return "xlsx"
	case ".xes":
		return "xes"
	default:
		return ""
	}
}

// generateOutputPath creates output path from input path.
func generateOutputPath(inputPath string) string {
	dir := filepath.Dir(inputPath)
	base := filepath.Base(inputPath)

	// Remove all extensions (including .gz)
	for {
		ext := filepath.Ext(base)
		if ext == "" {
			break
		}
		base = strings.TrimSuffix(base, ext)
	}

	return filepath.Join(dir, base+".parquet")
}

// escapePath escapes a path for use in SQL.
func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}
