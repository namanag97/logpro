package decoders

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	_ "github.com/marcboeker/go-duckdb"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// DuckDBDecoder uses DuckDB for high-performance decoding.
type DuckDBDecoder struct {
	db      *sql.DB
	threads int
}

// NewDuckDBDecoder creates a DuckDB decoder.
func NewDuckDBDecoder() (*DuckDBDecoder, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	return &DuckDBDecoder{
		db:      db,
		threads: runtime.NumCPU(),
	}, nil
}

// Close releases resources.
func (d *DuckDBDecoder) Close() error {
	return d.db.Close()
}

// Formats returns supported formats.
func (d *DuckDBDecoder) Formats() []core.Format {
	return []core.Format{
		core.FormatCSV,
		core.FormatTSV,
		core.FormatJSON,
		core.FormatJSONL,
		core.FormatParquet,
	}
}

// Decode decodes a source into Arrow batches.
func (d *DuckDBDecoder) Decode(ctx context.Context, source core.Source, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	// For now, return nil - full implementation would stream batches
	return nil, fmt.Errorf("streaming decode not implemented, use ConvertToParquet")
}

// InferSchema infers the schema from a source.
func (d *DuckDBDecoder) InferSchema(ctx context.Context, source core.Source, sampleSize int) (*arrow.Schema, error) {
	// Use DuckDB to infer schema
	query := fmt.Sprintf(`DESCRIBE SELECT * FROM '%s' LIMIT 1`, escapePath(source.Location()))

	rows, err := d.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("schema inference failed: %w", err)
	}
	defer rows.Close()

	var fields []arrow.Field
	for rows.Next() {
		var name, dtype, null, key, defaultVal, extra sql.NullString
		if err := rows.Scan(&name, &dtype, &null, &key, &defaultVal, &extra); err != nil {
			continue
		}
		if name.Valid {
			fields = append(fields, arrow.Field{
				Name:     name.String,
				Type:     duckDBTypeToArrow(dtype.String),
				Nullable: null.String == "YES",
			})
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

// ConvertToParquet converts a file directly to Parquet.
func (d *DuckDBDecoder) ConvertToParquet(ctx context.Context, inputPath, outputPath string, opts ConvertOptions) (*ConvertResult, error) {
	// Configure threads
	d.db.ExecContext(ctx, fmt.Sprintf("SET threads=%d", d.threads))

	// Large file optimization
	if opts.FileSize > 1024*1024*1024 {
		d.db.ExecContext(ctx, "SET preserve_insertion_order=false")
	}

	// Ensure output directory
	if err := os.MkdirAll(filepath.Dir(outputPath), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	// Build query based on format
	var query string
	switch opts.Format {
	case core.FormatCSV, core.FormatTSV:
		csvOpts := buildCSVOptions(opts)
		query = fmt.Sprintf(`
			COPY (SELECT * FROM read_csv_auto('%s'%s))
			TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
		`, escapePath(inputPath), csvOpts, escapePath(outputPath), opts.Compression, opts.RowGroupSize)

	case core.FormatJSON:
		query = fmt.Sprintf(`
			COPY (SELECT * FROM read_json_auto('%s', format='auto', maximum_object_size=33554432))
			TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
		`, escapePath(inputPath), escapePath(outputPath), opts.Compression, opts.RowGroupSize)

	case core.FormatJSONL:
		query = fmt.Sprintf(`
			COPY (SELECT * FROM read_json_auto('%s', format='newline_delimited', maximum_object_size=33554432))
			TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
		`, escapePath(inputPath), escapePath(outputPath), opts.Compression, opts.RowGroupSize)

	case core.FormatParquet:
		query = fmt.Sprintf(`
			COPY (SELECT * FROM read_parquet('%s'))
			TO '%s' (FORMAT PARQUET, COMPRESSION '%s', ROW_GROUP_SIZE %d)
		`, escapePath(inputPath), escapePath(outputPath), opts.Compression, opts.RowGroupSize)

	default:
		return nil, fmt.Errorf("unsupported format: %s", opts.Format)
	}

	// Execute conversion
	if _, err := d.db.ExecContext(ctx, query); err != nil {
		return nil, fmt.Errorf("conversion failed: %w", err)
	}

	// Get stats
	rowCount, colCount, err := d.getParquetStats(outputPath)
	if err != nil {
		// Non-fatal, continue with zeros
		rowCount, colCount = 0, 0
	}

	return &ConvertResult{
		RowCount:    rowCount,
		ColumnCount: colCount,
	}, nil
}

// ConvertOptions configures conversion.
type ConvertOptions struct {
	Format      core.Format
	FileSize    int64
	Compression string
	RowGroupSize int
	HasHeader   bool
	Delimiter   byte
	QuoteChar   byte
	IsClean     bool
	Parallel    bool
}

// ConvertResult contains conversion results.
type ConvertResult struct {
	RowCount    int64
	ColumnCount int
}

// buildCSVOptions builds DuckDB read_csv options.
func buildCSVOptions(opts ConvertOptions) string {
	var options []string

	options = append(options, fmt.Sprintf("header=%v", opts.HasHeader))

	delimStr := string(opts.Delimiter)
	if opts.Delimiter == '\t' {
		delimStr = "\\t"
	}
	if opts.Delimiter != 0 {
		options = append(options, fmt.Sprintf("delim='%s'", delimStr))
	}

	if opts.QuoteChar != 0 {
		options = append(options, fmt.Sprintf("quote='%c'", opts.QuoteChar))
	}

	if opts.IsClean && opts.Parallel {
		options = append(options, "parallel=true")
	}

	options = append(options, "all_varchar=false")
	options = append(options, "sample_size=10000")

	if len(options) == 0 {
		return ""
	}
	return ", " + strings.Join(options, ", ")
}

// getParquetStats retrieves stats from a Parquet file.
func (d *DuckDBDecoder) getParquetStats(path string) (int64, int, error) {
	var rowCount int64
	query := fmt.Sprintf(`SELECT SUM(num_rows) FROM parquet_metadata('%s')`, escapePath(path))
	if err := d.db.QueryRow(query).Scan(&rowCount); err != nil {
		// Fallback to count
		query = fmt.Sprintf(`SELECT COUNT(*) FROM read_parquet('%s')`, escapePath(path))
		d.db.QueryRow(query).Scan(&rowCount)
	}

	var colCount int
	query = fmt.Sprintf(`SELECT COUNT(*) FROM parquet_schema('%s') WHERE name IS NOT NULL`, escapePath(path))
	d.db.QueryRow(query).Scan(&colCount)

	return rowCount, colCount, nil
}

// BulkConvert converts multiple files efficiently.
func (d *DuckDBDecoder) BulkConvert(ctx context.Context, inputs []string, outputDir string, opts ConvertOptions) ([]*ConvertResult, error) {
	results := make([]*ConvertResult, len(inputs))

	for i, input := range inputs {
		base := filepath.Base(input)
		ext := filepath.Ext(base)
		name := strings.TrimSuffix(base, ext)
		output := filepath.Join(outputDir, name+".parquet")

		result, err := d.ConvertToParquet(ctx, input, output, opts)
		if err != nil {
			return results, fmt.Errorf("failed to convert %s: %w", input, err)
		}
		results[i] = result
	}

	return results, nil
}

// escapePath escapes a path for DuckDB SQL.
func escapePath(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

// duckDBTypeToArrow converts DuckDB type to Arrow type.
func duckDBTypeToArrow(dtype string) arrow.DataType {
	dtype = strings.ToUpper(dtype)

	switch {
	case strings.Contains(dtype, "BIGINT"):
		return arrow.PrimitiveTypes.Int64
	case strings.Contains(dtype, "INTEGER"), strings.Contains(dtype, "INT"):
		return arrow.PrimitiveTypes.Int32
	case strings.Contains(dtype, "SMALLINT"):
		return arrow.PrimitiveTypes.Int16
	case strings.Contains(dtype, "TINYINT"):
		return arrow.PrimitiveTypes.Int8
	case strings.Contains(dtype, "DOUBLE"), strings.Contains(dtype, "FLOAT"):
		return arrow.PrimitiveTypes.Float64
	case strings.Contains(dtype, "BOOLEAN"), strings.Contains(dtype, "BOOL"):
		return arrow.FixedWidthTypes.Boolean
	case strings.Contains(dtype, "DATE"):
		return arrow.FixedWidthTypes.Date32
	case strings.Contains(dtype, "TIMESTAMP"):
		return arrow.FixedWidthTypes.Timestamp_us
	case strings.Contains(dtype, "TIME"):
		return arrow.FixedWidthTypes.Time64us
	default:
		return arrow.BinaryTypes.String
	}
}

// SetThreads sets the number of DuckDB threads.
func (d *DuckDBDecoder) SetThreads(n int) {
	d.threads = n
}

// ExecuteQuery executes a raw query.
func (d *DuckDBDecoder) ExecuteQuery(ctx context.Context, query string) error {
	_, err := d.db.ExecContext(ctx, query)
	return err
}

// Query executes a query and returns rows.
func (d *DuckDBDecoder) Query(ctx context.Context, query string) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, query)
}

func init() {
	// Register decoder on import
	decoder, err := NewDuckDBDecoder()
	if err == nil {
		Register(decoder)
	}
}
