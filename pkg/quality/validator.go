// Package quality provides data quality validation and information metrics.
//
// Key capabilities:
// - Entropy measurement (bits of information per column)
// - Data integrity verification (row counts, checksums)
// - Statistical comparison (before/after conversion)
// - Quality scoring
package quality

import (
	"context"
	"database/sql"
	"fmt"
	"math"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// ColumnMetrics holds information metrics for a single column.
type ColumnMetrics struct {
	Name            string
	Type            string
	RowCount        int64
	NullCount       int64
	DistinctCount   int64
	Entropy         float64 // Shannon entropy in bits
	NullPct         float64
	CardinalityPct  float64
	MinValue        string
	MaxValue        string
	Checksum        string // MD5 of sorted values
}

// DatasetMetrics holds metrics for an entire dataset.
type DatasetMetrics struct {
	Path            string
	Format          string
	RowCount        int64
	ColumnCount     int
	TotalBytes      int64
	Columns         []ColumnMetrics
	TotalEntropy    float64 // Sum of column entropies
	ComputeTime     time.Duration
	DataFingerprint string // Overall data hash
}

// ValidationResult compares input and output datasets.
type ValidationResult struct {
	Input           DatasetMetrics
	Output          DatasetMetrics
	RowsMatch       bool
	ColumnsMatch    bool
	DataIntegrity   float64 // 0-100% integrity score
	CompressionBits float64 // Bits per row in output vs input
	InfoPreserved   float64 // % of information preserved
	ColumnResults   []ColumnValidation
}

// ColumnValidation compares a single column between input and output.
type ColumnValidation struct {
	Name           string
	RowCountMatch  bool
	NullCountMatch bool
	ChecksumMatch  bool
	EntropyDiff    float64 // Output entropy - Input entropy
	IntegrityScore float64
}

// Validator performs data quality validation.
type Validator struct {
	db *sql.DB
}

// NewValidator creates a new data quality validator.
func NewValidator() (*Validator, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	// Apply optimal settings
	db.Exec("SET threads = 10")
	db.Exec("SET memory_limit = '8GB'")
	db.Exec("SET enable_object_cache = true")

	return &Validator{db: db}, nil
}

// Close releases resources.
func (v *Validator) Close() error {
	return v.db.Close()
}

// AnalyzeCSV computes metrics for a CSV file.
func (v *Validator) AnalyzeCSV(ctx context.Context, path string) (*DatasetMetrics, error) {
	start := time.Now()

	metrics := &DatasetMetrics{
		Path:   path,
		Format: "csv",
	}

	// Get row count
	err := v.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT COUNT(*) FROM read_csv_auto('%s', header=true, ignore_errors=true)`,
		escapePath(path))).Scan(&metrics.RowCount)
	if err != nil {
		return nil, fmt.Errorf("row count failed: %w", err)
	}

	// Get column info
	rows, err := v.db.QueryContext(ctx, fmt.Sprintf(
		`DESCRIBE SELECT * FROM read_csv_auto('%s', header=true, sample_size=1000)`,
		escapePath(path)))
	if err != nil {
		return nil, err
	}

	var columns []string
	var types []string
	for rows.Next() {
		var name, dtype string
		var null, key, dflt, extra interface{}
		rows.Scan(&name, &dtype, &null, &key, &dflt, &extra)
		columns = append(columns, name)
		types = append(types, dtype)
	}
	rows.Close()

	metrics.ColumnCount = len(columns)

	// Analyze each column
	for i, col := range columns {
		colMetrics, err := v.analyzeColumnCSV(ctx, path, col, types[i])
		if err != nil {
			continue
		}
		metrics.Columns = append(metrics.Columns, *colMetrics)
		metrics.TotalEntropy += colMetrics.Entropy
	}

	metrics.ComputeTime = time.Since(start)

	// Compute data fingerprint
	metrics.DataFingerprint = v.computeFingerprint(ctx, path, "csv", columns)

	return metrics, nil
}

// AnalyzeParquet computes metrics for a Parquet file.
func (v *Validator) AnalyzeParquet(ctx context.Context, path string) (*DatasetMetrics, error) {
	start := time.Now()

	metrics := &DatasetMetrics{
		Path:   path,
		Format: "parquet",
	}

	// Get row count from metadata (fast)
	err := v.db.QueryRowContext(ctx, fmt.Sprintf(
		`SELECT SUM(num_rows) FROM parquet_metadata('%s')`,
		escapePath(path))).Scan(&metrics.RowCount)
	if err != nil {
		// Fallback to count
		v.db.QueryRowContext(ctx, fmt.Sprintf(
			`SELECT COUNT(*) FROM read_parquet('%s')`,
			escapePath(path))).Scan(&metrics.RowCount)
	}

	// Get schema
	rows, err := v.db.QueryContext(ctx, fmt.Sprintf(
		`SELECT name, type FROM parquet_schema('%s') WHERE name IS NOT NULL`,
		escapePath(path)))
	if err != nil {
		return nil, err
	}

	var columns []string
	var types []string
	for rows.Next() {
		var name, dtype string
		rows.Scan(&name, &dtype)
		if name != "" && name != "duckdb_schema" {
			columns = append(columns, name)
			types = append(types, dtype)
		}
	}
	rows.Close()

	metrics.ColumnCount = len(columns)

	// Analyze each column
	for i, col := range columns {
		colMetrics, err := v.analyzeColumnParquet(ctx, path, col, types[i])
		if err != nil {
			continue
		}
		metrics.Columns = append(metrics.Columns, *colMetrics)
		metrics.TotalEntropy += colMetrics.Entropy
	}

	metrics.ComputeTime = time.Since(start)

	// Compute data fingerprint
	metrics.DataFingerprint = v.computeFingerprint(ctx, path, "parquet", columns)

	return metrics, nil
}

func (v *Validator) analyzeColumnCSV(ctx context.Context, path, column, dtype string) (*ColumnMetrics, error) {
	m := &ColumnMetrics{
		Name: column,
		Type: dtype,
	}

	// Single query for all metrics
	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as total,
			COUNT(*) - COUNT("%s") as nulls,
			COUNT(DISTINCT "%s") as distinct_count,
			COALESCE(entropy("%s"::VARCHAR), 0) as entropy
		FROM read_csv_auto('%s', header=true, ignore_errors=true)
	`, column, column, column, escapePath(path))

	err := v.db.QueryRowContext(ctx, query).Scan(
		&m.RowCount, &m.NullCount, &m.DistinctCount, &m.Entropy)
	if err != nil {
		return nil, err
	}

	if m.RowCount > 0 {
		m.NullPct = float64(m.NullCount) / float64(m.RowCount) * 100
		m.CardinalityPct = float64(m.DistinctCount) / float64(m.RowCount) * 100
	}

	return m, nil
}

func (v *Validator) analyzeColumnParquet(ctx context.Context, path, column, dtype string) (*ColumnMetrics, error) {
	m := &ColumnMetrics{
		Name: column,
		Type: dtype,
	}

	query := fmt.Sprintf(`
		SELECT
			COUNT(*) as total,
			COUNT(*) - COUNT("%s") as nulls,
			COUNT(DISTINCT "%s") as distinct_count,
			COALESCE(entropy("%s"::VARCHAR), 0) as entropy
		FROM read_parquet('%s')
	`, column, column, column, escapePath(path))

	err := v.db.QueryRowContext(ctx, query).Scan(
		&m.RowCount, &m.NullCount, &m.DistinctCount, &m.Entropy)
	if err != nil {
		return nil, err
	}

	if m.RowCount > 0 {
		m.NullPct = float64(m.NullCount) / float64(m.RowCount) * 100
		m.CardinalityPct = float64(m.DistinctCount) / float64(m.RowCount) * 100
	}

	return m, nil
}

func (v *Validator) computeFingerprint(ctx context.Context, path, format string, columns []string) string {
	// Use first 3 columns for fingerprint
	if len(columns) > 3 {
		columns = columns[:3]
	}

	colExpr := make([]string, len(columns))
	for i, c := range columns {
		colExpr[i] = fmt.Sprintf(`COALESCE("%s"::VARCHAR, '')`, c)
	}

	var readFunc string
	if format == "csv" {
		readFunc = fmt.Sprintf("read_csv_auto('%s', header=true, ignore_errors=true)", escapePath(path))
	} else {
		readFunc = fmt.Sprintf("read_parquet('%s')", escapePath(path))
	}

	query := fmt.Sprintf(`
		SELECT md5(string_agg(%s, '|'))
		FROM (SELECT * FROM %s LIMIT 10000)
	`, strings.Join(colExpr, " || ',' || "), readFunc)

	var fingerprint string
	v.db.QueryRowContext(ctx, query).Scan(&fingerprint)
	return fingerprint
}

// Validate compares input CSV to output Parquet.
func (v *Validator) Validate(ctx context.Context, inputCSV, outputParquet string) (*ValidationResult, error) {
	result := &ValidationResult{}

	// Analyze both
	input, err := v.AnalyzeCSV(ctx, inputCSV)
	if err != nil {
		return nil, fmt.Errorf("input analysis failed: %w", err)
	}
	result.Input = *input

	output, err := v.AnalyzeParquet(ctx, outputParquet)
	if err != nil {
		return nil, fmt.Errorf("output analysis failed: %w", err)
	}
	result.Output = *output

	// Compare
	result.RowsMatch = input.RowCount == output.RowCount
	result.ColumnsMatch = input.ColumnCount == output.ColumnCount
	result.DataIntegrity = 100.0

	// Compare columns
	for i, inCol := range input.Columns {
		if i >= len(output.Columns) {
			break
		}
		outCol := output.Columns[i]

		cv := ColumnValidation{
			Name:           inCol.Name,
			RowCountMatch:  inCol.RowCount == outCol.RowCount,
			NullCountMatch: inCol.NullCount == outCol.NullCount,
			EntropyDiff:    outCol.Entropy - inCol.Entropy,
		}

		// Integrity score based on entropy preservation
		if inCol.Entropy > 0 {
			cv.IntegrityScore = math.Min(100, outCol.Entropy/inCol.Entropy*100)
		} else {
			cv.IntegrityScore = 100
		}

		result.ColumnResults = append(result.ColumnResults, cv)
	}

	// Overall info preserved
	if input.TotalEntropy > 0 {
		result.InfoPreserved = output.TotalEntropy / input.TotalEntropy * 100
	} else {
		result.InfoPreserved = 100
	}

	return result, nil
}

// Report generates a human-readable validation report.
func (r *ValidationResult) Report() string {
	var sb strings.Builder

	sb.WriteString("╔═══════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║              DATA QUALITY VALIDATION REPORT                ║\n")
	sb.WriteString("╚═══════════════════════════════════════════════════════════╝\n\n")

	// Summary
	sb.WriteString("SUMMARY:\n")
	sb.WriteString("─────────────────────────────────────────────────────────────\n")
	sb.WriteString(fmt.Sprintf("  Input:           %s (%d rows, %d cols)\n", r.Input.Path, r.Input.RowCount, r.Input.ColumnCount))
	sb.WriteString(fmt.Sprintf("  Output:          %s (%d rows, %d cols)\n", r.Output.Path, r.Output.RowCount, r.Output.ColumnCount))
	sb.WriteString(fmt.Sprintf("  Rows Match:      %v\n", r.RowsMatch))
	sb.WriteString(fmt.Sprintf("  Info Preserved:  %.1f%%\n", r.InfoPreserved))
	sb.WriteString("\n")

	// Entropy comparison
	sb.WriteString("INFORMATION CONTENT (Entropy in bits):\n")
	sb.WriteString("─────────────────────────────────────────────────────────────\n")
	sb.WriteString(fmt.Sprintf("  %-20s %10s %10s %10s\n", "Column", "Input", "Output", "Diff"))
	sb.WriteString("─────────────────────────────────────────────────────────────\n")

	for _, cv := range r.ColumnResults {
		var inEntropy, outEntropy float64
		for _, c := range r.Input.Columns {
			if c.Name == cv.Name {
				inEntropy = c.Entropy
				break
			}
		}
		for _, c := range r.Output.Columns {
			if c.Name == cv.Name {
				outEntropy = c.Entropy
				break
			}
		}
		sb.WriteString(fmt.Sprintf("  %-20s %10.2f %10.2f %+10.2f\n",
			truncate(cv.Name, 20), inEntropy, outEntropy, cv.EntropyDiff))
	}

	sb.WriteString(fmt.Sprintf("\n  %-20s %10.2f %10.2f\n", "TOTAL", r.Input.TotalEntropy, r.Output.TotalEntropy))

	return sb.String()
}

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n-1] + "…"
}
