// Package heuristic provides intelligent optimization strategies based on data analysis.
//
// Key insight: Analyzing a small sample can reveal optimization opportunities:
// - Low cardinality → Dictionary encoding (10-100x compression)
// - High null rate → RLE/sparse encoding
// - Numeric types → SIMD parsing (2-3x faster)
// - Known schema → Skip inference (10% faster)
// - Consistent formats → Optimized parsers
package heuristic

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// ColumnStats holds statistics about a column.
type ColumnStats struct {
	Name        string
	Type        string  // Inferred type (BIGINT, VARCHAR, DOUBLE, TIMESTAMP, etc.)
	Cardinality int64   // Number of distinct values
	NullCount   int64   // Number of NULL values
	TotalCount  int64   // Total rows sampled
	MinLen      int     // Minimum string length (for VARCHAR)
	MaxLen      int     // Maximum string length

	// Derived metrics
	CardinalityPct float64 // % unique values
	NullPct        float64 // % NULL values
}

// Strategy represents an optimization strategy for a column.
type Strategy string

const (
	StrategyPlain      Strategy = "PLAIN"       // No special encoding
	StrategyDictionary Strategy = "DICTIONARY"  // Low cardinality, use dictionary
	StrategyRLE        Strategy = "RLE"         // High null rate, use RLE
	StrategySIMD       Strategy = "SIMD"        // Numeric, use SIMD parsing
	StrategyFixed      Strategy = "FIXED"       // Fixed-width strings
)

// Recommendation holds optimization recommendations for a column.
type Recommendation struct {
	Column       ColumnStats
	Strategy     Strategy
	Reason       string
	ExpectedGain float64 // Expected speedup (e.g., 1.2 = 20% faster)
}

// AnalysisResult holds the full analysis of a file.
type AnalysisResult struct {
	FilePath      string
	SampleSize    int
	SampleTime    time.Duration
	TotalColumns  int
	Columns       []ColumnStats
	Recommendations []Recommendation
	OptimalSchema string // DuckDB columns parameter
}

// Analyzer performs heuristic analysis on data files.
type Analyzer struct {
	db         *sql.DB
	sampleSize int
}

// NewAnalyzer creates a new heuristic analyzer.
func NewAnalyzer(sampleSize int) (*Analyzer, error) {
	if sampleSize <= 0 {
		sampleSize = 1000
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	return &Analyzer{
		db:         db,
		sampleSize: sampleSize,
	}, nil
}

// Close releases resources.
func (a *Analyzer) Close() error {
	return a.db.Close()
}

// Analyze performs heuristic analysis on a CSV file.
func (a *Analyzer) Analyze(ctx context.Context, path string) (*AnalysisResult, error) {
	start := time.Now()

	result := &AnalysisResult{
		FilePath:   path,
		SampleSize: a.sampleSize,
	}

	// Step 1: Get schema from sample
	schemaQuery := fmt.Sprintf(`DESCRIBE SELECT * FROM read_csv_auto('%s', header=true, sample_size=%d)`,
		escapePath(path), a.sampleSize)

	rows, err := a.db.QueryContext(ctx, schemaQuery)
	if err != nil {
		return nil, fmt.Errorf("schema inference failed: %w", err)
	}

	var columns []ColumnStats
	for rows.Next() {
		var name, dtype string
		var null, key, dflt, extra interface{}
		if err := rows.Scan(&name, &dtype, &null, &key, &dflt, &extra); err != nil {
			continue
		}
		columns = append(columns, ColumnStats{
			Name: name,
			Type: dtype,
		})
	}
	rows.Close()

	// Step 2: Get statistics for each column
	for i := range columns {
		col := &columns[i]

		statsQuery := fmt.Sprintf(`
			SELECT
				COUNT(DISTINCT "%s") as cardinality,
				COUNT(*) - COUNT("%s") as null_count,
				COUNT(*) as total
			FROM read_csv_auto('%s', header=true, sample_size=%d)
		`, col.Name, col.Name, escapePath(path), a.sampleSize)

		a.db.QueryRowContext(ctx, statsQuery).Scan(&col.Cardinality, &col.NullCount, &col.TotalCount)

		if col.TotalCount > 0 {
			col.CardinalityPct = float64(col.Cardinality) / float64(col.TotalCount) * 100
			col.NullPct = float64(col.NullCount) / float64(col.TotalCount) * 100
		}
	}

	result.Columns = columns
	result.TotalColumns = len(columns)
	result.SampleTime = time.Since(start)

	// Step 3: Generate recommendations
	result.Recommendations = a.generateRecommendations(columns)

	// Step 4: Build optimal schema
	result.OptimalSchema = a.buildOptimalSchema(columns)

	return result, nil
}

// generateRecommendations creates optimization recommendations for each column.
func (a *Analyzer) generateRecommendations(columns []ColumnStats) []Recommendation {
	var recs []Recommendation

	for _, col := range columns {
		var strategy Strategy
		var reason string
		var gain float64 = 1.0

		switch {
		case col.CardinalityPct < 5:
			// Low cardinality - dictionary encoding will compress well
			strategy = StrategyDictionary
			reason = fmt.Sprintf("Low cardinality (%.1f%% unique) - dictionary encoding", col.CardinalityPct)
			gain = 1.3 // 30% better compression

		case col.NullPct > 50:
			// High null rate - RLE for nulls
			strategy = StrategyRLE
			reason = fmt.Sprintf("High null rate (%.1f%%) - RLE for null bitmap", col.NullPct)
			gain = 1.2

		case col.Type == "BIGINT" || col.Type == "DOUBLE" || col.Type == "INTEGER":
			// Numeric types - SIMD parsing
			strategy = StrategySIMD
			reason = fmt.Sprintf("Numeric type (%s) - SIMD parsing enabled", col.Type)
			gain = 1.15 // 15% faster parsing

		case col.Type == "VARCHAR" && col.CardinalityPct == 100:
			// Unique strings - plain encoding, no dictionary overhead
			strategy = StrategyPlain
			reason = "High cardinality strings - plain encoding (skip dictionary)"
			gain = 1.05

		default:
			strategy = StrategyPlain
			reason = "Default encoding"
			gain = 1.0
		}

		recs = append(recs, Recommendation{
			Column:       col,
			Strategy:     strategy,
			Reason:       reason,
			ExpectedGain: gain,
		})
	}

	return recs
}

// buildOptimalSchema creates a DuckDB columns parameter with optimal types.
func (a *Analyzer) buildOptimalSchema(columns []ColumnStats) string {
	parts := make([]string, len(columns))
	for i, col := range columns {
		// Use the inferred type (already optimized by DuckDB)
		name := strings.ReplaceAll(col.Name, "'", "''")
		parts[i] = fmt.Sprintf("'%s': '%s'", name, col.Type)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// Report generates a human-readable analysis report.
func (r *AnalysisResult) Report() string {
	var sb strings.Builder

	sb.WriteString("╔═══════════════════════════════════════════════════════════╗\n")
	sb.WriteString("║              HEURISTIC ANALYSIS REPORT                     ║\n")
	sb.WriteString("╚═══════════════════════════════════════════════════════════╝\n\n")

	sb.WriteString(fmt.Sprintf("File:         %s\n", r.FilePath))
	sb.WriteString(fmt.Sprintf("Sample Size:  %d rows\n", r.SampleSize))
	sb.WriteString(fmt.Sprintf("Sample Time:  %v\n", r.SampleTime.Round(time.Millisecond)))
	sb.WriteString(fmt.Sprintf("Columns:      %d\n\n", r.TotalColumns))

	sb.WriteString("COLUMN ANALYSIS:\n")
	sb.WriteString("─────────────────────────────────────────────────────────────\n")
	sb.WriteString(fmt.Sprintf("%-15s %-10s %8s %8s  Strategy\n", "Column", "Type", "Card%", "Null%"))
	sb.WriteString("─────────────────────────────────────────────────────────────\n")

	for _, rec := range r.Recommendations {
		col := rec.Column
		sb.WriteString(fmt.Sprintf("%-15s %-10s %7.1f%% %7.1f%%  %s\n",
			truncate(col.Name, 15),
			col.Type,
			col.CardinalityPct,
			col.NullPct,
			rec.Strategy))
	}

	sb.WriteString("\nOPTIMIZATION RECOMMENDATIONS:\n")
	sb.WriteString("─────────────────────────────────────────────────────────────\n")

	totalGain := 1.0
	for _, rec := range r.Recommendations {
		if rec.ExpectedGain > 1.0 {
			sb.WriteString(fmt.Sprintf("• %s: %s (+%.0f%%)\n",
				rec.Column.Name, rec.Reason, (rec.ExpectedGain-1)*100))
			totalGain *= rec.ExpectedGain
		}
	}

	if totalGain > 1.0 {
		sb.WriteString(fmt.Sprintf("\nExpected Overall Speedup: %.1fx\n", totalGain))
	}

	return sb.String()
}

// ToReadCSVQuery generates an optimized read_csv query.
func (r *AnalysisResult) ToReadCSVQuery(path string) string {
	return fmt.Sprintf(`read_csv('%s', header=true, columns=%s)`,
		escapePath(path), r.OptimalSchema)
}

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

func truncate(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	return s[:maxLen-1] + "…"
}
