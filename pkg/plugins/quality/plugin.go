// Package quality provides data quality analysis as a plugin.
package quality

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/logflow/logflow/pkg/core"
)

// Plugin analyzes data quality.
type Plugin struct {
	db *sql.DB
}

// NewPlugin creates a new quality analyzer.
func NewPlugin() (*Plugin, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	return &Plugin{db: db}, nil
}

// Name implements core.Plugin.
func (p *Plugin) Name() string {
	return "quality"
}

// Description implements core.Plugin.
func (p *Plugin) Description() string {
	return "Analyze data quality (completeness, uniqueness, distributions)"
}

// CanProcess implements core.Plugin.
func (p *Plugin) CanProcess(result *core.ConversionResult) bool {
	return true // Can analyze any Parquet file
}

// Close releases resources.
func (p *Plugin) Close() error {
	return p.db.Close()
}

// Report holds quality analysis results.
type Report struct {
	RowCount     int64           `json:"row_count"`
	ColumnCount  int             `json:"column_count"`
	Columns      []ColumnQuality `json:"columns"`
	OverallScore float64         `json:"overall_score"` // 0-100
	Issues       []Issue         `json:"issues,omitempty"`
}

// ColumnQuality describes quality metrics for a column.
type ColumnQuality struct {
	Name         string  `json:"name"`
	Type         string  `json:"type"`
	NullCount    int64   `json:"null_count"`
	NullPercent  float64 `json:"null_percent"`
	UniqueCount  int64   `json:"unique_count"`
	UniquePercent float64 `json:"unique_percent"`
	Completeness float64 `json:"completeness"` // 100 - null_percent
}

// Issue describes a quality problem.
type Issue struct {
	Severity    string `json:"severity"` // warning, error
	Column      string `json:"column,omitempty"`
	Description string `json:"description"`
}

// Analyze implements core.Analyzer.
func (p *Plugin) Analyze(ctx context.Context, result *core.ConversionResult) (interface{}, error) {
	report := &Report{
		RowCount:    result.RowCount,
		ColumnCount: result.ColumnCount,
	}

	path := escapePath(result.OutputPath)

	// Analyze each column
	for _, col := range result.Columns {
		cq := ColumnQuality{
			Name: col.Name,
			Type: col.Type,
		}

		// Null count and unique count
		query := fmt.Sprintf(`
			SELECT
				SUM(CASE WHEN "%s" IS NULL THEN 1 ELSE 0 END) as nulls,
				COUNT(DISTINCT "%s") as uniques
			FROM read_parquet('%s')
		`, col.Name, col.Name, path)

		if err := p.db.QueryRowContext(ctx, query).Scan(&cq.NullCount, &cq.UniqueCount); err != nil {
			continue
		}

		if result.RowCount > 0 {
			cq.NullPercent = float64(cq.NullCount) * 100 / float64(result.RowCount)
			cq.UniquePercent = float64(cq.UniqueCount) * 100 / float64(result.RowCount)
			cq.Completeness = 100 - cq.NullPercent
		}

		report.Columns = append(report.Columns, cq)

		// Check for issues
		if cq.NullPercent > 50 {
			report.Issues = append(report.Issues, Issue{
				Severity:    "warning",
				Column:      col.Name,
				Description: fmt.Sprintf("High null rate: %.1f%%", cq.NullPercent),
			})
		}

		if cq.UniqueCount == 1 && result.RowCount > 1 {
			report.Issues = append(report.Issues, Issue{
				Severity:    "warning",
				Column:      col.Name,
				Description: "Column has only one unique value (constant)",
			})
		}
	}

	// Calculate overall score
	if len(report.Columns) > 0 {
		var totalCompleteness float64
		for _, col := range report.Columns {
			totalCompleteness += col.Completeness
		}
		report.OverallScore = totalCompleteness / float64(len(report.Columns))
	}

	return report, nil
}

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}
