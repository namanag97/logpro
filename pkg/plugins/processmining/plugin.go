// Package processmining provides optional process mining features.
// This is a PLUGIN - not required for basic conversion.
package processmining

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	_ "github.com/marcboeker/go-duckdb"

	"github.com/logflow/logflow/pkg/core"
)

// Config holds process mining configuration.
type Config struct {
	CaseIDColumn    string `json:"case_id_column"`
	ActivityColumn  string `json:"activity_column"`
	TimestampColumn string `json:"timestamp_column"`
	ResourceColumn  string `json:"resource_column,omitempty"` // Optional
}

// Plugin adds process mining capabilities.
type Plugin struct {
	config Config
	db     *sql.DB
}

// NewPlugin creates a new process mining plugin.
func NewPlugin(config Config) (*Plugin, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}
	return &Plugin{config: config, db: db}, nil
}

// Name implements core.Plugin.
func (p *Plugin) Name() string {
	return "process-mining"
}

// Description implements core.Plugin.
func (p *Plugin) Description() string {
	return "Extract process mining insights (case analysis, activity frequencies, variants)"
}

// CanProcess implements core.Plugin.
func (p *Plugin) CanProcess(result *core.ConversionResult) bool {
	// Check if required columns exist
	columns := make(map[string]bool)
	for _, col := range result.Columns {
		columns[strings.ToLower(col.Name)] = true
	}

	return columns[strings.ToLower(p.config.CaseIDColumn)] &&
		columns[strings.ToLower(p.config.ActivityColumn)] &&
		columns[strings.ToLower(p.config.TimestampColumn)]
}

// Close releases resources.
func (p *Plugin) Close() error {
	return p.db.Close()
}

// Analysis holds process mining insights.
type Analysis struct {
	TotalEvents     int64            `json:"total_events"`
	TotalCases      int64            `json:"total_cases"`
	UniqueActivities int64           `json:"unique_activities"`
	UniqueResources  int64           `json:"unique_resources,omitempty"`
	TimeRange       TimeRange        `json:"time_range"`
	CaseStats       CaseStats        `json:"case_stats"`
	TopActivities   []ActivityCount  `json:"top_activities"`
	TopVariants     []VariantCount   `json:"top_variants,omitempty"`
}

// TimeRange describes the time span of the log.
type TimeRange struct {
	Start    time.Time     `json:"start"`
	End      time.Time     `json:"end"`
	Duration time.Duration `json:"duration"`
}

// CaseStats describes case-level statistics.
type CaseStats struct {
	MinEventsPerCase int64   `json:"min_events_per_case"`
	MaxEventsPerCase int64   `json:"max_events_per_case"`
	AvgEventsPerCase float64 `json:"avg_events_per_case"`
	MinDuration      string  `json:"min_duration,omitempty"`
	MaxDuration      string  `json:"max_duration,omitempty"`
	AvgDuration      string  `json:"avg_duration,omitempty"`
}

// ActivityCount holds activity frequency.
type ActivityCount struct {
	Activity string  `json:"activity"`
	Count    int64   `json:"count"`
	Percent  float64 `json:"percent"`
}

// VariantCount holds process variant frequency.
type VariantCount struct {
	Variant string `json:"variant"`
	Count   int64  `json:"count"`
	Percent float64 `json:"percent"`
}

// Analyze implements core.Analyzer.
func (p *Plugin) Analyze(ctx context.Context, result *core.ConversionResult) (interface{}, error) {
	if !p.CanProcess(result) {
		return nil, fmt.Errorf("required columns not found in data")
	}

	path := escapePath(result.OutputPath)
	analysis := &Analysis{}

	// Total events
	analysis.TotalEvents = result.RowCount

	// Unique cases
	query := fmt.Sprintf(`
		SELECT COUNT(DISTINCT "%s") FROM read_parquet('%s')
	`, p.config.CaseIDColumn, path)
	p.db.QueryRowContext(ctx, query).Scan(&analysis.TotalCases)

	// Unique activities
	query = fmt.Sprintf(`
		SELECT COUNT(DISTINCT "%s") FROM read_parquet('%s')
	`, p.config.ActivityColumn, path)
	p.db.QueryRowContext(ctx, query).Scan(&analysis.UniqueActivities)

	// Unique resources (if configured)
	if p.config.ResourceColumn != "" {
		query = fmt.Sprintf(`
			SELECT COUNT(DISTINCT "%s") FROM read_parquet('%s')
			WHERE "%s" IS NOT NULL
		`, p.config.ResourceColumn, path, p.config.ResourceColumn)
		p.db.QueryRowContext(ctx, query).Scan(&analysis.UniqueResources)
	}

	// Time range
	query = fmt.Sprintf(`
		SELECT
			MIN("%s") as min_ts,
			MAX("%s") as max_ts
		FROM read_parquet('%s')
		WHERE "%s" IS NOT NULL
	`, p.config.TimestampColumn, p.config.TimestampColumn, path, p.config.TimestampColumn)

	var minTS, maxTS interface{}
	if err := p.db.QueryRowContext(ctx, query).Scan(&minTS, &maxTS); err == nil {
		if start, ok := parseTimestamp(minTS); ok {
			analysis.TimeRange.Start = start
		}
		if end, ok := parseTimestamp(maxTS); ok {
			analysis.TimeRange.End = end
		}
		analysis.TimeRange.Duration = analysis.TimeRange.End.Sub(analysis.TimeRange.Start)
	}

	// Case statistics
	query = fmt.Sprintf(`
		SELECT
			MIN(cnt) as min_events,
			MAX(cnt) as max_events,
			AVG(cnt) as avg_events
		FROM (
			SELECT "%s", COUNT(*) as cnt
			FROM read_parquet('%s')
			GROUP BY "%s"
		)
	`, p.config.CaseIDColumn, path, p.config.CaseIDColumn)
	p.db.QueryRowContext(ctx, query).Scan(
		&analysis.CaseStats.MinEventsPerCase,
		&analysis.CaseStats.MaxEventsPerCase,
		&analysis.CaseStats.AvgEventsPerCase,
	)

	// Top activities
	query = fmt.Sprintf(`
		SELECT
			"%s" as activity,
			COUNT(*) as cnt,
			COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct
		FROM read_parquet('%s')
		GROUP BY "%s"
		ORDER BY cnt DESC
		LIMIT 10
	`, p.config.ActivityColumn, path, p.config.ActivityColumn)

	rows, err := p.db.QueryContext(ctx, query)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var ac ActivityCount
			rows.Scan(&ac.Activity, &ac.Count, &ac.Percent)
			analysis.TopActivities = append(analysis.TopActivities, ac)
		}
	}

	// Top variants (process paths)
	query = fmt.Sprintf(`
		SELECT
			variant,
			COUNT(*) as cnt,
			COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as pct
		FROM (
			SELECT
				"%s",
				STRING_AGG("%s", ' -> ' ORDER BY "%s") as variant
			FROM read_parquet('%s')
			GROUP BY "%s"
		)
		GROUP BY variant
		ORDER BY cnt DESC
		LIMIT 10
	`, p.config.CaseIDColumn, p.config.ActivityColumn, p.config.TimestampColumn,
		path, p.config.CaseIDColumn)

	rows, err = p.db.QueryContext(ctx, query)
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var vc VariantCount
			rows.Scan(&vc.Variant, &vc.Count, &vc.Percent)
			analysis.TopVariants = append(analysis.TopVariants, vc)
		}
	}

	return analysis, nil
}

// AutoDetectColumns attempts to automatically detect PM columns.
func AutoDetectColumns(columns []core.ColumnInfo) *Config {
	config := &Config{}

	// Common column name patterns
	casePatterns := []string{"case_id", "caseid", "case", "order_id", "orderid", "ticket_id", "id"}
	activityPatterns := []string{"activity", "activity_name", "event", "action", "step"}
	timestampPatterns := []string{"timestamp", "time", "datetime", "date", "created_at", "event_time"}
	resourcePatterns := []string{"resource", "user", "agent", "handler", "operator", "employee"}

	colLower := make(map[string]string)
	for _, col := range columns {
		colLower[strings.ToLower(col.Name)] = col.Name
	}

	// Match patterns
	config.CaseIDColumn = matchPattern(colLower, casePatterns)
	config.ActivityColumn = matchPattern(colLower, activityPatterns)
	config.TimestampColumn = matchPattern(colLower, timestampPatterns)
	config.ResourceColumn = matchPattern(colLower, resourcePatterns)

	return config
}

// matchPattern finds first matching column.
func matchPattern(columns map[string]string, patterns []string) string {
	for _, pattern := range patterns {
		if original, ok := columns[pattern]; ok {
			return original
		}
		// Try partial match
		for lower, original := range columns {
			if strings.Contains(lower, pattern) {
				return original
			}
		}
	}
	return ""
}

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

func parseTimestamp(v interface{}) (time.Time, bool) {
	switch t := v.(type) {
	case time.Time:
		return t, true
	case int64:
		return time.Unix(0, t), true
	case string:
		if parsed, err := time.Parse(time.RFC3339, t); err == nil {
			return parsed, true
		}
	}
	return time.Time{}, false
}
