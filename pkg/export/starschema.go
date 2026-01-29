// Package export provides data export utilities for BI tools.
package export

import (
	"database/sql"
	"fmt"
	"os"
	"path/filepath"

	_ "github.com/marcboeker/go-duckdb"
)

// StarSchemaExporter generates a star schema from process mining data.
// Output: Fact_Events, Dim_Cases, Dim_Activities, Dim_Resources
type StarSchemaExporter struct {
	db         *sql.DB
	outputDir  string
	compression string
}

// NewStarSchemaExporter creates a new star schema exporter.
func NewStarSchemaExporter(outputDir, compression string) (*StarSchemaExporter, error) {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}

	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open duckdb: %w", err)
	}

	return &StarSchemaExporter{
		db:          db,
		outputDir:   outputDir,
		compression: compression,
	}, nil
}

// Export generates star schema from a Parquet input file.
func (e *StarSchemaExporter) Export(inputPath string) (*StarSchemaResult, error) {
	// Load source data
	_, err := e.db.Exec(fmt.Sprintf(`
		CREATE TABLE source AS
		SELECT * FROM read_parquet('%s')
	`, inputPath))
	if err != nil {
		return nil, fmt.Errorf("failed to load source: %w", err)
	}

	result := &StarSchemaResult{OutputDir: e.outputDir}

	// Generate Dim_Cases
	if err := e.generateDimCases(); err != nil {
		return nil, err
	}
	result.DimCases = filepath.Join(e.outputDir, "Dim_Cases.parquet")

	// Generate Dim_Activities
	if err := e.generateDimActivities(); err != nil {
		return nil, err
	}
	result.DimActivities = filepath.Join(e.outputDir, "Dim_Activities.parquet")

	// Generate Dim_Resources
	if err := e.generateDimResources(); err != nil {
		return nil, err
	}
	result.DimResources = filepath.Join(e.outputDir, "Dim_Resources.parquet")

	// Generate Dim_Time (Date dimension)
	if err := e.generateDimTime(); err != nil {
		return nil, err
	}
	result.DimTime = filepath.Join(e.outputDir, "Dim_Time.parquet")

	// Generate Fact_Events
	if err := e.generateFactEvents(); err != nil {
		return nil, err
	}
	result.FactEvents = filepath.Join(e.outputDir, "Fact_Events.parquet")

	return result, nil
}

// generateDimCases creates the case dimension table.
func (e *StarSchemaExporter) generateDimCases() error {
	query := fmt.Sprintf(`
		COPY (
			SELECT
				ROW_NUMBER() OVER (ORDER BY case_id) as case_key,
				case_id,
				MIN(timestamp) as case_start_time,
				MAX(timestamp) as case_end_time,
				COUNT(*) as event_count,
				(MAX(timestamp) - MIN(timestamp)) / 1000000000 as duration_seconds
			FROM source
			GROUP BY case_id
			ORDER BY case_id
		) TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, filepath.Join(e.outputDir, "Dim_Cases.parquet"), e.compression)

	_, err := e.db.Exec(query)
	return err
}

// generateDimActivities creates the activity dimension table.
func (e *StarSchemaExporter) generateDimActivities() error {
	query := fmt.Sprintf(`
		COPY (
			SELECT
				ROW_NUMBER() OVER (ORDER BY activity) as activity_key,
				activity as activity_name,
				COUNT(*) as total_occurrences
			FROM source
			GROUP BY activity
			ORDER BY activity
		) TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, filepath.Join(e.outputDir, "Dim_Activities.parquet"), e.compression)

	_, err := e.db.Exec(query)
	return err
}

// generateDimResources creates the resource dimension table.
func (e *StarSchemaExporter) generateDimResources() error {
	query := fmt.Sprintf(`
		COPY (
			SELECT
				ROW_NUMBER() OVER (ORDER BY COALESCE(resource, 'Unknown')) as resource_key,
				COALESCE(resource, 'Unknown') as resource_name,
				COUNT(*) as total_events
			FROM source
			GROUP BY resource
			ORDER BY resource
		) TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, filepath.Join(e.outputDir, "Dim_Resources.parquet"), e.compression)

	_, err := e.db.Exec(query)
	return err
}

// generateDimTime creates a date dimension table.
func (e *StarSchemaExporter) generateDimTime() error {
	query := fmt.Sprintf(`
		COPY (
			WITH dates AS (
				SELECT DISTINCT
					DATE_TRUNC('day', EPOCH_MS(CAST(timestamp / 1000000 AS BIGINT))) as date
				FROM source
				WHERE timestamp IS NOT NULL AND timestamp > 0
			)
			SELECT
				ROW_NUMBER() OVER (ORDER BY date) as time_key,
				date as full_date,
				EXTRACT(YEAR FROM date) as year,
				EXTRACT(QUARTER FROM date) as quarter,
				EXTRACT(MONTH FROM date) as month,
				EXTRACT(DAY FROM date) as day,
				EXTRACT(DAYOFWEEK FROM date) as day_of_week,
				EXTRACT(WEEK FROM date) as week_of_year,
				CASE WHEN EXTRACT(DAYOFWEEK FROM date) IN (0, 6) THEN 1 ELSE 0 END as is_weekend,
				CASE
					WHEN EXTRACT(MONTH FROM date) IN (1,2,3) THEN 'Q1'
					WHEN EXTRACT(MONTH FROM date) IN (4,5,6) THEN 'Q2'
					WHEN EXTRACT(MONTH FROM date) IN (7,8,9) THEN 'Q3'
					ELSE 'Q4'
				END as quarter_name,
				CASE EXTRACT(MONTH FROM date)
					WHEN 1 THEN 'January'
					WHEN 2 THEN 'February'
					WHEN 3 THEN 'March'
					WHEN 4 THEN 'April'
					WHEN 5 THEN 'May'
					WHEN 6 THEN 'June'
					WHEN 7 THEN 'July'
					WHEN 8 THEN 'August'
					WHEN 9 THEN 'September'
					WHEN 10 THEN 'October'
					WHEN 11 THEN 'November'
					WHEN 12 THEN 'December'
				END as month_name
			FROM dates
			ORDER BY date
		) TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, filepath.Join(e.outputDir, "Dim_Time.parquet"), e.compression)

	_, err := e.db.Exec(query)
	return err
}

// generateFactEvents creates the fact table with foreign keys.
func (e *StarSchemaExporter) generateFactEvents() error {
	// First create dimension lookup tables
	_, err := e.db.Exec(`
		CREATE TABLE dim_cases_lookup AS
		SELECT case_id, ROW_NUMBER() OVER (ORDER BY case_id) as case_key
		FROM (SELECT DISTINCT case_id FROM source);

		CREATE TABLE dim_activities_lookup AS
		SELECT activity, ROW_NUMBER() OVER (ORDER BY activity) as activity_key
		FROM (SELECT DISTINCT activity FROM source);

		CREATE TABLE dim_resources_lookup AS
		SELECT resource, ROW_NUMBER() OVER (ORDER BY COALESCE(resource, 'Unknown')) as resource_key
		FROM (SELECT DISTINCT resource FROM source);

		CREATE TABLE dim_time_lookup AS
		SELECT date, ROW_NUMBER() OVER (ORDER BY date) as time_key
		FROM (
			SELECT DISTINCT DATE_TRUNC('day', EPOCH_MS(CAST(timestamp / 1000000 AS BIGINT))) as date
			FROM source WHERE timestamp IS NOT NULL AND timestamp > 0
		);
	`)
	if err != nil {
		return fmt.Errorf("failed to create lookups: %w", err)
	}

	query := fmt.Sprintf(`
		COPY (
			SELECT
				ROW_NUMBER() OVER () as event_key,
				c.case_key,
				a.activity_key,
				r.resource_key,
				t.time_key,
				s.timestamp as timestamp_nanos,
				EPOCH_MS(CAST(s.timestamp / 1000000 AS BIGINT)) as event_datetime,
				-- Calculate time since previous event in same case
				LAG(s.timestamp) OVER (PARTITION BY s.case_id ORDER BY s.timestamp) as prev_timestamp,
				s.timestamp - COALESCE(LAG(s.timestamp) OVER (PARTITION BY s.case_id ORDER BY s.timestamp), s.timestamp) as time_since_prev_nanos
			FROM source s
			LEFT JOIN dim_cases_lookup c ON s.case_id = c.case_id
			LEFT JOIN dim_activities_lookup a ON s.activity = a.activity
			LEFT JOIN dim_resources_lookup r ON COALESCE(s.resource, 'Unknown') = COALESCE(r.resource, 'Unknown')
			LEFT JOIN dim_time_lookup t ON DATE_TRUNC('day', EPOCH_MS(CAST(s.timestamp / 1000000 AS BIGINT))) = t.date
			ORDER BY s.case_id, s.timestamp
		) TO '%s' (FORMAT PARQUET, COMPRESSION '%s')
	`, filepath.Join(e.outputDir, "Fact_Events.parquet"), e.compression)

	_, err = e.db.Exec(query)
	return err
}

// Close releases resources.
func (e *StarSchemaExporter) Close() error {
	return e.db.Close()
}

// StarSchemaResult contains the paths to generated files.
type StarSchemaResult struct {
	OutputDir     string `json:"output_dir"`
	FactEvents    string `json:"fact_events"`
	DimCases      string `json:"dim_cases"`
	DimActivities string `json:"dim_activities"`
	DimResources  string `json:"dim_resources"`
	DimTime       string `json:"dim_time"`
}

// Files returns all generated file paths.
func (r *StarSchemaResult) Files() []string {
	return []string{
		r.FactEvents,
		r.DimCases,
		r.DimActivities,
		r.DimResources,
		r.DimTime,
	}
}
