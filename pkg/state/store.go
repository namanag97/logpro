// Package state provides persistent storage for jobs, metrics, and profiles.
package state

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb" // We'll use DuckDB for SQLite compatibility
)

// Store manages persistent state.
type Store struct {
	db *sql.DB
	mu sync.RWMutex
}

// Job represents a conversion job record.
type Job struct {
	ID               string                 `json:"id"`
	Status           string                 `json:"status"`
	InputPath        string                 `json:"input_path"`
	InputFormat      string                 `json:"input_format,omitempty"`
	InputSize        int64                  `json:"input_size,omitempty"`
	OutputPath       string                 `json:"output_path,omitempty"`
	OutputSize       int64                  `json:"output_size,omitempty"`
	RowCount         int64                  `json:"row_count,omitempty"`
	ColumnCount      int                    `json:"column_count,omitempty"`
	CompressionRatio float64                `json:"compression_ratio,omitempty"`
	DurationMS       int64                  `json:"duration_ms,omitempty"`
	Throughput       float64                `json:"throughput,omitempty"`
	Error            string                 `json:"error,omitempty"`
	Config           map[string]interface{} `json:"config,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
	CompletedAt      *time.Time             `json:"completed_at,omitempty"`
}

// Metric represents a performance metric.
type Metric struct {
	ID          int64                  `json:"id"`
	JobID       string                 `json:"job_id"`
	MetricName  string                 `json:"metric_name"`
	MetricValue float64                `json:"metric_value"`
	Context     map[string]interface{} `json:"context,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
}

// Profile represents a saved configuration profile.
type Profile struct {
	Name       string                 `json:"name"`
	Config     map[string]interface{} `json:"config"`
	UsageCount int                    `json:"usage_count"`
	LastUsedAt *time.Time             `json:"last_used_at,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
}

// SchemaCache caches inferred schemas.
type SchemaCache struct {
	FileHash     string                 `json:"file_hash"`
	FilePath     string                 `json:"file_path"`
	FileSize     int64                  `json:"file_size"`
	Schema       map[string]interface{} `json:"schema"`
	PMSuggestion map[string]interface{} `json:"pm_suggestion,omitempty"`
	CreatedAt    time.Time              `json:"created_at"`
}

// NewStore creates a new state store.
func NewStore(dbPath string) (*Store, error) {
	db, err := sql.Open("duckdb", dbPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	store := &Store{db: db}
	if err := store.migrate(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to migrate database: %w", err)
	}

	return store, nil
}

// migrate runs database migrations.
func (s *Store) migrate() error {
	migrations := []string{
		// Jobs table
		`CREATE TABLE IF NOT EXISTS jobs (
			id TEXT PRIMARY KEY,
			status TEXT NOT NULL,
			input_path TEXT NOT NULL,
			input_format TEXT,
			input_size BIGINT,
			output_path TEXT,
			output_size BIGINT,
			row_count BIGINT,
			column_count INTEGER,
			compression_ratio DOUBLE,
			duration_ms BIGINT,
			throughput DOUBLE,
			error TEXT,
			config JSON,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			completed_at TIMESTAMP
		)`,

		// Schema cache table
		`CREATE TABLE IF NOT EXISTS schema_cache (
			file_hash TEXT PRIMARY KEY,
			file_path TEXT,
			file_size BIGINT,
			schema JSON,
			pm_suggestion JSON,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		// Profiles table
		`CREATE TABLE IF NOT EXISTS profiles (
			name TEXT PRIMARY KEY,
			config JSON NOT NULL,
			usage_count INTEGER DEFAULT 0,
			last_used_at TIMESTAMP,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		// Metrics table
		`CREATE TABLE IF NOT EXISTS metrics (
			id INTEGER PRIMARY KEY,
			job_id TEXT,
			metric_name TEXT NOT NULL,
			metric_value DOUBLE NOT NULL,
			context JSON,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		// Indexes
		`CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status)`,
		`CREATE INDEX IF NOT EXISTS idx_jobs_created ON jobs(created_at)`,
		`CREATE INDEX IF NOT EXISTS idx_metrics_name ON metrics(metric_name)`,
		`CREATE INDEX IF NOT EXISTS idx_metrics_job ON metrics(job_id)`,
	}

	for _, migration := range migrations {
		if _, err := s.db.Exec(migration); err != nil {
			return fmt.Errorf("migration failed: %w", err)
		}
	}

	return nil
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// --- Job Operations ---

// CreateJob creates a new job record.
func (s *Store) CreateJob(job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	configJSON, _ := json.Marshal(job.Config)

	_, err := s.db.Exec(`
		INSERT INTO jobs (id, status, input_path, input_format, input_size, config, created_at)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`, job.ID, job.Status, job.InputPath, job.InputFormat, job.InputSize, string(configJSON), job.CreatedAt)

	return err
}

// UpdateJob updates a job record.
func (s *Store) UpdateJob(job *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	configJSON, _ := json.Marshal(job.Config)

	_, err := s.db.Exec(`
		UPDATE jobs SET
			status = ?,
			output_path = ?,
			output_size = ?,
			row_count = ?,
			column_count = ?,
			compression_ratio = ?,
			duration_ms = ?,
			throughput = ?,
			error = ?,
			config = ?,
			completed_at = ?
		WHERE id = ?
	`, job.Status, job.OutputPath, job.OutputSize, job.RowCount, job.ColumnCount,
		job.CompressionRatio, job.DurationMS, job.Throughput, job.Error,
		string(configJSON), job.CompletedAt, job.ID)

	return err
}

// GetJob retrieves a job by ID.
func (s *Store) GetJob(id string) (*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job := &Job{}
	var configJSON sql.NullString
	var completedAt sql.NullTime

	err := s.db.QueryRow(`
		SELECT id, status, input_path, input_format, input_size, output_path,
		       output_size, row_count, column_count, compression_ratio,
		       duration_ms, throughput, error, config, created_at, completed_at
		FROM jobs WHERE id = ?
	`, id).Scan(
		&job.ID, &job.Status, &job.InputPath, &job.InputFormat, &job.InputSize,
		&job.OutputPath, &job.OutputSize, &job.RowCount, &job.ColumnCount,
		&job.CompressionRatio, &job.DurationMS, &job.Throughput, &job.Error,
		&configJSON, &job.CreatedAt, &completedAt,
	)

	if err != nil {
		return nil, err
	}

	if configJSON.Valid {
		json.Unmarshal([]byte(configJSON.String), &job.Config)
	}
	if completedAt.Valid {
		job.CompletedAt = &completedAt.Time
	}

	return job, nil
}

// ListJobs returns recent jobs.
func (s *Store) ListJobs(limit int) ([]*Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`
		SELECT id, status, input_path, input_format, input_size, output_path,
		       output_size, row_count, compression_ratio, duration_ms, throughput,
		       error, created_at, completed_at
		FROM jobs
		ORDER BY created_at DESC
		LIMIT ?
	`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var jobs []*Job
	for rows.Next() {
		job := &Job{}
		var completedAt sql.NullTime
		err := rows.Scan(
			&job.ID, &job.Status, &job.InputPath, &job.InputFormat, &job.InputSize,
			&job.OutputPath, &job.OutputSize, &job.RowCount, &job.CompressionRatio,
			&job.DurationMS, &job.Throughput, &job.Error, &job.CreatedAt, &completedAt,
		)
		if err != nil {
			continue
		}
		if completedAt.Valid {
			job.CompletedAt = &completedAt.Time
		}
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// --- Metrics Operations ---

// RecordMetric records a performance metric.
func (s *Store) RecordMetric(jobID, name string, value float64, context map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	contextJSON, _ := json.Marshal(context)

	_, err := s.db.Exec(`
		INSERT INTO metrics (job_id, metric_name, metric_value, context, created_at)
		VALUES (?, ?, ?, ?, ?)
	`, jobID, name, value, string(contextJSON), time.Now())

	return err
}

// GetMetricStats returns aggregate statistics for a metric.
func (s *Store) GetMetricStats(name string) (avg, min, max float64, count int64, err error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	err = s.db.QueryRow(`
		SELECT AVG(metric_value), MIN(metric_value), MAX(metric_value), COUNT(*)
		FROM metrics WHERE metric_name = ?
	`, name).Scan(&avg, &min, &max, &count)

	return
}

// GetThroughputByFormat returns average throughput by input format.
func (s *Store) GetThroughputByFormat() (map[string]float64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`
		SELECT input_format, AVG(throughput) as avg_throughput
		FROM jobs
		WHERE status = 'completed' AND throughput > 0
		GROUP BY input_format
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]float64)
	for rows.Next() {
		var format string
		var throughput float64
		if err := rows.Scan(&format, &throughput); err == nil {
			result[format] = throughput
		}
	}

	return result, nil
}

// --- Profile Operations ---

// SaveProfile saves or updates a profile.
func (s *Store) SaveProfile(name string, config map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	configJSON, _ := json.Marshal(config)

	_, err := s.db.Exec(`
		INSERT INTO profiles (name, config, created_at)
		VALUES (?, ?, ?)
		ON CONFLICT(name) DO UPDATE SET config = excluded.config
	`, name, string(configJSON), time.Now())

	return err
}

// GetProfile retrieves a profile by name.
func (s *Store) GetProfile(name string) (*Profile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	profile := &Profile{}
	var configJSON string
	var lastUsedAt sql.NullTime

	err := s.db.QueryRow(`
		SELECT name, config, usage_count, last_used_at, created_at
		FROM profiles WHERE name = ?
	`, name).Scan(&profile.Name, &configJSON, &profile.UsageCount, &lastUsedAt, &profile.CreatedAt)

	if err != nil {
		return nil, err
	}

	json.Unmarshal([]byte(configJSON), &profile.Config)
	if lastUsedAt.Valid {
		profile.LastUsedAt = &lastUsedAt.Time
	}

	return profile, nil
}

// ListProfiles returns all profiles.
func (s *Store) ListProfiles() ([]*Profile, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	rows, err := s.db.Query(`
		SELECT name, config, usage_count, last_used_at, created_at
		FROM profiles ORDER BY usage_count DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var profiles []*Profile
	for rows.Next() {
		profile := &Profile{}
		var configJSON string
		var lastUsedAt sql.NullTime
		if err := rows.Scan(&profile.Name, &configJSON, &profile.UsageCount, &lastUsedAt, &profile.CreatedAt); err != nil {
			continue
		}
		json.Unmarshal([]byte(configJSON), &profile.Config)
		if lastUsedAt.Valid {
			profile.LastUsedAt = &lastUsedAt.Time
		}
		profiles = append(profiles, profile)
	}

	return profiles, nil
}

// UseProfile increments usage count and updates last_used_at.
func (s *Store) UseProfile(name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	_, err := s.db.Exec(`
		UPDATE profiles SET usage_count = usage_count + 1, last_used_at = ?
		WHERE name = ?
	`, time.Now(), name)

	return err
}

// --- Schema Cache Operations ---

// CacheSchema caches a schema inference result.
func (s *Store) CacheSchema(hash, path string, size int64, schema, pmSuggestion map[string]interface{}) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	schemaJSON, _ := json.Marshal(schema)
	pmJSON, _ := json.Marshal(pmSuggestion)

	_, err := s.db.Exec(`
		INSERT INTO schema_cache (file_hash, file_path, file_size, schema, pm_suggestion, created_at)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(file_hash) DO UPDATE SET
			file_path = excluded.file_path,
			schema = excluded.schema,
			pm_suggestion = excluded.pm_suggestion,
			created_at = excluded.created_at
	`, hash, path, size, string(schemaJSON), string(pmJSON), time.Now())

	return err
}

// GetCachedSchema retrieves a cached schema by file hash.
func (s *Store) GetCachedSchema(hash string) (*SchemaCache, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	cache := &SchemaCache{}
	var schemaJSON, pmJSON string

	err := s.db.QueryRow(`
		SELECT file_hash, file_path, file_size, schema, pm_suggestion, created_at
		FROM schema_cache WHERE file_hash = ?
	`, hash).Scan(&cache.FileHash, &cache.FilePath, &cache.FileSize, &schemaJSON, &pmJSON, &cache.CreatedAt)

	if err != nil {
		return nil, err
	}

	json.Unmarshal([]byte(schemaJSON), &cache.Schema)
	json.Unmarshal([]byte(pmJSON), &cache.PMSuggestion)

	return cache, nil
}

// --- Cleanup Operations ---

// CleanupOldJobs removes jobs older than the retention period.
func (s *Store) CleanupOldJobs(retention time.Duration) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	cutoff := time.Now().Add(-retention)
	result, err := s.db.Exec(`DELETE FROM jobs WHERE created_at < ?`, cutoff)
	if err != nil {
		return 0, err
	}

	return result.RowsAffected()
}

// GetStats returns database statistics.
func (s *Store) GetStats() (map[string]interface{}, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	stats := make(map[string]interface{})

	// Job counts
	var total, completed, failed int64
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs`).Scan(&total)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE status = 'completed'`).Scan(&completed)
	s.db.QueryRow(`SELECT COUNT(*) FROM jobs WHERE status = 'failed'`).Scan(&failed)

	stats["total_jobs"] = total
	stats["completed_jobs"] = completed
	stats["failed_jobs"] = failed

	// Total data processed
	var totalInput, totalOutput int64
	s.db.QueryRow(`SELECT COALESCE(SUM(input_size), 0), COALESCE(SUM(output_size), 0) FROM jobs WHERE status = 'completed'`).Scan(&totalInput, &totalOutput)
	stats["total_input_bytes"] = totalInput
	stats["total_output_bytes"] = totalOutput

	// Profile count
	var profileCount int64
	s.db.QueryRow(`SELECT COUNT(*) FROM profiles`).Scan(&profileCount)
	stats["profile_count"] = profileCount

	return stats, nil
}
