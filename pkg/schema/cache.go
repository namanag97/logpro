// Package schema provides schema inference and caching for faster conversions.
//
// Key insight: When converting many similar files, schema inference (15% of time)
// can be done once and reused. Explicit schemas also enable 13-23% faster parsing.
package schema

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Column represents a column's schema.
type Column struct {
	Name     string `json:"name"`
	Type     string `json:"type"`
	Nullable bool   `json:"nullable"`
	Position int    `json:"position"`
}

// Schema represents a cached schema for a file type.
type Schema struct {
	Version    string    `json:"version"`
	InferredAt time.Time `json:"inferred_at"`
	SampleSize int       `json:"sample_size"`
	Columns    []Column  `json:"columns"`
	SourceFile string    `json:"source_file,omitempty"`
	Fingerprint string   `json:"fingerprint,omitempty"` // hash of column names
}

// Cache provides thread-safe schema caching.
type Cache struct {
	mu      sync.RWMutex
	schemas map[string]*Schema
	db      *sql.DB
}

// NewCache creates a new schema cache.
func NewCache() (*Cache, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to create DuckDB connection: %w", err)
	}

	return &Cache{
		schemas: make(map[string]*Schema),
		db:      db,
	}, nil
}

// Close releases resources.
func (c *Cache) Close() error {
	return c.db.Close()
}

// InferSchema infers the schema from a CSV file using DuckDB.
func (c *Cache) InferSchema(ctx context.Context, path string, sampleSize int) (*Schema, error) {
	if sampleSize <= 0 {
		sampleSize = 1000
	}

	query := fmt.Sprintf(`DESCRIBE SELECT * FROM read_csv_auto('%s', header=true, sample_size=%d)`,
		escapePath(path), sampleSize)

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("schema inference failed: %w", err)
	}
	defer rows.Close()

	var columns []Column
	position := 0
	for rows.Next() {
		var name, dtype string
		var null, key, dflt, extra interface{}
		if err := rows.Scan(&name, &dtype, &null, &key, &dflt, &extra); err != nil {
			return nil, err
		}

		columns = append(columns, Column{
			Name:     name,
			Type:     dtype,
			Nullable: null == "YES",
			Position: position,
		})
		position++
	}

	schema := &Schema{
		Version:    "1.0",
		InferredAt: time.Now(),
		SampleSize: sampleSize,
		Columns:    columns,
		SourceFile: path,
		Fingerprint: fingerprintColumns(columns),
	}

	return schema, nil
}

// Get retrieves a cached schema by fingerprint.
func (c *Cache) Get(fingerprint string) (*Schema, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	schema, ok := c.schemas[fingerprint]
	return schema, ok
}

// Put stores a schema in the cache.
func (c *Cache) Put(schema *Schema) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.schemas[schema.Fingerprint] = schema
}

// GetOrInfer returns a cached schema or infers a new one.
func (c *Cache) GetOrInfer(ctx context.Context, path string, sampleSize int) (*Schema, error) {
	// First, do a quick sample to get column names
	quickSchema, err := c.InferSchema(ctx, path, 10)
	if err != nil {
		return nil, err
	}

	// Check if we have a cached schema with the same fingerprint
	if cached, ok := c.Get(quickSchema.Fingerprint); ok {
		return cached, nil
	}

	// Full inference
	schema, err := c.InferSchema(ctx, path, sampleSize)
	if err != nil {
		return nil, err
	}

	c.Put(schema)
	return schema, nil
}

// ToDuckDBColumns generates a DuckDB columns parameter string.
func (s *Schema) ToDuckDBColumns() string {
	parts := make([]string, len(s.Columns))
	for i, col := range s.Columns {
		// Escape column name
		name := strings.ReplaceAll(col.Name, "'", "''")
		parts[i] = fmt.Sprintf("'%s': '%s'", name, col.Type)
	}
	return "{" + strings.Join(parts, ", ") + "}"
}

// ToReadCSVQuery generates a DuckDB read_csv query with explicit schema.
func (s *Schema) ToReadCSVQuery(path string) string {
	return fmt.Sprintf(`read_csv('%s', header=true, columns=%s)`,
		escapePath(path), s.ToDuckDBColumns())
}

// Save writes the schema to a JSON file.
func (s *Schema) Save(path string) error {
	data, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0644)
}

// Load reads a schema from a JSON file.
func Load(path string) (*Schema, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var schema Schema
	if err := json.Unmarshal(data, &schema); err != nil {
		return nil, err
	}

	return &schema, nil
}

// fingerprintColumns creates a simple fingerprint from column names.
func fingerprintColumns(columns []Column) string {
	names := make([]string, len(columns))
	for i, col := range columns {
		names[i] = col.Name
	}
	return strings.Join(names, ",")
}

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

// SchemaFile returns the standard schema file path for a data file.
func SchemaFile(dataPath string) string {
	ext := filepath.Ext(dataPath)
	base := strings.TrimSuffix(dataPath, ext)
	return base + ".schema.json"
}
