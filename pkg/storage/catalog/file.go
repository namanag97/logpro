// Package catalog provides catalog implementations for table metadata.
package catalog

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/logflow/logflow/pkg/interfaces"
)

// FileCatalog implements Catalog using the local filesystem.
type FileCatalog struct {
	root      string
	mu        sync.RWMutex
	databases map[string]*databaseMeta
}

type databaseMeta struct {
	Name      string                `json:"name"`
	Tables    map[string]*tableMeta `json:"tables"`
	CreatedAt time.Time             `json:"created_at"`
}

type tableMeta struct {
	Database     string                 `json:"database"`
	Name         string                 `json:"name"`
	Location     string                 `json:"location"`
	Format       interfaces.TableFormat `json:"format"`
	SchemaJSON   string                 `json:"schema"`
	Properties   map[string]string      `json:"properties"`
	RowCount     int64                  `json:"row_count"`
	SizeBytes    int64                  `json:"size_bytes"`
	FileCount    int                    `json:"file_count"`
	CreatedAt    time.Time              `json:"created_at"`
	UpdatedAt    time.Time              `json:"updated_at"`
	Partitioning interfaces.PartitionSpec `json:"partitioning"`
}

// NewFileCatalog creates a new file-based catalog.
func NewFileCatalog(root string) (*FileCatalog, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve root: %w", err)
	}

	if err := os.MkdirAll(absRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create catalog directory: %w", err)
	}

	cat := &FileCatalog{
		root:      absRoot,
		databases: make(map[string]*databaseMeta),
	}

	// Load existing metadata
	if err := cat.load(); err != nil {
		return nil, fmt.Errorf("failed to load catalog: %w", err)
	}

	return cat, nil
}

func (c *FileCatalog) load() error {
	metaPath := filepath.Join(c.root, "catalog.json")
	data, err := os.ReadFile(metaPath)
	if os.IsNotExist(err) {
		return nil // New catalog
	}
	if err != nil {
		return err
	}

	return json.Unmarshal(data, &c.databases)
}

func (c *FileCatalog) save() error {
	metaPath := filepath.Join(c.root, "catalog.json")
	data, err := json.MarshalIndent(c.databases, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(metaPath, data, 0644)
}

// CreateDatabase creates a new database.
func (c *FileCatalog) CreateDatabase(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.databases[name]; exists {
		return fmt.Errorf("database already exists: %s", name)
	}

	c.databases[name] = &databaseMeta{
		Name:      name,
		Tables:    make(map[string]*tableMeta),
		CreatedAt: time.Now(),
	}

	// Create directory
	dbPath := filepath.Join(c.root, name)
	if err := os.MkdirAll(dbPath, 0755); err != nil {
		return err
	}

	return c.save()
}

// ListDatabases lists all databases.
func (c *FileCatalog) ListDatabases(ctx context.Context) ([]string, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	names := make([]string, 0, len(c.databases))
	for name := range c.databases {
		names = append(names, name)
	}
	return names, nil
}

// DropDatabase drops a database.
func (c *FileCatalog) DropDatabase(ctx context.Context, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, exists := c.databases[name]; !exists {
		return fmt.Errorf("database not found: %s", name)
	}

	delete(c.databases, name)
	return c.save()
}

// DatabaseExists checks if a database exists.
func (c *FileCatalog) DatabaseExists(ctx context.Context, name string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, exists := c.databases[name]
	return exists, nil
}

// CreateTable creates a new table.
func (c *FileCatalog) CreateTable(ctx context.Context, spec interfaces.TableSpec) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	db, exists := c.databases[spec.Database]
	if !exists {
		return fmt.Errorf("database not found: %s", spec.Database)
	}

	if _, exists := db.Tables[spec.Name]; exists {
		return fmt.Errorf("table already exists: %s.%s", spec.Database, spec.Name)
	}

	schemaJSON := ""
	if spec.Schema != nil {
		bytes, _ := json.Marshal(schemaToJSON(spec.Schema))
		schemaJSON = string(bytes)
	}

	location := spec.Location
	if location == "" {
		location = filepath.Join(c.root, spec.Database, spec.Name)
	}

	db.Tables[spec.Name] = &tableMeta{
		Database:     spec.Database,
		Name:         spec.Name,
		Location:     location,
		Format:       spec.Format,
		SchemaJSON:   schemaJSON,
		Properties:   spec.Properties,
		Partitioning: spec.Partitioning,
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}

	// Create table directory
	if err := os.MkdirAll(location, 0755); err != nil {
		return err
	}

	return c.save()
}

// GetTable retrieves a table.
func (c *FileCatalog) GetTable(ctx context.Context, database, table string) (interfaces.Table, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, exists := c.databases[database]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", database)
	}

	meta, exists := db.Tables[table]
	if !exists {
		return nil, fmt.Errorf("table not found: %s.%s", database, table)
	}

	return &fileTable{meta: meta}, nil
}

// ListTables lists tables in a database.
func (c *FileCatalog) ListTables(ctx context.Context, database string) ([]interfaces.TableInfo, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, exists := c.databases[database]
	if !exists {
		return nil, fmt.Errorf("database not found: %s", database)
	}

	var tables []interfaces.TableInfo
	for _, meta := range db.Tables {
		tables = append(tables, interfaces.TableInfo{
			Database:   meta.Database,
			Name:       meta.Name,
			Format:     meta.Format,
			RowCount:   meta.RowCount,
			SizeBytes:  meta.SizeBytes,
			CreatedAt:  meta.CreatedAt,
			UpdatedAt:  meta.UpdatedAt,
			Properties: meta.Properties,
		})
	}
	return tables, nil
}

// DropTable drops a table.
func (c *FileCatalog) DropTable(ctx context.Context, database, table string) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	db, exists := c.databases[database]
	if !exists {
		return fmt.Errorf("database not found: %s", database)
	}

	if _, exists := db.Tables[table]; !exists {
		return fmt.Errorf("table not found: %s.%s", database, table)
	}

	delete(db.Tables, table)
	return c.save()
}

// TableExists checks if a table exists.
func (c *FileCatalog) TableExists(ctx context.Context, database, table string) (bool, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	db, exists := c.databases[database]
	if !exists {
		return false, nil
	}
	_, exists = db.Tables[table]
	return exists, nil
}

// Refresh reloads metadata from disk.
func (c *FileCatalog) Refresh(ctx context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.load()
}

// fileTable implements interfaces.Table
type fileTable struct {
	meta *tableMeta
}

func (t *fileTable) Name() string                            { return t.meta.Name }
func (t *fileTable) Database() string                        { return t.meta.Database }
func (t *fileTable) Location() string                        { return t.meta.Location }
func (t *fileTable) Format() interfaces.TableFormat          { return t.meta.Format }
func (t *fileTable) Properties() map[string]string           { return t.meta.Properties }
func (t *fileTable) RowCount() int64                         { return t.meta.RowCount }
func (t *fileTable) SizeBytes() int64                        { return t.meta.SizeBytes }
func (t *fileTable) FileCount() int                          { return t.meta.FileCount }
func (t *fileTable) Partitioning() interfaces.PartitionSpec  { return t.meta.Partitioning }

func (t *fileTable) Schema() *arrow.Schema {
	if t.meta.SchemaJSON == "" {
		return nil
	}
	// Schema deserialization would go here
	return nil
}

func (t *fileTable) CurrentSnapshot() interfaces.Snapshot {
	return interfaces.Snapshot{}
}

func (t *fileTable) Snapshots() []interfaces.Snapshot {
	return nil
}

func (t *fileTable) AsOf(version int64) (interfaces.Table, error) {
	return nil, fmt.Errorf("time travel not supported in file catalog")
}

func (t *fileTable) AsOfTime(timestamp time.Time) (interfaces.Table, error) {
	return nil, fmt.Errorf("time travel not supported in file catalog")
}

func (t *fileTable) DataFiles() []interfaces.DataFile {
	return nil
}

func (t *fileTable) ManifestFiles() []interfaces.ManifestFile {
	return nil
}

func schemaToJSON(schema *arrow.Schema) map[string]interface{} {
	fields := make([]map[string]interface{}, len(schema.Fields()))
	for i, f := range schema.Fields() {
		fields[i] = map[string]interface{}{
			"name":     f.Name,
			"type":     f.Type.String(),
			"nullable": f.Nullable,
		}
	}
	return map[string]interface{}{"fields": fields}
}

// Verify interface compliance
var _ interfaces.Catalog = (*FileCatalog)(nil)
