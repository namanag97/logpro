// Package ocel provides DuckDB-based storage for OCEL 2.0 logs.
package ocel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	_ "github.com/marcboeker/go-duckdb"
)

// Store provides persistent storage for OCEL 2.0 logs using DuckDB.
// Uses relational tables matching the OCEL 2.0 specification.
type Store struct {
	db   *sql.DB
	path string
}

// StoreConfig configures the OCEL store.
type StoreConfig struct {
	// Path is the database file path (":memory:" for in-memory)
	Path string

	// ReadOnly opens the database in read-only mode
	ReadOnly bool
}

// NewStore creates a new OCEL store.
func NewStore(path string) (*Store, error) {
	return NewStoreWithConfig(StoreConfig{Path: path})
}

// NewStoreWithConfig creates a store with custom configuration.
func NewStoreWithConfig(cfg StoreConfig) (*Store, error) {
	dsn := cfg.Path
	if cfg.ReadOnly {
		dsn += "?access_mode=read_only"
	}

	db, err := sql.Open("duckdb", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}

	store := &Store{db: db, path: cfg.Path}

	if err := store.initSchema(); err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}

	return store, nil
}

// initSchema creates the OCEL 2.0 tables.
func (s *Store) initSchema() error {
	schema := `
		-- Events table
		CREATE TABLE IF NOT EXISTS event (
			event_id    VARCHAR PRIMARY KEY,
			event_type  VARCHAR NOT NULL,
			timestamp   TIMESTAMP NOT NULL
		);

		-- Objects table
		CREATE TABLE IF NOT EXISTS object (
			object_id   VARCHAR PRIMARY KEY,
			object_type VARCHAR NOT NULL
		);

		-- Event-to-Object relations (E2O)
		CREATE TABLE IF NOT EXISTS event_object (
			event_id    VARCHAR NOT NULL,
			object_id   VARCHAR NOT NULL,
			qualifier   VARCHAR DEFAULT '',
			PRIMARY KEY (event_id, object_id, qualifier)
		);

		-- Object-to-Object relations (O2O)
		CREATE TABLE IF NOT EXISTS object_object (
			source_id   VARCHAR NOT NULL,
			target_id   VARCHAR NOT NULL,
			qualifier   VARCHAR DEFAULT '',
			PRIMARY KEY (source_id, target_id, qualifier)
		);

		-- Event attributes
		CREATE TABLE IF NOT EXISTS event_attribute (
			event_id    VARCHAR NOT NULL,
			attr_name   VARCHAR NOT NULL,
			attr_value  VARCHAR,
			attr_type   VARCHAR DEFAULT 'string',
			PRIMARY KEY (event_id, attr_name)
		);

		-- Object attributes (versioned)
		CREATE TABLE IF NOT EXISTS object_attribute (
			object_id   VARCHAR NOT NULL,
			attr_name   VARCHAR NOT NULL,
			attr_value  VARCHAR,
			attr_type   VARCHAR DEFAULT 'string',
			timestamp   TIMESTAMP NOT NULL,
			PRIMARY KEY (object_id, attr_name, timestamp)
		);

		-- Create indexes for common queries
		CREATE INDEX IF NOT EXISTS idx_event_type ON event(event_type);
		CREATE INDEX IF NOT EXISTS idx_event_timestamp ON event(timestamp);
		CREATE INDEX IF NOT EXISTS idx_object_type ON object(object_type);
		CREATE INDEX IF NOT EXISTS idx_e2o_event ON event_object(event_id);
		CREATE INDEX IF NOT EXISTS idx_e2o_object ON event_object(object_id);
		CREATE INDEX IF NOT EXISTS idx_o2o_source ON object_object(source_id);
		CREATE INDEX IF NOT EXISTS idx_o2o_target ON object_object(target_id);
	`

	_, err := s.db.Exec(schema)
	return err
}

// Close closes the database connection.
func (s *Store) Close() error {
	return s.db.Close()
}

// --- Write Operations ---

// InsertEvent inserts an event into the store.
func (s *Store) InsertEvent(ctx context.Context, event *Event) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO event (event_id, event_type, timestamp) VALUES (?, ?, ?)",
		event.ID, event.Type, event.Timestamp,
	)
	if err != nil {
		return fmt.Errorf("failed to insert event: %w", err)
	}

	// Insert attributes
	for name, value := range event.Attributes {
		valueStr, attrType := serializeAttribute(value)
		_, err := s.db.ExecContext(ctx,
			"INSERT INTO event_attribute (event_id, attr_name, attr_value, attr_type) VALUES (?, ?, ?, ?)",
			event.ID, name, valueStr, attrType,
		)
		if err != nil {
			return fmt.Errorf("failed to insert event attribute: %w", err)
		}
	}

	return nil
}

// InsertObject inserts an object into the store.
func (s *Store) InsertObject(ctx context.Context, object *Object) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO object (object_id, object_type) VALUES (?, ?)",
		object.ID, object.Type,
	)
	return err
}

// InsertE2O inserts an event-to-object relation.
func (s *Store) InsertE2O(ctx context.Context, e2o E2O) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO event_object (event_id, object_id, qualifier) VALUES (?, ?, ?)",
		e2o.EventID, e2o.ObjectID, e2o.Qualifier,
	)
	return err
}

// InsertO2O inserts an object-to-object relation.
func (s *Store) InsertO2O(ctx context.Context, o2o O2O) error {
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO object_object (source_id, target_id, qualifier) VALUES (?, ?, ?)",
		o2o.SourceID, o2o.TargetID, o2o.Qualifier,
	)
	return err
}

// InsertObjectAttribute inserts a versioned object attribute.
func (s *Store) InsertObjectAttribute(ctx context.Context, attr ObjectAttribute) error {
	valueStr, attrType := serializeAttribute(attr.Value)
	_, err := s.db.ExecContext(ctx,
		"INSERT INTO object_attribute (object_id, attr_name, attr_value, attr_type, timestamp) VALUES (?, ?, ?, ?, ?)",
		attr.ObjectID, attr.Name, valueStr, attrType, attr.Timestamp,
	)
	return err
}

// ImportLog imports a complete Log into the store.
func (s *Store) ImportLog(ctx context.Context, log *Log) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// Insert events
	for _, event := range log.Events {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO event (event_id, event_type, timestamp) VALUES (?, ?, ?)",
			event.ID, event.Type, event.Timestamp,
		)
		if err != nil {
			return fmt.Errorf("failed to insert event %s: %w", event.ID, err)
		}
	}

	// Insert objects
	for _, object := range log.Objects {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO object (object_id, object_type) VALUES (?, ?)",
			object.ID, object.Type,
		)
		if err != nil {
			return fmt.Errorf("failed to insert object %s: %w", object.ID, err)
		}
	}

	// Insert E2O relations
	for _, e2o := range log.E2ORelations {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO event_object (event_id, object_id, qualifier) VALUES (?, ?, ?)",
			e2o.EventID, e2o.ObjectID, e2o.Qualifier,
		)
		if err != nil {
			return fmt.Errorf("failed to insert E2O: %w", err)
		}
	}

	// Insert O2O relations
	for _, o2o := range log.O2ORelations {
		_, err := tx.ExecContext(ctx,
			"INSERT INTO object_object (source_id, target_id, qualifier) VALUES (?, ?, ?)",
			o2o.SourceID, o2o.TargetID, o2o.Qualifier,
		)
		if err != nil {
			return fmt.Errorf("failed to insert O2O: %w", err)
		}
	}

	// Insert object attributes
	for _, attr := range log.ObjectAttributes {
		valueStr, attrType := serializeAttribute(attr.Value)
		_, err := tx.ExecContext(ctx,
			"INSERT INTO object_attribute (object_id, attr_name, attr_value, attr_type, timestamp) VALUES (?, ?, ?, ?, ?)",
			attr.ObjectID, attr.Name, valueStr, attrType, attr.Timestamp,
		)
		if err != nil {
			return fmt.Errorf("failed to insert object attribute: %w", err)
		}
	}

	return tx.Commit()
}

// --- Read Operations ---

// GetEvent retrieves an event by ID.
func (s *Store) GetEvent(ctx context.Context, eventID string) (*Event, error) {
	var event Event
	err := s.db.QueryRowContext(ctx,
		"SELECT event_id, event_type, timestamp FROM event WHERE event_id = ?",
		eventID,
	).Scan(&event.ID, &event.Type, &event.Timestamp)
	if err != nil {
		return nil, err
	}

	// Load attributes
	event.Attributes = make(map[string]interface{})
	rows, err := s.db.QueryContext(ctx,
		"SELECT attr_name, attr_value, attr_type FROM event_attribute WHERE event_id = ?",
		eventID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var name, value, attrType string
		if err := rows.Scan(&name, &value, &attrType); err != nil {
			return nil, err
		}
		event.Attributes[name] = deserializeAttribute(value, attrType)
	}

	return &event, nil
}

// GetObject retrieves an object by ID.
func (s *Store) GetObject(ctx context.Context, objectID string) (*Object, error) {
	var object Object
	err := s.db.QueryRowContext(ctx,
		"SELECT object_id, object_type FROM object WHERE object_id = ?",
		objectID,
	).Scan(&object.ID, &object.Type)
	return &object, err
}

// GetEventsForObject returns all events related to an object.
func (s *Store) GetEventsForObject(ctx context.Context, objectID string) ([]*Event, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT e.event_id, e.event_type, e.timestamp
		FROM event e
		JOIN event_object eo ON e.event_id = eo.event_id
		WHERE eo.object_id = ?
		ORDER BY e.timestamp
	`, objectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []*Event
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.Type, &event.Timestamp); err != nil {
			return nil, err
		}
		events = append(events, &event)
	}
	return events, nil
}

// GetObjectsForEvent returns all objects related to an event.
func (s *Store) GetObjectsForEvent(ctx context.Context, eventID string) ([]*Object, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT o.object_id, o.object_type
		FROM object o
		JOIN event_object eo ON o.object_id = eo.object_id
		WHERE eo.event_id = ?
	`, eventID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []*Object
	for rows.Next() {
		var object Object
		if err := rows.Scan(&object.ID, &object.Type); err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

// GetObjectsByType returns all objects of a specific type.
func (s *Store) GetObjectsByType(ctx context.Context, objectType string) ([]*Object, error) {
	rows, err := s.db.QueryContext(ctx,
		"SELECT object_id, object_type FROM object WHERE object_type = ?",
		objectType,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []*Object
	for rows.Next() {
		var object Object
		if err := rows.Scan(&object.ID, &object.Type); err != nil {
			return nil, err
		}
		objects = append(objects, &object)
	}
	return objects, nil
}

// --- Projection Queries (Virtual Views) ---

// ProjectByObjectType creates a case-centric view for an object type.
// This is the "virtual pivot" - no data copy, just a SQL view.
func (s *Store) ProjectByObjectType(ctx context.Context, objectType string) (*sql.Rows, error) {
	return s.db.QueryContext(ctx, `
		SELECT
			o.object_id AS case_id,
			e.event_id,
			e.event_type AS activity,
			e.timestamp
		FROM event e
		JOIN event_object eo ON e.event_id = eo.event_id
		JOIN object o ON eo.object_id = o.object_id
		WHERE o.object_type = ?
		ORDER BY o.object_id, e.timestamp
	`, objectType)
}

// --- Statistics ---

// Stats returns store statistics.
func (s *Store) Stats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Event count
	var eventCount int64
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event").Scan(&eventCount)
	stats["events"] = eventCount

	// Object count
	var objectCount int64
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM object").Scan(&objectCount)
	stats["objects"] = objectCount

	// E2O count
	var e2oCount int64
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM event_object").Scan(&e2oCount)
	stats["e2o_relations"] = e2oCount

	// O2O count
	var o2oCount int64
	s.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM object_object").Scan(&o2oCount)
	stats["o2o_relations"] = o2oCount

	// Event types
	rows, _ := s.db.QueryContext(ctx, "SELECT DISTINCT event_type FROM event")
	var eventTypes []string
	for rows.Next() {
		var t string
		rows.Scan(&t)
		eventTypes = append(eventTypes, t)
	}
	rows.Close()
	stats["event_types"] = eventTypes

	// Object types with counts
	rows, _ = s.db.QueryContext(ctx, "SELECT object_type, COUNT(*) FROM object GROUP BY object_type")
	objectTypes := make(map[string]int64)
	for rows.Next() {
		var t string
		var c int64
		rows.Scan(&t, &c)
		objectTypes[t] = c
	}
	rows.Close()
	stats["object_types"] = objectTypes

	// Time range
	var minTime, maxTime time.Time
	s.db.QueryRowContext(ctx, "SELECT MIN(timestamp), MAX(timestamp) FROM event").Scan(&minTime, &maxTime)
	stats["time_range"] = map[string]string{
		"min": minTime.Format(time.RFC3339),
		"max": maxTime.Format(time.RFC3339),
	}

	return stats, nil
}

// --- Export ---

// ExportToLog exports the store contents to an in-memory Log.
func (s *Store) ExportToLog(ctx context.Context) (*Log, error) {
	log := NewLog()

	// Load events
	rows, err := s.db.QueryContext(ctx, "SELECT event_id, event_type, timestamp FROM event")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var event Event
		if err := rows.Scan(&event.ID, &event.Type, &event.Timestamp); err != nil {
			rows.Close()
			return nil, err
		}
		log.Events[event.ID] = &event
	}
	rows.Close()

	// Load objects
	rows, err = s.db.QueryContext(ctx, "SELECT object_id, object_type FROM object")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var object Object
		if err := rows.Scan(&object.ID, &object.Type); err != nil {
			rows.Close()
			return nil, err
		}
		log.Objects[object.ID] = &object
	}
	rows.Close()

	// Load E2O
	rows, err = s.db.QueryContext(ctx, "SELECT event_id, object_id, qualifier FROM event_object")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var e2o E2O
		if err := rows.Scan(&e2o.EventID, &e2o.ObjectID, &e2o.Qualifier); err != nil {
			rows.Close()
			return nil, err
		}
		log.E2ORelations = append(log.E2ORelations, e2o)
	}
	rows.Close()

	// Load O2O
	rows, err = s.db.QueryContext(ctx, "SELECT source_id, target_id, qualifier FROM object_object")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var o2o O2O
		if err := rows.Scan(&o2o.SourceID, &o2o.TargetID, &o2o.Qualifier); err != nil {
			rows.Close()
			return nil, err
		}
		log.O2ORelations = append(log.O2ORelations, o2o)
	}
	rows.Close()

	return log, nil
}

// DB returns the underlying database connection for advanced queries.
func (s *Store) DB() *sql.DB {
	return s.db
}

// --- Helpers ---

func serializeAttribute(value interface{}) (string, string) {
	switch v := value.(type) {
	case string:
		return v, "string"
	case int, int32, int64:
		return fmt.Sprintf("%d", v), "int"
	case float32, float64:
		return fmt.Sprintf("%f", v), "float"
	case bool:
		if v {
			return "true", "bool"
		}
		return "false", "bool"
	case time.Time:
		return v.Format(time.RFC3339), "timestamp"
	default:
		data, _ := json.Marshal(v)
		return string(data), "json"
	}
}

func deserializeAttribute(value, attrType string) interface{} {
	switch attrType {
	case "string":
		return value
	case "int":
		var v int64
		fmt.Sscanf(value, "%d", &v)
		return v
	case "float":
		var v float64
		fmt.Sscanf(value, "%f", &v)
		return v
	case "bool":
		return value == "true"
	case "timestamp":
		t, _ := time.Parse(time.RFC3339, value)
		return t
	case "json":
		var v interface{}
		json.Unmarshal([]byte(value), &v)
		return v
	default:
		return value
	}
}
