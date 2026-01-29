package ocel

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	_ "github.com/marcboeker/go-duckdb"
)

// ViewManager provides virtual views over OCEL Parquet files.
// Uses DuckDB's UNNEST to create flat views per object type.
type ViewManager struct {
	db          *sql.DB
	parquetPath string
	objectTypes []string
	metadata    map[string]string
}

// NewViewManager creates a view manager for an OCEL Parquet file.
func NewViewManager(parquetPath string) (*ViewManager, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, err
	}

	vm := &ViewManager{
		db:          db,
		parquetPath: parquetPath,
		metadata:    make(map[string]string),
	}

	// Load metadata and discover object types
	if err := vm.loadMetadata(); err != nil {
		db.Close()
		return nil, err
	}

	return vm, nil
}

// loadMetadata loads OCEL metadata from Parquet footer.
func (vm *ViewManager) loadMetadata() error {
	// Query parquet metadata
	query := fmt.Sprintf(`
		SELECT key, value
		FROM parquet_kv_metadata('%s')
		WHERE key LIKE 'ocel:%%'
	`, escapePath(vm.parquetPath))

	rows, err := vm.db.Query(query)
	if err != nil {
		// Metadata might not exist, try to discover object types from data
		return vm.discoverObjectTypes()
	}
	defer rows.Close()

	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			continue
		}
		vm.metadata[key] = value
	}

	// Parse object types
	if typesJSON, ok := vm.metadata[MetaKeyObjectTypes]; ok {
		json.Unmarshal([]byte(typesJSON), &vm.objectTypes)
	} else {
		return vm.discoverObjectTypes()
	}

	return nil
}

// discoverObjectTypes finds unique object types by scanning the data.
func (vm *ViewManager) discoverObjectTypes() error {
	query := fmt.Sprintf(`
		SELECT DISTINCT obj.object_type
		FROM read_parquet('%s'),
		UNNEST(objects) AS t(obj)
	`, escapePath(vm.parquetPath))

	rows, err := vm.db.Query(query)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var objType string
		if err := rows.Scan(&objType); err != nil {
			continue
		}
		vm.objectTypes = append(vm.objectTypes, objType)
	}

	return nil
}

// Close closes the view manager.
func (vm *ViewManager) Close() error {
	return vm.db.Close()
}

// ObjectTypes returns the discovered object types.
func (vm *ViewManager) ObjectTypes() []string {
	return vm.objectTypes
}

// --- Virtual Views ---

// FlatView returns events filtered and pivoted by object type.
// This creates a traditional "case-centric" view for a specific object type.
func (vm *ViewManager) FlatView(ctx context.Context, objectType string) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			event_id,
			activity,
			timestamp,
			obj.object_id as %s_id
		FROM read_parquet('%s'),
		UNNEST(objects) AS t(obj)
		WHERE obj.object_type = '%s'
		ORDER BY obj.object_id, timestamp
	`, sanitizeIdentifier(objectType), escapePath(vm.parquetPath), objectType)

	return vm.db.QueryContext(ctx, query)
}

// FlatViewSQL returns the SQL query for a flat view (for external use).
func (vm *ViewManager) FlatViewSQL(objectType string) string {
	return fmt.Sprintf(`
		SELECT
			event_id,
			activity,
			timestamp,
			obj.object_id as %s_id
		FROM read_parquet('%s'),
		UNNEST(objects) AS t(obj)
		WHERE obj.object_type = '%s'
		ORDER BY obj.object_id, timestamp
	`, sanitizeIdentifier(objectType), escapePath(vm.parquetPath), objectType)
}

// ObjectEvents returns all events for a specific object.
func (vm *ViewManager) ObjectEvents(ctx context.Context, objectID string) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT
			event_id,
			activity,
			timestamp,
			objects
		FROM read_parquet('%s'),
		UNNEST(objects) AS t(obj)
		WHERE obj.object_id = '%s'
		ORDER BY timestamp
	`, escapePath(vm.parquetPath), objectID)

	return vm.db.QueryContext(ctx, query)
}

// ObjectGraph returns the object interaction graph.
// Shows which object types appear together in events.
func (vm *ViewManager) ObjectGraph(ctx context.Context) ([]ObjectInteraction, error) {
	query := fmt.Sprintf(`
		WITH event_objects AS (
			SELECT
				event_id,
				obj.object_type
			FROM read_parquet('%s'),
			UNNEST(objects) AS t(obj)
		),
		pairs AS (
			SELECT DISTINCT
				a.object_type as type_a,
				b.object_type as type_b,
				a.event_id
			FROM event_objects a
			JOIN event_objects b ON a.event_id = b.event_id
			WHERE a.object_type < b.object_type
		)
		SELECT
			type_a,
			type_b,
			COUNT(*) as co_occurrence_count
		FROM pairs
		GROUP BY type_a, type_b
		ORDER BY co_occurrence_count DESC
	`, escapePath(vm.parquetPath))

	rows, err := vm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var interactions []ObjectInteraction
	for rows.Next() {
		var oi ObjectInteraction
		if err := rows.Scan(&oi.TypeA, &oi.TypeB, &oi.Count); err != nil {
			continue
		}
		interactions = append(interactions, oi)
	}

	return interactions, nil
}

// ObjectInteraction represents co-occurrence of object types.
type ObjectInteraction struct {
	TypeA string `json:"type_a"`
	TypeB string `json:"type_b"`
	Count int64  `json:"count"`
}

// --- Process Mining Views ---

// VariantsByObjectType returns process variants for a specific object type.
func (vm *ViewManager) VariantsByObjectType(ctx context.Context, objectType string, limit int) ([]Variant, error) {
	query := fmt.Sprintf(`
		WITH traces AS (
			SELECT
				obj.object_id,
				STRING_AGG(activity, ' -> ' ORDER BY timestamp) as variant
			FROM read_parquet('%s'),
			UNNEST(objects) AS t(obj)
			WHERE obj.object_type = '%s'
			GROUP BY obj.object_id
		)
		SELECT
			variant,
			COUNT(*) as count,
			COUNT(*) * 100.0 / SUM(COUNT(*)) OVER () as percent
		FROM traces
		GROUP BY variant
		ORDER BY count DESC
		LIMIT %d
	`, escapePath(vm.parquetPath), objectType, limit)

	rows, err := vm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var variants []Variant
	for rows.Next() {
		var v Variant
		if err := rows.Scan(&v.Path, &v.Count, &v.Percent); err != nil {
			continue
		}
		variants = append(variants, v)
	}

	return variants, nil
}

// Variant represents a process variant.
type Variant struct {
	Path    string  `json:"path"`
	Count   int64   `json:"count"`
	Percent float64 `json:"percent"`
}

// ActivityTransitions returns activity transitions with object context.
func (vm *ViewManager) ActivityTransitions(ctx context.Context, objectType string) ([]Transition, error) {
	query := fmt.Sprintf(`
		WITH ordered_events AS (
			SELECT
				obj.object_id,
				activity,
				timestamp,
				LEAD(activity) OVER (PARTITION BY obj.object_id ORDER BY timestamp) as next_activity
			FROM read_parquet('%s'),
			UNNEST(objects) AS t(obj)
			WHERE obj.object_type = '%s'
		)
		SELECT
			activity as from_activity,
			next_activity as to_activity,
			COUNT(*) as count
		FROM ordered_events
		WHERE next_activity IS NOT NULL
		GROUP BY activity, next_activity
		ORDER BY count DESC
	`, escapePath(vm.parquetPath), objectType)

	rows, err := vm.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transitions []Transition
	for rows.Next() {
		var t Transition
		if err := rows.Scan(&t.From, &t.To, &t.Count); err != nil {
			continue
		}
		transitions = append(transitions, t)
	}

	return transitions, nil
}

// Transition represents an activity transition.
type Transition struct {
	From  string `json:"from"`
	To    string `json:"to"`
	Count int64  `json:"count"`
}

// --- Statistics ---

// Statistics returns comprehensive OCEL statistics.
func (vm *ViewManager) Statistics(ctx context.Context) (*Statistics, error) {
	stats := &Statistics{
		ObjectTypeCounts: make(map[string]int64),
		ActivityCounts:   make(map[string]int64),
	}

	// Event count
	vm.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COUNT(*) FROM read_parquet('%s')
	`, escapePath(vm.parquetPath))).Scan(&stats.EventCount)

	// Object count
	vm.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT COUNT(DISTINCT obj.object_id)
		FROM read_parquet('%s'),
		UNNEST(objects) AS t(obj)
	`, escapePath(vm.parquetPath))).Scan(&stats.ObjectCount)

	// Object type counts
	rows, _ := vm.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT obj.object_type, COUNT(DISTINCT obj.object_id)
		FROM read_parquet('%s'),
		UNNEST(objects) AS t(obj)
		GROUP BY obj.object_type
	`, escapePath(vm.parquetPath)))
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var objType string
			var count int64
			if rows.Scan(&objType, &count) == nil {
				stats.ObjectTypeCounts[objType] = count
				stats.ObjectTypes = append(stats.ObjectTypes, objType)
			}
		}
	}

	// Activity counts
	rows, _ = vm.db.QueryContext(ctx, fmt.Sprintf(`
		SELECT activity, COUNT(*)
		FROM read_parquet('%s')
		GROUP BY activity
	`, escapePath(vm.parquetPath)))
	if rows != nil {
		defer rows.Close()
		for rows.Next() {
			var activity string
			var count int64
			if rows.Scan(&activity, &count) == nil {
				stats.ActivityCounts[activity] = count
			}
		}
	}

	// Time range
	vm.db.QueryRowContext(ctx, fmt.Sprintf(`
		SELECT MIN(timestamp), MAX(timestamp)
		FROM read_parquet('%s')
	`, escapePath(vm.parquetPath))).Scan(&stats.TimeRange.Start, &stats.TimeRange.End)

	return stats, nil
}

// --- Helpers ---

func escapePath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

func sanitizeIdentifier(s string) string {
	// Replace non-alphanumeric with underscore
	result := make([]byte, 0, len(s))
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '_' {
			result = append(result, c)
		} else {
			result = append(result, '_')
		}
	}
	return string(result)
}
