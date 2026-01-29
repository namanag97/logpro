package ocel

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// ParquetQuerier provides query capabilities over OCEL-enriched Parquet files
// using DuckDB's read_parquet() and UNNEST() functions. Flat columns are
// queried directly via SQL; the nested ocel_objects column is accessed via
// UNNEST to join objects with event attributes.
type ParquetQuerier struct {
	db *sql.DB
}

// NewParquetQuerier creates a querier backed by an in-memory DuckDB instance.
func NewParquetQuerier() (*ParquetQuerier, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("failed to open DuckDB: %w", err)
	}
	return &ParquetQuerier{db: db}, nil
}

// Close releases database resources.
func (q *ParquetQuerier) Close() error {
	return q.db.Close()
}

// QueryFlat executes a SQL query on the flat columns of a Parquet file.
// Pass specific column names to project, or leave empty for SELECT *.
func (q *ParquetQuerier) QueryFlat(ctx context.Context, parquetPath string, whereClause string, columns ...string) (*sql.Rows, error) {
	cols := "*"
	if len(columns) > 0 {
		cols = strings.Join(quoteColumns(columns), ", ")
	}

	query := fmt.Sprintf(`SELECT %s FROM read_parquet('%s')`,
		cols, escapeSQLPath(parquetPath))

	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	return q.db.QueryContext(ctx, query)
}

// QueryWithObjects executes a query that UNNESTs the ocel_objects column,
// joining flat attributes with their associated object references.
func (q *ParquetQuerier) QueryWithObjects(ctx context.Context, parquetPath string, whereClause string) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT t.*, obj.object_id, obj.object_type
		FROM read_parquet('%s') t,
		UNNEST(t.ocel_objects) AS obj(object_id, object_type)
	`, escapeSQLPath(parquetPath))

	if whereClause != "" {
		query += " WHERE " + whereClause
	}

	return q.db.QueryContext(ctx, query)
}

// QueryByObjectType returns events associated with a specific object type
// by UNNESTing ocel_objects and filtering on object_type.
func (q *ParquetQuerier) QueryByObjectType(ctx context.Context, parquetPath, objectType string) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT t.*, obj.object_id, obj.object_type
		FROM read_parquet('%s') t,
		UNNEST(t.ocel_objects) AS obj(object_id, object_type)
		WHERE obj.object_type = ?
	`, escapeSQLPath(parquetPath))

	return q.db.QueryContext(ctx, query, objectType)
}

// QueryByObjectID returns events associated with a specific object.
func (q *ParquetQuerier) QueryByObjectID(ctx context.Context, parquetPath, objectID string) (*sql.Rows, error) {
	query := fmt.Sprintf(`
		SELECT t.*, obj.object_id, obj.object_type
		FROM read_parquet('%s') t,
		UNNEST(t.ocel_objects) AS obj(object_id, object_type)
		WHERE obj.object_id = ?
	`, escapeSQLPath(parquetPath))

	return q.db.QueryContext(ctx, query, objectID)
}

// QueryObjectTypes returns distinct object types found in the Parquet file.
func (q *ParquetQuerier) QueryObjectTypes(ctx context.Context, parquetPath string) ([]string, error) {
	query := fmt.Sprintf(`
		SELECT DISTINCT obj.object_type
		FROM read_parquet('%s') t,
		UNNEST(t.ocel_objects) AS obj(object_id, object_type)
		ORDER BY obj.object_type
	`, escapeSQLPath(parquetPath))

	rows, err := q.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var types []string
	for rows.Next() {
		var t string
		if err := rows.Scan(&t); err != nil {
			return nil, err
		}
		types = append(types, t)
	}
	return types, rows.Err()
}

// QueryObjectCounts returns the count of distinct objects per type.
func (q *ParquetQuerier) QueryObjectCounts(ctx context.Context, parquetPath string) (map[string]int64, error) {
	query := fmt.Sprintf(`
		SELECT obj.object_type, COUNT(DISTINCT obj.object_id) AS cnt
		FROM read_parquet('%s') t,
		UNNEST(t.ocel_objects) AS obj(object_id, object_type)
		GROUP BY obj.object_type
		ORDER BY cnt DESC
	`, escapeSQLPath(parquetPath))

	rows, err := q.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	counts := make(map[string]int64)
	for rows.Next() {
		var t string
		var c int64
		if err := rows.Scan(&t, &c); err != nil {
			return nil, err
		}
		counts[t] = c
	}
	return counts, rows.Err()
}

// DiscoverDFGFromParquet discovers an Object-Centric Directly-Follows Graph
// from an OCEL-enriched Parquet file. It uses DuckDB window functions on the
// UNNESTed object references to extract per-object-type DFG edges.
func (q *ParquetQuerier) DiscoverDFGFromParquet(ctx context.Context, parquetPath, activityCol, timestampCol string) (*OCDFG, error) {
	dfg := NewOCDFG()

	edgeQuery := fmt.Sprintf(`
		WITH unnested AS (
			SELECT
				t."%s" AS activity,
				t."%s" AS ts,
				obj.object_id,
				obj.object_type
			FROM read_parquet('%s') t,
			UNNEST(t.ocel_objects) AS obj(object_id, object_type)
		),
		with_next AS (
			SELECT
				object_type,
				activity,
				LEAD(activity) OVER (PARTITION BY object_id ORDER BY ts) AS next_activity
			FROM unnested
		)
		SELECT object_type, activity, next_activity, COUNT(*) AS cnt
		FROM with_next
		WHERE next_activity IS NOT NULL
		GROUP BY object_type, activity, next_activity
	`, activityCol, timestampCol, escapeSQLPath(parquetPath))

	rows, err := q.db.QueryContext(ctx, edgeQuery)
	if err != nil {
		return nil, fmt.Errorf("edge discovery failed: %w", err)
	}
	defer rows.Close()

	activitySet := make(map[string]bool)
	objectTypeSet := make(map[string]bool)

	for rows.Next() {
		var objectType, activity, nextActivity string
		var count int64
		if err := rows.Scan(&objectType, &activity, &nextActivity, &count); err != nil {
			return nil, err
		}

		activitySet[activity] = true
		activitySet[nextActivity] = true
		objectTypeSet[objectType] = true

		if dfg.Edges[objectType] == nil {
			dfg.Edges[objectType] = make(map[string]map[string]int64)
		}
		if dfg.Edges[objectType][activity] == nil {
			dfg.Edges[objectType][activity] = make(map[string]int64)
		}
		dfg.Edges[objectType][activity][nextActivity] = count
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	for a := range activitySet {
		dfg.Activities = append(dfg.Activities, a)
	}
	for ot := range objectTypeSet {
		dfg.ObjectTypes = append(dfg.ObjectTypes, ot)
		if dfg.ActivityFrequency[ot] == nil {
			dfg.ActivityFrequency[ot] = make(map[string]int64)
		}
		if dfg.StartActivities[ot] == nil {
			dfg.StartActivities[ot] = make(map[string]int64)
		}
		if dfg.EndActivities[ot] == nil {
			dfg.EndActivities[ot] = make(map[string]int64)
		}
	}

	return dfg, nil
}

// Raw executes a raw SQL query on the DuckDB instance.
// Callers can use read_parquet() and UNNEST() directly.
func (q *ParquetQuerier) Raw(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return q.db.QueryContext(ctx, query, args...)
}

// DB returns the underlying database connection for advanced queries.
func (q *ParquetQuerier) DB() *sql.DB {
	return q.db
}

func escapeSQLPath(path string) string {
	return strings.ReplaceAll(path, "'", "''")
}

func quoteColumns(cols []string) []string {
	quoted := make([]string, len(cols))
	for i, c := range cols {
		quoted[i] = fmt.Sprintf(`"%s"`, c)
	}
	return quoted
}
