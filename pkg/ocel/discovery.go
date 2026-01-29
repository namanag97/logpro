// Package ocel provides Object-Centric process discovery algorithms.
// Implements OC-DFG (Object-Centric Directly-Follows Graph) discovery.
package ocel

import (
	"context"
	"sort"
)

// OCDFG represents an Object-Centric Directly-Follows Graph.
// Mathematical definition: G = (A, OT, F) where:
//   - A: Set of activities
//   - OT: Set of object types
//   - F: Directly-follows edges colored by object type
type OCDFG struct {
	// Activities is the set of activity names
	Activities []string

	// ObjectTypes is the set of object types
	ObjectTypes []string

	// Edges contains directly-follows relations grouped by object type
	// ObjectType -> (Source Activity -> Target Activity -> count)
	Edges map[string]map[string]map[string]int64

	// ActivityFrequency counts how often each activity occurs per object type
	ActivityFrequency map[string]map[string]int64

	// StartActivities tracks which activities start traces per object type
	StartActivities map[string]map[string]int64

	// EndActivities tracks which activities end traces per object type
	EndActivities map[string]map[string]int64
}

// Edge represents a single edge in the OC-DFG.
type Edge struct {
	Source     string
	Target     string
	ObjectType string
	Count      int64
}

// NewOCDFG creates a new empty OC-DFG.
func NewOCDFG() *OCDFG {
	return &OCDFG{
		Activities:        make([]string, 0),
		ObjectTypes:       make([]string, 0),
		Edges:             make(map[string]map[string]map[string]int64),
		ActivityFrequency: make(map[string]map[string]int64),
		StartActivities:   make(map[string]map[string]int64),
		EndActivities:     make(map[string]map[string]int64),
	}
}

// DiscoverOCDFG discovers an Object-Centric Directly-Follows Graph from an OCEL log.
// Algorithm:
//  1. For each object type, flatten the log to get object-specific traces
//  2. For each trace, extract directly-follows pairs
//  3. Aggregate counts per object type
func DiscoverOCDFG(log *Log) *OCDFG {
	dfg := NewOCDFG()

	// Collect all activities and object types
	activitySet := make(map[string]bool)
	objectTypeSet := make(map[string]bool)

	for _, event := range log.Events {
		activitySet[event.Type] = true
	}
	for _, obj := range log.Objects {
		objectTypeSet[obj.Type] = true
	}

	for a := range activitySet {
		dfg.Activities = append(dfg.Activities, a)
	}
	sort.Strings(dfg.Activities)

	for ot := range objectTypeSet {
		dfg.ObjectTypes = append(dfg.ObjectTypes, ot)
	}
	sort.Strings(dfg.ObjectTypes)

	// Initialize maps for each object type
	for _, ot := range dfg.ObjectTypes {
		dfg.Edges[ot] = make(map[string]map[string]int64)
		dfg.ActivityFrequency[ot] = make(map[string]int64)
		dfg.StartActivities[ot] = make(map[string]int64)
		dfg.EndActivities[ot] = make(map[string]int64)

		for _, a := range dfg.Activities {
			dfg.Edges[ot][a] = make(map[string]int64)
		}
	}

	// For each object type, discover DFG edges
	for _, objectType := range dfg.ObjectTypes {
		// Get all objects of this type
		objects := log.GetObjectsByType(objectType)

		for _, obj := range objects {
			// Get events for this object, sorted by timestamp
			events := log.GetEventsForObject(obj.ID)
			sortEventsByTime(events)

			if len(events) == 0 {
				continue
			}

			// Track start activity
			dfg.StartActivities[objectType][events[0].Type]++

			// Track end activity
			dfg.EndActivities[objectType][events[len(events)-1].Type]++

			// Extract directly-follows pairs
			for i := 0; i < len(events); i++ {
				activity := events[i].Type
				dfg.ActivityFrequency[objectType][activity]++

				if i < len(events)-1 {
					nextActivity := events[i+1].Type
					dfg.Edges[objectType][activity][nextActivity]++
				}
			}
		}
	}

	return dfg
}

// DiscoverOCDFGFromStore discovers OC-DFG directly from a Store using SQL.
func DiscoverOCDFGFromStore(ctx context.Context, store *Store) (*OCDFG, error) {
	dfg := NewOCDFG()

	// Get all activities
	rows, err := store.db.QueryContext(ctx, "SELECT DISTINCT event_type FROM event ORDER BY event_type")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var activity string
		rows.Scan(&activity)
		dfg.Activities = append(dfg.Activities, activity)
	}
	rows.Close()

	// Get all object types
	rows, err = store.db.QueryContext(ctx, "SELECT DISTINCT object_type FROM object ORDER BY object_type")
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var objectType string
		rows.Scan(&objectType)
		dfg.ObjectTypes = append(dfg.ObjectTypes, objectType)
	}
	rows.Close()

	// Initialize maps
	for _, ot := range dfg.ObjectTypes {
		dfg.Edges[ot] = make(map[string]map[string]int64)
		dfg.ActivityFrequency[ot] = make(map[string]int64)
		dfg.StartActivities[ot] = make(map[string]int64)
		dfg.EndActivities[ot] = make(map[string]int64)

		for _, a := range dfg.Activities {
			dfg.Edges[ot][a] = make(map[string]int64)
		}
	}

	// Discover edges using SQL window functions (efficient!)
	// This query finds directly-follows pairs per object
	edgeQuery := `
		WITH object_events AS (
			SELECT
				o.object_id,
				o.object_type,
				e.event_type AS activity,
				e.timestamp,
				LEAD(e.event_type) OVER (
					PARTITION BY o.object_id
					ORDER BY e.timestamp
				) AS next_activity
			FROM event e
			JOIN event_object eo ON e.event_id = eo.event_id
			JOIN object o ON eo.object_id = o.object_id
		)
		SELECT
			object_type,
			activity,
			next_activity,
			COUNT(*) as edge_count
		FROM object_events
		WHERE next_activity IS NOT NULL
		GROUP BY object_type, activity, next_activity
	`

	rows, err = store.db.QueryContext(ctx, edgeQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var objectType, activity, nextActivity string
		var count int64
		rows.Scan(&objectType, &activity, &nextActivity, &count)
		dfg.Edges[objectType][activity][nextActivity] = count
	}
	rows.Close()

	// Get activity frequencies
	freqQuery := `
		SELECT
			o.object_type,
			e.event_type AS activity,
			COUNT(*) as freq
		FROM event e
		JOIN event_object eo ON e.event_id = eo.event_id
		JOIN object o ON eo.object_id = o.object_id
		GROUP BY o.object_type, e.event_type
	`
	rows, err = store.db.QueryContext(ctx, freqQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var objectType, activity string
		var freq int64
		rows.Scan(&objectType, &activity, &freq)
		dfg.ActivityFrequency[objectType][activity] = freq
	}
	rows.Close()

	// Get start activities
	startQuery := `
		WITH first_events AS (
			SELECT
				o.object_id,
				o.object_type,
				e.event_type AS activity,
				ROW_NUMBER() OVER (PARTITION BY o.object_id ORDER BY e.timestamp) AS rn
			FROM event e
			JOIN event_object eo ON e.event_id = eo.event_id
			JOIN object o ON eo.object_id = o.object_id
		)
		SELECT object_type, activity, COUNT(*) as cnt
		FROM first_events
		WHERE rn = 1
		GROUP BY object_type, activity
	`
	rows, err = store.db.QueryContext(ctx, startQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var objectType, activity string
		var cnt int64
		rows.Scan(&objectType, &activity, &cnt)
		dfg.StartActivities[objectType][activity] = cnt
	}
	rows.Close()

	// Get end activities
	endQuery := `
		WITH last_events AS (
			SELECT
				o.object_id,
				o.object_type,
				e.event_type AS activity,
				ROW_NUMBER() OVER (PARTITION BY o.object_id ORDER BY e.timestamp DESC) AS rn
			FROM event e
			JOIN event_object eo ON e.event_id = eo.event_id
			JOIN object o ON eo.object_id = o.object_id
		)
		SELECT object_type, activity, COUNT(*) as cnt
		FROM last_events
		WHERE rn = 1
		GROUP BY object_type, activity
	`
	rows, err = store.db.QueryContext(ctx, endQuery)
	if err != nil {
		return nil, err
	}
	for rows.Next() {
		var objectType, activity string
		var cnt int64
		rows.Scan(&objectType, &activity, &cnt)
		dfg.EndActivities[objectType][activity] = cnt
	}
	rows.Close()

	return dfg, nil
}

// --- OC-DFG Query Methods ---

// GetEdges returns all edges for a specific object type.
func (g *OCDFG) GetEdges(objectType string) []Edge {
	var edges []Edge
	if edgeMap, ok := g.Edges[objectType]; ok {
		for source, targets := range edgeMap {
			for target, count := range targets {
				if count > 0 {
					edges = append(edges, Edge{
						Source:     source,
						Target:     target,
						ObjectType: objectType,
						Count:      count,
					})
				}
			}
		}
	}

	// Sort by count (descending)
	sort.Slice(edges, func(i, j int) bool {
		return edges[i].Count > edges[j].Count
	})

	return edges
}

// GetAllEdges returns all edges across all object types.
func (g *OCDFG) GetAllEdges() []Edge {
	var edges []Edge
	for objectType := range g.Edges {
		edges = append(edges, g.GetEdges(objectType)...)
	}

	sort.Slice(edges, func(i, j int) bool {
		return edges[i].Count > edges[j].Count
	})

	return edges
}

// FilterByFrequency returns a new OC-DFG with edges below threshold removed.
func (g *OCDFG) FilterByFrequency(minCount int64) *OCDFG {
	filtered := NewOCDFG()
	filtered.Activities = g.Activities
	filtered.ObjectTypes = g.ObjectTypes

	for ot, edges := range g.Edges {
		filtered.Edges[ot] = make(map[string]map[string]int64)
		for source, targets := range edges {
			filtered.Edges[ot][source] = make(map[string]int64)
			for target, count := range targets {
				if count >= minCount {
					filtered.Edges[ot][source][target] = count
				}
			}
		}
	}

	// Copy other maps
	for ot, freqs := range g.ActivityFrequency {
		filtered.ActivityFrequency[ot] = make(map[string]int64)
		for a, f := range freqs {
			filtered.ActivityFrequency[ot][a] = f
		}
	}
	for ot, starts := range g.StartActivities {
		filtered.StartActivities[ot] = make(map[string]int64)
		for a, c := range starts {
			filtered.StartActivities[ot][a] = c
		}
	}
	for ot, ends := range g.EndActivities {
		filtered.EndActivities[ot] = make(map[string]int64)
		for a, c := range ends {
			filtered.EndActivities[ot][a] = c
		}
	}

	return filtered
}

// GetSharedActivities returns activities that appear in multiple object types.
func (g *OCDFG) GetSharedActivities() []string {
	activityTypes := make(map[string][]string) // activity -> object types

	for ot, freqs := range g.ActivityFrequency {
		for activity, count := range freqs {
			if count > 0 {
				activityTypes[activity] = append(activityTypes[activity], ot)
			}
		}
	}

	var shared []string
	for activity, types := range activityTypes {
		if len(types) > 1 {
			shared = append(shared, activity)
		}
	}

	sort.Strings(shared)
	return shared
}

// Stats returns statistics about the OC-DFG.
func (g *OCDFG) Stats() map[string]interface{} {
	totalEdges := 0
	edgesByType := make(map[string]int)

	for ot, edges := range g.Edges {
		count := 0
		for _, targets := range edges {
			for _, c := range targets {
				if c > 0 {
					count++
				}
			}
		}
		edgesByType[ot] = count
		totalEdges += count
	}

	return map[string]interface{}{
		"activities":       len(g.Activities),
		"object_types":     len(g.ObjectTypes),
		"total_edges":      totalEdges,
		"edges_by_type":    edgesByType,
		"shared_activities": g.GetSharedActivities(),
	}
}

// --- Helpers ---

func sortEventsByTime(events []*Event) {
	sort.Slice(events, func(i, j int) bool {
		return events[i].Timestamp.Before(events[j].Timestamp)
	})
}
