// Package ocel provides Object-Centric Event Log (OCEL 2.0) support.
// Uses Parquet nested types (LIST of STRUCT) for native OCEL storage.
package ocel

import (
	"github.com/apache/arrow/go/v14/arrow"
)

// ObjectRef represents a reference to an object in an event.
type ObjectRef struct {
	ObjectID   string `json:"object_id" parquet:"object_id"`
	ObjectType string `json:"object_type" parquet:"object_type"`
}

// Event represents an OCEL event with multiple object references.
type Event struct {
	EventID    string                 `json:"event_id"`
	Activity   string                 `json:"activity"`
	Timestamp  int64                  `json:"timestamp"` // Unix nanos
	Objects    []ObjectRef            `json:"objects"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// Object represents an object in the object catalog.
type Object struct {
	ObjectID   string                 `json:"object_id"`
	ObjectType string                 `json:"object_type"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// Relationship defines a relationship between object types.
type Relationship struct {
	SourceType string `json:"source_type"`
	TargetType string `json:"target_type"`
	Name       string `json:"name,omitempty"`
}

// Log represents a complete OCEL log.
type Log struct {
	// Metadata
	Version      string         `json:"version"`
	ObjectTypes  []string       `json:"object_types"`
	Relationships []Relationship `json:"relationships,omitempty"`
	DefaultView  string         `json:"default_view,omitempty"` // Primary object type for flat view

	// Data
	Events  []Event  `json:"events"`
	Objects []Object `json:"objects,omitempty"`
}

// Metadata keys for Parquet footer.
const (
	MetaKeyVersion       = "ocel:version"
	MetaKeyObjectTypes   = "ocel:object_types"
	MetaKeyRelationships = "ocel:relationships"
	MetaKeyDefaultView   = "ocel:default_view"
	MetaKeyEventCount    = "ocel:event_count"
	MetaKeyObjectCount   = "ocel:object_count"
)

// ArrowSchema returns the Arrow schema for OCEL events.
// Uses nested LIST of STRUCT for object references.
func ArrowSchema() *arrow.Schema {
	// Object reference struct: {object_id: string, object_type: string}
	objectRefStruct := arrow.StructOf(
		arrow.Field{Name: "object_id", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "object_type", Type: arrow.BinaryTypes.String, Nullable: false},
	)

	// Main schema with nested objects list
	return arrow.NewSchema([]arrow.Field{
		{Name: "event_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "activity", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "objects", Type: arrow.ListOf(objectRefStruct), Nullable: false},
	}, nil)
}

// ArrowSchemaWithAttributes returns schema including dynamic attributes.
func ArrowSchemaWithAttributes(attrTypes map[string]arrow.DataType) *arrow.Schema {
	objectRefStruct := arrow.StructOf(
		arrow.Field{Name: "object_id", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "object_type", Type: arrow.BinaryTypes.String, Nullable: false},
	)

	fields := []arrow.Field{
		{Name: "event_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "activity", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "objects", Type: arrow.ListOf(objectRefStruct), Nullable: false},
	}

	// Add attribute columns
	for name, dtype := range attrTypes {
		fields = append(fields, arrow.Field{
			Name:     "attr_" + name,
			Type:     dtype,
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil)
}

// ObjectCatalogSchema returns the Arrow schema for the object catalog.
func ObjectCatalogSchema() *arrow.Schema {
	return arrow.NewSchema([]arrow.Field{
		{Name: "object_id", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "object_type", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)
}

// Statistics holds OCEL log statistics.
type Statistics struct {
	EventCount       int64            `json:"event_count"`
	ObjectCount      int64            `json:"object_count"`
	ObjectTypes      []string         `json:"object_types"`
	ObjectTypeCounts map[string]int64 `json:"object_type_counts"`
	ActivityCounts   map[string]int64 `json:"activity_counts"`
	TimeRange        TimeRange        `json:"time_range"`
}

// TimeRange represents a time span.
type TimeRange struct {
	Start int64 `json:"start"` // Unix nanos
	End   int64 `json:"end"`   // Unix nanos
}
