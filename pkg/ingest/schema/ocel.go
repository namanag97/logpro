package schema

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
)

// OCEL column name.
const OCELObjectsColumn = "ocel_objects"

// ObjectRole indicates whether a column represents an object ID or type.
type ObjectRole uint8

const (
	ObjectRoleID   ObjectRole = iota // Column contains object identifiers
	ObjectRoleType                   // Column contains object type labels
)

// ObjectColumnHint describes a column identified as an OCEL object reference.
type ObjectColumnHint struct {
	FieldIndex int
	FieldName  string
	ObjectType string     // Inferred object type (e.g., "order" from "order_id")
	Role       ObjectRole // Whether this is an ID or type column
}

// OCELObjectType returns the Arrow type for the ocel_objects column:
// LIST<STRUCT<object_id: STRING, object_type: STRING>>
func OCELObjectType() *arrow.ListType {
	return arrow.ListOf(
		arrow.StructOf(
			arrow.Field{Name: "object_id", Type: arrow.BinaryTypes.String, Nullable: false},
			arrow.Field{Name: "object_type", Type: arrow.BinaryTypes.String, Nullable: false},
		),
	)
}

// OCELObjectField returns an Arrow field for the ocel_objects column.
func OCELObjectField() arrow.Field {
	return arrow.Field{
		Name:     OCELObjectsColumn,
		Type:     OCELObjectType(),
		Nullable: true,
	}
}

// InjectOCELColumn appends the ocel_objects nested column to a schema
// if it is not already present. The original metadata is preserved.
func InjectOCELColumn(s *arrow.Schema) *arrow.Schema {
	for _, f := range s.Fields() {
		if f.Name == OCELObjectsColumn {
			return s
		}
	}
	fields := make([]arrow.Field, 0, s.NumFields()+1)
	fields = append(fields, s.Fields()...)
	fields = append(fields, OCELObjectField())
	md := s.Metadata()
	return arrow.NewSchema(fields, &md)
}

// IsOCELObjectColumn returns true if the field is the ocel_objects nested column.
func IsOCELObjectColumn(f arrow.Field) bool {
	return f.Name == OCELObjectsColumn
}

// IdentifyObjectColumns scans schema field names for common OCEL object-reference
// patterns (e.g., "order_id", "customer_type") and returns hints about which
// columns likely represent object references. Columns named "case_id" or
// "event_id" are excluded as they are standard process mining identifiers.
func IdentifyObjectColumns(s *arrow.Schema) []ObjectColumnHint {
	var hints []ObjectColumnHint
	for i, f := range s.Fields() {
		name := strings.ToLower(f.Name)
		if strings.HasSuffix(name, "_id") {
			objType := strings.TrimSuffix(name, "_id")
			if objType != "" && objType != "case" && objType != "event" {
				hints = append(hints, ObjectColumnHint{
					FieldIndex: i,
					FieldName:  f.Name,
					ObjectType: objType,
					Role:       ObjectRoleID,
				})
			}
		} else if strings.HasSuffix(name, "_type") {
			objType := strings.TrimSuffix(name, "_type")
			if objType != "" && objType != "event" && objType != "attr" {
				hints = append(hints, ObjectColumnHint{
					FieldIndex: i,
					FieldName:  f.Name,
					ObjectType: objType,
					Role:       ObjectRoleType,
				})
			}
		}
	}
	return hints
}

// EnrichBatchWithOCEL creates a new RecordBatch with the ocel_objects column
// populated from the identified object columns in the original batch.
// The returned record has all original columns plus the ocel_objects column.
func EnrichBatchWithOCEL(alloc memory.Allocator, batch arrow.Record, hints []ObjectColumnHint) (arrow.Record, error) {
	enrichedSchema := InjectOCELColumn(batch.Schema())

	// Collect existing columns
	cols := make([]arrow.Array, 0, int(batch.NumCols())+1)
	for i := 0; i < int(batch.NumCols()); i++ {
		col := batch.Column(i)
		col.Retain()
		cols = append(cols, col)
	}

	// Build ocel_objects column
	ocelStructType := OCELObjectType().Elem().(*arrow.StructType)
	listBuilder := array.NewListBuilder(alloc, ocelStructType)
	defer listBuilder.Release()
	structBuilder := listBuilder.ValueBuilder().(*array.StructBuilder)

	nRows := int(batch.NumRows())
	for row := 0; row < nRows; row++ {
		listBuilder.Append(true) // start a new list for this row

		for _, hint := range hints {
			if hint.Role != ObjectRoleID {
				continue
			}
			if hint.FieldIndex >= int(batch.NumCols()) {
				continue
			}

			col := batch.Column(hint.FieldIndex)
			if col.IsNull(row) {
				continue
			}

			objectID := extractColumnStringValue(col, row)
			if objectID == "" {
				continue
			}

			structBuilder.Append(true)
			structBuilder.FieldBuilder(0).(*array.StringBuilder).Append(objectID)
			structBuilder.FieldBuilder(1).(*array.StringBuilder).Append(hint.ObjectType)
		}
	}

	ocelCol := listBuilder.NewArray()
	cols = append(cols, ocelCol)

	record := array.NewRecord(enrichedSchema, cols, batch.NumRows())

	// Release our retained references and the built column
	for i := 0; i < int(batch.NumCols()); i++ {
		cols[i].Release()
	}
	ocelCol.Release()

	return record, nil
}

// extractColumnStringValue extracts a string value from an Arrow array at the given row.
func extractColumnStringValue(col arrow.Array, row int) string {
	switch c := col.(type) {
	case *array.String:
		return c.Value(row)
	case *array.Int64:
		return fmt.Sprintf("%d", c.Value(row))
	case *array.Float64:
		return fmt.Sprintf("%g", c.Value(row))
	case *array.Boolean:
		if c.Value(row) {
			return "true"
		}
		return "false"
	case *array.Timestamp:
		return fmt.Sprintf("%d", c.Value(row))
	default:
		return ""
	}
}
