package schema

import (
	"fmt"

	"github.com/apache/arrow/go/v14/arrow"
)

// Change represents a schema change.
type Change struct {
	Type       ChangeType
	FieldName  string
	OldType    arrow.DataType
	NewType    arrow.DataType
	OldIndex   int
	NewIndex   int
}

// ChangeType categorizes schema changes.
type ChangeType int

const (
	ChangeTypeAdded ChangeType = iota
	ChangeTypeRemoved
	ChangeTypeTypeChanged
	ChangeTypeRenamed
	ChangeTypeNullabilityChanged
)

func (c ChangeType) String() string {
	switch c {
	case ChangeTypeAdded:
		return "added"
	case ChangeTypeRemoved:
		return "removed"
	case ChangeTypeTypeChanged:
		return "type_changed"
	case ChangeTypeRenamed:
		return "renamed"
	case ChangeTypeNullabilityChanged:
		return "nullability_changed"
	default:
		return "unknown"
	}
}

// Diff contains the differences between two schemas.
type Diff struct {
	Changes       []Change
	IsCompatible  bool
	BreakingCount int
}

// Compare compares two schemas and returns differences.
func Compare(old, new *arrow.Schema) *Diff {
	diff := &Diff{
		IsCompatible: true,
	}

	oldFields := make(map[string]fieldInfo)
	for i, f := range old.Fields() {
		oldFields[f.Name] = fieldInfo{
			index:    i,
			field:    f,
		}
	}

	newFields := make(map[string]fieldInfo)
	for i, f := range new.Fields() {
		newFields[f.Name] = fieldInfo{
			index:    i,
			field:    f,
		}
	}

	// Check for removed or changed fields
	for name, oldInfo := range oldFields {
		newInfo, exists := newFields[name]
		if !exists {
			diff.Changes = append(diff.Changes, Change{
				Type:      ChangeTypeRemoved,
				FieldName: name,
				OldType:   oldInfo.field.Type,
				OldIndex:  oldInfo.index,
			})
			diff.IsCompatible = false
			diff.BreakingCount++
			continue
		}

		// Check type change
		if !arrow.TypeEqual(oldInfo.field.Type, newInfo.field.Type) {
			change := Change{
				Type:      ChangeTypeTypeChanged,
				FieldName: name,
				OldType:   oldInfo.field.Type,
				NewType:   newInfo.field.Type,
				OldIndex:  oldInfo.index,
				NewIndex:  newInfo.index,
			}
			diff.Changes = append(diff.Changes, change)

			// Check if type widening (compatible) or narrowing (breaking)
			if !isCompatibleTypeChange(oldInfo.field.Type, newInfo.field.Type) {
				diff.IsCompatible = false
				diff.BreakingCount++
			}
		}

		// Check nullability change
		if oldInfo.field.Nullable != newInfo.field.Nullable {
			diff.Changes = append(diff.Changes, Change{
				Type:      ChangeTypeNullabilityChanged,
				FieldName: name,
				OldIndex:  oldInfo.index,
				NewIndex:  newInfo.index,
			})
			// Making non-nullable → nullable is compatible
			// Making nullable → non-nullable is breaking
			if oldInfo.field.Nullable && !newInfo.field.Nullable {
				diff.IsCompatible = false
				diff.BreakingCount++
			}
		}
	}

	// Check for added fields
	for name, newInfo := range newFields {
		if _, exists := oldFields[name]; !exists {
			diff.Changes = append(diff.Changes, Change{
				Type:      ChangeTypeAdded,
				FieldName: name,
				NewType:   newInfo.field.Type,
				NewIndex:  newInfo.index,
			})
			// Added columns must be nullable to be compatible
			if !newInfo.field.Nullable {
				diff.IsCompatible = false
				diff.BreakingCount++
			}
		}
	}

	return diff
}

type fieldInfo struct {
	index int
	field arrow.Field
}

// isCompatibleTypeChange checks if a type change is safe (widening).
func isCompatibleTypeChange(old, new arrow.DataType) bool {
	// Same type is always compatible
	if arrow.TypeEqual(old, new) {
		return true
	}

	// Int widening: int8 → int16 → int32 → int64
	if isIntType(old) && isIntType(new) {
		return intBitWidth(new) >= intBitWidth(old)
	}

	// Float widening: float32 → float64
	if old.ID() == arrow.FLOAT32 && new.ID() == arrow.FLOAT64 {
		return true
	}

	// Int to float is compatible
	if isIntType(old) && isFloatType(new) {
		return true
	}

	// Any type to string is compatible
	if new.ID() == arrow.STRING || new.ID() == arrow.LARGE_STRING {
		return true
	}

	return false
}

func isIntType(t arrow.DataType) bool {
	switch t.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return true
	case arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return true
	}
	return false
}

func isFloatType(t arrow.DataType) bool {
	return t.ID() == arrow.FLOAT32 || t.ID() == arrow.FLOAT64
}

func intBitWidth(t arrow.DataType) int {
	switch t.ID() {
	case arrow.INT8, arrow.UINT8:
		return 8
	case arrow.INT16, arrow.UINT16:
		return 16
	case arrow.INT32, arrow.UINT32:
		return 32
	case arrow.INT64, arrow.UINT64:
		return 64
	}
	return 0
}

// Merge merges two schemas according to a policy.
func Merge(old, new *arrow.Schema, policy Policy) (*arrow.Schema, error) {
	switch policy {
	case PolicyStrict:
		return mergeStrict(old, new)
	case PolicyMergeNullable:
		return mergeMergeNullable(old, new)
	case PolicyEvolving:
		return mergeEvolving(old, new)
	default:
		return nil, fmt.Errorf("unknown policy: %v", policy)
	}
}

func mergeStrict(old, new *arrow.Schema) (*arrow.Schema, error) {
	diff := Compare(old, new)
	if len(diff.Changes) > 0 {
		return nil, fmt.Errorf("schema mismatch: %d changes detected", len(diff.Changes))
	}
	return old, nil
}

func mergeMergeNullable(old, new *arrow.Schema) (*arrow.Schema, error) {
	diff := Compare(old, new)

	// Check for breaking changes
	for _, change := range diff.Changes {
		if change.Type == ChangeTypeRemoved {
			return nil, fmt.Errorf("cannot remove field %s in merge_nullable mode", change.FieldName)
		}
		if change.Type == ChangeTypeTypeChanged && !isCompatibleTypeChange(change.OldType, change.NewType) {
			return nil, fmt.Errorf("incompatible type change for field %s: %v → %v",
				change.FieldName, change.OldType, change.NewType)
		}
	}

	// Build merged schema: old fields + new nullable fields
	fields := make([]arrow.Field, 0, old.NumFields())

	// Keep old fields (widen types if needed)
	for i := 0; i < old.NumFields(); i++ {
		oldField := old.Field(i)
		newFieldIdx := new.FieldIndices(oldField.Name)
		if len(newFieldIdx) > 0 {
			newField := new.Field(newFieldIdx[0])
			// Use wider type if compatible
			if isCompatibleTypeChange(oldField.Type, newField.Type) {
				fields = append(fields, arrow.Field{
					Name:     oldField.Name,
					Type:     newField.Type,
					Nullable: oldField.Nullable || newField.Nullable,
				})
				continue
			}
		}
		fields = append(fields, oldField)
	}

	// Add new nullable fields
	for i := 0; i < new.NumFields(); i++ {
		newField := new.Field(i)
		if len(old.FieldIndices(newField.Name)) == 0 {
			fields = append(fields, arrow.Field{
				Name:     newField.Name,
				Type:     newField.Type,
				Nullable: true, // Force nullable
			})
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func mergeEvolving(old, new *arrow.Schema) (*arrow.Schema, error) {
	// Most permissive: allow all safe changes
	fields := make([]arrow.Field, 0, max(old.NumFields(), new.NumFields()))

	// Process all fields from both schemas
	seen := make(map[string]bool)

	// First, process old fields
	for i := 0; i < old.NumFields(); i++ {
		oldField := old.Field(i)
		seen[oldField.Name] = true

		newFieldIdx := new.FieldIndices(oldField.Name)
		if len(newFieldIdx) > 0 {
			newField := new.Field(newFieldIdx[0])
			// Pick wider type
			mergedType := widerType(oldField.Type, newField.Type)
			fields = append(fields, arrow.Field{
				Name:     oldField.Name,
				Type:     mergedType,
				Nullable: true, // Allow nulls for evolution
			})
		} else {
			// Field removed in new schema - keep as nullable
			fields = append(fields, arrow.Field{
				Name:     oldField.Name,
				Type:     oldField.Type,
				Nullable: true,
			})
		}
	}

	// Add new fields
	for i := 0; i < new.NumFields(); i++ {
		newField := new.Field(i)
		if !seen[newField.Name] {
			fields = append(fields, arrow.Field{
				Name:     newField.Name,
				Type:     newField.Type,
				Nullable: true,
			})
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func widerType(a, b arrow.DataType) arrow.DataType {
	if arrow.TypeEqual(a, b) {
		return a
	}

	// String is widest
	if a.ID() == arrow.STRING || b.ID() == arrow.STRING {
		return arrow.BinaryTypes.String
	}

	// Float64 > Float32
	if a.ID() == arrow.FLOAT64 || b.ID() == arrow.FLOAT64 {
		return arrow.PrimitiveTypes.Float64
	}

	// Int64 > Int32 > Int16 > Int8
	if isIntType(a) && isIntType(b) {
		if intBitWidth(a) >= intBitWidth(b) {
			return a
		}
		return b
	}

	// Int to Float
	if isIntType(a) && isFloatType(b) {
		return b
	}
	if isFloatType(a) && isIntType(b) {
		return a
	}

	// Default to string
	return arrow.BinaryTypes.String
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
