package schema

import (
	"testing"

	"github.com/apache/arrow/go/v14/arrow"
)

// Tests ported from pkg/schema/policy_test.go (consolidated)

func TestPolicy_String(t *testing.T) {
	tests := []struct {
		policy   Policy
		expected string
	}{
		{PolicyStrict, "strict"},
		{PolicyMergeNullable, "merge_nullable"},
		{PolicyEvolving, "evolving"},
		{Policy(99), "unknown"},
	}

	for _, tt := range tests {
		got := tt.policy.String()
		if got != tt.expected {
			t.Errorf("Policy(%d).String() = %q, want %q", tt.policy, got, tt.expected)
		}
	}
}

func TestParsePolicy(t *testing.T) {
	tests := []struct {
		input    string
		expected Policy
	}{
		{"strict", PolicyStrict},
		{"merge_nullable", PolicyMergeNullable},
		{"merge-nullable", PolicyMergeNullable},
		{"merge", PolicyMergeNullable},
		{"evolving", PolicyEvolving},
		{"evolve", PolicyEvolving},
		{"STRICT", PolicyStrict},
		{"unknown", PolicyStrict}, // default
	}

	for _, tt := range tests {
		got := ParsePolicy(tt.input)
		if got != tt.expected {
			t.Errorf("ParsePolicy(%q) = %v, want %v", tt.input, got, tt.expected)
		}
	}
}

func TestCompare_NoChanges(t *testing.T) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	diff := Compare(schema, schema)

	if len(diff.Changes) != 0 {
		t.Error("Expected no changes for identical schemas")
	}
	if !diff.IsCompatible {
		t.Error("Expected compatible for identical schemas")
	}
}

func TestCompare_AddedColumns(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	new := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	diff := Compare(old, new)

	addedCount := 0
	for _, c := range diff.Changes {
		if c.Type == ChangeTypeAdded {
			addedCount++
			if c.FieldName != "email" {
				t.Errorf("Expected added field 'email', got %q", c.FieldName)
			}
		}
	}
	if addedCount != 1 {
		t.Errorf("Expected 1 added column, got %d", addedCount)
	}
}

func TestCompare_RemovedColumns(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	new := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	diff := Compare(old, new)

	removedCount := 0
	for _, c := range diff.Changes {
		if c.Type == ChangeTypeRemoved {
			removedCount++
		}
	}
	if removedCount != 1 {
		t.Errorf("Expected 1 removed column, got %d", removedCount)
	}
	if diff.IsCompatible {
		t.Error("Removing columns should be incompatible")
	}
}

func TestCompare_TypeWidening(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	new := arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	diff := Compare(old, new)

	typeChanges := 0
	for _, c := range diff.Changes {
		if c.Type == ChangeTypeTypeChanged {
			typeChanges++
		}
	}
	if typeChanges != 1 {
		t.Errorf("Expected 1 type change, got %d", typeChanges)
	}
	if !diff.IsCompatible {
		t.Error("Int32 -> Int64 widening should be compatible")
	}
}

func TestCompare_TypeNarrowing(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	new := arrow.NewSchema([]arrow.Field{
		{Name: "count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	diff := Compare(old, new)

	if diff.IsCompatible {
		t.Error("Int64 -> Int32 narrowing should be incompatible")
	}
}

func TestCompare_NullabilityChange(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	new := arrow.NewSchema([]arrow.Field{
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: false},
	}, nil)

	diff := Compare(old, new)

	nullChanges := 0
	for _, c := range diff.Changes {
		if c.Type == ChangeTypeNullabilityChanged {
			nullChanges++
		}
	}
	if nullChanges != 1 {
		t.Errorf("Expected 1 nullability change, got %d", nullChanges)
	}
	if diff.IsCompatible {
		t.Error("Nullable -> non-nullable should be incompatible")
	}
}

func TestMerge_Strict(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Adding column should fail strict
	withNew := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	_, err := Merge(old, withNew, PolicyStrict)
	if err == nil {
		t.Error("Strict policy should reject added columns")
	}

	// Identical should pass
	_, err = Merge(old, old, PolicyStrict)
	if err != nil {
		t.Errorf("Strict policy should accept identical schema: %v", err)
	}
}

func TestMerge_MergeNullable(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	// Adding nullable column should pass
	withNullable := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	merged, err := Merge(old, withNullable, PolicyMergeNullable)
	if err != nil {
		t.Errorf("MergeNullable should accept nullable additions: %v", err)
	}
	if merged.NumFields() != 2 {
		t.Errorf("Merged schema should have 2 fields, got %d", merged.NumFields())
	}
}

func TestMerge_Evolving(t *testing.T) {
	old := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "count", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
	}, nil)

	// Type widening + new column should pass
	evolved := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: false}, // widened
		{Name: "email", Type: arrow.BinaryTypes.String, Nullable: false},   // new
	}, nil)

	merged, err := Merge(old, evolved, PolicyEvolving)
	if err != nil {
		t.Errorf("Evolving should accept widening and additions: %v", err)
	}
	if merged.NumFields() != 3 {
		t.Errorf("Merged schema should have 3 fields, got %d", merged.NumFields())
	}

	// Verify count was widened to Int64
	countIdx := merged.FieldIndices("count")
	if len(countIdx) > 0 {
		countField := merged.Field(countIdx[0])
		if countField.Type.ID() != arrow.INT64 {
			t.Errorf("Count field should be widened to Int64, got %v", countField.Type)
		}
	}
}

func TestValidator(t *testing.T) {
	initial := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	v := NewValidator(initial, PolicyMergeNullable)

	// Same schema should pass
	_, err := v.Validate(initial)
	if err != nil {
		t.Errorf("Validation of identical schema failed: %v", err)
	}

	// Schema with new nullable column should pass
	extended := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	merged, err := v.Validate(extended)
	if err != nil {
		t.Errorf("Validation of extended schema failed: %v", err)
	}
	if merged.NumFields() != 2 {
		t.Errorf("Merged schema should have 2 fields, got %d", merged.NumFields())
	}
}

func TestMasterSchema(t *testing.T) {
	ms := NewMasterSchema(PolicyEvolving)

	// First update sets the schema
	schema1 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
	}, nil)

	s, err := ms.Update(schema1)
	if err != nil {
		t.Fatal(err)
	}
	if s.NumFields() != 1 {
		t.Errorf("Expected 1 field, got %d", s.NumFields())
	}
	if ms.Version != 1 {
		t.Errorf("Expected version 1, got %d", ms.Version)
	}

	// Second update evolves
	schema2 := arrow.NewSchema([]arrow.Field{
		{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: false},
		{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	s, err = ms.Update(schema2)
	if err != nil {
		t.Fatal(err)
	}
	if s.NumFields() != 2 {
		t.Errorf("Expected 2 fields, got %d", s.NumFields())
	}
	if ms.Version != 2 {
		t.Errorf("Expected version 2, got %d", ms.Version)
	}
}

func TestIsCompatibleTypeChange(t *testing.T) {
	tests := []struct {
		old      arrow.DataType
		new      arrow.DataType
		expected bool
	}{
		{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int64, true},
		{arrow.PrimitiveTypes.Int64, arrow.PrimitiveTypes.Int32, false},
		{arrow.PrimitiveTypes.Float32, arrow.PrimitiveTypes.Float64, true},
		{arrow.PrimitiveTypes.Float64, arrow.PrimitiveTypes.Float32, false},
		{arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Float64, true},
		{arrow.PrimitiveTypes.Int64, arrow.BinaryTypes.String, true}, // any to string
	}

	for _, tt := range tests {
		got := isCompatibleTypeChange(tt.old, tt.new)
		if got != tt.expected {
			t.Errorf("isCompatibleTypeChange(%v, %v) = %v, want %v",
				tt.old, tt.new, got, tt.expected)
		}
	}
}
