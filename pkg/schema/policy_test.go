package schema

import (
	"testing"
)

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
		{"evolving", PolicyEvolving},
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

func TestCompareSchemas_NoChanges(t *testing.T) {
	old := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false, Position: 0},
			{Name: "name", Type: "VARCHAR", Nullable: true, Position: 1},
		},
	}

	new := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false, Position: 0},
			{Name: "name", Type: "VARCHAR", Nullable: true, Position: 1},
		},
	}

	diff := CompareSchemas(old, new)

	if diff.HasChanges() {
		t.Error("Expected no changes for identical schemas")
	}
	if !diff.IsCompatible {
		t.Error("Expected compatible for identical schemas")
	}
}

func TestCompareSchemas_AddedColumns(t *testing.T) {
	old := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
		},
	}

	new := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "email", Type: "VARCHAR", Nullable: true},
		},
	}

	diff := CompareSchemas(old, new)

	if len(diff.AddedColumns) != 1 {
		t.Errorf("Expected 1 added column, got %d", len(diff.AddedColumns))
	}
	if diff.AddedColumns[0].Name != "email" {
		t.Errorf("Expected added column 'email', got %q", diff.AddedColumns[0].Name)
	}
	if !diff.IsCompatible {
		t.Error("Adding columns should be compatible")
	}
}

func TestCompareSchemas_RemovedColumns(t *testing.T) {
	old := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "email", Type: "VARCHAR", Nullable: true},
		},
	}

	new := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
		},
	}

	diff := CompareSchemas(old, new)

	if len(diff.RemovedColumns) != 1 {
		t.Errorf("Expected 1 removed column, got %d", len(diff.RemovedColumns))
	}
	if diff.IsCompatible {
		t.Error("Removing columns should be incompatible")
	}
}

func TestCompareSchemas_TypeWidening(t *testing.T) {
	old := &Schema{
		Columns: []Column{
			{Name: "count", Type: "INTEGER", Nullable: false},
		},
	}

	new := &Schema{
		Columns: []Column{
			{Name: "count", Type: "BIGINT", Nullable: false},
		},
	}

	diff := CompareSchemas(old, new)

	if len(diff.TypeChanges) != 1 {
		t.Errorf("Expected 1 type change, got %d", len(diff.TypeChanges))
	}
	if !diff.TypeChanges[0].IsWidening {
		t.Error("INTEGER -> BIGINT should be widening")
	}
	if !diff.IsCompatible {
		t.Error("Type widening should be compatible")
	}
}

func TestCompareSchemas_TypeNarrowing(t *testing.T) {
	old := &Schema{
		Columns: []Column{
			{Name: "count", Type: "BIGINT", Nullable: false},
		},
	}

	new := &Schema{
		Columns: []Column{
			{Name: "count", Type: "INTEGER", Nullable: false},
		},
	}

	diff := CompareSchemas(old, new)

	if len(diff.TypeChanges) != 1 {
		t.Errorf("Expected 1 type change, got %d", len(diff.TypeChanges))
	}
	if diff.TypeChanges[0].IsWidening {
		t.Error("BIGINT -> INTEGER should not be widening")
	}
	if diff.IsCompatible {
		t.Error("Type narrowing should be incompatible")
	}
}

func TestCompareSchemas_NullabilityChange(t *testing.T) {
	old := &Schema{
		Columns: []Column{
			{Name: "email", Type: "VARCHAR", Nullable: true},
		},
	}

	new := &Schema{
		Columns: []Column{
			{Name: "email", Type: "VARCHAR", Nullable: false},
		},
	}

	diff := CompareSchemas(old, new)

	if len(diff.NullabilityChanges) != 1 {
		t.Errorf("Expected 1 nullability change, got %d", len(diff.NullabilityChanges))
	}
	if diff.IsCompatible {
		t.Error("Nullable -> non-nullable should be incompatible")
	}
}

func TestPolicyEnforcer_Strict(t *testing.T) {
	enforcer := NewPolicyEnforcer(PolicyStrict)

	old := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
		},
	}

	// Adding column should fail strict policy
	newWithAddition := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "email", Type: "VARCHAR", Nullable: true},
		},
	}

	_, err := enforcer.Enforce(old, newWithAddition)
	if err == nil {
		t.Error("Strict policy should reject added columns")
	}

	// Identical schema should pass
	identical := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
		},
	}

	_, err = enforcer.Enforce(old, identical)
	if err != nil {
		t.Errorf("Strict policy should accept identical schema: %v", err)
	}
}

func TestPolicyEnforcer_MergeNullable(t *testing.T) {
	enforcer := NewPolicyEnforcer(PolicyMergeNullable)

	old := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
		},
	}

	// Adding nullable column should pass
	newWithNullable := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "email", Type: "VARCHAR", Nullable: true},
		},
	}

	merged, err := enforcer.Enforce(old, newWithNullable)
	if err != nil {
		t.Errorf("MergeNullable should accept nullable additions: %v", err)
	}
	if len(merged.Columns) != 2 {
		t.Errorf("Merged schema should have 2 columns, got %d", len(merged.Columns))
	}

	// Adding non-nullable column should fail
	newWithNonNullable := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "email", Type: "VARCHAR", Nullable: false},
		},
	}

	_, err = enforcer.Enforce(old, newWithNonNullable)
	if err == nil {
		t.Error("MergeNullable should reject non-nullable additions")
	}
}

func TestPolicyEnforcer_Evolving(t *testing.T) {
	enforcer := NewPolicyEnforcer(PolicyEvolving)

	old := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "count", Type: "INTEGER", Nullable: false},
		},
	}

	// Type widening + new column should pass
	newEvolved := &Schema{
		Columns: []Column{
			{Name: "id", Type: "INTEGER", Nullable: false},
			{Name: "count", Type: "BIGINT", Nullable: false}, // widened
			{Name: "email", Type: "VARCHAR", Nullable: false}, // new non-nullable OK
		},
	}

	merged, err := enforcer.Enforce(old, newEvolved)
	if err != nil {
		t.Errorf("Evolving should accept widening and additions: %v", err)
	}
	if len(merged.Columns) != 3 {
		t.Errorf("Merged schema should have 3 columns, got %d", len(merged.Columns))
	}

	// Find count column and verify widened
	for _, col := range merged.Columns {
		if col.Name == "count" && col.Type != "BIGINT" {
			t.Error("Count column should be widened to BIGINT")
		}
	}

	// Type narrowing should fail
	newNarrowed := &Schema{
		Columns: []Column{
			{Name: "id", Type: "SMALLINT", Nullable: false}, // narrowed
		},
	}

	_, err = enforcer.Enforce(old, newNarrowed)
	if err == nil {
		t.Error("Evolving should reject type narrowing")
	}
}

func TestPolicyEnforcer_OnChangeCallback(t *testing.T) {
	enforcer := NewPolicyEnforcer(PolicyEvolving)

	var callbackCalled bool
	var capturedDecision PolicyDecision
	enforcer.OnSchemaChange(func(old, new *Schema, diff *SchemaDiff, decision PolicyDecision) {
		callbackCalled = true
		capturedDecision = decision
	})

	old := &Schema{Columns: []Column{{Name: "id", Type: "INTEGER"}}}
	new := &Schema{Columns: []Column{{Name: "id", Type: "INTEGER"}, {Name: "new", Type: "VARCHAR"}}}

	enforcer.Enforce(old, new)

	if !callbackCalled {
		t.Error("OnSchemaChange callback should be called")
	}
	if capturedDecision != DecisionMerge {
		t.Errorf("Expected DecisionMerge, got %v", capturedDecision)
	}
}

func TestSchemaDiff_Summary(t *testing.T) {
	diff := &SchemaDiff{
		AddedColumns:       []Column{{Name: "a"}, {Name: "b"}},
		RemovedColumns:     []Column{{Name: "c"}},
		TypeChanges:        []TypeChange{{Column: "d"}},
		NullabilityChanges: []NullabilityChange{{Column: "e"}},
	}

	summary := diff.Summary()

	if summary == "" {
		t.Error("Expected non-empty summary")
	}
	if !diff.HasChanges() {
		t.Error("Expected HasChanges=true")
	}
}

func TestIsWideningConversion(t *testing.T) {
	tests := []struct {
		oldType  string
		newType  string
		expected bool
	}{
		{"INTEGER", "BIGINT", true},
		{"INT32", "INT64", true},
		{"FLOAT", "DOUBLE", true},
		{"VARCHAR", "TEXT", true},
		{"DATE", "TIMESTAMP", true},
		{"BIGINT", "INTEGER", false},
		{"DOUBLE", "FLOAT", false},
		{"VARCHAR", "INTEGER", false},
	}

	for _, tt := range tests {
		got := isWideningConversion(tt.oldType, tt.newType)
		if got != tt.expected {
			t.Errorf("isWideningConversion(%q, %q) = %v, want %v",
				tt.oldType, tt.newType, got, tt.expected)
		}
	}
}
