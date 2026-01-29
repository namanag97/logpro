// Package schema provides schema policy enforcement and comparison.
package schema

import (
	"fmt"
	"strings"
)

// Policy determines how schema changes are handled.
type Policy int

const (
	// PolicyStrict rejects any schema changes.
	PolicyStrict Policy = iota
	// PolicyMergeNullable allows new nullable columns, never drops or narrows types.
	PolicyMergeNullable
	// PolicyEvolving allows type widening and compatible changes.
	PolicyEvolving
)

func (p Policy) String() string {
	switch p {
	case PolicyStrict:
		return "strict"
	case PolicyMergeNullable:
		return "merge_nullable"
	case PolicyEvolving:
		return "evolving"
	default:
		return "unknown"
	}
}

// ParsePolicy parses a string into a Policy.
func ParsePolicy(s string) Policy {
	switch strings.ToLower(s) {
	case "strict":
		return PolicyStrict
	case "merge_nullable", "merge-nullable":
		return PolicyMergeNullable
	case "evolving":
		return PolicyEvolving
	default:
		return PolicyStrict
	}
}

// SchemaDiff represents differences between two schemas.
type SchemaDiff struct {
	// AddedColumns are columns present in new but not in old.
	AddedColumns []Column
	// RemovedColumns are columns present in old but not in new.
	RemovedColumns []Column
	// TypeChanges are columns with different types.
	TypeChanges []TypeChange
	// NullabilityChanges are columns with different nullability.
	NullabilityChanges []NullabilityChange
	// IsCompatible indicates if the new schema is backward compatible.
	IsCompatible bool
	// CompatibilityReason explains why schemas are incompatible.
	CompatibilityReason string
}

// TypeChange represents a type change between schemas.
type TypeChange struct {
	Column  string
	OldType string
	NewType string
	// IsWidening is true if this is a safe type promotion (e.g., INT32 -> INT64).
	IsWidening bool
}

// NullabilityChange represents a nullability change.
type NullabilityChange struct {
	Column      string
	WasNullable bool
	IsNullable  bool
}

// CompareSchemas compares two schemas and returns their differences.
func CompareSchemas(old, new *Schema) *SchemaDiff {
	diff := &SchemaDiff{
		IsCompatible: true,
	}

	// Build maps for quick lookup
	oldCols := make(map[string]Column)
	for _, col := range old.Columns {
		oldCols[col.Name] = col
	}

	newCols := make(map[string]Column)
	for _, col := range new.Columns {
		newCols[col.Name] = col
	}

	// Find added columns
	for _, col := range new.Columns {
		if _, exists := oldCols[col.Name]; !exists {
			diff.AddedColumns = append(diff.AddedColumns, col)
		}
	}

	// Find removed columns
	for _, col := range old.Columns {
		if _, exists := newCols[col.Name]; !exists {
			diff.RemovedColumns = append(diff.RemovedColumns, col)
			diff.IsCompatible = false
			diff.CompatibilityReason = fmt.Sprintf("column '%s' was removed", col.Name)
		}
	}

	// Find type and nullability changes
	for _, oldCol := range old.Columns {
		newCol, exists := newCols[oldCol.Name]
		if !exists {
			continue
		}

		// Check type change
		if oldCol.Type != newCol.Type {
			change := TypeChange{
				Column:     oldCol.Name,
				OldType:    oldCol.Type,
				NewType:    newCol.Type,
				IsWidening: isWideningConversion(oldCol.Type, newCol.Type),
			}
			diff.TypeChanges = append(diff.TypeChanges, change)

			if !change.IsWidening {
				diff.IsCompatible = false
				diff.CompatibilityReason = fmt.Sprintf("column '%s' type narrowed from %s to %s",
					oldCol.Name, oldCol.Type, newCol.Type)
			}
		}

		// Check nullability change
		if oldCol.Nullable != newCol.Nullable {
			diff.NullabilityChanges = append(diff.NullabilityChanges, NullabilityChange{
				Column:      oldCol.Name,
				WasNullable: oldCol.Nullable,
				IsNullable:  newCol.Nullable,
			})

			// Going from nullable to non-nullable is breaking
			if oldCol.Nullable && !newCol.Nullable {
				diff.IsCompatible = false
				diff.CompatibilityReason = fmt.Sprintf("column '%s' changed from nullable to non-nullable",
					oldCol.Name)
			}
		}
	}

	return diff
}

// isWideningConversion checks if a type change is a safe widening conversion.
func isWideningConversion(oldType, newType string) bool {
	// Normalize type names
	oldType = strings.ToUpper(oldType)
	newType = strings.ToUpper(newType)

	// Define safe widening conversions
	widenings := map[string][]string{
		"TINYINT":  {"SMALLINT", "INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"SMALLINT": {"INTEGER", "BIGINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"INTEGER":  {"BIGINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"INT":      {"BIGINT", "FLOAT", "DOUBLE", "DECIMAL"},
		"INT32":    {"INT64", "FLOAT", "DOUBLE", "DECIMAL"},
		"INT64":    {"DOUBLE", "DECIMAL"},
		"BIGINT":   {"DOUBLE", "DECIMAL"},
		"FLOAT":    {"DOUBLE"},
		"REAL":     {"DOUBLE"},
		"VARCHAR":  {"TEXT", "STRING"},
		"CHAR":     {"VARCHAR", "TEXT", "STRING"},
		"DATE":     {"TIMESTAMP", "TIMESTAMPTZ"},
	}

	allowedTargets, ok := widenings[oldType]
	if !ok {
		return false
	}

	for _, target := range allowedTargets {
		if newType == target || strings.Contains(newType, target) {
			return true
		}
	}

	return false
}

// PolicyEnforcer enforces schema policies.
type PolicyEnforcer struct {
	policy   Policy
	onchange func(old, new *Schema, diff *SchemaDiff, decision PolicyDecision)
}

// PolicyDecision represents the outcome of a policy check.
type PolicyDecision int

const (
	DecisionAccept PolicyDecision = iota
	DecisionReject
	DecisionMerge
)

func (d PolicyDecision) String() string {
	switch d {
	case DecisionAccept:
		return "accept"
	case DecisionReject:
		return "reject"
	case DecisionMerge:
		return "merge"
	default:
		return "unknown"
	}
}

// NewPolicyEnforcer creates a new policy enforcer.
func NewPolicyEnforcer(policy Policy) *PolicyEnforcer {
	return &PolicyEnforcer{
		policy: policy,
	}
}

// OnSchemaChange sets a callback for schema changes.
func (e *PolicyEnforcer) OnSchemaChange(fn func(old, new *Schema, diff *SchemaDiff, decision PolicyDecision)) *PolicyEnforcer {
	e.onchange = fn
	return e
}

// Enforce checks if a new schema is acceptable under the policy.
// Returns the merged schema (if applicable) and any error.
func (e *PolicyEnforcer) Enforce(old, new *Schema) (*Schema, error) {
	diff := CompareSchemas(old, new)
	var decision PolicyDecision
	var merged *Schema
	var err error

	switch e.policy {
	case PolicyStrict:
		decision, merged, err = e.enforceStrict(old, new, diff)
	case PolicyMergeNullable:
		decision, merged, err = e.enforceMergeNullable(old, new, diff)
	case PolicyEvolving:
		decision, merged, err = e.enforceEvolving(old, new, diff)
	default:
		return nil, fmt.Errorf("unknown policy: %v", e.policy)
	}

	// Invoke callback
	if e.onchange != nil {
		e.onchange(old, new, diff, decision)
	}

	return merged, err
}

func (e *PolicyEnforcer) enforceStrict(old, new *Schema, diff *SchemaDiff) (PolicyDecision, *Schema, error) {
	// Strict policy: reject any changes
	if len(diff.AddedColumns) > 0 {
		return DecisionReject, nil, fmt.Errorf("strict policy: %d new columns added", len(diff.AddedColumns))
	}
	if len(diff.RemovedColumns) > 0 {
		return DecisionReject, nil, fmt.Errorf("strict policy: %d columns removed", len(diff.RemovedColumns))
	}
	if len(diff.TypeChanges) > 0 {
		return DecisionReject, nil, fmt.Errorf("strict policy: %d type changes detected", len(diff.TypeChanges))
	}
	if len(diff.NullabilityChanges) > 0 {
		return DecisionReject, nil, fmt.Errorf("strict policy: %d nullability changes detected", len(diff.NullabilityChanges))
	}

	return DecisionAccept, new, nil
}

func (e *PolicyEnforcer) enforceMergeNullable(old, new *Schema, diff *SchemaDiff) (PolicyDecision, *Schema, error) {
	// Reject removed columns
	if len(diff.RemovedColumns) > 0 {
		return DecisionReject, nil, fmt.Errorf("merge_nullable policy: columns cannot be removed: %v",
			columnNames(diff.RemovedColumns))
	}

	// Reject type narrowing
	for _, tc := range diff.TypeChanges {
		if !tc.IsWidening {
			return DecisionReject, nil, fmt.Errorf("merge_nullable policy: type narrowing not allowed for column '%s' (%s -> %s)",
				tc.Column, tc.OldType, tc.NewType)
		}
	}

	// Reject nullable -> non-nullable
	for _, nc := range diff.NullabilityChanges {
		if nc.WasNullable && !nc.IsNullable {
			return DecisionReject, nil, fmt.Errorf("merge_nullable policy: cannot make nullable column '%s' non-nullable",
				nc.Column)
		}
	}

	// New columns must be nullable
	for _, col := range diff.AddedColumns {
		if !col.Nullable {
			return DecisionReject, nil, fmt.Errorf("merge_nullable policy: new column '%s' must be nullable", col.Name)
		}
	}

	// Merge: keep old columns and add new nullable ones
	merged := mergeSchemas(old, new, diff)
	return DecisionMerge, merged, nil
}

func (e *PolicyEnforcer) enforceEvolving(old, new *Schema, diff *SchemaDiff) (PolicyDecision, *Schema, error) {
	// Reject removed columns (data loss)
	if len(diff.RemovedColumns) > 0 {
		return DecisionReject, nil, fmt.Errorf("evolving policy: columns cannot be removed: %v",
			columnNames(diff.RemovedColumns))
	}

	// Reject type narrowing (data loss)
	for _, tc := range diff.TypeChanges {
		if !tc.IsWidening {
			return DecisionReject, nil, fmt.Errorf("evolving policy: type narrowing not allowed for column '%s' (%s -> %s)",
				tc.Column, tc.OldType, tc.NewType)
		}
	}

	// Accept all other changes (widening, new columns, nullability)
	merged := mergeSchemas(old, new, diff)
	return DecisionMerge, merged, nil
}

// mergeSchemas creates a merged schema from old and new.
func mergeSchemas(old, new *Schema, diff *SchemaDiff) *Schema {
	merged := &Schema{
		Version:    new.Version,
		InferredAt: new.InferredAt,
		SampleSize: new.SampleSize,
		SourceFile: new.SourceFile,
	}

	// Start with old columns (preserving order)
	oldCols := make(map[string]bool)
	for _, col := range old.Columns {
		oldCols[col.Name] = true

		// Check if this column was widened
		mergedCol := col
		for _, tc := range diff.TypeChanges {
			if tc.Column == col.Name && tc.IsWidening {
				mergedCol.Type = tc.NewType
				break
			}
		}

		// Check if nullability changed (to nullable is always safe)
		for _, nc := range diff.NullabilityChanges {
			if nc.Column == col.Name && !nc.WasNullable && nc.IsNullable {
				mergedCol.Nullable = true
				break
			}
		}

		merged.Columns = append(merged.Columns, mergedCol)
	}

	// Add new columns at the end
	for _, col := range diff.AddedColumns {
		merged.Columns = append(merged.Columns, col)
	}

	// Update positions
	for i := range merged.Columns {
		merged.Columns[i].Position = i
	}

	// Update fingerprint
	merged.Fingerprint = fingerprintColumns(merged.Columns)

	return merged
}

func columnNames(cols []Column) []string {
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name
	}
	return names
}

// HasChanges returns true if there are any schema differences.
func (d *SchemaDiff) HasChanges() bool {
	return len(d.AddedColumns) > 0 ||
		len(d.RemovedColumns) > 0 ||
		len(d.TypeChanges) > 0 ||
		len(d.NullabilityChanges) > 0
}

// Summary returns a human-readable summary of the diff.
func (d *SchemaDiff) Summary() string {
	if !d.HasChanges() {
		return "no changes"
	}

	var parts []string
	if len(d.AddedColumns) > 0 {
		parts = append(parts, fmt.Sprintf("+%d columns", len(d.AddedColumns)))
	}
	if len(d.RemovedColumns) > 0 {
		parts = append(parts, fmt.Sprintf("-%d columns", len(d.RemovedColumns)))
	}
	if len(d.TypeChanges) > 0 {
		parts = append(parts, fmt.Sprintf("%d type changes", len(d.TypeChanges)))
	}
	if len(d.NullabilityChanges) > 0 {
		parts = append(parts, fmt.Sprintf("%d nullability changes", len(d.NullabilityChanges)))
	}

	return strings.Join(parts, ", ")
}
