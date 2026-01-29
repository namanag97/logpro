package schema

import (
	"github.com/apache/arrow/go/v14/arrow"
)

// Policy defines how schema mismatches are handled.
type Policy int

const (
	// PolicyStrict rejects any schema mismatch.
	PolicyStrict Policy = iota

	// PolicyMergeNullable allows adding new nullable columns, but no removals.
	PolicyMergeNullable

	// PolicyEvolving allows compatible type widening and column additions.
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

// ParsePolicy parses a policy string.
func ParsePolicy(s string) Policy {
	switch s {
	case "strict":
		return PolicyStrict
	case "merge_nullable", "merge":
		return PolicyMergeNullable
	case "evolving", "evolve":
		return PolicyEvolving
	default:
		return PolicyStrict
	}
}

// Validator validates data against a schema.
type Validator struct {
	Schema         *arrow.Schema
	Policy         Policy
	OnSchemaChange func(old, new *arrow.Schema, diff *Diff)
}

// NewValidator creates a schema validator.
func NewValidator(schema *arrow.Schema, policy Policy) *Validator {
	return &Validator{
		Schema: schema,
		Policy: policy,
	}
}

// Validate validates incoming schema against expected schema.
func (v *Validator) Validate(incoming *arrow.Schema) (*arrow.Schema, error) {
	if v.Schema == nil {
		v.Schema = incoming
		return incoming, nil
	}

	diff := Compare(v.Schema, incoming)

	if len(diff.Changes) > 0 && v.OnSchemaChange != nil {
		v.OnSchemaChange(v.Schema, incoming, diff)
	}

	merged, err := Merge(v.Schema, incoming, v.Policy)
	if err != nil {
		return nil, err
	}

	// Update stored schema
	v.Schema = merged
	return merged, nil
}

// MasterSchema manages the master schema for a dataset.
type MasterSchema struct {
	Schema    *arrow.Schema
	Version   int
	Policy    Policy
	History   []SchemaVersion
	MaxHistory int
}

// SchemaVersion tracks schema history.
type SchemaVersion struct {
	Version int
	Schema  *arrow.Schema
	Changes []Change
}

// NewMasterSchema creates a master schema manager.
func NewMasterSchema(policy Policy) *MasterSchema {
	return &MasterSchema{
		Policy:     policy,
		MaxHistory: 100,
	}
}

// Update updates the master schema with incoming data.
func (m *MasterSchema) Update(incoming *arrow.Schema) (*arrow.Schema, error) {
	if m.Schema == nil {
		m.Schema = incoming
		m.Version = 1
		m.History = append(m.History, SchemaVersion{
			Version: 1,
			Schema:  incoming,
		})
		return incoming, nil
	}

	diff := Compare(m.Schema, incoming)

	if len(diff.Changes) == 0 {
		return m.Schema, nil
	}

	merged, err := Merge(m.Schema, incoming, m.Policy)
	if err != nil {
		return nil, err
	}

	m.Version++
	m.Schema = merged

	// Track history
	if len(m.History) >= m.MaxHistory {
		m.History = m.History[1:]
	}
	m.History = append(m.History, SchemaVersion{
		Version: m.Version,
		Schema:  merged,
		Changes: diff.Changes,
	})

	return merged, nil
}

// GetVersion returns a specific schema version.
func (m *MasterSchema) GetVersion(version int) *arrow.Schema {
	for _, v := range m.History {
		if v.Version == version {
			return v.Schema
		}
	}
	return nil
}
