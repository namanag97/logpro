// Package model defines core data structures for LogFlow.
package model

// Event represents a single process mining event.
// All fields use primitive types optimized for Arrow columnar storage.
// Timestamps are stored as int64 nanoseconds since Unix epoch.
type Event struct {
	// CaseID identifies the process instance (trace).
	CaseID []byte

	// Activity is the event name/activity label.
	Activity []byte

	// Timestamp in nanoseconds since Unix epoch.
	Timestamp int64

	// Resource is the actor/resource performing the activity.
	Resource []byte

	// Attributes holds additional key-value pairs.
	// Keys and values are stored as byte slices to avoid allocations.
	Attributes []Attribute
}

// Attribute represents a key-value pair for event metadata.
type Attribute struct {
	Key   []byte
	Value []byte
	Type  AttrType
}

// AttrType indicates the semantic type of an attribute value.
type AttrType uint8

const (
	AttrTypeString AttrType = iota
	AttrTypeInt
	AttrTypeFloat
	AttrTypeBool
	AttrTypeTimestamp
)

// Reset clears the event for reuse from a pool.
func (e *Event) Reset() {
	e.CaseID = e.CaseID[:0]
	e.Activity = e.Activity[:0]
	e.Timestamp = 0
	e.Resource = e.Resource[:0]
	e.Attributes = e.Attributes[:0]
}

// EventBatch holds a slice of events for batch processing.
type EventBatch struct {
	Events []Event
	Size   int
}

// Reset clears the batch for reuse.
func (b *EventBatch) Reset() {
	b.Size = 0
	for i := range b.Events {
		b.Events[i].Reset()
	}
}
