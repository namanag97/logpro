// Package ocel provides Object-Centric Event Log (OCEL) 2.0 support for process mining.
// Implements the full OCEL 2.0 standard: https://arxiv.org/html/2403.01975v1
//
// Mathematical Model:
//   L = (E, O, EA, OA, evtype, time, objtype, eatype, oatype, eaval, oaval, E2O, O2O)
//
// Where:
//   - E ⊆ 𝕌_ev           : Set of events
//   - O ⊆ 𝕌_obj          : Set of objects
//   - E2O ⊆ E × 𝕌_qual × O : Event-to-Object relations (qualified)
//   - O2O ⊆ O × 𝕌_qual × O : Object-to-Object relations (qualified)
package ocel

import (
	"encoding/json"
	"fmt"
	"time"
)

// --- Core Types (matching OCEL 2.0 spec) ---

// Event represents an event in the OCEL log.
// Maps to: E ⊆ 𝕌_ev with evtype: E → 𝕌_etype and time: E → 𝕌_time
type Event struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`       // Activity/event type
	Timestamp  time.Time              `json:"timestamp"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// Object represents an object in the OCEL log.
// Maps to: O ⊆ 𝕌_obj with objtype: O → 𝕌_otype
type Object struct {
	ID         string                 `json:"id"`
	Type       string                 `json:"type"`
	Attributes map[string]interface{} `json:"attributes,omitempty"`
}

// E2O represents an Event-to-Object relation with qualifier.
// Maps to: E2O ⊆ E × 𝕌_qual × O
type E2O struct {
	EventID   string `json:"event_id"`
	ObjectID  string `json:"object_id"`
	Qualifier string `json:"qualifier"` // e.g., "primary", "resource", "item"
}

// O2O represents an Object-to-Object relation with qualifier.
// Maps to: O2O ⊆ O × 𝕌_qual × O
type O2O struct {
	SourceID  string `json:"source_id"`
	TargetID  string `json:"target_id"`
	Qualifier string `json:"qualifier"` // e.g., "contains", "belongs_to", "part_of"
}

// ObjectAttribute represents a versioned object attribute.
// Supports tracking attribute changes over time.
type ObjectAttribute struct {
	ObjectID  string      `json:"object_id"`
	Name      string      `json:"name"`
	Value     interface{} `json:"value"`
	Type      string      `json:"type"` // "string", "int", "float", "bool", "timestamp"
	Timestamp time.Time   `json:"timestamp"` // When this value became active
}

// EventAttribute represents an event attribute.
type EventAttribute struct {
	EventID string      `json:"event_id"`
	Name    string      `json:"name"`
	Value   interface{} `json:"value"`
	Type    string      `json:"type"`
}

// --- OCEL Log (complete representation) ---

// Log represents a complete OCEL 2.0 log.
// L = (E, O, EA, OA, evtype, time, objtype, eatype, oatype, eaval, oaval, E2O, O2O)
type Log struct {
	// Core sets
	Events  map[string]*Event  `json:"events"`  // E: event_id -> Event
	Objects map[string]*Object `json:"objects"` // O: object_id -> Object

	// Relations
	E2ORelations []E2O `json:"e2o"` // Event-to-Object
	O2ORelations []O2O `json:"o2o"` // Object-to-Object

	// Attributes (versioned for objects)
	EventAttributes  []EventAttribute  `json:"event_attributes,omitempty"`
	ObjectAttributes []ObjectAttribute `json:"object_attributes,omitempty"`

	// Metadata
	Metadata *Metadata `json:"metadata"`
}

// Metadata contains OCEL log metadata.
type Metadata struct {
	Version       string   `json:"version"` // "2.0"
	EventTypes    []string `json:"event_types"`
	ObjectTypes   []string `json:"object_types"`
	AttributeNames struct {
		Event  []string            `json:"event"`
		Object map[string][]string `json:"object"` // object_type -> attribute names
	} `json:"attribute_names"`
	Qualifiers struct {
		E2O []string `json:"e2o"` // Valid E2O qualifiers
		O2O []string `json:"o2o"` // Valid O2O qualifiers
	} `json:"qualifiers"`
	TimeRange struct {
		Min time.Time `json:"min"`
		Max time.Time `json:"max"`
	} `json:"time_range"`
	Statistics struct {
		EventCount  int64            `json:"event_count"`
		ObjectCount int64            `json:"object_count"`
		E2OCount    int64            `json:"e2o_count"`
		O2OCount    int64            `json:"o2o_count"`
		ByType      map[string]int64 `json:"by_type"` // object_type -> count
	} `json:"statistics"`
}

// NewLog creates a new empty OCEL 2.0 log.
func NewLog() *Log {
	return &Log{
		Events:           make(map[string]*Event),
		Objects:          make(map[string]*Object),
		E2ORelations:     make([]E2O, 0),
		O2ORelations:     make([]O2O, 0),
		EventAttributes:  make([]EventAttribute, 0),
		ObjectAttributes: make([]ObjectAttribute, 0),
		Metadata: &Metadata{
			Version:     "2.0",
			EventTypes:  make([]string, 0),
			ObjectTypes: make([]string, 0),
		},
	}
}

// --- Log Operations ---

// AddEvent adds an event to the log.
func (l *Log) AddEvent(event *Event) {
	l.Events[event.ID] = event
	l.updateMetadataForEvent(event)
}

// AddObject adds an object to the log.
func (l *Log) AddObject(object *Object) {
	l.Objects[object.ID] = object
	l.updateMetadataForObject(object)
}

// AddE2O adds an event-to-object relation.
func (l *Log) AddE2O(eventID, objectID, qualifier string) error {
	if _, ok := l.Events[eventID]; !ok {
		return fmt.Errorf("event %s not found", eventID)
	}
	if _, ok := l.Objects[objectID]; !ok {
		return fmt.Errorf("object %s not found", objectID)
	}

	l.E2ORelations = append(l.E2ORelations, E2O{
		EventID:   eventID,
		ObjectID:  objectID,
		Qualifier: qualifier,
	})

	l.addQualifier("e2o", qualifier)
	return nil
}

// AddO2O adds an object-to-object relation.
func (l *Log) AddO2O(sourceID, targetID, qualifier string) error {
	if _, ok := l.Objects[sourceID]; !ok {
		return fmt.Errorf("source object %s not found", sourceID)
	}
	if _, ok := l.Objects[targetID]; !ok {
		return fmt.Errorf("target object %s not found", targetID)
	}

	l.O2ORelations = append(l.O2ORelations, O2O{
		SourceID:  sourceID,
		TargetID:  targetID,
		Qualifier: qualifier,
	})

	l.addQualifier("o2o", qualifier)
	return nil
}

// SetObjectAttribute sets a versioned attribute on an object.
func (l *Log) SetObjectAttribute(objectID, name string, value interface{}, attrType string, timestamp time.Time) error {
	if _, ok := l.Objects[objectID]; !ok {
		return fmt.Errorf("object %s not found", objectID)
	}

	l.ObjectAttributes = append(l.ObjectAttributes, ObjectAttribute{
		ObjectID:  objectID,
		Name:      name,
		Value:     value,
		Type:      attrType,
		Timestamp: timestamp,
	})

	return nil
}

// --- Query Operations ---

// GetObjectsForEvent returns all objects related to an event.
func (l *Log) GetObjectsForEvent(eventID string) []*Object {
	var result []*Object
	for _, e2o := range l.E2ORelations {
		if e2o.EventID == eventID {
			if obj, ok := l.Objects[e2o.ObjectID]; ok {
				result = append(result, obj)
			}
		}
	}
	return result
}

// GetObjectsForEventByQualifier returns objects with a specific qualifier.
func (l *Log) GetObjectsForEventByQualifier(eventID, qualifier string) []*Object {
	var result []*Object
	for _, e2o := range l.E2ORelations {
		if e2o.EventID == eventID && e2o.Qualifier == qualifier {
			if obj, ok := l.Objects[e2o.ObjectID]; ok {
				result = append(result, obj)
			}
		}
	}
	return result
}

// GetEventsForObject returns all events related to an object.
func (l *Log) GetEventsForObject(objectID string) []*Event {
	var result []*Event
	for _, e2o := range l.E2ORelations {
		if e2o.ObjectID == objectID {
			if event, ok := l.Events[e2o.EventID]; ok {
				result = append(result, event)
			}
		}
	}
	return result
}

// GetRelatedObjects returns objects related to a source object via O2O.
func (l *Log) GetRelatedObjects(objectID string) []*Object {
	var result []*Object
	for _, o2o := range l.O2ORelations {
		if o2o.SourceID == objectID {
			if obj, ok := l.Objects[o2o.TargetID]; ok {
				result = append(result, obj)
			}
		}
	}
	return result
}

// GetObjectsByType returns all objects of a specific type.
func (l *Log) GetObjectsByType(objectType string) []*Object {
	var result []*Object
	for _, obj := range l.Objects {
		if obj.Type == objectType {
			result = append(result, obj)
		}
	}
	return result
}

// GetObjectAttributeAtTime returns the value of an object attribute at a specific time.
func (l *Log) GetObjectAttributeAtTime(objectID, attrName string, atTime time.Time) interface{} {
	var latestValue interface{}
	var latestTime time.Time

	for _, attr := range l.ObjectAttributes {
		if attr.ObjectID == objectID && attr.Name == attrName {
			if attr.Timestamp.Before(atTime) || attr.Timestamp.Equal(atTime) {
				if latestTime.IsZero() || attr.Timestamp.After(latestTime) {
					latestValue = attr.Value
					latestTime = attr.Timestamp
				}
			}
		}
	}

	return latestValue
}

// --- Projection (Flattening) ---

// ProjectToCase flattens the log to traditional case-centric format for an object type.
// This creates a "virtual view" where each object becomes a case.
func (l *Log) ProjectToCase(objectType string) []CaseLog {
	// Group events by object
	objectEvents := make(map[string][]*Event)

	for _, e2o := range l.E2ORelations {
		obj, ok := l.Objects[e2o.ObjectID]
		if !ok || obj.Type != objectType {
			continue
		}

		event, ok := l.Events[e2o.EventID]
		if !ok {
			continue
		}

		objectEvents[e2o.ObjectID] = append(objectEvents[e2o.ObjectID], event)
	}

	// Create case logs
	var result []CaseLog
	for objectID, events := range objectEvents {
		result = append(result, CaseLog{
			CaseID:     objectID,
			ObjectType: objectType,
			Events:     events,
		})
	}

	return result
}

// CaseLog represents a flattened case-centric view.
type CaseLog struct {
	CaseID     string
	ObjectType string
	Events     []*Event
}

// --- Metadata Helpers ---

func (l *Log) updateMetadataForEvent(event *Event) {
	// Update event types
	found := false
	for _, t := range l.Metadata.EventTypes {
		if t == event.Type {
			found = true
			break
		}
	}
	if !found {
		l.Metadata.EventTypes = append(l.Metadata.EventTypes, event.Type)
	}

	// Update time range
	if l.Metadata.TimeRange.Min.IsZero() || event.Timestamp.Before(l.Metadata.TimeRange.Min) {
		l.Metadata.TimeRange.Min = event.Timestamp
	}
	if event.Timestamp.After(l.Metadata.TimeRange.Max) {
		l.Metadata.TimeRange.Max = event.Timestamp
	}

	l.Metadata.Statistics.EventCount++
}

func (l *Log) updateMetadataForObject(object *Object) {
	// Update object types
	found := false
	for _, t := range l.Metadata.ObjectTypes {
		if t == object.Type {
			found = true
			break
		}
	}
	if !found {
		l.Metadata.ObjectTypes = append(l.Metadata.ObjectTypes, object.Type)
	}

	l.Metadata.Statistics.ObjectCount++

	if l.Metadata.Statistics.ByType == nil {
		l.Metadata.Statistics.ByType = make(map[string]int64)
	}
	l.Metadata.Statistics.ByType[object.Type]++
}

func (l *Log) addQualifier(relationType, qualifier string) {
	if qualifier == "" {
		return
	}

	var list *[]string
	if relationType == "e2o" {
		list = &l.Metadata.Qualifiers.E2O
	} else {
		list = &l.Metadata.Qualifiers.O2O
	}

	for _, q := range *list {
		if q == qualifier {
			return
		}
	}
	*list = append(*list, qualifier)
}

// --- Serialization ---

// ToJSON serializes the log to JSON (OCEL 2.0 JSON format).
func (l *Log) ToJSON() ([]byte, error) {
	return json.MarshalIndent(l, "", "  ")
}

// FromJSON deserializes a log from JSON.
func FromJSON(data []byte) (*Log, error) {
	var log Log
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, err
	}
	return &log, nil
}

// --- Validation ---

// Validate checks the log for consistency.
func (l *Log) Validate() []string {
	var errors []string

	// Check E2O references
	for _, e2o := range l.E2ORelations {
		if _, ok := l.Events[e2o.EventID]; !ok {
			errors = append(errors, fmt.Sprintf("E2O references non-existent event: %s", e2o.EventID))
		}
		if _, ok := l.Objects[e2o.ObjectID]; !ok {
			errors = append(errors, fmt.Sprintf("E2O references non-existent object: %s", e2o.ObjectID))
		}
	}

	// Check O2O references
	for _, o2o := range l.O2ORelations {
		if _, ok := l.Objects[o2o.SourceID]; !ok {
			errors = append(errors, fmt.Sprintf("O2O references non-existent source: %s", o2o.SourceID))
		}
		if _, ok := l.Objects[o2o.TargetID]; !ok {
			errors = append(errors, fmt.Sprintf("O2O references non-existent target: %s", o2o.TargetID))
		}
	}

	// Check object attributes
	for _, attr := range l.ObjectAttributes {
		if _, ok := l.Objects[attr.ObjectID]; !ok {
			errors = append(errors, fmt.Sprintf("Attribute references non-existent object: %s", attr.ObjectID))
		}
	}

	return errors
}

// --- Statistics ---

// Stats returns statistics about the log.
func (l *Log) Stats() map[string]interface{} {
	return map[string]interface{}{
		"events":        len(l.Events),
		"objects":       len(l.Objects),
		"e2o_relations": len(l.E2ORelations),
		"o2o_relations": len(l.O2ORelations),
		"event_types":   len(l.Metadata.EventTypes),
		"object_types":  len(l.Metadata.ObjectTypes),
		"time_range": map[string]string{
			"min": l.Metadata.TimeRange.Min.Format(time.RFC3339),
			"max": l.Metadata.TimeRange.Max.Format(time.RFC3339),
		},
	}
}
