package ocel

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

// Parser parses OCEL 2.0 JSON files.
type Parser struct{}

// NewParser creates a new OCEL parser.
func NewParser() *Parser {
	return &Parser{}
}

// Parse parses an OCEL 2.0 JSON file and streams events.
func (p *Parser) Parse(ctx context.Context, r io.Reader, out chan<- *Event) error {
	decoder := json.NewDecoder(r)

	// Read opening brace
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("failed to read JSON: %w", err)
	}
	if delim, ok := token.(json.Delim); !ok || delim != '{' {
		return fmt.Errorf("expected object, got %v", token)
	}

	// Parse top-level fields
	for decoder.More() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// Read key
		token, err := decoder.Token()
		if err != nil {
			return err
		}
		key, ok := token.(string)
		if !ok {
			continue
		}

		switch key {
		case "events":
			if err := p.parseEvents(ctx, decoder, out); err != nil {
				return err
			}
		default:
			// Skip other fields (objects, objectTypes, etc.)
			var skip interface{}
			decoder.Decode(&skip)
		}
	}

	return nil
}

// parseEvents parses the events array.
func (p *Parser) parseEvents(ctx context.Context, decoder *json.Decoder, out chan<- *Event) error {
	// Read array start
	token, err := decoder.Token()
	if err != nil {
		return err
	}
	if delim, ok := token.(json.Delim); !ok || delim != '[' {
		return fmt.Errorf("expected array, got %v", token)
	}

	for decoder.More() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		var rawEvent struct {
			ID         string                 `json:"id"`
			Activity   string                 `json:"activity"`
			Timestamp  string                 `json:"timestamp"`
			Objects    []RawObjectRef         `json:"omap"`
			Attributes map[string]interface{} `json:"vmap"`
		}

		if err := decoder.Decode(&rawEvent); err != nil {
			return fmt.Errorf("failed to decode event: %w", err)
		}

		// Convert to Event
		event := &Event{
			EventID:    rawEvent.ID,
			Activity:   rawEvent.Activity,
			Attributes: rawEvent.Attributes,
		}

		// Parse timestamp
		if ts, err := time.Parse(time.RFC3339, rawEvent.Timestamp); err == nil {
			event.Timestamp = ts.UnixNano()
		} else if ts, err := time.Parse("2006-01-02T15:04:05", rawEvent.Timestamp); err == nil {
			event.Timestamp = ts.UnixNano()
		}

		// Convert object references
		for _, ref := range rawEvent.Objects {
			event.Objects = append(event.Objects, ObjectRef{
				ObjectID:   ref.ObjectID,
				ObjectType: ref.Qualifier,
			})
		}

		select {
		case out <- event:
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	return nil
}

// RawObjectRef is the OCEL 2.0 object reference format.
type RawObjectRef struct {
	ObjectID  string `json:"oid"`
	Qualifier string `json:"qualifier"`
}

// ParseFile parses an OCEL JSON file.
func (p *Parser) ParseFile(ctx context.Context, path string, out chan<- *Event) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	return p.Parse(ctx, file, out)
}

// LoadLog loads a complete OCEL log from a JSON file.
func LoadLog(path string) (*Log, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var log Log
	if err := json.Unmarshal(data, &log); err != nil {
		return nil, err
	}

	return &log, nil
}
