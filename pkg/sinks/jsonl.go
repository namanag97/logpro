package sinks

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/logflow/logflow/pkg/pipeline"
	"github.com/logflow/logflow/pkg/registry"
)

func init() {
	registry.RegisterSink("jsonl", JSONLSinkFactory)
}

// JSONLSink writes events as newline-delimited JSON.
type JSONLSink struct {
	cfg  pipeline.Config
	path string
}

// NewJSONLSink creates a JSONL sink.
func NewJSONLSink(cfg pipeline.Config) (*JSONLSink, error) {
	path := cfg.SinkPath
	if path == "" {
		return nil, fmt.Errorf("sink path required for JSONL sink")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("failed to create output directory: %w", err)
	}
	return &JSONLSink{cfg: cfg, path: path}, nil
}

func (s *JSONLSink) Name() string { return "jsonl" }

// jsonEvent is the serialization format for events.
type jsonEvent struct {
	CaseID    string            `json:"case_id"`
	Activity  string            `json:"activity"`
	Timestamp int64             `json:"timestamp"`
	Resource  string            `json:"resource,omitempty"`
	Attrs     map[string]string `json:"attributes,omitempty"`
}

func (s *JSONLSink) Write(ctx context.Context, in <-chan *pipeline.Event) error {
	file, err := os.Create(s.path)
	if err != nil {
		return fmt.Errorf("failed to create JSONL file: %w", err)
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	defer w.Flush()

	enc := json.NewEncoder(w)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}
			je := jsonEvent{
				CaseID:    string(event.CaseID),
				Activity:  string(event.Activity),
				Timestamp: event.Timestamp,
				Resource:  string(event.Resource),
			}
			if len(event.Attributes) > 0 {
				je.Attrs = make(map[string]string, len(event.Attributes))
				for _, attr := range event.Attributes {
					je.Attrs[string(attr.Key)] = string(attr.Value)
				}
			}
			if err := enc.Encode(je); err != nil {
				return fmt.Errorf("failed to encode event: %w", err)
			}
		}
	}
}

func (s *JSONLSink) Close() error { return nil }

func JSONLSinkFactory(cfg pipeline.Config) (pipeline.Sink, error) {
	return NewJSONLSink(cfg)
}
