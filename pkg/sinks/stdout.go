package sinks

import (
	"context"
	"encoding/json"
	"fmt"
	"os"

	"github.com/logflow/logflow/pkg/pipeline"
	"github.com/logflow/logflow/pkg/registry"
)

func init() {
	registry.RegisterSink("stdout", StdoutSinkFactory)
}

// StdoutSink writes events to standard output for debugging/piping.
type StdoutSink struct {
	format string // "json" or "text"
}

// NewStdoutSink creates a stdout sink.
func NewStdoutSink(cfg pipeline.Config) (*StdoutSink, error) {
	format := "text"
	if f, ok := cfg.ProcessorOptions["stdout_format"].(string); ok {
		format = f
	}
	return &StdoutSink{format: format}, nil
}

func (s *StdoutSink) Name() string { return "stdout" }

func (s *StdoutSink) Write(ctx context.Context, in <-chan *pipeline.Event) error {
	enc := json.NewEncoder(os.Stdout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}
			if s.format == "json" {
				enc.Encode(jsonEvent{
					CaseID:    string(event.CaseID),
					Activity:  string(event.Activity),
					Timestamp: event.Timestamp,
					Resource:  string(event.Resource),
				})
			} else {
				fmt.Fprintf(os.Stdout, "%s\t%s\t%d\t%s\n",
					event.CaseID, event.Activity, event.Timestamp, event.Resource)
			}
		}
	}
}

func (s *StdoutSink) Close() error { return nil }

func StdoutSinkFactory(cfg pipeline.Config) (pipeline.Sink, error) {
	return NewStdoutSink(cfg)
}
