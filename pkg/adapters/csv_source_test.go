package adapters

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/logflow/logflow/pkg/pipeline"
)

func TestCSVSource_BasicParsing(t *testing.T) {
	cfg := pipeline.DefaultConfig()
	cfg.CaseIDColumn = "case_id"
	cfg.ActivityColumn = "activity"
	cfg.TimestampColumn = "timestamp"

	source, err := NewCSVSource(cfg)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}

	csv := `case_id,activity,timestamp,resource
1,Start,2024-01-01 10:00:00,Alice
1,Process,2024-01-01 10:05:00,Bob
2,Start,2024-01-01 11:00:00,Charlie`

	ctx := context.Background()
	out := make(chan *pipeline.Event, 10)

	go func() {
		err := source.Read(ctx, strings.NewReader(csv), out)
		if err != nil {
			t.Errorf("Read error: %v", err)
		}
		close(out)
	}()

	var events []*pipeline.Event
	for event := range out {
		events = append(events, event)
	}

	if len(events) != 3 {
		t.Errorf("Expected 3 events, got %d", len(events))
	}

	// Verify first event
	if string(events[0].CaseID) != "1" {
		t.Errorf("Expected CaseID '1', got %q", events[0].CaseID)
	}
	if string(events[0].Activity) != "Start" {
		t.Errorf("Expected Activity 'Start', got %q", events[0].Activity)
	}
}

func TestCSVSource_ErrorPolicyStrict(t *testing.T) {
	cfg := pipeline.DefaultConfig()
	cfg.CaseIDColumn = "case_id"
	cfg.ActivityColumn = "activity"
	cfg.TimestampColumn = "timestamp"
	cfg.ErrorPolicy = pipeline.ErrorPolicyStrict

	source, err := NewCSVSource(cfg)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}

	// CSV with ragged row (wrong column count)
	csv := `case_id,activity,timestamp
1,Start,2024-01-01 10:00:00
2,Process
3,End,2024-01-01 11:00:00`

	ctx := context.Background()
	out := make(chan *pipeline.Event, 10)

	errChan := make(chan error, 1)
	go func() {
		err := source.Read(ctx, strings.NewReader(csv), out)
		errChan <- err
		close(out)
	}()

	// Drain events
	for range out {
	}

	err = <-errChan
	if err == nil {
		t.Error("Expected error with strict policy on malformed row")
	}
}

func TestCSVSource_ErrorPolicySkip(t *testing.T) {
	cfg := pipeline.DefaultConfig()
	cfg.CaseIDColumn = "case_id"
	cfg.ActivityColumn = "activity"
	cfg.TimestampColumn = "timestamp"
	cfg.ErrorPolicy = pipeline.ErrorPolicySkip

	var skippedRows []int64
	cfg.OnSkip = func(row int64, reason string) {
		skippedRows = append(skippedRows, row)
	}

	source, err := NewCSVSource(cfg)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}

	// CSV with ragged row
	csv := `case_id,activity,timestamp
1,Start,2024-01-01 10:00:00
2,Process
3,End,2024-01-01 11:00:00`

	ctx := context.Background()
	out := make(chan *pipeline.Event, 10)

	go func() {
		source.Read(ctx, strings.NewReader(csv), out)
		close(out)
	}()

	var events []*pipeline.Event
	for event := range out {
		events = append(events, event)
	}

	// Should have skipped the bad row and processed the others
	if len(events) != 2 {
		t.Errorf("Expected 2 events (skipping bad row), got %d", len(events))
	}

	if len(skippedRows) != 1 {
		t.Errorf("Expected 1 skipped row, got %d", len(skippedRows))
	}

	stats := source.ErrorStats()
	if stats.SkippedCount != 1 {
		t.Errorf("Expected SkippedCount=1, got %d", stats.SkippedCount)
	}
}

func TestCSVSource_QuotedFields(t *testing.T) {
	cfg := pipeline.DefaultConfig()
	cfg.CaseIDColumn = "case_id"
	cfg.ActivityColumn = "activity"
	cfg.TimestampColumn = "timestamp"

	source, err := NewCSVSource(cfg)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}

	csv := `case_id,activity,timestamp,notes
1,"Start, Begin",2024-01-01 10:00:00,"A ""quoted"" note"`

	ctx := context.Background()
	out := make(chan *pipeline.Event, 10)

	go func() {
		source.Read(ctx, strings.NewReader(csv), out)
		close(out)
	}()

	var events []*pipeline.Event
	for event := range out {
		events = append(events, event)
	}

	if len(events) != 1 {
		t.Fatalf("Expected 1 event, got %d", len(events))
	}

	// Check that comma in quoted field is preserved
	if string(events[0].Activity) != "Start, Begin" {
		t.Errorf("Expected Activity 'Start, Begin', got %q", events[0].Activity)
	}
}

func TestCSVSource_ProgressCallback(t *testing.T) {
	cfg := pipeline.DefaultConfig()
	cfg.CaseIDColumn = "case_id"
	cfg.ActivityColumn = "activity"
	cfg.TimestampColumn = "timestamp"

	var progressCalls int
	cfg.OnProgress = func(rows, bytes int64) {
		progressCalls++
	}

	source, err := NewCSVSource(cfg)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}

	// Generate enough rows to trigger progress callback (every 1000 rows)
	var sb strings.Builder
	sb.WriteString("case_id,activity,timestamp\n")
	for i := 0; i < 2500; i++ {
		sb.WriteString("1,Activity,2024-01-01 10:00:00\n")
	}

	ctx := context.Background()
	out := make(chan *pipeline.Event, 100)

	go func() {
		source.Read(ctx, strings.NewReader(sb.String()), out)
		close(out)
	}()

	for range out {
	}

	// Should have been called at least twice (at 1000 and 2000 rows)
	if progressCalls < 2 {
		t.Errorf("Expected at least 2 progress calls, got %d", progressCalls)
	}
}

func TestCSVSource_MissingColumns(t *testing.T) {
	cfg := pipeline.DefaultConfig()
	cfg.CaseIDColumn = "case_id"
	cfg.ActivityColumn = "activity"
	cfg.TimestampColumn = "timestamp"

	source, err := NewCSVSource(cfg)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}

	// CSV missing required timestamp column
	csv := `case_id,activity
1,Start`

	ctx := context.Background()
	out := make(chan *pipeline.Event, 10)

	errChan := make(chan error, 1)
	go func() {
		errChan <- source.Read(ctx, strings.NewReader(csv), out)
		close(out)
	}()

	for range out {
	}

	err = <-errChan
	if err == nil {
		t.Error("Expected error for missing required columns")
	}

	colErr, ok := err.(*ColumnError)
	if !ok {
		t.Errorf("Expected ColumnError, got %T", err)
	} else if len(colErr.Missing) == 0 {
		t.Error("Expected missing columns to be listed")
	}
}

func TestCSVSource_ContextCancellation(t *testing.T) {
	cfg := pipeline.DefaultConfig()
	cfg.CaseIDColumn = "case_id"
	cfg.ActivityColumn = "activity"
	cfg.TimestampColumn = "timestamp"

	source, err := NewCSVSource(cfg)
	if err != nil {
		t.Fatalf("Failed to create CSV source: %v", err)
	}

	// Generate a lot of data
	var sb strings.Builder
	sb.WriteString("case_id,activity,timestamp\n")
	for i := 0; i < 10000; i++ {
		sb.WriteString("1,Activity,2024-01-01 10:00:00\n")
	}

	ctx, cancel := context.WithCancel(context.Background())
	out := make(chan *pipeline.Event, 10)

	errChan := make(chan error, 1)
	go func() {
		errChan <- source.Read(ctx, strings.NewReader(sb.String()), out)
		close(out)
	}()

	// Read a few events then cancel
	count := 0
	for range out {
		count++
		if count == 100 {
			cancel()
			break
		}
	}

	// Wait a bit for cancellation to propagate
	time.Sleep(10 * time.Millisecond)

	err = <-errChan
	if err != context.Canceled {
		t.Errorf("Expected context.Canceled, got %v", err)
	}
}
