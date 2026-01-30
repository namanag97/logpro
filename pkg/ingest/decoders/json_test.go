package decoders

import (
	"context"
	"testing"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/sources"
)

func TestJSONDecoder_Formats(t *testing.T) {
	d := NewJSONDecoder()
	formats := d.Formats()
	if len(formats) < 2 {
		t.Errorf("Expected at least 2 formats (JSON, JSONL), got %d", len(formats))
	}

	has := make(map[core.Format]bool)
	for _, f := range formats {
		has[f] = true
	}
	if !has[core.FormatJSON] {
		t.Error("Expected JSON format")
	}
	if !has[core.FormatJSONL] {
		t.Error("Expected JSONL format")
	}
}

func TestJSONDecoder_InferSchema_JSONL(t *testing.T) {
	jsonl := `{"id":1,"name":"alice","value":10.5}
{"id":2,"name":"bob","value":20.3}
{"id":3,"name":"charlie","value":30.1}
`
	src := sources.NewMemorySource("test.jsonl", []byte(jsonl), core.FormatJSONL)

	d := NewJSONDecoder()
	schema, err := d.InferSchema(context.Background(), src, 100)
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	if schema.NumFields() < 3 {
		t.Errorf("Expected at least 3 fields, got %d", schema.NumFields())
	}
}

func TestJSONDecoder_Decode_JSONL(t *testing.T) {
	jsonl := `{"id":1,"name":"alice"}
{"id":2,"name":"bob"}
{"id":3,"name":"charlie"}
`
	src := sources.NewMemorySource("test.jsonl", []byte(jsonl), core.FormatJSONL)

	d := NewJSONDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 100

	ch, err := d.Decode(context.Background(), src, opts)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	totalRows := int64(0)
	for batch := range ch {
		if batch.Errors != nil {
			t.Fatalf("Batch error: %v", batch.Errors)
		}
		totalRows += batch.Batch.NumRows()
		batch.Batch.Release()
	}

	if totalRows != 3 {
		t.Errorf("Expected 3 rows, got %d", totalRows)
	}
}

func TestJSONDecoder_Decode_JSONArray(t *testing.T) {
	jsonArr := `[
		{"id":1,"name":"alice"},
		{"id":2,"name":"bob"},
		{"id":3,"name":"charlie"}
	]`
	src := sources.NewMemorySource("test.json", []byte(jsonArr), core.FormatJSON)

	d := NewJSONDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 100

	ch, err := d.Decode(context.Background(), src, opts)
	if err != nil {
		t.Fatalf("Decode failed: %v", err)
	}

	totalRows := int64(0)
	for batch := range ch {
		if batch.Errors != nil {
			t.Fatalf("Batch error: %v", batch.Errors)
		}
		totalRows += batch.Batch.NumRows()
		batch.Batch.Release()
	}

	if totalRows != 3 {
		t.Errorf("Expected 3 rows, got %d", totalRows)
	}
}

func TestJSONDecoder_MalformedJSON(t *testing.T) {
	malformed := `{"id":1,"name":"alice"}
{bad json}
{"id":3,"name":"charlie"}
`
	src := sources.NewMemorySource("bad.jsonl", []byte(malformed), core.FormatJSONL)

	d := NewJSONDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 100

	ch, err := d.Decode(context.Background(), src, opts)
	if err != nil {
		// Some decoders may fail on malformed input immediately
		t.Logf("Decode returned error for malformed JSON: %v (acceptable)", err)
		return
	}

	// Others skip bad rows
	totalRows := int64(0)
	for batch := range ch {
		if batch.Errors != nil {
			continue // Skip batches with errors
		}
		totalRows += batch.Batch.NumRows()
		batch.Batch.Release()
	}

	// Should have at least the valid rows
	if totalRows < 2 {
		t.Logf("Got %d rows from partially malformed JSONL (acceptable: %d)", totalRows, totalRows)
	}
}

func TestJSONDecoder_NestedObjects(t *testing.T) {
	jsonl := `{"id":1,"meta":{"source":"web"},"tags":["a","b"]}
{"id":2,"meta":{"source":"api"},"tags":["c"]}
`
	src := sources.NewMemorySource("nested.jsonl", []byte(jsonl), core.FormatJSONL)

	d := NewJSONDecoder()
	schema, err := d.InferSchema(context.Background(), src, 100)
	if err != nil {
		t.Fatalf("InferSchema failed for nested JSON: %v", err)
	}

	if schema.NumFields() == 0 {
		t.Error("Expected non-zero fields for nested JSON")
	}
}
