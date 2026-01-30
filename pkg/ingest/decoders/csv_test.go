package decoders

import (
	"context"
	"testing"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/sources"
)

func TestCSVDecoder_Formats(t *testing.T) {
	d := NewCSVDecoder()
	formats := d.Formats()
	if len(formats) < 2 {
		t.Errorf("Expected at least 2 formats (CSV, TSV), got %d", len(formats))
	}

	has := make(map[core.Format]bool)
	for _, f := range formats {
		has[f] = true
	}
	if !has[core.FormatCSV] {
		t.Error("Expected CSV format")
	}
	if !has[core.FormatTSV] {
		t.Error("Expected TSV format")
	}
}

func TestCSVDecoder_InferSchema_Basic(t *testing.T) {
	csv := "id,name,value\n1,alice,10.5\n2,bob,20.3\n3,charlie,30.1\n"
	src := sources.NewMemorySource("test.csv", []byte(csv), core.FormatCSV)

	d := NewCSVDecoder()
	schema, err := d.InferSchema(context.Background(), src, 100)
	if err != nil {
		t.Fatalf("InferSchema failed: %v", err)
	}

	if schema.NumFields() != 3 {
		t.Errorf("Expected 3 fields, got %d", schema.NumFields())
	}

	// Check field names
	names := make(map[string]bool)
	for i := 0; i < schema.NumFields(); i++ {
		names[schema.Field(i).Name] = true
	}
	for _, expected := range []string{"id", "name", "value"} {
		if !names[expected] {
			t.Errorf("Missing field %q", expected)
		}
	}
}

func TestCSVDecoder_Decode_Basic(t *testing.T) {
	csv := "id,name,value\n1,alice,10.5\n2,bob,20.3\n3,charlie,30.1\n"
	src := sources.NewMemorySource("test.csv", []byte(csv), core.FormatCSV)

	d := NewCSVDecoder()
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
		totalRows += batch.Record.NumRows()
		batch.Record.Release()
	}

	if totalRows != 3 {
		t.Errorf("Expected 3 rows, got %d", totalRows)
	}
}

func TestCSVDecoder_QuotedFields(t *testing.T) {
	csv := `id,name,description
1,"Alice","Hello, World"
2,"Bob","He said ""hi"""
3,"Charlie","Line 1"
`
	src := sources.NewMemorySource("quoted.csv", []byte(csv), core.FormatCSV)

	d := NewCSVDecoder()
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
		totalRows += batch.Record.NumRows()
		batch.Record.Release()
	}

	if totalRows != 3 {
		t.Errorf("Expected 3 rows, got %d", totalRows)
	}
}

func TestCSVDecoder_EmptyFile(t *testing.T) {
	csv := "id,name\n"
	src := sources.NewMemorySource("empty.csv", []byte(csv), core.FormatCSV)

	d := NewCSVDecoder()
	_, err := d.InferSchema(context.Background(), src, 100)
	// Should handle gracefully, either with schema or error
	if err != nil {
		// Acceptable - empty files may return error
		t.Logf("Empty CSV InferSchema: %v (acceptable)", err)
	}
}

func TestCSVDecoder_TSV(t *testing.T) {
	tsv := "id\tname\tvalue\n1\talice\t10.5\n2\tbob\t20.3\n"
	src := sources.NewMemorySource("test.tsv", []byte(tsv), core.FormatTSV)

	d := NewCSVDecoder()
	schema, err := d.InferSchema(context.Background(), src, 100)
	if err != nil {
		t.Fatalf("InferSchema for TSV failed: %v", err)
	}

	if schema.NumFields() != 3 {
		t.Errorf("Expected 3 fields, got %d", schema.NumFields())
	}
}
