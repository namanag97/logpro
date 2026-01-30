package core

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestConverter_ConvertCSV(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "convert-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test CSV
	csvPath := filepath.Join(tmpDir, "test.csv")
	csvData := "id,name,value\n1,alice,10.5\n2,bob,20.3\n3,charlie,30.1\n"
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatal(err)
	}

	conv, err := NewConverter()
	if err != nil {
		t.Fatalf("NewConverter failed: %v", err)
	}
	defer conv.Close()

	outputPath := filepath.Join(tmpDir, "test.parquet")
	result, err := conv.Convert(context.Background(), csvPath, ConversionOptions{
		OutputPath: outputPath,
	})
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
	if result.ColumnCount != 3 {
		t.Errorf("Expected 3 columns, got %d", result.ColumnCount)
	}
	if result.OutputSize <= 0 {
		t.Error("Expected positive output size")
	}
	if result.Compression <= 0 {
		t.Error("Expected positive compression ratio")
	}

	// Verify output file exists
	if _, err := os.Stat(outputPath); err != nil {
		t.Errorf("Output file not found: %v", err)
	}
}

func TestConverter_ConvertJSON(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "convert-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create test JSONL
	jsonlPath := filepath.Join(tmpDir, "test.jsonl")
	jsonlData := `{"id":1,"name":"alice","value":10.5}
{"id":2,"name":"bob","value":20.3}
{"id":3,"name":"charlie","value":30.1}
`
	if err := os.WriteFile(jsonlPath, []byte(jsonlData), 0644); err != nil {
		t.Fatal(err)
	}

	conv, err := NewConverter()
	if err != nil {
		t.Fatalf("NewConverter failed: %v", err)
	}
	defer conv.Close()

	outputPath := filepath.Join(tmpDir, "test.parquet")
	result, err := conv.Convert(context.Background(), jsonlPath, ConversionOptions{
		OutputPath: outputPath,
	})
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}

	if result.RowCount != 3 {
		t.Errorf("Expected 3 rows, got %d", result.RowCount)
	}
}

func TestConverter_RoundTrip(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "convert-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create CSV with typed data
	csvPath := filepath.Join(tmpDir, "data.csv")
	csvData := "id,name,amount,active\n1,alice,100.50,true\n2,bob,200.75,false\n"
	if err := os.WriteFile(csvPath, []byte(csvData), 0644); err != nil {
		t.Fatal(err)
	}

	conv, err := NewConverter()
	if err != nil {
		t.Fatal(err)
	}
	defer conv.Close()

	// Step 1: CSV -> Parquet
	parquetPath := filepath.Join(tmpDir, "data.parquet")
	result1, err := conv.Convert(context.Background(), csvPath, ConversionOptions{
		OutputPath:  parquetPath,
		Compression: "snappy",
	})
	if err != nil {
		t.Fatalf("CSV->Parquet failed: %v", err)
	}

	// Step 2: Parquet -> Parquet (re-compression)
	parquet2Path := filepath.Join(tmpDir, "data2.parquet")
	result2, err := conv.Convert(context.Background(), parquetPath, ConversionOptions{
		OutputPath:  parquet2Path,
		Compression: "zstd",
	})
	if err != nil {
		t.Fatalf("Parquet->Parquet failed: %v", err)
	}

	// Verify row count preserved
	if result1.RowCount != result2.RowCount {
		t.Errorf("Row count mismatch: %d vs %d", result1.RowCount, result2.RowCount)
	}
	// Verify column count preserved
	if result1.ColumnCount != result2.ColumnCount {
		t.Errorf("Column count mismatch: %d vs %d", result1.ColumnCount, result2.ColumnCount)
	}
}

func TestConverter_DefaultOptions(t *testing.T) {
	opts := DefaultOptions()
	if opts.Compression != "snappy" {
		t.Errorf("Expected default compression 'snappy', got %q", opts.Compression)
	}
}

func TestDetectFormat(t *testing.T) {
	tests := []struct {
		path     string
		expected string
	}{
		{"data.csv", "csv"},
		{"data.json", "json"},
		{"data.jsonl", "jsonl"},
		{"data.ndjson", "jsonl"},
		{"data.parquet", "parquet"},
		{"data.xlsx", "xlsx"},
		{"data.xes", "xes"},
		{"data.csv.gz", "csv"},
		{"data.unknown", ""},
	}

	for _, tt := range tests {
		got := detectFormat(tt.path)
		if got != tt.expected {
			t.Errorf("detectFormat(%q) = %q, want %q", tt.path, got, tt.expected)
		}
	}
}

func TestGenerateOutputPath(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"/data/test.csv", "/data/test.parquet"},
		{"/data/test.json", "/data/test.parquet"},
		{"/data/test.csv.gz", "/data/test.parquet"},
	}

	for _, tt := range tests {
		got := generateOutputPath(tt.input)
		if got != tt.expected {
			t.Errorf("generateOutputPath(%q) = %q, want %q", tt.input, got, tt.expected)
		}
	}
}

func TestConverter_UnsupportedFormat(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "convert-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Create unsupported file
	badPath := filepath.Join(tmpDir, "test.xyz")
	os.WriteFile(badPath, []byte("data"), 0644)

	conv, err := NewConverter()
	if err != nil {
		t.Fatal(err)
	}
	defer conv.Close()

	_, err = conv.Convert(context.Background(), badPath, DefaultOptions())
	if err == nil {
		t.Error("Expected error for unsupported format")
	}
}

func TestConverter_LargerCSV(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "convert-test-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)

	// Generate CSV with 1000 rows
	csvPath := filepath.Join(tmpDir, "large.csv")
	f, err := os.Create(csvPath)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Fprintln(f, "id,name,value,category")
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(f, "%d,name%d,%.2f,cat%d\n", i, i%50, float64(i)*1.1, i%5)
	}
	f.Close()

	conv, err := NewConverter()
	if err != nil {
		t.Fatal(err)
	}
	defer conv.Close()

	outputPath := filepath.Join(tmpDir, "large.parquet")
	result, err := conv.Convert(context.Background(), csvPath, ConversionOptions{
		OutputPath: outputPath,
	})
	if err != nil {
		t.Fatalf("Convert failed: %v", err)
	}

	if result.RowCount != 1000 {
		t.Errorf("Expected 1000 rows, got %d", result.RowCount)
	}
	if result.ColumnCount != 4 {
		t.Errorf("Expected 4 columns, got %d", result.ColumnCount)
	}
	if result.Throughput <= 0 {
		t.Error("Expected positive throughput")
	}
}
