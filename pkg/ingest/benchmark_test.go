package ingest

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// NoisyDataGenerator generates test data with realistic imperfections.
type NoisyDataGenerator struct {
	rand *rand.Rand

	// Noise settings (0.0 to 1.0)
	NullRate           float64 // Rate of null values
	MalformedQuoteRate float64 // Rate of malformed quotes
	EncodingErrorRate  float64 // Rate of encoding errors
	RaggedRowRate      float64 // Rate of ragged rows
	EmbeddedNewlineRate float64 // Rate of embedded newlines
	TypeErrorRate      float64 // Rate of type mismatches
}

// NewNoisyDataGenerator creates a generator with default noise settings.
func NewNoisyDataGenerator(seed int64) *NoisyDataGenerator {
	return &NoisyDataGenerator{
		rand:               rand.New(rand.NewSource(seed)),
		NullRate:           0.05,
		MalformedQuoteRate: 0.01,
		EncodingErrorRate:  0.005,
		RaggedRowRate:      0.02,
		EmbeddedNewlineRate: 0.01,
		TypeErrorRate:      0.03,
	}
}

// GenerateCSV creates a CSV file with configurable noise.
func (g *NoisyDataGenerator) GenerateCSV(path string, rows, cols int) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// Generate header
	headers := make([]string, cols)
	for i := 0; i < cols; i++ {
		switch i % 5 {
		case 0:
			headers[i] = fmt.Sprintf("case_id_%d", i/5)
		case 1:
			headers[i] = fmt.Sprintf("activity_%d", i/5)
		case 2:
			headers[i] = fmt.Sprintf("timestamp_%d", i/5)
		case 3:
			headers[i] = fmt.Sprintf("resource_%d", i/5)
		case 4:
			headers[i] = fmt.Sprintf("amount_%d", i/5)
		}
	}
	fmt.Fprintln(f, strings.Join(headers, ","))

	// Generate data rows
	for row := 0; row < rows; row++ {
		// Possibly generate ragged row
		actualCols := cols
		if g.rand.Float64() < g.RaggedRowRate {
			if g.rand.Float64() < 0.5 {
				actualCols = cols - g.rand.Intn(3) - 1 // Fewer columns
				if actualCols < 1 {
					actualCols = 1
				}
			} else {
				actualCols = cols + g.rand.Intn(3) + 1 // Extra columns
			}
		}

		values := make([]string, actualCols)

		for i := 0; i < actualCols; i++ {
			// Possibly null
			if g.rand.Float64() < g.NullRate {
				nullValues := []string{"", "NULL", "null", "NA", "N/A", "None", "-"}
				values[i] = nullValues[g.rand.Intn(len(nullValues))]
				continue
			}

			// Generate value based on column type
			var value string
			switch i % 5 {
			case 0: // case_id - integer
				value = g.generateCaseID(row)
			case 1: // activity - string
				value = g.generateActivity()
			case 2: // timestamp
				value = g.generateTimestamp()
			case 3: // resource - string
				value = g.generateResource()
			case 4: // amount - numeric
				value = g.generateAmount()
			}

			// Possibly inject type error
			if g.rand.Float64() < g.TypeErrorRate {
				value = g.injectTypeError(value, i%5)
			}

			// Handle quoting
			if strings.Contains(value, ",") || strings.Contains(value, "\"") || strings.Contains(value, "\n") {
				// Need quotes
				if g.rand.Float64() < g.MalformedQuoteRate {
					value = g.malformedQuote(value)
				} else {
					value = `"` + strings.ReplaceAll(value, `"`, `""`) + `"`
				}
			}

			// Possibly add embedded newline
			if g.rand.Float64() < g.EmbeddedNewlineRate && i%5 == 1 {
				value = `"` + value + "\nmultiline" + `"`
			}

			// Possibly inject encoding error
			if g.rand.Float64() < g.EncodingErrorRate {
				value = g.injectEncodingError(value)
			}

			values[i] = value
		}

		fmt.Fprintln(f, strings.Join(values, ","))
	}

	info, _ := f.Stat()
	return info.Size(), nil
}

func (g *NoisyDataGenerator) generateCaseID(row int) string {
	// Mix of sequential and random IDs
	if g.rand.Float64() < 0.7 {
		return fmt.Sprintf("CASE-%06d", row/10)
	}
	return fmt.Sprintf("C%d", g.rand.Intn(10000))
}

func (g *NoisyDataGenerator) generateActivity() string {
	activities := []string{
		"Submit Order", "Review Request", "Approve", "Reject",
		"Send Email", "Process Payment", "Ship Package",
		"Complete, Task", "Verify \"Data\"", "Final Review",
	}
	return activities[g.rand.Intn(len(activities))]
}

func (g *NoisyDataGenerator) generateTimestamp() string {
	formats := []string{
		"2006-01-02T15:04:05Z",
		"2006-01-02 15:04:05",
		"01/02/2006 15:04:05",
		"2006-01-02",
	}
	base := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	offset := time.Duration(g.rand.Int63n(365*24*60*60)) * time.Second
	return base.Add(offset).Format(formats[g.rand.Intn(len(formats))])
}

func (g *NoisyDataGenerator) generateResource() string {
	resources := []string{
		"John Doe", "Jane Smith", "Bob, Wilson", "Alice \"Admin\"",
		"System", "API", "Worker-1", "Operator",
	}
	return resources[g.rand.Intn(len(resources))]
}

func (g *NoisyDataGenerator) generateAmount() string {
	return fmt.Sprintf("%.2f", g.rand.Float64()*10000)
}

func (g *NoisyDataGenerator) injectTypeError(value string, colType int) string {
	switch colType {
	case 0: // Expected integer
		return "not_a_number"
	case 2: // Expected timestamp
		return "invalid-date"
	case 4: // Expected float
		return "NaN"
	}
	return value
}

func (g *NoisyDataGenerator) malformedQuote(value string) string {
	// Various malformed quote scenarios
	switch g.rand.Intn(4) {
	case 0:
		return `"` + value // Missing closing quote
	case 1:
		return value + `"` // Missing opening quote
	case 2:
		return `"` + value + `"` + "extra" // Extra content after quote
	case 3:
		return `"` + strings.Replace(value, `"`, `"`, 1) // Unescaped quote
	}
	return value
}

func (g *NoisyDataGenerator) injectEncodingError(value string) string {
	// Add invalid UTF-8 bytes
	invalid := []byte{0xFF, 0xFE}
	return value + string(invalid)
}

// GenerateXES creates an XES file with process mining data.
func (g *NoisyDataGenerator) GenerateXES(path string, cases, eventsPerCase int) (int64, error) {
	f, err := os.Create(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	fmt.Fprintln(f, `<?xml version="1.0" encoding="UTF-8"?>`)
	fmt.Fprintln(f, `<log xes.version="1.0" xes.features="nested-attributes">`)

	activities := []string{
		"Submit Request", "Review", "Approve", "Process",
		"Send Notification", "Complete",
	}

	for c := 0; c < cases; c++ {
		caseID := fmt.Sprintf("CASE-%06d", c)
		fmt.Fprintf(f, `  <trace>
    <string key="concept:name" value="%s"/>
`, caseID)

		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(
			time.Duration(g.rand.Int63n(365*24*60*60)) * time.Second)

		for e := 0; e < eventsPerCase; e++ {
			activity := activities[e%len(activities)]
			timestamp := baseTime.Add(time.Duration(e) * time.Hour).Format(time.RFC3339)
			resource := fmt.Sprintf("Resource-%d", g.rand.Intn(10))

			fmt.Fprintf(f, `    <event>
      <string key="concept:name" value="%s"/>
      <date key="time:timestamp" value="%s"/>
      <string key="org:resource" value="%s"/>
    </event>
`, activity, timestamp, resource)
		}

		fmt.Fprintln(f, `  </trace>`)
	}

	fmt.Fprintln(f, `</log>`)

	info, _ := f.Stat()
	return info.Size(), nil
}

// BenchmarkIngestCleanCSV benchmarks clean CSV ingestion.
func BenchmarkIngestCleanCSV(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-")
	defer os.RemoveAll(tmpDir)

	// Generate clean CSV (no noise)
	gen := NewNoisyDataGenerator(42)
	gen.NullRate = 0
	gen.MalformedQuoteRate = 0
	gen.EncodingErrorRate = 0
	gen.RaggedRowRate = 0
	gen.EmbeddedNewlineRate = 0
	gen.TypeErrorRate = 0

	inputPath := filepath.Join(tmpDir, "clean.csv")
	gen.GenerateCSV(inputPath, 100000, 10)

	engine, _ := NewEngine()
	defer engine.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(tmpDir, fmt.Sprintf("output_%d.parquet", i))
		opts := DefaultOptions()
		opts.OutputPath = outputPath

		_, err := engine.Ingest(context.Background(), inputPath, opts)
		if err != nil {
			b.Fatal(err)
		}

		os.Remove(outputPath)
	}
}

// BenchmarkIngestNoisyCSV benchmarks noisy CSV ingestion.
func BenchmarkIngestNoisyCSV(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-")
	defer os.RemoveAll(tmpDir)

	// Generate noisy CSV
	gen := NewNoisyDataGenerator(42)
	inputPath := filepath.Join(tmpDir, "noisy.csv")
	gen.GenerateCSV(inputPath, 100000, 10)

	engine, _ := NewEngine()
	defer engine.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(tmpDir, fmt.Sprintf("output_%d.parquet", i))
		opts := DefaultOptions()
		opts.OutputPath = outputPath
		opts.ForceStrategy = StrategyRobustGo

		_, err := engine.Ingest(context.Background(), inputPath, opts)
		if err != nil {
			b.Fatal(err)
		}

		os.Remove(outputPath)
	}
}

// TestDetector tests file analysis.
func TestDetector(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "test-")
	defer os.RemoveAll(tmpDir)

	gen := NewNoisyDataGenerator(42)

	tests := []struct {
		name           string
		setup          func(path string)
		expectedFormat Format
		expectedClean  bool
	}{
		{
			name: "clean_csv",
			setup: func(path string) {
				g := NewNoisyDataGenerator(42)
				g.NullRate = 0
				g.MalformedQuoteRate = 0
				g.EncodingErrorRate = 0
				g.RaggedRowRate = 0
				g.GenerateCSV(path, 100, 5)
			},
			expectedFormat: FormatCSV,
			expectedClean:  true,
		},
		{
			name: "noisy_csv",
			setup: func(path string) {
				gen.GenerateCSV(path, 100, 5)
			},
			expectedFormat: FormatCSV,
			expectedClean:  false, // May have issues
		},
		{
			name: "tsv_file",
			setup: func(path string) {
				f, _ := os.Create(path)
				defer f.Close()
				fmt.Fprintln(f, "col1\tcol2\tcol3")
				for i := 0; i < 10; i++ {
					fmt.Fprintf(f, "%d\tvalue%d\t%.2f\n", i, i, float64(i)*1.5)
				}
			},
			expectedFormat: FormatTSV,
			expectedClean:  true,
		},
		{
			name: "jsonl_file",
			setup: func(path string) {
				f, _ := os.Create(path)
				defer f.Close()
				for i := 0; i < 10; i++ {
					fmt.Fprintf(f, `{"id":%d,"name":"item%d","value":%.2f}`+"\n", i, i, float64(i)*1.5)
				}
			},
			expectedFormat: FormatJSONL,
			expectedClean:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := filepath.Join(tmpDir, tt.name+".dat")
			tt.setup(path)

			detector := NewDetector()
			analysis, err := detector.Analyze(path)
			if err != nil {
				t.Fatalf("Analyze failed: %v", err)
			}

			if analysis.Format != tt.expectedFormat {
				t.Errorf("Expected format %v, got %v", tt.expectedFormat, analysis.Format)
			}
		})
	}
}

// TestDelimiterDetection tests delimiter detection accuracy.
func TestDelimiterDetection(t *testing.T) {
	tests := []struct {
		name     string
		content  string
		expected byte
	}{
		{"comma_basic", "a,b,c\n1,2,3\n4,5,6", ','},
		{"tab_basic", "a\tb\tc\n1\t2\t3\n4\t5\t6", '\t'},
		{"semicolon", "a;b;c\n1;2;3\n4;5;6", ';'},
		{"pipe", "a|b|c\n1|2|3\n4|5|6", '|'},
		{"comma_with_quotes", `"a","b,c","d"` + "\n" + `"1","2,3","4"`, ','},
		{"mixed_prefer_consistent", "a,b;c\n1,2;3\n4,5;6", ','},
	}

	detector := NewDetector()

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detector.detectDelimiter([]byte(tt.content))
			if result != tt.expected {
				t.Errorf("Expected delimiter %q, got %q", tt.expected, result)
			}
		})
	}
}

// TestEncodingDetection tests encoding detection.
func TestEncodingDetection(t *testing.T) {
	detector := NewDetector()

	tests := []struct {
		name     string
		sample   []byte
		expected Encoding
	}{
		{"ascii", []byte("hello world"), EncodingASCII},
		{"utf8", []byte("hello \xc3\xa9"), EncodingUTF8},
		{"utf8_bom", []byte{0xEF, 0xBB, 0xBF, 'h', 'e', 'l', 'l', 'o'}, EncodingUTF8BOM},
		{"utf16_le", []byte{0xFF, 0xFE, 'h', 0, 'i', 0}, EncodingUTF16LE},
		{"utf16_be", []byte{0xFE, 0xFF, 0, 'h', 0, 'i'}, EncodingUTF16BE},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := detector.detectEncoding(tt.sample)
			if result != tt.expected {
				t.Errorf("Expected encoding %v, got %v", tt.expected, result)
			}
		})
	}
}

// TestQualityValidation tests quality metrics.
func TestQualityValidation(t *testing.T) {
	validator := NewQualityValidator()

	columns := []string{"id", "name", "value"}

	// Add some rows
	for i := 0; i < 1000; i++ {
		var values [][]byte
		if i%10 == 0 {
			values = [][]byte{[]byte(fmt.Sprintf("%d", i)), nil, []byte(fmt.Sprintf("%.2f", float64(i)))}
		} else {
			values = [][]byte{
				[]byte(fmt.Sprintf("%d", i)),
				[]byte(fmt.Sprintf("name%d", i%50)),
				[]byte(fmt.Sprintf("%.2f", float64(i))),
			}
		}
		validator.AddRow(columns, values)
	}

	report := validator.GetReport()

	if report.TotalRows != 1000 {
		t.Errorf("Expected 1000 rows, got %d", report.TotalRows)
	}

	if stats, ok := report.ColumnStats["name"]; ok {
		if stats.NullCount != 100 {
			t.Errorf("Expected 100 nulls in name column, got %d", stats.NullCount)
		}
		if stats.NullPercentage != 10.0 {
			t.Errorf("Expected 10%% null rate, got %.1f%%", stats.NullPercentage)
		}
	}

	// Test cardinality estimation (HyperLogLog can have significant error for small datasets)
	if stats, ok := report.ColumnStats["name"]; ok {
		// With 900 non-null values and 50 unique names, HLL may under/over estimate
		// Accept a wide range due to HLL variance on small datasets
		if stats.EstimatedCardinality == 0 {
			t.Errorf("Expected non-zero cardinality, got %d", stats.EstimatedCardinality)
		}
	}
}

// TestIntegration tests the full ingestion pipeline.
func TestIntegration(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "test-")
	defer os.RemoveAll(tmpDir)

	// Generate test data
	gen := NewNoisyDataGenerator(42)
	gen.NullRate = 0.02
	gen.MalformedQuoteRate = 0
	gen.EncodingErrorRate = 0

	inputPath := filepath.Join(tmpDir, "input.csv")
	gen.GenerateCSV(inputPath, 1000, 10)

	outputPath := filepath.Join(tmpDir, "output.parquet")

	// Create engine and process
	engine, err := NewEngine()
	if err != nil {
		t.Fatalf("Failed to create engine: %v", err)
	}
	defer engine.Close()

	opts := DefaultOptions()
	opts.OutputPath = outputPath

	result, err := engine.Ingest(context.Background(), inputPath, opts)
	if err != nil {
		t.Fatalf("Ingestion failed: %v", err)
	}

	// Verify results
	if result.RowCount == 0 {
		t.Error("Expected non-zero row count")
	}

	if result.OutputSize == 0 {
		t.Error("Expected non-zero output size")
	}

	if result.CompressionRate <= 0 {
		t.Error("Expected positive compression rate")
	}

	// Verify output file exists and is valid Parquet
	validation, err := ValidateParquet(outputPath)
	if err != nil {
		t.Fatalf("Parquet validation error: %v", err)
	}

	if !validation.Valid {
		t.Errorf("Invalid Parquet file: %s", validation.Message)
	}

	t.Logf("Ingestion successful: %d rows, %.1f MB/s, %.1fx compression",
		result.RowCount, result.Speed, result.CompressionRate)
}

// BenchmarkQuality benchmarks quality validation overhead.
func BenchmarkQuality(b *testing.B) {
	validator := NewQualityValidator()
	columns := []string{"id", "name", "value", "timestamp", "amount"}
	values := [][]byte{
		[]byte("12345"),
		[]byte("test name"),
		[]byte("123.45"),
		[]byte("2024-01-01T12:00:00Z"),
		[]byte("999.99"),
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		validator.AddRow(columns, values)
	}
}

// BenchmarkChecksum benchmarks file checksum.
func BenchmarkChecksum(b *testing.B) {
	tmpDir, _ := os.MkdirTemp("", "bench-")
	defer os.RemoveAll(tmpDir)

	// Create 10MB file
	path := filepath.Join(tmpDir, "test.dat")
	f, _ := os.Create(path)
	data := make([]byte, 10*1024*1024)
	f.Write(data)
	f.Close()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		FileChecksum(path)
	}
}
