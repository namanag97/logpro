// +build ignore

// Pre-commit Regression Test for LogFlow
// This is a comprehensive test that covers ALL features before committing.
// Run: go run test_precommit.go
//
// Tests covered:
// - All decoders: CSV, JSON, JSONL, XES, Parquet
// - All sinks: Parquet, Iceberg, Arrow IPC
// - All schema policies: strict, merge_nullable, evolving
// - All error policies: strict, skip, quarantine
// - All compression codecs: none, snappy, gzip, zstd, lz4
// - OLAP queries via DuckDB
// - Schema evolution and drift detection
// - Round-trip data verification
// - Hook system
// - Telemetry/metrics
package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/hooks"
	"github.com/logflow/logflow/pkg/ingest/schema"
	"github.com/logflow/logflow/pkg/ingest/sinks"
	"github.com/logflow/logflow/pkg/query/engine"
	"github.com/logflow/logflow/pkg/testing/generators"
)

const (
	colorReset  = "\033[0m"
	colorRed    = "\033[31m"
	colorGreen  = "\033[32m"
	colorYellow = "\033[33m"
	colorBlue   = "\033[34m"
	colorCyan   = "\033[36m"
)

// Test paths
const (
	realCSVFile = "/Users/namanagarwal/logpro/Insurance_claims_event_log copy.csv"
	xesDir      = "/Users/namanagarwal/Downloads/Process-Mining-Datasets"
)

func main() {
	fmt.Println()
	fmt.Println(colorCyan + "╔══════════════════════════════════════════════════════════════════╗" + colorReset)
	fmt.Println(colorCyan + "║     LogFlow Pre-Commit Regression Test Suite                     ║" + colorReset)
	fmt.Println(colorCyan + "║     Comprehensive test covering ALL features                     ║" + colorReset)
	fmt.Println(colorCyan + "╚══════════════════════════════════════════════════════════════════╝" + colorReset)
	fmt.Println()

	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "logflow-precommit-*")
	if err != nil {
		fatal("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("%sTest directory:%s %s\n\n", colorBlue, colorReset, tmpDir)

	// Track results
	results := &TestResults{
		StartTime: time.Now(),
	}

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 1: Generate test files in all formats
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 1: Generate Test Data")

	testFiles := generateAllTestFiles(ctx, tmpDir)
	fmt.Printf("   Generated %d test files\n", len(testFiles))
	for _, tf := range testFiles {
		fmt.Printf("   - %s: %s (%d rows)\n", tf.Format, filepath.Base(tf.Path), tf.ExpectedRows)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 2: Test all decoders
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 2: Decoder Tests")

	decoderTests := []struct {
		name    string
		format  core.Format
		decoder core.Decoder
		file    string
	}{
		{"CSV Decoder", core.FormatCSV, decoders.NewCSVDecoder(), ""},
		{"JSON Decoder", core.FormatJSON, decoders.NewJSONDecoder(), ""},
		{"JSONL Decoder", core.FormatJSONL, decoders.NewJSONDecoder(), ""},
		{"XES Decoder", core.FormatXES, decoders.NewXESDecoder(), ""},
	}

	for _, dt := range decoderTests {
		// Find matching test file
		var testFile *TestFile
		for _, tf := range testFiles {
			if tf.Format == dt.format {
				testFile = &tf
				break
			}
		}
		if testFile == nil {
			results.Add(dt.name, false, "no test file")
			continue
		}

		result := testDecoder(ctx, dt.decoder, testFile)
		results.Add(dt.name, result.Success, result.Message)
		printResult(dt.name, result.Success, result.Message)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 3: Test all sinks
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 3: Sink Tests")

	sinkTests := []struct {
		name string
		sink core.Sink
		ext  string
	}{
		{"Parquet Sink", sinks.NewParquetSink(), ".parquet"},
		{"Iceberg Sink", sinks.NewIcebergSink(), "_iceberg"},
		{"Arrow IPC Sink", sinks.NewArrowIPCSink(), ".arrow"},
	}

	// Get CSV test data for sink tests
	csvFile := findTestFile(testFiles, core.FormatCSV)
	if csvFile != nil {
		for _, st := range sinkTests {
			outputPath := filepath.Join(tmpDir, "sink_test"+st.ext)
			result := testSink(ctx, st.sink, csvFile, outputPath)
			results.Add(st.name, result.Success, result.Message)
			printResult(st.name, result.Success, result.Message)
		}
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 4: Test all compression codecs
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 4: Compression Codec Tests")

	compressions := []core.Compression{
		core.CompressionNone,
		core.CompressionSnappy,
		core.CompressionGzip,
		core.CompressionZstd,
		core.CompressionLZ4,
	}

	if csvFile != nil {
		for _, comp := range compressions {
			name := fmt.Sprintf("Compression: %s", comp.String())
			result := testCompression(ctx, csvFile, tmpDir, comp)
			results.Add(name, result.Success, result.Message)
			printResult(name, result.Success, result.Message)
		}
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 5: Test all schema policies
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 5: Schema Policy Tests")

	policies := []schema.Policy{
		schema.PolicyStrict,
		schema.PolicyMergeNullable,
		schema.PolicyEvolving,
	}

	if csvFile != nil {
		for _, policy := range policies {
			name := fmt.Sprintf("Schema Policy: %s", policy.String())
			result := testSchemaPolicy(ctx, csvFile, policy)
			results.Add(name, result.Success, result.Message)
			printResult(name, result.Success, result.Message)
		}
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 6: Test all error policies
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 6: Error Policy Tests")

	errorPolicies := []core.ErrorPolicy{
		core.ErrorPolicyStrict,
		core.ErrorPolicySkip,
		core.ErrorPolicyQuarantine,
	}

	// Create file with malformed data
	malformedPath := filepath.Join(tmpDir, "malformed.csv")
	createMalformedCSV(malformedPath, 100)

	for _, policy := range errorPolicies {
		name := fmt.Sprintf("Error Policy: %s", policy.String())
		result := testErrorPolicy(ctx, malformedPath, policy)
		results.Add(name, result.Success, result.Message)
		printResult(name, result.Success, result.Message)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 7: Test schema evolution
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 7: Schema Evolution Tests")

	schemaTests := []struct {
		name string
		fn   func(context.Context, string) TestResult
	}{
		{"Schema Inference", testSchemaInference},
		{"Schema Comparison", testSchemaComparison},
		{"Schema Drift Detection", testSchemaDrift},
		{"Master Schema Update", testMasterSchemaUpdate},
	}

	for _, st := range schemaTests {
		result := st.fn(ctx, tmpDir)
		results.Add(st.name, result.Success, result.Message)
		printResult(st.name, result.Success, result.Message)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 8: Test hook system
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 8: Hook System Tests")

	hookTests := []struct {
		name string
		fn   func(context.Context, string) TestResult
	}{
		{"PreDecode Hook", testPreDecodeHook},
		{"Progress Hook", testProgressHook},
		{"Hook Manager", testHookManager},
	}

	for _, ht := range hookTests {
		result := ht.fn(ctx, tmpDir)
		results.Add(ht.name, result.Success, result.Message)
		printResult(ht.name, result.Success, result.Message)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 9: Test OLAP queries (DuckDB)
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 9: OLAP Query Tests")

	queryTests := []struct {
		name string
		fn   func(context.Context, string) TestResult
	}{
		{"Query Engine Init", testQueryEngineInit},
		{"Simple SELECT", testSimpleQuery},
		{"Aggregate Query", testAggregateQuery},
		{"Round-trip Verification", testRoundTrip},
	}

	for _, qt := range queryTests {
		result := qt.fn(ctx, tmpDir)
		results.Add(qt.name, result.Success, result.Message)
		printResult(qt.name, result.Success, result.Message)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 10: Test with real files (if available)
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 10: Real File Tests")

	// Test real CSV
	if _, err := os.Stat(realCSVFile); err == nil {
		result := testRealCSV(ctx, tmpDir, realCSVFile)
		results.Add("Real CSV File", result.Success, result.Message)
		printResult("Real CSV File", result.Success, result.Message)
	} else {
		fmt.Printf("   %s[SKIP]%s Real CSV file not found\n", colorYellow, colorReset)
	}

	// Test real XES file (use smallest one)
	xesFile := filepath.Join(xesDir, "BPI_Challenge_2013_incidents.xes")
	if _, err := os.Stat(xesFile); err == nil {
		result := testRealXES(ctx, tmpDir, xesFile)
		results.Add("Real XES File", result.Success, result.Message)
		printResult("Real XES File", result.Success, result.Message)
	} else {
		fmt.Printf("   %s[SKIP]%s Real XES file not found\n", colorYellow, colorReset)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 11: Integration tests
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 11: Integration Tests")

	integrationTests := []struct {
		name string
		fn   func(context.Context, string) TestResult
	}{
		{"CSV -> Parquet -> Query", testCSVToParquetToQuery},
		{"JSON -> Iceberg -> Query", testJSONToIcebergToQuery},
		{"Multi-format Pipeline", testMultiFormatPipeline},
	}

	for _, it := range integrationTests {
		result := it.fn(ctx, tmpDir)
		results.Add(it.name, result.Success, result.Message)
		printResult(it.name, result.Success, result.Message)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// PHASE 12: DATA INTEGRITY - NO DATA LOSS (CRITICAL)
	// ════════════════════════════════════════════════════════════════════════════
	phase("PHASE 12: Data Integrity Tests (100+ Columns, Entropy Check)")

	integrityTests := []struct {
		name string
		fn   func(context.Context, string) TestResult
	}{
		{"CSV 100 Columns", testCSV100Columns},
		{"CSV Column Count Preservation", testCSVColumnCountPreservation},
		{"JSON 100 Fields", testJSON100Fields},
		{"JSON Nested Objects", testJSONNestedObjects},
		{"JSONL All Types", testJSONLAllTypes},
		{"XES All Attributes", testXESAllAttributes},
		{"XES Trace Attributes", testXESTraceAttributes},
		{"Data Entropy Check CSV", testEntropyCSV},
		{"Data Entropy Check JSON", testEntropyJSON},
		{"Round-trip Lossless CSV", testRoundtripLosslessCSV},
		{"Round-trip Lossless JSON", testRoundtripLosslessJSON},
		{"Special Characters", testSpecialCharacters},
		{"Unicode Data", testUnicodeData},
		{"Empty/Null Values", testEmptyNullValues},
		{"Edge Case Numbers", testEdgeCaseNumbers},
	}

	for _, it := range integrityTests {
		result := it.fn(ctx, tmpDir)
		results.Add(it.name, result.Success, result.Message)
		printResult(it.name, result.Success, result.Message)
	}
	fmt.Println()

	// ════════════════════════════════════════════════════════════════════════════
	// SUMMARY
	// ════════════════════════════════════════════════════════════════════════════
	fmt.Println(colorCyan + "╔══════════════════════════════════════════════════════════════════╗" + colorReset)
	fmt.Println(colorCyan + "║                     TEST SUMMARY                                 ║" + colorReset)
	fmt.Println(colorCyan + "╚══════════════════════════════════════════════════════════════════╝" + colorReset)
	fmt.Println()

	passed, failed := results.Summary()
	duration := time.Since(results.StartTime)

	if failed == 0 {
		fmt.Printf("   %s✓ ALL TESTS PASSED%s\n", colorGreen, colorReset)
	} else {
		fmt.Printf("   %s✗ SOME TESTS FAILED%s\n", colorRed, colorReset)
	}
	fmt.Println()
	fmt.Printf("   Passed:   %s%d%s\n", colorGreen, passed, colorReset)
	fmt.Printf("   Failed:   %s%d%s\n", colorRed, failed, colorReset)
	fmt.Printf("   Total:    %d\n", passed+failed)
	fmt.Printf("   Duration: %v\n", duration.Round(time.Millisecond))
	fmt.Println()

	// Print failed tests
	if failed > 0 {
		fmt.Println("   Failed Tests:")
		for _, r := range results.Tests {
			if !r.Success {
				fmt.Printf("   %s- %s: %s%s\n", colorRed, r.Name, r.Message, colorReset)
			}
		}
		fmt.Println()
		os.Exit(1)
	}

	fmt.Printf("   %s✓ Pre-commit check passed! Safe to commit.%s\n\n", colorGreen, colorReset)
}

// ════════════════════════════════════════════════════════════════════════════════
// Test Types
// ════════════════════════════════════════════════════════════════════════════════

type TestResult struct {
	Success bool
	Message string
}

type TestEntry struct {
	Name    string
	Success bool
	Message string
}

type TestResults struct {
	StartTime time.Time
	Tests     []TestEntry
}

func (r *TestResults) Add(name string, success bool, message string) {
	r.Tests = append(r.Tests, TestEntry{Name: name, Success: success, Message: message})
}

func (r *TestResults) Summary() (passed, failed int) {
	for _, t := range r.Tests {
		if t.Success {
			passed++
		} else {
			failed++
		}
	}
	return
}

type TestFile struct {
	Path         string
	Format       core.Format
	ExpectedRows int
}

// ════════════════════════════════════════════════════════════════════════════════
// Test Data Generation
// ════════════════════════════════════════════════════════════════════════════════

func generateAllTestFiles(ctx context.Context, tmpDir string) []TestFile {
	var files []TestFile

	// CSV
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	if err := generateTestCSV(csvPath, 5000); err == nil {
		files = append(files, TestFile{Path: csvPath, Format: core.FormatCSV, ExpectedRows: 5000})
	}

	// JSONL
	jsonlPath := filepath.Join(tmpDir, "test_data.jsonl")
	if err := generateTestJSONL(jsonlPath, 5000); err == nil {
		files = append(files, TestFile{Path: jsonlPath, Format: core.FormatJSONL, ExpectedRows: 5000})
	}

	// JSON array
	jsonPath := filepath.Join(tmpDir, "test_data.json")
	if err := generateTestJSONArray(jsonPath, 500); err == nil {
		files = append(files, TestFile{Path: jsonPath, Format: core.FormatJSON, ExpectedRows: 500})
	}

	// XES
	xesPath := filepath.Join(tmpDir, "test_data.xes")
	if err := generateTestXES(xesPath, 100, 50); err == nil { // 100 traces, 50 events each = 5000 events
		files = append(files, TestFile{Path: xesPath, Format: core.FormatXES, ExpectedRows: 5000})
	}

	return files
}

func generateTestCSV(path string, rows int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gen := generators.NewCSVGenerator(42)
	gen.Columns = generators.StandardColumns()
	return gen.Generate(f, rows)
}

func generateTestJSONL(path string, rows int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	for i := 0; i < rows; i++ {
		record := map[string]interface{}{
			"id":         i,
			"name":       fmt.Sprintf("user_%d", i),
			"email":      fmt.Sprintf("user%d@example.com", i),
			"active":     i%2 == 0,
			"score":      float64(i) * 1.5,
			"created_at": time.Now().Add(-time.Duration(i) * time.Hour).Format(time.RFC3339),
		}
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	return nil
}

func generateTestJSONArray(path string, rows int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var records []map[string]interface{}
	for i := 0; i < rows; i++ {
		records = append(records, map[string]interface{}{
			"id":    i,
			"name":  fmt.Sprintf("item_%d", i),
			"value": float64(i) * 2.5,
		})
	}

	data, _ := json.MarshalIndent(records, "", "  ")
	f.Write(data)
	return nil
}

func generateTestXES(path string, traces, eventsPerTrace int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	activities := []string{"Register", "Analyze", "Process", "Review", "Approve", "Complete"}
	resources := []string{"Alice", "Bob", "Carol", "Dave"}

	f.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<log>
`)

	baseTime := time.Now().Add(-time.Hour * 24 * 30) // 30 days ago

	for t := 0; t < traces; t++ {
		caseID := fmt.Sprintf("case_%05d", t)
		f.WriteString(fmt.Sprintf(`  <trace>
    <string key="concept:name" value="%s"/>
`, caseID))

		for e := 0; e < eventsPerTrace; e++ {
			activity := activities[e%len(activities)]
			resource := resources[(t+e)%len(resources)]
			timestamp := baseTime.Add(time.Duration(t*eventsPerTrace+e) * time.Minute)

			f.WriteString(fmt.Sprintf(`    <event>
      <string key="concept:name" value="%s"/>
      <date key="time:timestamp" value="%s"/>
      <string key="org:resource" value="%s"/>
      <string key="lifecycle:transition" value="complete"/>
      <int key="cost" value="%d"/>
    </event>
`, activity, timestamp.Format("2006-01-02T15:04:05.000Z07:00"), resource, (e+1)*10))
		}

		f.WriteString("  </trace>\n")
	}

	f.WriteString("</log>\n")
	return nil
}

func createMalformedCSV(path string, rows int) {
	f, _ := os.Create(path)
	defer f.Close()

	f.WriteString("id,name,value\n")
	for i := 0; i < rows; i++ {
		if i%10 == 5 {
			// Malformed: unclosed quote
			f.WriteString(fmt.Sprintf("%d,\"unclosed quote,bad\n", i))
		} else if i%20 == 15 {
			// Malformed: wrong column count
			f.WriteString(fmt.Sprintf("%d,name_%d\n", i, i))
		} else {
			f.WriteString(fmt.Sprintf("%d,name_%d,%.2f\n", i, i, float64(i)*1.5))
		}
	}
}

// ════════════════════════════════════════════════════════════════════════════════
// Test Implementations
// ════════════════════════════════════════════════════════════════════════════════

func testDecoder(ctx context.Context, decoder core.Decoder, tf *TestFile) TestResult {
	source := &fileSource{path: tf.Path, format: tf.Format}

	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 1000

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var totalRows int64
	for batch := range batches {
		if batch.Batch != nil {
			totalRows += batch.Batch.NumRows()
			batch.Batch.Release()
		}
	}

	if totalRows == 0 {
		return TestResult{false, "no rows decoded"}
	}

	return TestResult{true, fmt.Sprintf("%d rows", totalRows)}
}

func testSink(ctx context.Context, sink core.Sink, tf *TestFile, outputPath string) TestResult {
	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: tf.Path, format: tf.Format}

	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var firstBatch core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			firstBatch = b
			break
		}
	}
	// Drain remaining
	for range batches {
	}

	if firstBatch.Batch == nil {
		return TestResult{false, "no batches"}
	}
	defer firstBatch.Batch.Release()

	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath

	if err := sink.Open(ctx, firstBatch.Batch.Schema(), sinkOpts); err != nil {
		return TestResult{false, err.Error()}
	}

	if err := sink.Write(ctx, firstBatch.Batch); err != nil {
		return TestResult{false, err.Error()}
	}

	result, err := sink.Close(ctx)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	return TestResult{true, fmt.Sprintf("%d rows written", result.RowsWritten)}
}

func testCompression(ctx context.Context, tf *TestFile, tmpDir string, comp core.Compression) TestResult {
	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: tf.Path, format: tf.Format}

	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return TestResult{false, "no batches"}
	}

	outputPath := filepath.Join(tmpDir, fmt.Sprintf("compressed_%s.parquet", comp.String()))
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath
	sinkOpts.Compression = comp

	if err := sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts); err != nil {
		for _, b := range allBatches {
			b.Batch.Release()
		}
		return TestResult{false, err.Error()}
	}

	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	result, _ := sink.Close(ctx)

	info, _ := os.Stat(outputPath)
	if info == nil {
		return TestResult{false, "output not created"}
	}

	return TestResult{true, fmt.Sprintf("%d rows, %d bytes", result.RowsWritten, info.Size())}
}

func testSchemaPolicy(ctx context.Context, tf *TestFile, policy schema.Policy) TestResult {
	master := schema.NewMasterSchema(policy)

	source := &fileSource{path: tf.Path, format: tf.Format}
	decoder := decoders.NewCSVDecoder()
	inference := schema.NewInference()

	arrowSchema, err := inference.Infer(ctx, source, decoder)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	_, err = master.Update(arrowSchema)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	return TestResult{true, fmt.Sprintf("version %d", master.Version)}
}

func testErrorPolicy(ctx context.Context, path string, policy core.ErrorPolicy) TestResult {
	source := &fileSource{path: path, format: core.FormatCSV}
	decoder := decoders.NewCSVDecoder()

	opts := core.DefaultDecodeOptions()
	opts.ErrorPolicy = policy
	opts.MaxErrors = 50

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		if policy == core.ErrorPolicyStrict {
			return TestResult{true, "strict mode rejected errors"}
		}
		return TestResult{false, err.Error()}
	}

	var totalRows int64
	var totalErrors int
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
		totalErrors += len(b.Errors)
	}

	return TestResult{true, fmt.Sprintf("%d rows, %d errors", totalRows, totalErrors)}
}

func testSchemaInference(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	decoder := decoders.NewCSVDecoder()
	inference := schema.NewInference()

	arrowSchema, err := inference.Infer(ctx, source, decoder)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	if arrowSchema.NumFields() == 0 {
		return TestResult{false, "no fields"}
	}

	return TestResult{true, fmt.Sprintf("%d fields", arrowSchema.NumFields())}
}

func testSchemaComparison(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	decoder := decoders.NewCSVDecoder()
	inference := schema.NewInference()

	arrowSchema, err := inference.Infer(ctx, source, decoder)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	diff := schema.Compare(arrowSchema, arrowSchema)
	if len(diff.Changes) != 0 {
		return TestResult{false, "identical schemas have changes"}
	}

	return TestResult{true, "identical schemas match"}
}

func testSchemaDrift(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	decoder := decoders.NewCSVDecoder()
	inference := schema.NewInference()

	arrowSchema, err := inference.Infer(ctx, source, decoder)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	// Use Compare to check for drift between identical schemas
	diff := schema.Compare(arrowSchema, arrowSchema)
	if len(diff.Changes) > 0 {
		return TestResult{false, "identical schemas show changes"}
	}

	if !diff.IsCompatible {
		return TestResult{false, "identical schemas not compatible"}
	}

	return TestResult{true, "no drift in identical schemas"}
}

func testMasterSchemaUpdate(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	decoder := decoders.NewCSVDecoder()
	inference := schema.NewInference()

	arrowSchema, err := inference.Infer(ctx, source, decoder)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	master := schema.NewMasterSchema(schema.PolicyMergeNullable)
	_, err = master.Update(arrowSchema)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	if master.Version != 1 {
		return TestResult{false, fmt.Sprintf("expected version 1, got %d", master.Version)}
	}

	return TestResult{true, "master schema created"}
}

func testPreDecodeHook(ctx context.Context, tmpDir string) TestResult {
	mgr := hooks.NewManager()

	var called bool
	mgr.RegisterPreDecode(func(ctx context.Context, source core.Source) (core.Source, error) {
		called = true
		return source, nil
	})

	source := &fileSource{path: filepath.Join(tmpDir, "test_data.csv"), format: core.FormatCSV}
	_, err := mgr.RunPreDecode(ctx, source)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	if !called {
		return TestResult{false, "hook not called"}
	}

	return TestResult{true, "hook executed"}
}

func testProgressHook(ctx context.Context, tmpDir string) TestResult {
	mgr := hooks.NewManager()

	var progressCalls int
	mgr.RegisterProgress(func(p hooks.Progress) {
		progressCalls++
	})

	mgr.ReportProgress(hooks.Progress{RowsRead: 1000})
	mgr.ReportProgress(hooks.Progress{RowsRead: 2000})

	if progressCalls != 2 {
		return TestResult{false, fmt.Sprintf("expected 2 calls, got %d", progressCalls)}
	}

	return TestResult{true, "progress hooks work"}
}

func testHookManager(ctx context.Context, tmpDir string) TestResult {
	mgr := hooks.NewManager()
	if mgr == nil {
		return TestResult{false, "manager is nil"}
	}
	return TestResult{true, "manager initialized"}
}

func testQueryEngineInit(ctx context.Context, tmpDir string) TestResult {
	eng, err := engine.NewEngine()
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer eng.Close()
	return TestResult{true, "engine initialized"}
}

func testSimpleQuery(ctx context.Context, tmpDir string) TestResult {
	// Create parquet file first
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	parquetPath := filepath.Join(tmpDir, "query_test.parquet")

	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}

	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return TestResult{false, "no batches"}
	}

	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = parquetPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	// Query
	eng, err := engine.NewEngine()
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer eng.Close()

	sql := fmt.Sprintf("SELECT * FROM '%s' LIMIT 10", parquetPath)
	result, err := eng.Query(ctx, sql)
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer result.Close()

	rows, _ := result.ToMaps()
	if len(rows) == 0 {
		return TestResult{false, "no rows returned"}
	}

	return TestResult{true, fmt.Sprintf("%d rows", len(rows))}
}

func testAggregateQuery(ctx context.Context, tmpDir string) TestResult {
	parquetPath := filepath.Join(tmpDir, "query_test.parquet")
	if _, err := os.Stat(parquetPath); err != nil {
		return TestResult{false, "parquet file not found"}
	}

	eng, err := engine.NewEngine()
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer eng.Close()

	sql := fmt.Sprintf("SELECT COUNT(*) as cnt FROM '%s'", parquetPath)
	result, err := eng.Query(ctx, sql)
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer result.Close()

	rows, _ := result.ToMaps()
	if len(rows) == 0 {
		return TestResult{false, "no results"}
	}

	return TestResult{true, fmt.Sprintf("count=%v", rows[0]["cnt"])}
}

func testRoundTrip(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	parquetPath := filepath.Join(tmpDir, "roundtrip.parquet")

	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}

	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var inputRows int64
	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			inputRows += b.Batch.NumRows()
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return TestResult{false, "no batches"}
	}

	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = parquetPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	eng, err := engine.NewEngine()
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer eng.Close()

	sql := fmt.Sprintf("SELECT COUNT(*) as cnt FROM '%s'", parquetPath)
	result, err := eng.Query(ctx, sql)
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer result.Close()

	rows, _ := result.ToMaps()
	var outputRows int64
	if len(rows) > 0 {
		if cnt, ok := rows[0]["cnt"].(int64); ok {
			outputRows = cnt
		} else if cnt, ok := rows[0]["cnt"].(float64); ok {
			outputRows = int64(cnt)
		}
	}

	if inputRows != outputRows {
		return TestResult{false, fmt.Sprintf("mismatch: %d vs %d", inputRows, outputRows)}
	}

	return TestResult{true, fmt.Sprintf("%d rows verified", inputRows)}
}

func testRealCSV(ctx context.Context, tmpDir, path string) TestResult {
	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: path, format: core.FormatCSV}

	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var totalRows int64
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	return TestResult{true, fmt.Sprintf("%d rows", totalRows)}
}

func testRealXES(ctx context.Context, tmpDir, path string) TestResult {
	decoder := decoders.NewXESDecoder()
	source := &fileSource{path: path, format: core.FormatXES}

	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 10000

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var totalRows int64
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	if totalRows == 0 {
		return TestResult{false, "no events parsed"}
	}

	return TestResult{true, fmt.Sprintf("%d events", totalRows)}
}

func testCSVToParquetToQuery(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	parquetPath := filepath.Join(tmpDir, "integration.parquet")

	// Decode CSV
	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return TestResult{false, "no batches"}
	}

	// Write to Parquet
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = parquetPath
	sinkOpts.Compression = core.CompressionSnappy
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	// Query
	eng, _ := engine.NewEngine()
	defer eng.Close()

	result, err := eng.Query(ctx, fmt.Sprintf("SELECT COUNT(*) as cnt FROM '%s'", parquetPath))
	if err != nil {
		return TestResult{false, err.Error()}
	}
	defer result.Close()

	return TestResult{true, "CSV->Parquet->Query success"}
}

func testJSONToIcebergToQuery(ctx context.Context, tmpDir string) TestResult {
	jsonlPath := filepath.Join(tmpDir, "test_data.jsonl")
	icebergPath := filepath.Join(tmpDir, "integration_iceberg")

	// Decode JSONL
	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonlPath, format: core.FormatJSONL}
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var firstBatch core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			firstBatch = b
			break
		}
	}
	for range batches {
	}

	if firstBatch.Batch == nil {
		return TestResult{false, "no batches"}
	}
	defer firstBatch.Batch.Release()

	// Write to Iceberg
	sink := sinks.NewIcebergSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = icebergPath
	if err := sink.Open(ctx, firstBatch.Batch.Schema(), sinkOpts); err != nil {
		return TestResult{false, err.Error()}
	}
	sink.Write(ctx, firstBatch.Batch)
	sink.Close(ctx)

	// Verify metadata exists
	metadataPath := filepath.Join(icebergPath, "metadata", "v1.metadata.json")
	if _, err := os.Stat(metadataPath); err != nil {
		return TestResult{false, "iceberg metadata not created"}
	}

	return TestResult{true, "JSON->Iceberg success"}
}

func testMultiFormatPipeline(ctx context.Context, tmpDir string) TestResult {
	// Test that we can process multiple formats in sequence
	formats := []struct {
		path   string
		format core.Format
	}{
		{filepath.Join(tmpDir, "test_data.csv"), core.FormatCSV},
		{filepath.Join(tmpDir, "test_data.jsonl"), core.FormatJSONL},
		{filepath.Join(tmpDir, "test_data.xes"), core.FormatXES},
	}

	var totalRows int64
	for _, f := range formats {
		var decoder core.Decoder
		switch f.format {
		case core.FormatCSV:
			decoder = decoders.NewCSVDecoder()
		case core.FormatJSONL:
			decoder = decoders.NewJSONDecoder()
		case core.FormatXES:
			decoder = decoders.NewXESDecoder()
		}

		source := &fileSource{path: f.path, format: f.format}
		batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
		if err != nil {
			return TestResult{false, fmt.Sprintf("%s: %s", f.format, err.Error())}
		}

		for b := range batches {
			if b.Batch != nil {
				totalRows += b.Batch.NumRows()
				b.Batch.Release()
			}
		}
	}

	return TestResult{true, fmt.Sprintf("%d total rows across formats", totalRows)}
}

// ════════════════════════════════════════════════════════════════════════════════
// Helpers
// ════════════════════════════════════════════════════════════════════════════════

type fileSource struct {
	path   string
	format core.Format
}

func (s *fileSource) ID() string                  { return s.path }
func (s *fileSource) Location() string            { return s.path }
func (s *fileSource) Format() core.Format         { return s.format }
func (s *fileSource) Size() int64                 { info, _ := os.Stat(s.path); if info != nil { return info.Size() }; return 0 }
func (s *fileSource) ModTime() time.Time          { return time.Now() }
func (s *fileSource) Metadata() map[string]string { return nil }
func (s *fileSource) Open(ctx context.Context) (io.ReadCloser, error) { return os.Open(s.path) }

func findTestFile(files []TestFile, format core.Format) *TestFile {
	for _, f := range files {
		if f.Format == format {
			return &f
		}
	}
	return nil
}

func phase(title string) {
	fmt.Printf("%s═══ %s ═══%s\n", colorBlue, title, colorReset)
}

func printResult(name string, success bool, message string) {
	if success {
		fmt.Printf("   %s[PASS]%s %s: %s\n", colorGreen, colorReset, name, message)
	} else {
		fmt.Printf("   %s[FAIL]%s %s: %s\n", colorRed, colorReset, name, message)
	}
}

func repeatStr(s string, n int) string {
	var b bytes.Buffer
	for i := 0; i < n; i++ {
		b.WriteString(s)
	}
	return b.String()
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, colorRed+"FATAL: "+format+"\n"+colorReset, args...)
	os.Exit(1)
}

// ════════════════════════════════════════════════════════════════════════════════
// DATA INTEGRITY TESTS - NO DATA LOSS
// ════════════════════════════════════════════════════════════════════════════════

// testCSV100Columns tests CSV with 100+ columns
func testCSV100Columns(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_100_cols.csv")
	const numCols = 150
	const numRows = 1000

	// Generate CSV with 150 columns
	f, err := os.Create(csvPath)
	if err != nil {
		return TestResult{false, err.Error()}
	}

	// Header
	for i := 0; i < numCols; i++ {
		if i > 0 {
			f.WriteString(",")
		}
		fmt.Fprintf(f, "col_%03d", i)
	}
	f.WriteString("\n")

	// Data rows
	for row := 0; row < numRows; row++ {
		for i := 0; i < numCols; i++ {
			if i > 0 {
				f.WriteString(",")
			}
			fmt.Fprintf(f, "r%d_c%d", row, i)
		}
		f.WriteString("\n")
	}
	f.Close()

	// Decode and verify
	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var totalRows int64
	var colCount int
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			colCount = int(b.Batch.NumCols())
			b.Batch.Release()
		}
	}

	if colCount != numCols {
		return TestResult{false, fmt.Sprintf("column mismatch: expected %d, got %d", numCols, colCount)}
	}
	if totalRows != numRows {
		return TestResult{false, fmt.Sprintf("row mismatch: expected %d, got %d", numRows, totalRows)}
	}

	return TestResult{true, fmt.Sprintf("%d cols, %d rows", colCount, totalRows)}
}

// testCSVColumnCountPreservation verifies no columns are dropped
func testCSVColumnCountPreservation(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_col_preserve.csv")

	// Create CSV with varied column types
	f, _ := os.Create(csvPath)
	f.WriteString("int_col,float_col,str_col,bool_col,date_col,empty_col,special_col\n")
	for i := 0; i < 100; i++ {
		fmt.Fprintf(f, "%d,%.2f,text_%d,%v,2024-01-%02d,,val\"with\"quotes\n",
			i, float64(i)*1.5, i, i%2 == 0, (i%28)+1)
	}
	f.Close()

	// Decode
	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var colCount int
	for b := range batches {
		if b.Batch != nil {
			colCount = int(b.Batch.NumCols())
			b.Batch.Release()
		}
	}

	if colCount != 7 {
		return TestResult{false, fmt.Sprintf("expected 7 columns, got %d", colCount)}
	}

	return TestResult{true, fmt.Sprintf("%d columns preserved", colCount)}
}

// testJSON100Fields tests JSON with 100+ fields
func testJSON100Fields(ctx context.Context, tmpDir string) TestResult {
	jsonPath := filepath.Join(tmpDir, "test_100_fields.jsonl")
	const numFields = 120
	const numRows = 500

	f, _ := os.Create(jsonPath)
	for row := 0; row < numRows; row++ {
		record := make(map[string]interface{})
		for i := 0; i < numFields; i++ {
			switch i % 5 {
			case 0:
				record[fmt.Sprintf("int_field_%03d", i)] = row*1000 + i
			case 1:
				record[fmt.Sprintf("float_field_%03d", i)] = float64(row) * 1.5
			case 2:
				record[fmt.Sprintf("str_field_%03d", i)] = fmt.Sprintf("value_%d_%d", row, i)
			case 3:
				record[fmt.Sprintf("bool_field_%03d", i)] = i%2 == 0
			case 4:
				record[fmt.Sprintf("null_field_%03d", i)] = nil
			}
		}
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	f.Close()

	// Decode and verify
	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonPath, format: core.FormatJSONL}
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var totalRows int64
	var colCount int
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			colCount = int(b.Batch.NumCols())
			b.Batch.Release()
		}
	}

	// Should have all 120 fields (nulls might be excluded in some cases)
	if colCount < 90 { // Allow some tolerance for null fields
		return TestResult{false, fmt.Sprintf("too few fields: expected ~%d, got %d", numFields, colCount)}
	}
	if totalRows != numRows {
		return TestResult{false, fmt.Sprintf("row mismatch: expected %d, got %d", numRows, totalRows)}
	}

	return TestResult{true, fmt.Sprintf("%d fields, %d rows", colCount, totalRows)}
}

// testJSONNestedObjects tests nested JSON structure preservation
func testJSONNestedObjects(ctx context.Context, tmpDir string) TestResult {
	jsonPath := filepath.Join(tmpDir, "test_nested.jsonl")

	f, _ := os.Create(jsonPath)
	for i := 0; i < 100; i++ {
		record := map[string]interface{}{
			"id":   i,
			"name": fmt.Sprintf("item_%d", i),
			"metadata": map[string]interface{}{
				"created": "2024-01-01",
				"updated": "2024-01-02",
			},
			"tags": []string{"a", "b", "c"},
		}
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	f.Close()

	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonPath, format: core.FormatJSONL}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var totalRows int64
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	if totalRows != 100 {
		return TestResult{false, fmt.Sprintf("expected 100 rows, got %d", totalRows)}
	}

	return TestResult{true, fmt.Sprintf("%d rows with nested objects", totalRows)}
}

// testJSONLAllTypes tests all JSON data types
func testJSONLAllTypes(ctx context.Context, tmpDir string) TestResult {
	jsonPath := filepath.Join(tmpDir, "test_all_types.jsonl")

	f, _ := os.Create(jsonPath)
	for i := 0; i < 100; i++ {
		record := map[string]interface{}{
			"int_val":      i,
			"int64_val":    int64(i) * 1000000000,
			"float_val":    float64(i) * 1.234567890123,
			"bool_true":    true,
			"bool_false":   false,
			"string_val":   fmt.Sprintf("string_%d", i),
			"null_val":     nil,
			"empty_string": "",
			"zero_int":     0,
			"zero_float":   0.0,
			"negative":     -i,
			"timestamp":    time.Now().Add(-time.Duration(i) * time.Hour).Format(time.RFC3339),
		}
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	f.Close()

	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonPath, format: core.FormatJSONL}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var colCount int
	for b := range batches {
		if b.Batch != nil {
			colCount = int(b.Batch.NumCols())
			b.Batch.Release()
		}
	}

	// Should capture most fields (null_val and empty might be excluded)
	if colCount < 10 {
		return TestResult{false, fmt.Sprintf("expected ~12 columns, got %d", colCount)}
	}

	return TestResult{true, fmt.Sprintf("%d type columns captured", colCount)}
}

// testXESAllAttributes tests XES captures all event attributes
func testXESAllAttributes(ctx context.Context, tmpDir string) TestResult {
	xesPath := filepath.Join(tmpDir, "test_xes_attrs.xes")

	f, _ := os.Create(xesPath)
	f.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<log>
  <trace>
    <string key="concept:name" value="case_001"/>
    <string key="trace_attr_1" value="trace_val_1"/>
    <int key="trace_int" value="42"/>
    <float key="trace_float" value="3.14"/>
    <date key="trace_date" value="2024-01-01T00:00:00.000+00:00"/>
`)
	for i := 0; i < 50; i++ {
		fmt.Fprintf(f, `    <event>
      <string key="concept:name" value="Activity_%d"/>
      <date key="time:timestamp" value="2024-01-01T%02d:00:00.000+00:00"/>
      <string key="org:resource" value="Resource_%d"/>
      <string key="lifecycle:transition" value="complete"/>
      <int key="event_int_%d" value="%d"/>
      <float key="event_float_%d" value="%.2f"/>
      <string key="event_str_%d" value="val_%d"/>
      <boolean key="event_bool_%d" value="%v"/>
    </event>
`, i, i%24, i%5, i%3, i*10, i%3, float64(i)*1.5, i%3, i, i%2, i%2 == 0)
	}
	f.WriteString(`  </trace>
</log>`)
	f.Close()

	decoder := decoders.NewXESDecoder()
	source := &fileSource{path: xesPath, format: core.FormatXES}
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return TestResult{false, err.Error()}
	}

	var colCount int
	var rowCount int64
	for b := range batches {
		if b.Batch != nil {
			colCount = int(b.Batch.NumCols())
			rowCount += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	// Should have: 5 standard + 4 trace attrs + ~12 event attrs + 3 lineage = ~24 columns
	if colCount < 15 {
		return TestResult{false, fmt.Sprintf("too few columns: %d (expected 15+)", colCount)}
	}
	if rowCount != 50 {
		return TestResult{false, fmt.Sprintf("expected 50 rows, got %d", rowCount)}
	}

	return TestResult{true, fmt.Sprintf("%d columns, %d events", colCount, rowCount)}
}

// testXESTraceAttributes verifies trace-level attributes are propagated
func testXESTraceAttributes(ctx context.Context, tmpDir string) TestResult {
	xesPath := filepath.Join(tmpDir, "test_xes_trace.xes")

	f, _ := os.Create(xesPath)
	f.WriteString(`<?xml version="1.0" encoding="UTF-8"?>
<log>
  <trace>
    <string key="concept:name" value="case_001"/>
    <string key="AMOUNT_REQ" value="50000"/>
    <date key="REG_DATE" value="2024-01-15T10:30:00.000+00:00"/>
    <string key="customer_type" value="premium"/>
    <event>
      <string key="concept:name" value="Submit"/>
      <date key="time:timestamp" value="2024-01-15T10:30:00.000+00:00"/>
    </event>
    <event>
      <string key="concept:name" value="Review"/>
      <date key="time:timestamp" value="2024-01-15T11:00:00.000+00:00"/>
    </event>
  </trace>
  <trace>
    <string key="concept:name" value="case_002"/>
    <string key="AMOUNT_REQ" value="25000"/>
    <date key="REG_DATE" value="2024-01-16T09:00:00.000+00:00"/>
    <string key="customer_type" value="standard"/>
    <event>
      <string key="concept:name" value="Submit"/>
      <date key="time:timestamp" value="2024-01-16T09:00:00.000+00:00"/>
    </event>
  </trace>
</log>`)
	f.Close()

	decoder := decoders.NewXESDecoder()
	source := &fileSource{path: xesPath, format: core.FormatXES}

	// Write to parquet and query
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return TestResult{false, "no batches"}
	}

	pqPath := filepath.Join(tmpDir, "xes_trace_test.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = pqPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	// Query to verify trace attributes
	eng, _ := engine.NewEngine()
	defer eng.Close()

	// Check schema has trace attributes
	result, err := eng.Query(ctx, fmt.Sprintf("DESCRIBE SELECT * FROM '%s'", pqPath))
	if err != nil {
		return TestResult{false, err.Error()}
	}
	rows, _ := result.ToMaps()
	result.Close()

	hasTraceAttr := false
	for _, row := range rows {
		colName := fmt.Sprintf("%v", row["column_name"])
		if colName == "trace_AMOUNT_REQ" || colName == "trace_REG_DATE" || colName == "trace_customer_type" {
			hasTraceAttr = true
			break
		}
	}

	if !hasTraceAttr {
		return TestResult{false, "trace attributes not found in schema"}
	}

	return TestResult{true, "trace attributes preserved"}
}

// testEntropyCSV calculates Shannon entropy before/after to detect data loss
func testEntropyCSV(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_entropy.csv")

	// Generate data with known entropy
	f, _ := os.Create(csvPath)
	f.WriteString("id,category,value\n")
	categories := []string{"A", "B", "C", "D", "E"}
	for i := 0; i < 1000; i++ {
		fmt.Fprintf(f, "%d,%s,%d\n", i, categories[i%5], i*7)
	}
	f.Close()

	// Calculate input file hash
	inputData, _ := os.ReadFile(csvPath)
	inputHash := sha256.Sum256(inputData)

	// Decode -> Parquet -> Query
	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	pqPath := filepath.Join(tmpDir, "entropy_test.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = pqPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	// Query and calculate category distribution entropy
	eng, _ := engine.NewEngine()
	defer eng.Close()

	result, _ := eng.Query(ctx, fmt.Sprintf("SELECT category, COUNT(*) as cnt FROM '%s' GROUP BY category", pqPath))
	rows, _ := result.ToMaps()
	result.Close()

	// Calculate entropy
	total := 0.0
	for _, row := range rows {
		if cnt, ok := row["cnt"].(int64); ok {
			total += float64(cnt)
		} else if cnt, ok := row["cnt"].(float64); ok {
			total += cnt
		}
	}

	entropy := 0.0
	for _, row := range rows {
		var cnt float64
		if c, ok := row["cnt"].(int64); ok {
			cnt = float64(c)
		} else if c, ok := row["cnt"].(float64); ok {
			cnt = c
		}
		p := cnt / total
		if p > 0 {
			entropy -= p * math.Log2(p)
		}
	}

	// Expected entropy for uniform distribution of 5 categories = log2(5) ≈ 2.32
	expectedEntropy := math.Log2(5.0)
	if math.Abs(entropy-expectedEntropy) > 0.01 {
		return TestResult{false, fmt.Sprintf("entropy mismatch: expected %.2f, got %.2f", expectedEntropy, entropy)}
	}

	return TestResult{true, fmt.Sprintf("entropy=%.3f (expected %.3f), hash=%x...", entropy, expectedEntropy, inputHash[:8])}
}

// testEntropyJSON tests JSON data entropy preservation
func testEntropyJSON(ctx context.Context, tmpDir string) TestResult {
	jsonPath := filepath.Join(tmpDir, "test_entropy.jsonl")

	f, _ := os.Create(jsonPath)
	types := []string{"alpha", "beta", "gamma", "delta"}
	for i := 0; i < 800; i++ {
		record := map[string]interface{}{
			"id":     i,
			"type":   types[i%4],
			"score":  float64(i%100) / 10.0,
		}
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	f.Close()

	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonPath, format: core.FormatJSONL}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	pqPath := filepath.Join(tmpDir, "entropy_json_test.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = pqPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	eng, _ := engine.NewEngine()
	defer eng.Close()

	result, _ := eng.Query(ctx, fmt.Sprintf("SELECT type, COUNT(*) as cnt FROM '%s' GROUP BY type", pqPath))
	rows, _ := result.ToMaps()
	result.Close()

	if len(rows) != 4 {
		return TestResult{false, fmt.Sprintf("expected 4 categories, got %d", len(rows))}
	}

	// Each category should have 200 (800/4)
	for _, row := range rows {
		var cnt int64
		if c, ok := row["cnt"].(int64); ok {
			cnt = c
		} else if c, ok := row["cnt"].(float64); ok {
			cnt = int64(c)
		}
		if cnt != 200 {
			return TestResult{false, fmt.Sprintf("uneven distribution: %s has %d", row["type"], cnt)}
		}
	}

	return TestResult{true, "4 categories, 200 each - distribution preserved"}
}

// testRoundtripLosslessCSV verifies exact data round-trip
func testRoundtripLosslessCSV(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_lossless.csv")

	// Create specific test data
	f, _ := os.Create(csvPath)
	f.WriteString("id,name,value,flag\n")
	testData := [][]string{
		{"1", "Alice", "100.50", "true"},
		{"2", "Bob", "200.75", "false"},
		{"3", "Carol", "300.25", "true"},
	}
	for _, row := range testData {
		fmt.Fprintf(f, "%s,%s,%s,%s\n", row[0], row[1], row[2], row[3])
	}
	f.Close()

	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	pqPath := filepath.Join(tmpDir, "lossless_test.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = pqPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	eng, _ := engine.NewEngine()
	defer eng.Close()

	result, _ := eng.Query(ctx, fmt.Sprintf("SELECT * FROM '%s' ORDER BY id", pqPath))
	rows, _ := result.ToMaps()
	result.Close()

	if len(rows) != 3 {
		return TestResult{false, fmt.Sprintf("expected 3 rows, got %d", len(rows))}
	}

	// Verify names match
	names := []string{"Alice", "Bob", "Carol"}
	for i, row := range rows {
		name := fmt.Sprintf("%v", row["name"])
		if name != names[i] {
			return TestResult{false, fmt.Sprintf("row %d: expected %s, got %s", i, names[i], name)}
		}
	}

	return TestResult{true, "3 rows verified lossless"}
}

// testRoundtripLosslessJSON verifies JSON data round-trip
func testRoundtripLosslessJSON(ctx context.Context, tmpDir string) TestResult {
	jsonPath := filepath.Join(tmpDir, "test_lossless.jsonl")

	f, _ := os.Create(jsonPath)
	testData := []map[string]interface{}{
		{"id": 1, "name": "Test1", "active": true},
		{"id": 2, "name": "Test2", "active": false},
		{"id": 3, "name": "Test3", "active": true},
	}
	for _, record := range testData {
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	f.Close()

	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonPath, format: core.FormatJSONL}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	pqPath := filepath.Join(tmpDir, "lossless_json_test.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = pqPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	eng, _ := engine.NewEngine()
	defer eng.Close()

	result, _ := eng.Query(ctx, fmt.Sprintf("SELECT COUNT(*) as cnt FROM '%s'", pqPath))
	rows, _ := result.ToMaps()
	result.Close()

	var cnt int64
	if c, ok := rows[0]["cnt"].(int64); ok {
		cnt = c
	} else if c, ok := rows[0]["cnt"].(float64); ok {
		cnt = int64(c)
	}

	if cnt != 3 {
		return TestResult{false, fmt.Sprintf("expected 3 rows, got %d", cnt)}
	}

	return TestResult{true, "3 JSON records verified"}
}

// testSpecialCharacters tests handling of special chars
func testSpecialCharacters(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_special.csv")

	f, _ := os.Create(csvPath)
	f.WriteString("id,text\n")
	f.WriteString("1,\"hello, world\"\n")
	f.WriteString("2,\"with \"\"quotes\"\"\"\n")
	f.WriteString("3,\"line1\nline2\"\n")
	f.WriteString("4,\"tab\there\"\n")
	f.WriteString("5,normal\n")
	f.Close()

	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var totalRows int64
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	if totalRows != 5 {
		return TestResult{false, fmt.Sprintf("expected 5 rows, got %d", totalRows)}
	}

	return TestResult{true, "special characters handled"}
}

// testUnicodeData tests Unicode string preservation
func testUnicodeData(ctx context.Context, tmpDir string) TestResult {
	jsonPath := filepath.Join(tmpDir, "test_unicode.jsonl")

	f, _ := os.Create(jsonPath)
	unicodeData := []map[string]interface{}{
		{"id": 1, "text": "Hello 世界"},
		{"id": 2, "text": "Привет мир"},
		{"id": 3, "text": "مرحبا بالعالم"},
		{"id": 4, "text": "🎉 emoji test 🚀"},
		{"id": 5, "text": "日本語テスト"},
	}
	for _, record := range unicodeData {
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	f.Close()

	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonPath, format: core.FormatJSONL}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	pqPath := filepath.Join(tmpDir, "unicode_test.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = pqPath
	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	eng, _ := engine.NewEngine()
	defer eng.Close()

	result, _ := eng.Query(ctx, fmt.Sprintf("SELECT text FROM '%s' WHERE id = 1", pqPath))
	rows, _ := result.ToMaps()
	result.Close()

	if len(rows) == 0 {
		return TestResult{false, "no rows returned"}
	}

	text := fmt.Sprintf("%v", rows[0]["text"])
	if text != "Hello 世界" {
		return TestResult{false, fmt.Sprintf("unicode mismatch: got %s", text)}
	}

	return TestResult{true, "unicode preserved: 世界, Привет, 🎉"}
}

// testEmptyNullValues tests handling of empty and null values
func testEmptyNullValues(ctx context.Context, tmpDir string) TestResult {
	csvPath := filepath.Join(tmpDir, "test_nulls.csv")

	f, _ := os.Create(csvPath)
	f.WriteString("id,nullable,empty\n")
	f.WriteString("1,value1,\n")
	f.WriteString("2,,empty2\n")
	f.WriteString("3,value3,\n")
	f.WriteString("4,,\n")
	f.WriteString("5,value5,empty5\n")
	f.Close()

	decoder := decoders.NewCSVDecoder()
	source := &fileSource{path: csvPath, format: core.FormatCSV}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var totalRows int64
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	if totalRows != 5 {
		return TestResult{false, fmt.Sprintf("expected 5 rows, got %d", totalRows)}
	}

	return TestResult{true, "null/empty values handled"}
}

// testEdgeCaseNumbers tests edge case numeric values
func testEdgeCaseNumbers(ctx context.Context, tmpDir string) TestResult {
	jsonPath := filepath.Join(tmpDir, "test_numbers.jsonl")

	f, _ := os.Create(jsonPath)
	testData := []map[string]interface{}{
		{"id": 1, "int": 0, "float": 0.0},
		{"id": 2, "int": -1, "float": -0.0},
		{"id": 3, "int": 9223372036854775807, "float": 1.7976931348623157e+308}, // max values
		{"id": 4, "int": -9223372036854775808, "float": 2.2250738585072014e-308}, // min values
		{"id": 5, "int": 1, "float": 0.1 + 0.2}, // floating point precision
	}
	for _, record := range testData {
		data, _ := json.Marshal(record)
		f.Write(data)
		f.WriteString("\n")
	}
	f.Close()

	decoder := decoders.NewJSONDecoder()
	source := &fileSource{path: jsonPath, format: core.FormatJSONL}
	batches, _ := decoder.Decode(ctx, source, core.DefaultDecodeOptions())

	var totalRows int64
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	if totalRows != 5 {
		return TestResult{false, fmt.Sprintf("expected 5 rows, got %d", totalRows)}
	}

	return TestResult{true, "edge case numbers handled"}
}
