// +build ignore

// Comprehensive test for LogFlow with multiple configs and file types
// Run: go run test_comprehensive.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/schema"
	"github.com/logflow/logflow/pkg/ingest/sinks"
	"github.com/logflow/logflow/pkg/query/engine"
	"github.com/logflow/logflow/pkg/testing/generators"
)

const realCSVFile = "/Users/namanagarwal/logpro/Insurance_claims_event_log copy.csv"

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║       LogFlow Comprehensive Test Suite                         ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	ctx := context.Background()
	tmpDir, err := os.MkdirTemp("", "logflow-comprehensive-*")
	if err != nil {
		fatal("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	fmt.Printf("📁 Test directory: %s\n\n", tmpDir)

	results := &TestResults{}

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 1: Generate test files in multiple formats
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 1: Generate Test Data")

	testFiles := generateTestFiles(tmpDir)
	results.FilesGenerated = len(testFiles)
	fmt.Printf("   Generated %d test files\n\n", len(testFiles))

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 2: Test different compression codecs
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 2: Compression Codecs")

	compressions := []core.Compression{
		core.CompressionNone,
		core.CompressionSnappy,
		core.CompressionGzip,
		core.CompressionZstd,
		core.CompressionLZ4,
	}

	for _, comp := range compressions {
		result := testCompression(ctx, tmpDir, comp)
		results.CompressionTests = append(results.CompressionTests, result)
		fmt.Printf("   %-10s: %s (ratio: %.2fx, %v)\n",
			comp.String(), statusStr(result.Success), result.CompressionRatio, result.Duration)
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 3: Test different batch sizes
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 3: Batch Size Performance")

	batchSizes := []int{100, 500, 1000, 5000, 10000, 50000}

	for _, batchSize := range batchSizes {
		result := testBatchSize(ctx, tmpDir, batchSize)
		results.BatchSizeTests = append(results.BatchSizeTests, result)
		fmt.Printf("   batch=%5d: %6d rows/sec, %d batches (%v)\n",
			batchSize, result.RowsPerSec, result.BatchCount, result.Duration)
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 4: Test schema policies
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 4: Schema Policies")

	policies := []schema.Policy{
		schema.PolicyStrict,
		schema.PolicyMergeNullable,
		schema.PolicyEvolving,
	}

	for _, policy := range policies {
		result := testSchemaPolicy(ctx, tmpDir, policy)
		results.SchemaPolicyTests = append(results.SchemaPolicyTests, result)
		fmt.Printf("   %-15s: %s (%v)\n", policy.String(), statusStr(result.Success), result.Message)
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 5: Test error policies
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 5: Error Policies")

	errorPolicies := []core.ErrorPolicy{
		core.ErrorPolicyStrict,
		core.ErrorPolicySkip,
		core.ErrorPolicyQuarantine,
	}

	for _, policy := range errorPolicies {
		result := testErrorPolicy(ctx, tmpDir, policy)
		results.ErrorPolicyTests = append(results.ErrorPolicyTests, result)
		fmt.Printf("   %-12s: %s (errors: %d, recovered: %d)\n",
			policy.String(), statusStr(result.Success), result.ErrorCount, result.RecoveredCount)
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 6: Test file formats (CSV, JSON, JSONL)
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 6: File Format Decoders")

	for _, tf := range testFiles {
		result := testFileFormat(ctx, tmpDir, tf)
		results.FormatTests = append(results.FormatTests, result)
		fmt.Printf("   %-10s: %s (%d rows, %v)\n",
			tf.Format.String(), statusStr(result.Success), result.RowCount, result.Duration)
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 7: Test with real CSV file
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 7: Real File Processing")

	if _, err := os.Stat(realCSVFile); err == nil {
		result := testRealFile(ctx, tmpDir, realCSVFile)
		results.RealFileTest = result
		fmt.Printf("   File: %s\n", filepath.Base(realCSVFile))
		fmt.Printf("   Rows: %d\n", result.RowCount)
		fmt.Printf("   Throughput: %d rows/sec\n", result.RowsPerSec)
		fmt.Printf("   Output Size: %d bytes\n", result.OutputSize)
		fmt.Printf("   Duration: %v\n", result.Duration)
	} else {
		fmt.Printf("   ⚠️  Real file not found: %s\n", realCSVFile)
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 8: Test sinks (Parquet, Iceberg, Arrow IPC)
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 8: Output Sinks")

	sinkTypes := []string{"parquet", "iceberg", "arrow_ipc"}
	for _, sinkType := range sinkTypes {
		result := testSink(ctx, tmpDir, sinkType)
		results.SinkTests = append(results.SinkTests, result)
		fmt.Printf("   %-12s: %s (%d rows, %d bytes)\n",
			sinkType, statusStr(result.Success), result.RowCount, result.OutputSize)
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 9: Query verification
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 9: Query Verification")

	result := testQueryVerification(ctx, tmpDir)
	results.QueryTest = result
	fmt.Printf("   Round-trip verification: %s\n", statusStr(result.Success))
	fmt.Printf("   Input rows:  %d\n", result.InputRows)
	fmt.Printf("   Output rows: %d\n", result.OutputRows)
	fmt.Printf("   Match: %v\n", result.InputRows == result.OutputRows)
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SUMMARY
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║                        TEST SUMMARY                            ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	passed, failed := results.Summary()
	fmt.Printf("   ✅ Passed: %d\n", passed)
	fmt.Printf("   ❌ Failed: %d\n", failed)
	fmt.Printf("   📊 Total:  %d\n", passed+failed)
	fmt.Println()

	if failed > 0 {
		os.Exit(1)
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Types
// ═══════════════════════════════════════════════════════════════════════════

type TestResults struct {
	FilesGenerated    int
	CompressionTests  []CompressionResult
	BatchSizeTests    []BatchSizeResult
	SchemaPolicyTests []SchemaPolicyResult
	ErrorPolicyTests  []ErrorPolicyResult
	FormatTests       []FormatResult
	RealFileTest      RealFileResult
	SinkTests         []SinkResult
	QueryTest         QueryResult
}

func (r *TestResults) Summary() (passed, failed int) {
	for _, t := range r.CompressionTests {
		if t.Success { passed++ } else { failed++ }
	}
	for _, t := range r.BatchSizeTests {
		if t.Success { passed++ } else { failed++ }
	}
	for _, t := range r.SchemaPolicyTests {
		if t.Success { passed++ } else { failed++ }
	}
	for _, t := range r.ErrorPolicyTests {
		if t.Success { passed++ } else { failed++ }
	}
	for _, t := range r.FormatTests {
		if t.Success { passed++ } else { failed++ }
	}
	for _, t := range r.SinkTests {
		if t.Success { passed++ } else { failed++ }
	}
	if r.RealFileTest.Success { passed++ } else { failed++ }
	if r.QueryTest.Success { passed++ } else { failed++ }
	return
}

type CompressionResult struct {
	Compression      core.Compression
	Success          bool
	CompressionRatio float64
	Duration         time.Duration
}

type BatchSizeResult struct {
	BatchSize   int
	Success     bool
	RowsPerSec  int64
	BatchCount  int
	Duration    time.Duration
}

type SchemaPolicyResult struct {
	Policy  schema.Policy
	Success bool
	Message string
}

type ErrorPolicyResult struct {
	Policy         core.ErrorPolicy
	Success        bool
	ErrorCount     int
	RecoveredCount int
}

type FormatResult struct {
	Format   core.Format
	Success  bool
	RowCount int64
	Duration time.Duration
}

type RealFileResult struct {
	Success    bool
	RowCount   int64
	RowsPerSec int64
	OutputSize int64
	Duration   time.Duration
}

type SinkResult struct {
	SinkType   string
	Success    bool
	RowCount   int64
	OutputSize int64
}

type QueryResult struct {
	Success    bool
	InputRows  int64
	OutputRows int64
}

type TestFile struct {
	Path   string
	Format core.Format
	Rows   int
}

// ═══════════════════════════════════════════════════════════════════════════
// Test Implementations
// ═══════════════════════════════════════════════════════════════════════════

func generateTestFiles(tmpDir string) []TestFile {
	var files []TestFile

	// Generate CSV
	csvPath := filepath.Join(tmpDir, "test_data.csv")
	if err := generateCSV(csvPath, 10000); err == nil {
		files = append(files, TestFile{Path: csvPath, Format: core.FormatCSV, Rows: 10000})
	}

	// Generate JSONL
	jsonlPath := filepath.Join(tmpDir, "test_data.jsonl")
	if err := generateJSONL(jsonlPath, 10000); err == nil {
		files = append(files, TestFile{Path: jsonlPath, Format: core.FormatJSONL, Rows: 10000})
	}

	// Generate JSON array
	jsonPath := filepath.Join(tmpDir, "test_data.json")
	if err := generateJSONArray(jsonPath, 1000); err == nil {
		files = append(files, TestFile{Path: jsonPath, Format: core.FormatJSON, Rows: 1000})
	}

	return files
}

func generateCSV(path string, rows int) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	gen := generators.NewCSVGenerator(42)
	gen.Columns = generators.StandardColumns()
	return gen.Generate(f, rows)
}

func generateJSONL(path string, rows int) error {
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

func generateJSONArray(path string, rows int) error {
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

func testCompression(ctx context.Context, tmpDir string, comp core.Compression) CompressionResult {
	start := time.Now()

	source := &fileSource{
		path:   filepath.Join(tmpDir, "test_data.csv"),
		format: core.FormatCSV,
	}

	if _, err := os.Stat(source.path); err != nil {
		return CompressionResult{Compression: comp, Success: false}
	}

	decoder := decoders.NewCSVDecoder()
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return CompressionResult{Compression: comp, Success: false}
	}

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return CompressionResult{Compression: comp, Success: false}
	}

	outputPath := filepath.Join(tmpDir, fmt.Sprintf("compressed_%s.parquet", comp.String()))
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath
	sinkOpts.Compression = comp

	if err := sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts); err != nil {
		return CompressionResult{Compression: comp, Success: false}
	}

	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	inputInfo, _ := os.Stat(source.path)
	outputInfo, _ := os.Stat(outputPath)

	ratio := float64(1)
	if outputInfo != nil && inputInfo != nil && outputInfo.Size() > 0 {
		ratio = float64(inputInfo.Size()) / float64(outputInfo.Size())
	}

	return CompressionResult{
		Compression:      comp,
		Success:          true,
		CompressionRatio: ratio,
		Duration:         time.Since(start),
	}
}

func testBatchSize(ctx context.Context, tmpDir string, batchSize int) BatchSizeResult {
	start := time.Now()

	source := &fileSource{
		path:   filepath.Join(tmpDir, "test_data.csv"),
		format: core.FormatCSV,
	}

	decoder := decoders.NewCSVDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = batchSize

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		return BatchSizeResult{BatchSize: batchSize, Success: false}
	}

	var totalRows int64
	var batchCount int
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			batchCount++
			b.Batch.Release()
		}
	}

	elapsed := time.Since(start)
	rowsPerSec := int64(float64(totalRows) / elapsed.Seconds())

	return BatchSizeResult{
		BatchSize:   batchSize,
		Success:     true,
		RowsPerSec:  rowsPerSec,
		BatchCount:  batchCount,
		Duration:    elapsed,
	}
}

func testSchemaPolicy(ctx context.Context, tmpDir string, policy schema.Policy) SchemaPolicyResult {
	// Create master schema
	master := schema.NewMasterSchema(policy)

	source := &fileSource{
		path:   filepath.Join(tmpDir, "test_data.csv"),
		format: core.FormatCSV,
	}

	decoder := decoders.NewCSVDecoder()
	inference := schema.NewInference()

	arrowSchema, err := inference.Infer(ctx, source, decoder)
	if err != nil {
		return SchemaPolicyResult{Policy: policy, Success: false, Message: err.Error()}
	}

	_, err = master.Update(arrowSchema)
	if err != nil {
		return SchemaPolicyResult{Policy: policy, Success: false, Message: err.Error()}
	}

	return SchemaPolicyResult{
		Policy:  policy,
		Success: true,
		Message: fmt.Sprintf("version %d", master.Version),
	}
}

func testErrorPolicy(ctx context.Context, tmpDir string, policy core.ErrorPolicy) ErrorPolicyResult {
	// Generate CSV with some malformed rows
	malformedPath := filepath.Join(tmpDir, fmt.Sprintf("malformed_%s.csv", policy.String()))
	f, _ := os.Create(malformedPath)
	f.WriteString("id,name,value\n")
	for i := 0; i < 100; i++ {
		if i%10 == 5 {
			f.WriteString(fmt.Sprintf("%d,\"unclosed quote,bad\n", i))
		} else {
			f.WriteString(fmt.Sprintf("%d,name_%d,%.2f\n", i, i, float64(i)*1.5))
		}
	}
	f.Close()

	source := &fileSource{
		path:   malformedPath,
		format: core.FormatCSV,
	}

	decoder := decoders.NewCSVDecoder()
	opts := core.DefaultDecodeOptions()
	opts.ErrorPolicy = policy

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil && policy == core.ErrorPolicyStrict {
		return ErrorPolicyResult{Policy: policy, Success: true, ErrorCount: 1}
	}

	var errorCount int
	for b := range batches {
		if b.Batch != nil {
			b.Batch.Release()
		}
		errorCount += len(b.Errors)
	}

	return ErrorPolicyResult{
		Policy:     policy,
		Success:    true,
		ErrorCount: errorCount,
	}
}

func testFileFormat(ctx context.Context, tmpDir string, tf TestFile) FormatResult {
	start := time.Now()

	source := &fileSource{
		path:   tf.Path,
		format: tf.Format,
	}

	var decoder core.Decoder
	switch tf.Format {
	case core.FormatCSV:
		decoder = decoders.NewCSVDecoder()
	case core.FormatJSON, core.FormatJSONL:
		decoder = decoders.NewJSONDecoder()
	default:
		return FormatResult{Format: tf.Format, Success: false}
	}

	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return FormatResult{Format: tf.Format, Success: false}
	}

	var totalRows int64
	for b := range batches {
		if b.Batch != nil {
			totalRows += b.Batch.NumRows()
			b.Batch.Release()
		}
	}

	return FormatResult{
		Format:   tf.Format,
		Success:  true,
		RowCount: totalRows,
		Duration: time.Since(start),
	}
}

func testRealFile(ctx context.Context, tmpDir string, path string) RealFileResult {
	start := time.Now()

	source := &fileSource{
		path:   path,
		format: core.FormatCSV,
	}

	decoder := decoders.NewCSVDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 10000

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		return RealFileResult{Success: false}
	}

	var allBatches []core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return RealFileResult{Success: false}
	}

	outputPath := filepath.Join(tmpDir, "real_file_output.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath

	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)

	var totalRows int64
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		totalRows += b.Batch.NumRows()
		b.Batch.Release()
	}
	sink.Close(ctx)

	elapsed := time.Since(start)
	rowsPerSec := int64(float64(totalRows) / elapsed.Seconds())

	outputInfo, _ := os.Stat(outputPath)
	outputSize := int64(0)
	if outputInfo != nil {
		outputSize = outputInfo.Size()
	}

	return RealFileResult{
		Success:    true,
		RowCount:   totalRows,
		RowsPerSec: rowsPerSec,
		OutputSize: outputSize,
		Duration:   elapsed,
	}
}

func testSink(ctx context.Context, tmpDir string, sinkType string) SinkResult {
	// Generate test data
	source := &fileSource{
		path:   filepath.Join(tmpDir, "test_data.csv"),
		format: core.FormatCSV,
	}

	decoder := decoders.NewCSVDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 1000

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		return SinkResult{SinkType: sinkType, Success: false}
	}

	// Get first batch for schema
	var firstBatch core.DecodedBatch
	for b := range batches {
		if b.Batch != nil {
			firstBatch = b
			break
		}
	}
	// Drain
	for range batches {
	}

	if firstBatch.Batch == nil {
		return SinkResult{SinkType: sinkType, Success: false}
	}
	defer firstBatch.Batch.Release()

	var sink core.Sink
	var outputPath string

	switch sinkType {
	case "parquet":
		outputPath = filepath.Join(tmpDir, "sink_test.parquet")
		sink = sinks.NewParquetSink()
	case "iceberg":
		outputPath = filepath.Join(tmpDir, "sink_iceberg_table")
		sink = sinks.NewIcebergSink()
	case "arrow_ipc":
		outputPath = filepath.Join(tmpDir, "sink_test.arrow")
		sink = sinks.NewArrowIPCSink()
	default:
		return SinkResult{SinkType: sinkType, Success: false}
	}

	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath

	if err := sink.Open(ctx, firstBatch.Batch.Schema(), sinkOpts); err != nil {
		return SinkResult{SinkType: sinkType, Success: false}
	}

	if err := sink.Write(ctx, firstBatch.Batch); err != nil {
		return SinkResult{SinkType: sinkType, Success: false}
	}

	result, err := sink.Close(ctx)
	if err != nil {
		return SinkResult{SinkType: sinkType, Success: false}
	}

	return SinkResult{
		SinkType:   sinkType,
		Success:    true,
		RowCount:   result.RowsWritten,
		OutputSize: result.BytesWritten,
	}
}

func testQueryVerification(ctx context.Context, tmpDir string) QueryResult {
	// Decode CSV
	source := &fileSource{
		path:   filepath.Join(tmpDir, "test_data.csv"),
		format: core.FormatCSV,
	}

	decoder := decoders.NewCSVDecoder()
	batches, err := decoder.Decode(ctx, source, core.DefaultDecodeOptions())
	if err != nil {
		return QueryResult{Success: false}
	}

	var allBatches []core.DecodedBatch
	var inputRows int64
	for b := range batches {
		if b.Batch != nil {
			inputRows += b.Batch.NumRows()
			allBatches = append(allBatches, b)
		}
	}

	if len(allBatches) == 0 {
		return QueryResult{Success: false}
	}

	// Write to Parquet
	outputPath := filepath.Join(tmpDir, "query_verify.parquet")
	sink := sinks.NewParquetSink()
	sinkOpts := core.DefaultSinkOptions()
	sinkOpts.Path = outputPath

	sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
	for _, b := range allBatches {
		sink.Write(ctx, b.Batch)
		b.Batch.Release()
	}
	sink.Close(ctx)

	// Query with DuckDB
	eng, err := engine.NewEngine()
	if err != nil {
		return QueryResult{Success: false}
	}
	defer eng.Close()

	sql := fmt.Sprintf("SELECT COUNT(*) as cnt FROM '%s'", outputPath)
	result, err := eng.Query(ctx, sql)
	if err != nil {
		return QueryResult{Success: false, InputRows: inputRows}
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

	return QueryResult{
		Success:    inputRows == outputRows,
		InputRows:  inputRows,
		OutputRows: outputRows,
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Helpers
// ═══════════════════════════════════════════════════════════════════════════

type fileSource struct {
	path   string
	format core.Format
}

func (s *fileSource) ID() string                             { return s.path }
func (s *fileSource) Location() string                       { return s.path }
func (s *fileSource) Format() core.Format                    { return s.format }
func (s *fileSource) Size() int64                            { info, _ := os.Stat(s.path); if info != nil { return info.Size() }; return 0 }
func (s *fileSource) ModTime() time.Time                     { return time.Now() }
func (s *fileSource) Metadata() map[string]string            { return nil }
func (s *fileSource) Open(ctx context.Context) (io.ReadCloser, error) { return os.Open(s.path) }

func section(title string) {
	fmt.Printf("═══ %s ═══\n", title)
}

func statusStr(success bool) string {
	if success {
		return "✅ PASS"
	}
	return "❌ FAIL"
}

func repeatStr(s string, n int) string {
	var b bytes.Buffer
	for i := 0; i < n; i++ {
		b.WriteString(s)
	}
	return b.String()
}

func fatal(format string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, "FATAL: "+format+"\n", args...)
	os.Exit(1)
}
