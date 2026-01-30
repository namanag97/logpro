package ingest

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// benchFormat runs a benchmark for the given format key and noise variant.
// It generates the test file once during setup, then runs engine.Ingest()
// in the b.N loop. Unsupported formats are skipped.
func benchFormat(b *testing.B, key string, noisy bool) {
	b.Helper()

	dir, err := os.MkdirTemp("", "bench-fmt-"+key+"-")
	if err != nil {
		b.Fatalf("MkdirTemp: %v", err)
	}
	defer os.RemoveAll(dir)

	specs := buildFormatSpecs()
	var spec formatSpec
	found := false
	for _, s := range specs {
		if s.key == key {
			spec = s
			found = true
			break
		}
	}
	if !found {
		b.Fatalf("unknown format key: %s", key)
	}

	if !spec.supported {
		b.Skipf("format %s not supported by engine", key)
	}

	// Use a testing.T wrapper for setup (formatSpec.setup requires *testing.T).
	// We run setup outside the benchmark loop.
	var inputPath string
	var inputSize int64
	var rowCount int

	// Setup: generate test file directly (not via formatSpec.setup which needs *testing.T)
	b.StopTimer()
	{
		rows := 1000
		if noisy {
			rows = 2000
		}

		switch key {
		case "csv":
			p := filepath.Join(dir, "data.csv")
			_, _, err := genCSV(p, rows, noisy)
			if err != nil {
				b.Fatalf("genCSV: %v", err)
			}
			inputPath = p
			rowCount = rows
		case "tsv":
			p := filepath.Join(dir, "data.tsv")
			_, _, err := genTSV(p, rows, noisy)
			if err != nil {
				b.Fatalf("genTSV: %v", err)
			}
			inputPath = p
			rowCount = rows
		case "json":
			p := filepath.Join(dir, "data.json")
			_, _, err := genJSON(p, rows, noisy)
			if err != nil {
				b.Fatalf("genJSON: %v", err)
			}
			inputPath = p
			rowCount = rows
		case "jsonl":
			p := filepath.Join(dir, "data.jsonl")
			_, _, err := genJSONL(p, rows, noisy)
			if err != nil {
				b.Fatalf("genJSONL: %v", err)
			}
			inputPath = p
			rowCount = rows
		case "parquet":
			// Generate CSV then ingest to Parquet for source
			csvPath := filepath.Join(dir, "source.csv")
			_, _, err := genCSV(csvPath, rows, false)
			if err != nil {
				b.Fatalf("genCSV for parquet: %v", err)
			}
			eng, err := NewEngine()
			if err != nil {
				b.Fatalf("engine: %v", err)
			}
			parquetSrc := filepath.Join(dir, "source.parquet")
			opts := DefaultOptions()
			opts.OutputPath = parquetSrc
			if _, err := eng.Ingest(context.Background(), csvPath, opts); err != nil {
				eng.Close()
				b.Fatalf("ingest for parquet source: %v", err)
			}
			eng.Close()
			inputPath = parquetSrc
			rowCount = rows
		case "csv_gz":
			csvPath := filepath.Join(dir, "data.csv")
			_, _, err := genCSV(csvPath, rows, noisy)
			if err != nil {
				b.Fatalf("genCSV for gz: %v", err)
			}
			gzPath := filepath.Join(dir, "data.csv.gz")
			if err := gzipFile(csvPath, gzPath); err != nil {
				b.Fatalf("gzipFile: %v", err)
			}
			inputPath = gzPath
			rowCount = rows
		case "jsonl_gz":
			jsonlPath := filepath.Join(dir, "data.jsonl")
			_, _, err := genJSONL(jsonlPath, rows, noisy)
			if err != nil {
				b.Fatalf("genJSONL for gz: %v", err)
			}
			gzPath := filepath.Join(dir, "data.jsonl.gz")
			if err := gzipFile(jsonlPath, gzPath); err != nil {
				b.Fatalf("gzipFile: %v", err)
			}
			inputPath = gzPath
			rowCount = rows
		case "csv_bom":
			p := filepath.Join(dir, "data_bom.csv")
			_, _, err := genBOMCSV(p, rows, noisy)
			if err != nil {
				b.Fatalf("genBOMCSV: %v", err)
			}
			inputPath = p
			rowCount = rows
		default:
			b.Skipf("benchmark not implemented for %s", key)
		}

		info, err := os.Stat(inputPath)
		if err != nil {
			b.Fatalf("stat input: %v", err)
		}
		inputSize = info.Size()
		_ = setupDone
		_ = t
	}
	b.StartTimer()

	b.SetBytes(inputSize)
	b.ReportAllocs()

	engine, err := NewEngine()
	if err != nil {
		b.Fatalf("NewEngine: %v", err)
	}
	defer engine.Close()

	for i := 0; i < b.N; i++ {
		outputPath := filepath.Join(dir, fmt.Sprintf("output_%d.parquet", i))
		opts := DefaultOptions()
		opts.OutputPath = outputPath

		start := time.Now()
		result, err := engine.Ingest(context.Background(), inputPath, opts)
		elapsed := time.Since(start)

		if err != nil {
			b.Fatalf("Ingest failed: %v", err)
		}

		// Report custom metrics
		if elapsed.Seconds() > 0 {
			b.ReportMetric(float64(result.RowCount)/elapsed.Seconds(), "rows/sec")
		}

		os.Remove(outputPath)
	}

	_ = rowCount
}

// ---------------------------------------------------------------------------
// CSV benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_CSV_Clean(b *testing.B)  { benchFormat(b, "csv", false) }
func BenchmarkIngest_CSV_Noisy(b *testing.B)  { benchFormat(b, "csv", true) }

// ---------------------------------------------------------------------------
// TSV benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_TSV_Clean(b *testing.B)  { benchFormat(b, "tsv", false) }
func BenchmarkIngest_TSV_Noisy(b *testing.B)  { benchFormat(b, "tsv", true) }

// ---------------------------------------------------------------------------
// JSON benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_JSON_Clean(b *testing.B) { benchFormat(b, "json", false) }
func BenchmarkIngest_JSON_Noisy(b *testing.B) { benchFormat(b, "json", true) }

// ---------------------------------------------------------------------------
// JSONL benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_JSONL_Clean(b *testing.B) { benchFormat(b, "jsonl", false) }
func BenchmarkIngest_JSONL_Noisy(b *testing.B) { benchFormat(b, "jsonl", true) }

// ---------------------------------------------------------------------------
// Parquet round-trip benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_Parquet_Clean(b *testing.B) { benchFormat(b, "parquet", false) }
func BenchmarkIngest_Parquet_Noisy(b *testing.B) { benchFormat(b, "parquet", true) }

// ---------------------------------------------------------------------------
// Gzip CSV benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_CSV_GZ_Clean(b *testing.B) { benchFormat(b, "csv_gz", false) }
func BenchmarkIngest_CSV_GZ_Noisy(b *testing.B) { benchFormat(b, "csv_gz", true) }

// ---------------------------------------------------------------------------
// Gzip JSONL benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_JSONL_GZ_Clean(b *testing.B) { benchFormat(b, "jsonl_gz", false) }
func BenchmarkIngest_JSONL_GZ_Noisy(b *testing.B) { benchFormat(b, "jsonl_gz", true) }

// ---------------------------------------------------------------------------
// BOM CSV benchmarks
// ---------------------------------------------------------------------------

func BenchmarkIngest_CSV_BOM_Clean(b *testing.B) { benchFormat(b, "csv_bom", false) }
func BenchmarkIngest_CSV_BOM_Noisy(b *testing.B) { benchFormat(b, "csv_bom", true) }

// ---------------------------------------------------------------------------
// Unsupported format benchmarks (skipped)
// ---------------------------------------------------------------------------

func BenchmarkIngest_XES_Clean(b *testing.B)       { benchFormat(b, "xes", false) }
func BenchmarkIngest_XES_Noisy(b *testing.B)       { benchFormat(b, "xes", true) }
func BenchmarkIngest_XML_Clean(b *testing.B)       { benchFormat(b, "xml", false) }
func BenchmarkIngest_XML_Noisy(b *testing.B)       { benchFormat(b, "xml", true) }
func BenchmarkIngest_XLSX_Clean(b *testing.B)      { benchFormat(b, "xlsx", false) }
func BenchmarkIngest_XLSX_Noisy(b *testing.B)      { benchFormat(b, "xlsx", true) }
func BenchmarkIngest_AccessLog_Clean(b *testing.B) { benchFormat(b, "accesslog", false) }
func BenchmarkIngest_AccessLog_Noisy(b *testing.B) { benchFormat(b, "accesslog", true) }
func BenchmarkIngest_Latin1_Clean(b *testing.B)    { benchFormat(b, "csv_latin1", false) }
func BenchmarkIngest_Latin1_Noisy(b *testing.B)    { benchFormat(b, "csv_latin1", true) }
func BenchmarkIngest_YAML_Clean(b *testing.B)      { benchFormat(b, "yaml", false) }
func BenchmarkIngest_YAML_Noisy(b *testing.B)      { benchFormat(b, "yaml", true) }
func BenchmarkIngest_Avro_Clean(b *testing.B)      { benchFormat(b, "avro", false) }
func BenchmarkIngest_Avro_Noisy(b *testing.B)      { benchFormat(b, "avro", true) }
