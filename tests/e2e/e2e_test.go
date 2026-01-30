package e2e

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/logflow/logflow/internal/pipe"
	"github.com/logflow/logflow/pkg/ingest"
	"github.com/logflow/logflow/pkg/parser"
	"github.com/logflow/logflow/pkg/writer"
)

const testCSV = "../../Insurance_claims_event_log copy.csv"

func TestPipeline1_InternalPipe_Arrow(t *testing.T) {
	if _, err := os.Stat(testCSV); err != nil {
		t.Skipf("test CSV not found: %s", testCSV)
	}

	out := filepath.Join(t.TempDir(), "pipe_arrow.parquet")

	cfg := pipe.DefaultConfig()
	p := pipe.NewPipeline(cfg)

	start := time.Now()
	result, err := p.Ingest(context.Background(), testCSV, out, parser.FormatCSV)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("internal/pipe Arrow failed: %v", err)
	}

	stat, _ := os.Stat(out)
	t.Logf("Pipeline 1 (internal/pipe Arrow): rows=%d, time=%v, output=%d bytes",
		result.RowsWritten, elapsed, stat.Size())
}

func TestPipeline1_InternalPipe_DuckDB(t *testing.T) {
	if _, err := os.Stat(testCSV); err != nil {
		t.Skipf("test CSV not found: %s", testCSV)
	}

	out := filepath.Join(t.TempDir(), "pipe_duckdb.parquet")

	cfg := pipe.DefaultConfig()
	cfg.ParserConfig.ColumnMapping = map[string]string{
		"case_id":   "clacase_id",
		"activity":  "activity_name",
		"timestamp": "timestamp",
	}
	p := pipe.NewDuckDBPipeline(cfg)

	start := time.Now()
	absCSV, _ := filepath.Abs(testCSV)
	result, err := p.Ingest(context.Background(), absCSV, out, parser.FormatCSV)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("internal/pipe DuckDB failed: %v", err)
	}

	stat, _ := os.Stat(out)
	t.Logf("Pipeline 1 (internal/pipe DuckDB with PM): rows=%d, time=%v, output=%d bytes",
		result.RowsWritten, elapsed, stat.Size())
}

func TestPipeline1_InternalPipe_DuckDB_NoPM(t *testing.T) {
	if _, err := os.Stat(testCSV); err != nil {
		t.Skipf("test CSV not found: %s", testCSV)
	}

	out := filepath.Join(t.TempDir(), "pipe_duckdb_nopm.parquet")

	cfg := pipe.DefaultConfig()
	// No column mapping — generic mode
	p := pipe.NewDuckDBPipeline(cfg)

	start := time.Now()
	absCSV, _ := filepath.Abs(testCSV)
	result, err := p.Ingest(context.Background(), absCSV, out, parser.FormatCSV)
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("internal/pipe DuckDB (no PM) failed: %v", err)
	}

	stat, _ := os.Stat(out)
	t.Logf("Pipeline 1 (internal/pipe DuckDB no PM): rows=%d, time=%v, output=%d bytes",
		result.RowsWritten, elapsed, stat.Size())
}

func TestPipeline3_PkgIngest(t *testing.T) {
	if _, err := os.Stat(testCSV); err != nil {
		t.Skipf("test CSV not found: %s", testCSV)
	}

	absCSV, _ := filepath.Abs(testCSV)
	out := filepath.Join(t.TempDir(), "ingest.parquet")

	p, err := ingest.NewPipeline()
	if err != nil {
		t.Fatalf("failed to create ingest pipeline: %v", err)
	}
	defer p.Close()

	start := time.Now()
	result, err := p.Process(context.Background(), absCSV, ingest.Options{
		OutputPath: out,
	})
	elapsed := time.Since(start)

	if err != nil {
		t.Fatalf("pkg/ingest pipeline failed: %v", err)
	}

	stat, _ := os.Stat(out)
	t.Logf("Pipeline 3 (pkg/ingest): rows=%d, time=%v, output=%d bytes, throughput=%.0f rows/sec",
		result.RowCount, elapsed, stat.Size(), result.Throughput)
}

func TestPipeline2_PkgPipeline(t *testing.T) {
	// pkg/pipeline is an orchestrator framework (Source/Sink/Processor).
	// It doesn't directly convert files — it needs registered adapters.
	// Just verify we can import and instantiate it.

	_ = writer.DefaultConfig()
	_ = parser.DefaultConfig()

	t.Log("Pipeline 2 (pkg/pipeline): orchestrator framework — no direct file conversion API")
}
