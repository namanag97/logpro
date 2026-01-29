// +build ignore

// Full integration test with XES, OLAP, and metadata
// Run: go run test_full_integration.go
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/logflow/logflow/pkg/api/rest"
	"github.com/logflow/logflow/pkg/defaults/auth"
	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/sinks"
	"github.com/logflow/logflow/pkg/query/engine"
	"github.com/logflow/logflow/pkg/storage/catalog"
)

var (
	csvFile  = "/Users/namanagarwal/logpro/Insurance_claims_event_log copy.csv"
	xesFile  = "/Users/namanagarwal/Downloads/Process-Mining-Datasets/BPI_Challenge_2013_incidents.xes"
	xesFile2 = "/Users/namanagarwal/Downloads/BPI Challenge 2017.xes"
)

func main() {
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║       Full Integration Test: XES, OLAP, Metadata               ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
	fmt.Println()

	ctx := context.Background()
	tmpDir, _ := os.MkdirTemp("", "logflow-integration-*")
	defer os.RemoveAll(tmpDir)

	fmt.Printf("📁 Test directory: %s\n\n", tmpDir)

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 1: Test XES File Processing (via CLI)
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 1: XES File Processing")

	xesOutput := filepath.Join(tmpDir, "xes_output.parquet")

	// Check which XES file exists
	xesPath := ""
	for _, path := range []string{xesFile, xesFile2} {
		if _, err := os.Stat(path); err == nil {
			xesPath = path
			break
		}
	}

	if xesPath != "" {
		fmt.Printf("   Found XES file: %s\n", filepath.Base(xesPath))

		// Use CLI to convert XES
		cmd := exec.Command("go", "run", "./cmd/logflow",
			"convert",
			"-i", xesPath,
			"-o", xesOutput,
			"--format", "xes",
			"--duckdb",
		)
		cmd.Dir = "/Users/namanagarwal/logpro"
		output, err := cmd.CombinedOutput()
		if err != nil {
			fmt.Printf("   ⚠️  XES conversion error: %v\n", err)
			fmt.Printf("   Output: %s\n", string(output))
		} else {
			fmt.Printf("   ✅ XES converted successfully\n")
			if info, err := os.Stat(xesOutput); err == nil {
				fmt.Printf("   Output size: %d bytes\n", info.Size())
			}
		}
	} else {
		fmt.Printf("   ⚠️  No XES file found\n")
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 2: Convert CSV with Full Metadata
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 2: CSV Conversion with Metadata")

	csvOutput := filepath.Join(tmpDir, "csv_output.parquet")

	// Convert CSV
	source := &fileSource{path: csvFile, format: core.FormatCSV}
	decoder := decoders.NewCSVDecoder()
	opts := core.DefaultDecodeOptions()
	opts.BatchSize = 10000

	batches, err := decoder.Decode(ctx, source, opts)
	if err != nil {
		fmt.Printf("   ❌ Decode error: %v\n", err)
	} else {
		var allBatches []core.DecodedBatch
		for b := range batches {
			if b.Batch != nil {
				allBatches = append(allBatches, b)
			}
		}

		if len(allBatches) > 0 {
			sink := sinks.NewParquetSink()
			sinkOpts := core.DefaultSinkOptions()
			sinkOpts.Path = csvOutput
			sinkOpts.Metadata = map[string]string{
				"logflow.version":     "0.1.0",
				"logflow.source":      csvFile,
				"logflow.format":      "csv",
				"logflow.compression": "snappy",
				"logflow.created_at":  time.Now().Format(time.RFC3339),
				"pm.case_id_column":   "clacase_id",
				"pm.activity_column":  "activity_name",
				"pm.timestamp_column": "timestamp",
			}

			sink.Open(ctx, allBatches[0].Batch.Schema(), sinkOpts)
			for _, b := range allBatches {
				sink.Write(ctx, b.Batch)
				b.Batch.Release()
			}
			result, _ := sink.Close(ctx)

			fmt.Printf("   ✅ CSV converted: %d rows, %d bytes\n", result.RowsWritten, result.BytesWritten)

			// Print schema
			fmt.Printf("   Schema:\n")
			schema := allBatches[0].Batch.Schema()
			for i := 0; i < min(10, schema.NumFields()); i++ {
				f := schema.Field(i)
				fmt.Printf("     - %-20s %s\n", f.Name, f.Type)
			}
			if schema.NumFields() > 10 {
				fmt.Printf("     ... and %d more fields\n", schema.NumFields()-10)
			}
		}
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 3: Query Parquet Metadata
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 3: Parquet Metadata Inspection")

	eng, err := engine.NewEngine()
	if err != nil {
		fmt.Printf("   ❌ Engine error: %v\n", err)
	} else {
		defer eng.Close()

		// Query metadata
		sql := fmt.Sprintf("SELECT * FROM parquet_metadata('%s')", csvOutput)
		result, err := eng.Query(ctx, sql)
		if err != nil {
			fmt.Printf("   ⚠️  Metadata query not supported in this DuckDB version\n")
		} else {
			rows, _ := result.ToMaps()
			fmt.Printf("   Parquet metadata rows: %d\n", len(rows))
			result.Close()
		}

		// Query schema
		sql = fmt.Sprintf("DESCRIBE SELECT * FROM '%s'", csvOutput)
		result, err = eng.Query(ctx, sql)
		if err == nil {
			rows, _ := result.ToMaps()
			fmt.Printf("   Schema columns: %d\n", len(rows))
			for i, row := range rows {
				if i < 5 {
					fmt.Printf("     - %v: %v\n", row["column_name"], row["column_type"])
				}
			}
			if len(rows) > 5 {
				fmt.Printf("     ... and %d more\n", len(rows)-5)
			}
			result.Close()
		}
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 4: OLAP Queries (Process Mining Style)
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 4: OLAP Queries")

	if eng != nil {
		queries := []struct {
			name string
			sql  string
		}{
			{
				"Total Events",
				fmt.Sprintf("SELECT COUNT(*) as total_events FROM '%s'", csvOutput),
			},
			{
				"Unique Cases",
				fmt.Sprintf("SELECT COUNT(DISTINCT clacase_id) as unique_cases FROM '%s'", csvOutput),
			},
			{
				"Activity Distribution",
				fmt.Sprintf(`SELECT activity_name, COUNT(*) as count
					FROM '%s'
					GROUP BY activity_name
					ORDER BY count DESC
					LIMIT 5`, csvOutput),
			},
			{
				"Events per Case (Avg)",
				fmt.Sprintf(`SELECT AVG(cnt) as avg_events_per_case
					FROM (SELECT clacase_id, COUNT(*) as cnt FROM '%s' GROUP BY clacase_id)`, csvOutput),
			},
			{
				"Time Range",
				fmt.Sprintf(`SELECT MIN(timestamp) as start_time, MAX(timestamp) as end_time FROM '%s'`, csvOutput),
			},
		}

		for _, q := range queries {
			result, err := eng.Query(ctx, q.sql)
			if err != nil {
				fmt.Printf("   ❌ %s: %v\n", q.name, err)
			} else {
				rows, _ := result.ToMaps()
				if len(rows) > 0 {
					fmt.Printf("   ✅ %s:\n", q.name)
					for _, row := range rows {
						for k, v := range row {
							fmt.Printf("      %s: %v\n", k, v)
						}
					}
				}
				result.Close()
			}
		}
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 5: REST API OLAP Endpoints
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 5: REST API OLAP Endpoints")

	// Create catalog
	cat, _ := catalog.NewFileCatalog(filepath.Join(tmpDir, "catalog"))

	// Start API server
	server := rest.NewServer(rest.Config{
		Addr:          ":18081",
		QueryEngine:   eng,
		Catalog:       cat,
		Authenticator: auth.NewNoopAuthenticator(),
	})

	go server.Start()
	time.Sleep(200 * time.Millisecond)

	// Test query endpoint
	queryReq := map[string]interface{}{
		"sql": fmt.Sprintf("SELECT activity_name, COUNT(*) as cnt FROM '%s' GROUP BY activity_name LIMIT 3", csvOutput),
	}
	reqBody, _ := json.Marshal(queryReq)

	resp, err := http.Post("http://localhost:18081/v1/query", "application/json", bytes.NewReader(reqBody))
	if err != nil {
		fmt.Printf("   ❌ Query endpoint error: %v\n", err)
	} else {
		var result map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&result)
		resp.Body.Close()

		if rows, ok := result["rows"].([]interface{}); ok {
			fmt.Printf("   ✅ Query endpoint returned %d rows:\n", len(rows))
			for i, row := range rows {
				if i < 3 {
					fmt.Printf("      %v\n", row)
				}
			}
		}
		if dur, ok := result["duration"]; ok {
			fmt.Printf("      Duration: %v\n", dur)
		}
	}

	// Test health endpoint
	resp, _ = http.Get("http://localhost:18081/health")
	if resp != nil {
		var health map[string]string
		json.NewDecoder(resp.Body).Decode(&health)
		resp.Body.Close()
		fmt.Printf("   ✅ Health: %s\n", health["status"])
	}

	// Shutdown
	shutdownCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	server.Shutdown(shutdownCtx)
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SECTION 6: Data Profiling
	// ═══════════════════════════════════════════════════════════════════════
	section("SECTION 6: Data Profiling")

	if eng != nil {
		profileQueries := []struct {
			name string
			sql  string
		}{
			{
				"Column Stats",
				fmt.Sprintf(`SELECT
					'claim_amount' as column_name,
					MIN(claim_amount) as min_val,
					MAX(claim_amount) as max_val,
					AVG(claim_amount) as avg_val,
					COUNT(*) - COUNT(claim_amount) as null_count
				FROM '%s'`, csvOutput),
			},
			{
				"Cardinality",
				fmt.Sprintf(`SELECT
					COUNT(DISTINCT clacase_id) as case_cardinality,
					COUNT(DISTINCT activity_name) as activity_cardinality,
					COUNT(DISTINCT agent_name) as agent_cardinality
				FROM '%s'`, csvOutput),
			},
		}

		for _, q := range profileQueries {
			result, err := eng.Query(ctx, q.sql)
			if err != nil {
				fmt.Printf("   ❌ %s: %v\n", q.name, err)
			} else {
				rows, _ := result.ToMaps()
				if len(rows) > 0 {
					fmt.Printf("   ✅ %s:\n", q.name)
					for k, v := range rows[0] {
						fmt.Printf("      %s: %v\n", k, v)
					}
				}
				result.Close()
			}
		}
	}
	fmt.Println()

	// ═══════════════════════════════════════════════════════════════════════
	// SUMMARY
	// ═══════════════════════════════════════════════════════════════════════
	fmt.Println("╔════════════════════════════════════════════════════════════════╗")
	fmt.Println("║               INTEGRATION TEST COMPLETE                        ║")
	fmt.Println("╚════════════════════════════════════════════════════════════════╝")
}

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

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
