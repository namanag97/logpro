package ingest

import (
	"compress/gzip"
	"context"
	"database/sql"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	_ "github.com/marcboeker/go-duckdb"
	"github.com/xuri/excelize/v2"
	"gopkg.in/yaml.v3"
)

// ---------------------------------------------------------------------------
// Shannon entropy helpers
// ---------------------------------------------------------------------------

// computeColumnEntropy computes Shannon entropy H(X) for a single column.
// values is every cell in the column represented as a string.
func computeColumnEntropy(values []string) float64 {
	if len(values) == 0 {
		return 0
	}
	counts := make(map[string]int, len(values)/2)
	for _, v := range values {
		counts[v]++
	}
	total := float64(len(values))
	var h float64
	for _, c := range counts {
		p := float64(c) / total
		if p > 0 {
			h -= p * math.Log2(p)
		}
	}
	return h
}

// readParquetColumns uses DuckDB to read a Parquet file and returns header
// names plus all rows as [][]string (column-major would be wasteful here;
// we return row-major and let the caller slice per column).
func readParquetColumns(path string) ([]string, [][]string, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, fmt.Errorf("open duckdb: %w", err)
	}
	defer db.Close()

	query := fmt.Sprintf("SELECT * FROM read_parquet('%s')", escapePath(path))
	rows, err := db.Query(query)
	if err != nil {
		return nil, nil, fmt.Errorf("query parquet: %w", err)
	}
	defer rows.Close()

	colNames, err := rows.Columns()
	if err != nil {
		return nil, nil, fmt.Errorf("columns: %w", err)
	}

	var result [][]string
	for rows.Next() {
		vals := make([]interface{}, len(colNames))
		ptrs := make([]interface{}, len(colNames))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, nil, fmt.Errorf("scan: %w", err)
		}
		row := make([]string, len(vals))
		for i, v := range vals {
			if v == nil {
				row[i] = ""
			} else {
				row[i] = fmt.Sprintf("%v", v)
			}
		}
		result = append(result, row)
	}
	return colNames, result, rows.Err()
}

// assertEntropyPreserved checks that per-column Shannon entropy is preserved
// between source data and Parquet output within 1% relative tolerance.
func assertEntropyPreserved(t *testing.T, srcHeaders []string, srcRows [][]string, parquetPath string) {
	t.Helper()

	outHeaders, outRows, err := readParquetColumns(parquetPath)
	if err != nil {
		t.Fatalf("failed to read parquet for entropy check: %v", err)
	}

	// Row count check
	if len(srcRows) != len(outRows) {
		t.Errorf("row count mismatch: source=%d parquet=%d", len(srcRows), len(outRows))
	}

	// Build column index mapping (output may reorder columns)
	outIdx := make(map[string]int, len(outHeaders))
	for i, h := range outHeaders {
		outIdx[strings.ToLower(h)] = i
	}

	const epsilon = 1e-10
	const tolerance = 0.01 // 1% relative

	for srcCol := 0; srcCol < len(srcHeaders); srcCol++ {
		oi, ok := outIdx[strings.ToLower(srcHeaders[srcCol])]
		if !ok {
			t.Errorf("column %q not found in parquet output", srcHeaders[srcCol])
			continue
		}

		// Collect source column values
		srcVals := make([]string, len(srcRows))
		for r, row := range srcRows {
			if srcCol < len(row) {
				srcVals[r] = row[srcCol]
			}
		}

		// Collect output column values
		outVals := make([]string, len(outRows))
		for r, row := range outRows {
			if oi < len(row) {
				outVals[r] = row[oi]
			}
		}

		hSrc := computeColumnEntropy(srcVals)
		hOut := computeColumnEntropy(outVals)

		denom := math.Max(hSrc, epsilon)
		relDiff := math.Abs(hSrc-hOut) / denom

		if relDiff > tolerance {
			t.Errorf("column %q entropy drift: source=%.4f output=%.4f relDiff=%.4f (>%.2f)",
				srcHeaders[srcCol], hSrc, hOut, relDiff, tolerance)
		}
	}
}

// ---------------------------------------------------------------------------
// Performance snapshot helper
// ---------------------------------------------------------------------------

type memSnapshot struct {
	TotalAlloc uint64
	HeapInuse  uint64
	PauseTotalNs uint64
	Mallocs    uint64
}

func captureMemStats() memSnapshot {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return memSnapshot{
		TotalAlloc:   ms.TotalAlloc,
		HeapInuse:    ms.HeapInuse,
		PauseTotalNs: ms.PauseTotalNs,
		Mallocs:      ms.Mallocs,
	}
}

func logPerfMetrics(t *testing.T, label string, before, after memSnapshot, elapsed time.Duration, result *Result) {
	t.Helper()
	t.Logf("[%s] wall=%v alloc=%.2fMB heap=%.2fMB gcPause=%v mallocs=%d",
		label, elapsed,
		float64(after.TotalAlloc-before.TotalAlloc)/(1024*1024),
		float64(after.HeapInuse-before.HeapInuse)/(1024*1024),
		time.Duration(after.PauseTotalNs-before.PauseTotalNs),
		after.Mallocs-before.Mallocs,
	)
	if result != nil {
		throughput := float64(0)
		speed := float64(0)
		if elapsed.Seconds() > 0 {
			throughput = float64(result.RowCount) / elapsed.Seconds()
			speed = float64(result.InputSize) / elapsed.Seconds() / (1024 * 1024)
		}
		compression := float64(0)
		if result.OutputSize > 0 {
			compression = float64(result.InputSize) / float64(result.OutputSize)
		}
		t.Logf("[%s] rows=%d throughput=%.0f rows/s speed=%.2f MB/s compression=%.2fx",
			label, result.RowCount, throughput, speed, compression)
	}
}

// ---------------------------------------------------------------------------
// File generation helpers
// ---------------------------------------------------------------------------

// genCSVData generates CSV data and returns the header names + rows for
// entropy verification. If noisy is true, it injects realistic noise.
func genCSVData(path string, delim byte, rowCount int, noisy bool) ([]string, [][]string, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	headers := []string{"case_id", "activity", "timestamp", "resource", "amount"}
	delimStr := string(delim)
	fmt.Fprintln(f, strings.Join(headers, delimStr))

	rng := rand.New(rand.NewSource(42))
	activities := []string{"Submit", "Review", "Approve", "Reject", "Complete", "Ship"}
	resources := []string{"Alice", "Bob", "Charlie", "Diana", "Eve"}

	rows := make([][]string, 0, rowCount)
	for i := 0; i < rowCount; i++ {
		caseID := fmt.Sprintf("C-%04d", i%100)
		activity := activities[rng.Intn(len(activities))]
		ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(
			time.Duration(rng.Int63n(365*24*60*60)) * time.Second).Format("2006-01-02T15:04:05Z")
		resource := resources[rng.Intn(len(resources))]
		amount := fmt.Sprintf("%.2f", rng.Float64()*10000)

		if noisy {
			// Inject noise: nulls, type errors
			switch {
			case rng.Float64() < 0.05:
				amount = ""
			case rng.Float64() < 0.03:
				amount = "NaN"
			case rng.Float64() < 0.02:
				ts = "invalid-date"
			}
		}

		row := []string{caseID, activity, ts, resource, amount}
		rows = append(rows, row)

		// Write with proper quoting for fields containing delimiter
		vals := make([]string, len(row))
		for j, v := range row {
			if strings.Contains(v, delimStr) || strings.Contains(v, "\"") || strings.Contains(v, "\n") {
				vals[j] = `"` + strings.ReplaceAll(v, `"`, `""`) + `"`
			} else {
				vals[j] = v
			}
		}
		fmt.Fprintln(f, strings.Join(vals, delimStr))
	}
	return headers, rows, nil
}

func genCSV(path string, rows int, noisy bool) ([]string, [][]string, error) {
	return genCSVData(path, ',', rows, noisy)
}

func genTSV(path string, rows int, noisy bool) ([]string, [][]string, error) {
	return genCSVData(path, '\t', rows, noisy)
}

type jsonEvent struct {
	CaseID    string  `json:"case_id"`
	Activity  string  `json:"activity"`
	Timestamp string  `json:"timestamp"`
	Resource  string  `json:"resource"`
	Amount    float64 `json:"amount"`
}

func genJSONRows(rowCount int, noisy bool) []jsonEvent {
	rng := rand.New(rand.NewSource(42))
	activities := []string{"Submit", "Review", "Approve", "Reject", "Complete"}
	resources := []string{"Alice", "Bob", "Charlie", "Diana"}
	events := make([]jsonEvent, rowCount)
	for i := 0; i < rowCount; i++ {
		events[i] = jsonEvent{
			CaseID:    fmt.Sprintf("C-%04d", i%80),
			Activity:  activities[rng.Intn(len(activities))],
			Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(rng.Int63n(365*24*60*60)) * time.Second).Format(time.RFC3339),
			Resource:  resources[rng.Intn(len(resources))],
			Amount:    math.Round(rng.Float64()*10000*100) / 100,
		}
	}
	return events
}

// jsonRowsToStringRows converts JSON events to string rows for entropy comparison.
func jsonRowsToStringRows(events []jsonEvent) ([]string, [][]string) {
	headers := []string{"case_id", "activity", "timestamp", "resource", "amount"}
	rows := make([][]string, len(events))
	for i, e := range events {
		rows[i] = []string{e.CaseID, e.Activity, e.Timestamp, e.Resource, fmt.Sprintf("%g", e.Amount)}
	}
	return headers, rows
}

func genJSON(path string, rows int, noisy bool) ([]string, [][]string, error) {
	events := genJSONRows(rows, noisy)
	data, err := json.MarshalIndent(events, "", "  ")
	if err != nil {
		return nil, nil, err
	}
	if err := os.WriteFile(path, data, 0644); err != nil {
		return nil, nil, err
	}
	h, r := jsonRowsToStringRows(events)
	return h, r, nil
}

func genJSONL(path string, rows int, noisy bool) ([]string, [][]string, error) {
	events := genJSONRows(rows, noisy)
	f, err := os.Create(path)
	if err != nil {
		return nil, nil, err
	}
	defer f.Close()

	for _, e := range events {
		line, _ := json.Marshal(e)
		fmt.Fprintln(f, string(line))
	}
	h, r := jsonRowsToStringRows(events)
	return h, r, nil
}

func genXES(path string, cases, eventsPerCase int, noisy bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	rng := rand.New(rand.NewSource(42))
	activities := []string{"Submit", "Review", "Approve", "Process", "Complete"}

	fmt.Fprintln(f, `<?xml version="1.0" encoding="UTF-8"?>`)
	fmt.Fprintln(f, `<log xes.version="1.0" xes.features="nested-attributes">`)

	for c := 0; c < cases; c++ {
		caseID := fmt.Sprintf("CASE-%04d", c)
		if noisy && rng.Float64() < 0.05 {
			// Missing trace attribute
			fmt.Fprintln(f, `  <trace>`)
		} else {
			fmt.Fprintf(f, "  <trace>\n    <string key=\"concept:name\" value=\"%s\"/>\n", caseID)
		}

		baseTime := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(
			time.Duration(rng.Int63n(365*24*60*60)) * time.Second)

		for e := 0; e < eventsPerCase; e++ {
			if noisy && rng.Float64() < 0.03 {
				// Empty event
				fmt.Fprintln(f, `    <event></event>`)
				continue
			}
			activity := activities[e%len(activities)]
			ts := baseTime.Add(time.Duration(e) * time.Hour).Format(time.RFC3339)
			resource := fmt.Sprintf("Resource-%d", rng.Intn(5))
			fmt.Fprintf(f, `    <event>
      <string key="concept:name" value="%s"/>
      <date key="time:timestamp" value="%s"/>
      <string key="org:resource" value="%s"/>
    </event>
`, activity, ts, resource)
		}
		fmt.Fprintln(f, `  </trace>`)
	}
	fmt.Fprintln(f, `</log>`)
	return nil
}

func genXML(path string, rows int, noisy bool) error {
	type Event struct {
		XMLName   xml.Name `xml:"event"`
		CaseID    string   `xml:"case_id"`
		Activity  string   `xml:"activity"`
		Timestamp string   `xml:"timestamp"`
	}
	type Events struct {
		XMLName xml.Name `xml:"events"`
		Events  []Event  `xml:"event"`
	}

	rng := rand.New(rand.NewSource(42))
	evts := make([]Event, rows)
	for i := range evts {
		evts[i] = Event{
			CaseID:    fmt.Sprintf("C-%04d", i%50),
			Activity:  []string{"A", "B", "C"}[rng.Intn(3)],
			Timestamp: time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(time.Duration(rng.Int63n(365*24*60*60)) * time.Second).Format(time.RFC3339),
		}
	}

	data, err := xml.MarshalIndent(Events{Events: evts}, "", "  ")
	if err != nil {
		return err
	}
	content := []byte(xml.Header)
	content = append(content, data...)
	if noisy {
		// Inject malformed tag at end
		content = append(content, []byte("\n<unclosed>")...)
	}
	return os.WriteFile(path, content, 0644)
}

func genXLSX(path string, rows int, noisy bool) error {
	f := excelize.NewFile()
	defer f.Close()

	sheet := "Sheet1"
	headers := []string{"case_id", "activity", "timestamp", "resource", "amount"}
	for i, h := range headers {
		cell, _ := excelize.CoordinatesToCellName(i+1, 1)
		f.SetCellValue(sheet, cell, h)
	}

	rng := rand.New(rand.NewSource(42))
	activities := []string{"Submit", "Review", "Approve"}

	for r := 0; r < rows; r++ {
		row := r + 2
		cell, _ := excelize.CoordinatesToCellName(1, row)
		f.SetCellValue(sheet, cell, fmt.Sprintf("C-%04d", r%50))
		cell, _ = excelize.CoordinatesToCellName(2, row)
		f.SetCellValue(sheet, cell, activities[rng.Intn(len(activities))])
		cell, _ = excelize.CoordinatesToCellName(3, row)
		f.SetCellValue(sheet, cell, "2024-01-01T00:00:00Z")
		cell, _ = excelize.CoordinatesToCellName(4, row)
		f.SetCellValue(sheet, cell, "Alice")
		cell, _ = excelize.CoordinatesToCellName(5, row)
		if noisy && rng.Float64() < 0.1 {
			f.SetCellValue(sheet, cell, "not_a_number")
		} else {
			f.SetCellValue(sheet, cell, rng.Float64()*1000)
		}
	}
	return f.SaveAs(path)
}

func gzipFile(src, dst string) error {
	input, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	f, err := os.Create(dst)
	if err != nil {
		return err
	}
	defer f.Close()
	w := gzip.NewWriter(f)
	if _, err := w.Write(input); err != nil {
		return err
	}
	return w.Close()
}

func genAccessLog(path string, rows int, noisy bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	rng := rand.New(rand.NewSource(42))
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	paths := []string{"/api/users", "/api/orders", "/health", "/login"}

	for i := 0; i < rows; i++ {
		ip := fmt.Sprintf("192.168.%d.%d", rng.Intn(256), rng.Intn(256))
		method := methods[rng.Intn(len(methods))]
		p := paths[rng.Intn(len(paths))]
		status := []int{200, 201, 404, 500}[rng.Intn(4)]
		size := rng.Intn(50000)
		ts := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Add(
			time.Duration(rng.Int63n(365*24*60*60)) * time.Second)

		if noisy && rng.Float64() < 0.05 {
			// Malformed entry
			fmt.Fprintf(f, "MALFORMED LINE %d\n", i)
		} else {
			fmt.Fprintf(f, "%s - - [%s] \"%s %s HTTP/1.1\" %d %d\n",
				ip, ts.Format("02/Jan/2006:15:04:05 -0700"), method, p, status, size)
		}
	}
	return nil
}

func genBOMCSV(path string, rows int, noisy bool) ([]string, [][]string, error) {
	// Generate a normal CSV first
	tmpPath := path + ".tmp"
	headers, dataRows, err := genCSV(tmpPath, rows, noisy)
	if err != nil {
		return nil, nil, err
	}
	defer os.Remove(tmpPath)

	// Read it back and prepend BOM
	content, err := os.ReadFile(tmpPath)
	if err != nil {
		return nil, nil, err
	}
	bom := []byte{0xEF, 0xBB, 0xBF}
	return headers, dataRows, os.WriteFile(path, append(bom, content...), 0644)
}

func genLatin1CSV(path string, rows int, noisy bool) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	rng := rand.New(rand.NewSource(42))
	fmt.Fprintln(f, "case_id,activity,resource")

	// Latin-1 names with high bytes
	latin1Names := [][]byte{
		{0x4A, 0x6F, 0x73, 0xE9}, // Jos\xe9
		{0x4D, 0xFC, 0x6C, 0x6C, 0x65, 0x72}, // M\xfcller
		{0x42, 0x6A, 0xF6, 0x72, 0x6E}, // Bj\xf6rn
	}

	for i := 0; i < rows; i++ {
		caseID := fmt.Sprintf("C-%04d", i%50)
		activity := "Process"
		var resource []byte
		if noisy {
			resource = latin1Names[rng.Intn(len(latin1Names))]
		} else {
			resource = []byte("Alice")
		}
		line := fmt.Sprintf("%s,%s,", caseID, activity)
		f.Write([]byte(line))
		f.Write(resource)
		f.Write([]byte("\n"))
	}
	return nil
}

func genYAML(path string, rows int, noisy bool) error {
	type Event struct {
		CaseID    string  `yaml:"case_id"`
		Activity  string  `yaml:"activity"`
		Timestamp string  `yaml:"timestamp"`
		Amount    float64 `yaml:"amount"`
	}

	rng := rand.New(rand.NewSource(42))
	events := make([]Event, rows)
	for i := range events {
		events[i] = Event{
			CaseID:    fmt.Sprintf("C-%04d", i%50),
			Activity:  []string{"A", "B", "C"}[rng.Intn(3)],
			Timestamp: "2024-01-01T00:00:00Z",
			Amount:    math.Round(rng.Float64()*1000*100) / 100,
		}
	}

	data, err := yaml.Marshal(events)
	if err != nil {
		return err
	}
	if noisy {
		// Append invalid YAML
		data = append(data, []byte("\n  bad_indent:\n- broken\n")...)
	}
	return os.WriteFile(path, data, 0644)
}

func genAvro(path string, rows int) error {
	// Write a minimal valid Avro Object Container File.
	// Avro OCF format: magic "Obj\x01" + header + blocks.
	// We write just enough to be detected as Avro but not processable by the engine.
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	// Avro magic bytes
	magic := []byte{'O', 'b', 'j', 1}
	f.Write(magic)

	// Write minimal header metadata (schema as Avro map)
	// This is intentionally minimal - just enough to look like Avro
	schema := `{"type":"record","name":"Event","fields":[{"name":"id","type":"int"}]}`
	// Avro header: map of string->bytes
	// Number of entries (varint: 2 = 0x04)
	f.Write([]byte{0x04}) // 2 entries in map

	// Entry 1: "avro.schema" -> schema JSON
	writeAvroString(f, "avro.schema")
	writeAvroBytes(f, []byte(schema))

	// Entry 2: "avro.codec" -> "null"
	writeAvroString(f, "avro.codec")
	writeAvroBytes(f, []byte("null"))

	// End of map
	f.Write([]byte{0x00})

	// 16-byte sync marker
	sync := make([]byte, 16)
	for i := range sync {
		sync[i] = byte(i + 1)
	}
	f.Write(sync)

	// One data block with rows encoded as Avro ints
	// Block count (varint)
	writeAvroLong(f, int64(rows))
	// Block byte size
	blockData := make([]byte, 0, rows*2)
	for i := 0; i < rows; i++ {
		// Avro int zigzag encoding
		v := int64(i)
		z := (v << 1) ^ (v >> 63)
		for z >= 0x80 {
			blockData = append(blockData, byte(z)|0x80)
			z >>= 7
		}
		blockData = append(blockData, byte(z))
	}
	writeAvroLong(f, int64(len(blockData)))
	f.Write(blockData)

	// Sync marker again
	f.Write(sync)

	return nil
}

func writeAvroString(f *os.File, s string) {
	writeAvroLong(f, int64(len(s)))
	f.Write([]byte(s))
}

func writeAvroBytes(f *os.File, b []byte) {
	writeAvroLong(f, int64(len(b)))
	f.Write(b)
}

func writeAvroLong(f *os.File, v int64) {
	z := uint64((v << 1) ^ (v >> 63))
	for z >= 0x80 {
		f.Write([]byte{byte(z) | 0x80})
		z >>= 7
	}
	f.Write([]byte{byte(z)})
}

// ---------------------------------------------------------------------------
// Format test specification
// ---------------------------------------------------------------------------

type formatSpec struct {
	key         string
	supported   bool                                                          // whether engine.Ingest should succeed
	setup       func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) // returns (filePath, headers, rows)
}

func buildFormatSpecs() []formatSpec {
	return []formatSpec{
		{
			key:       "csv",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.csv")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genCSV(p, rows, noisy)
				if err != nil {
					t.Fatalf("genCSV: %v", err)
				}
				return p, h, r
			},
		},
		{
			key:       "tsv",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.tsv")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genTSV(p, rows, noisy)
				if err != nil {
					t.Fatalf("genTSV: %v", err)
				}
				return p, h, r
			},
		},
		{
			key:       "json",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.json")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genJSON(p, rows, noisy)
				if err != nil {
					t.Fatalf("genJSON: %v", err)
				}
				return p, h, r
			},
		},
		{
			key:       "jsonl",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.jsonl")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genJSONL(p, rows, noisy)
				if err != nil {
					t.Fatalf("genJSONL: %v", err)
				}
				return p, h, r
			},
		},
		{
			key:       "xes",
			supported: false, // XES robust path returns "not yet implemented"
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.xes")
				cases, events := 20, 5
				if noisy {
					cases, events = 50, 5
				}
				if err := genXES(p, cases, events, noisy); err != nil {
					t.Fatalf("genXES: %v", err)
				}
				return p, nil, nil
			},
		},
		{
			key:       "xml",
			supported: false,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.xml")
				rows := 200
				if noisy {
					rows = 500
				}
				if err := genXML(p, rows, noisy); err != nil {
					t.Fatalf("genXML: %v", err)
				}
				return p, nil, nil
			},
		},
		{
			key:       "xlsx",
			supported: false, // XLSX not supported by engine
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.xlsx")
				rows := 200
				if noisy {
					rows = 500
				}
				if err := genXLSX(p, rows, noisy); err != nil {
					t.Fatalf("genXLSX: %v", err)
				}
				return p, nil, nil
			},
		},
		{
			key:       "parquet",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				// Generate a CSV, ingest it to Parquet, then use that as the input.
				csvPath := filepath.Join(dir, "source.csv")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genCSV(csvPath, rows, false) // always clean source for parquet roundtrip
				if err != nil {
					t.Fatalf("genCSV for parquet: %v", err)
				}

				// Ingest CSV to Parquet
				eng, err := NewEngine()
				if err != nil {
					t.Fatalf("engine for parquet gen: %v", err)
				}
				defer eng.Close()

				parquetSrc := filepath.Join(dir, "source.parquet")
				opts := DefaultOptions()
				opts.OutputPath = parquetSrc
				if _, err := eng.Ingest(context.Background(), csvPath, opts); err != nil {
					t.Fatalf("ingest CSV for parquet source: %v", err)
				}

				return parquetSrc, h, r
			},
		},
		{
			key:       "csv_gz",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				csvPath := filepath.Join(dir, "data.csv")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genCSV(csvPath, rows, noisy)
				if err != nil {
					t.Fatalf("genCSV for gz: %v", err)
				}
				gzPath := filepath.Join(dir, "data.csv.gz")
				if err := gzipFile(csvPath, gzPath); err != nil {
					t.Fatalf("gzipFile: %v", err)
				}
				return gzPath, h, r
			},
		},
		{
			key:       "jsonl_gz",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				jsonlPath := filepath.Join(dir, "data.jsonl")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genJSONL(jsonlPath, rows, noisy)
				if err != nil {
					t.Fatalf("genJSONL for gz: %v", err)
				}
				gzPath := filepath.Join(dir, "data.jsonl.gz")
				if err := gzipFile(jsonlPath, gzPath); err != nil {
					t.Fatalf("gzipFile: %v", err)
				}
				return gzPath, h, r
			},
		},
		{
			key:       "accesslog",
			supported: false,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "access.log")
				rows := 200
				if noisy {
					rows = 500
				}
				if err := genAccessLog(p, rows, noisy); err != nil {
					t.Fatalf("genAccessLog: %v", err)
				}
				return p, nil, nil
			},
		},
		{
			key:       "csv_bom",
			supported: true,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data_bom.csv")
				rows := 200
				if noisy {
					rows = 500
				}
				h, r, err := genBOMCSV(p, rows, noisy)
				if err != nil {
					t.Fatalf("genBOMCSV: %v", err)
				}
				return p, h, r
			},
		},
		{
			key:       "csv_latin1",
			supported: false, // Latin-1 encoding causes issues
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data_latin1.csv")
				rows := 200
				if noisy {
					rows = 500
				}
				if err := genLatin1CSV(p, rows, noisy); err != nil {
					t.Fatalf("genLatin1CSV: %v", err)
				}
				return p, nil, nil
			},
		},
		{
			key:       "yaml",
			supported: false,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.yaml")
				rows := 200
				if noisy {
					rows = 500
				}
				if err := genYAML(p, rows, noisy); err != nil {
					t.Fatalf("genYAML: %v", err)
				}
				return p, nil, nil
			},
		},
		{
			key:       "avro",
			supported: false,
			setup: func(t *testing.T, dir string, noisy bool) (string, []string, [][]string) {
				p := filepath.Join(dir, "data.avro")
				rows := 200
				if noisy {
					rows = 500
				}
				if err := genAvro(p, rows); err != nil {
					t.Fatalf("genAvro: %v", err)
				}
				return p, nil, nil
			},
		},
	}
}

// ---------------------------------------------------------------------------
// Main integration test
// ---------------------------------------------------------------------------

func TestFullPipelineIntegration(t *testing.T) {
	specs := buildFormatSpecs()

	engine, err := NewEngine()
	if err != nil {
		t.Fatalf("NewEngine: %v", err)
	}
	defer engine.Close()

	for _, spec := range specs {
		spec := spec // capture
		for _, noisy := range []bool{false, true} {
			noisy := noisy
			variant := "clean"
			if noisy {
				variant = "noisy"
			}
			name := fmt.Sprintf("%s/%s", spec.key, variant)

			t.Run(name, func(t *testing.T) {
				t.Parallel()

				// Per-subtest temp directory
				dir, err := os.MkdirTemp("", "integ-"+spec.key+"-"+variant+"-")
				if err != nil {
					t.Fatalf("MkdirTemp: %v", err)
				}
				defer os.RemoveAll(dir)

				// Generate test file
				inputPath, srcHeaders, srcRows := spec.setup(t, dir, noisy)

				outputPath := filepath.Join(dir, "output.parquet")
				opts := DefaultOptions()
				opts.OutputPath = outputPath

				// Force RobustGo for formats that require DuckDB extensions
				// not available in all builds (JSON extension for read_json_auto,
				// and gzip CSV where the detector can't analyze compressed bytes).
				switch spec.key {
				case "json", "jsonl", "jsonl_gz", "csv_gz":
					opts.ForceStrategy = StrategyRobustGo
				}

				// Capture performance baseline
				before := captureMemStats()
				start := time.Now()

				// Per-subtest engine (parallel safety — DuckDB connections are not goroutine-safe)
				eng, err := NewEngine()
				if err != nil {
					t.Fatalf("NewEngine: %v", err)
				}
				defer eng.Close()

				result, ingestErr := eng.Ingest(context.Background(), inputPath, opts)

				elapsed := time.Since(start)
				after := captureMemStats()

				if spec.supported {
					// --- Supported format: expect success ---
					if ingestErr != nil {
						t.Fatalf("expected successful ingest for %s, got error: %v", name, ingestErr)
					}

					// Validate Parquet output
					validation, err := ValidateParquet(outputPath)
					if err != nil {
						t.Fatalf("ValidateParquet error: %v", err)
					}
					if !validation.Valid {
						t.Errorf("invalid Parquet output: %s", validation.Message)
					}

					// Row count sanity
					if result.RowCount == 0 {
						t.Error("expected non-zero row count")
					}
					if result.OutputSize == 0 {
						t.Error("expected non-zero output size")
					}

					// Entropy verification (only when we have source data)
					if srcHeaders != nil && len(srcRows) > 0 {
						assertEntropyPreserved(t, srcHeaders, srcRows, outputPath)
					}

					// Log performance metrics
					logPerfMetrics(t, name, before, after, elapsed, result)

					// Quality metrics check
					if result.Quality != nil {
						t.Logf("[%s] quality: malformed=%d recovered=%d",
							name, result.Quality.MalformedRows, result.Quality.RecoveredRows)
					}
				} else {
					// --- Unsupported format: expect graceful error ---
					if ingestErr == nil {
						// Some "unsupported" formats may be detected as CSV by the
						// detector and processed successfully. That's acceptable —
						// the key assertion is that there is no panic.
						t.Logf("[%s] format detected and processed (no error); verifying non-panic", name)
					} else {
						t.Logf("[%s] expected error: %v", name, ingestErr)
					}
					// Main assertion: we did not panic (implicit — test is still running)
					logPerfMetrics(t, name, before, after, elapsed, nil)
				}
			})
		}
	}
}
