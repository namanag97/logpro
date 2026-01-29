// +build ignore

package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"
	"github.com/logflow/logflow/pkg/ingest/core"
	"github.com/logflow/logflow/pkg/ingest/decoders"
	"github.com/logflow/logflow/pkg/ingest/sinks"
	"github.com/logflow/logflow/pkg/query/engine"
)

func main() {
	ctx := context.Background()
	xes := "/Users/namanagarwal/Downloads/Process-Mining-Datasets/BPI_Challenge_2012.xes"
	pq := "/tmp/test_xes.parquet"

	dec := decoders.NewXESDecoder()
	src := &fs{xes, core.FormatXES}
	batches, _ := dec.Decode(ctx, src, core.DefaultDecodeOptions())

	var all []core.DecodedBatch
	for b := range batches { if b.Batch != nil { all = append(all, b) } }

	sink := sinks.NewParquetSink()
	opts := core.DefaultSinkOptions()
	opts.Path = pq
	sink.Open(ctx, all[0].Batch.Schema(), opts)
	for _, b := range all { sink.Write(ctx, b.Batch); b.Batch.Release() }
	sink.Close(ctx)

	eng, _ := engine.NewEngine()
	defer eng.Close()

	r, _ := eng.Query(ctx, "DESCRIBE SELECT * FROM '"+pq+"'")
	rows, _ := r.ToMaps()
	fmt.Println("SCHEMA:")
	for _, row := range rows { fmt.Printf("  %s: %s\n", row["column_name"], row["column_type"]) }
	r.Close()

	r, _ = eng.Query(ctx, "SELECT COUNT(*) as events, COUNT(DISTINCT case_id) as cases, COUNT(DISTINCT activity) as activities FROM '"+pq+"'")
	rows, _ = r.ToMaps()
	fmt.Printf("\nSTATS: Events=%v Cases=%v Activities=%v\n", rows[0]["events"], rows[0]["cases"], rows[0]["activities"])
	r.Close()

	r, _ = eng.Query(ctx, "SELECT case_id, activity, timestamp, resource FROM '"+pq+"' LIMIT 3")
	rows, _ = r.ToMaps()
	fmt.Println("\nSAMPLE:")
	for _, row := range rows { fmt.Printf("  %v | %v | %v | %v\n", row["case_id"], row["activity"], row["timestamp"], row["resource"]) }
	r.Close()

	os.Remove(pq)
	fmt.Println("\n✓ Cleaned up")
}

type fs struct { p string; f core.Format }
func (s *fs) ID() string { return s.p }
func (s *fs) Location() string { return s.p }
func (s *fs) Format() core.Format { return s.f }
func (s *fs) Size() int64 { return 0 }
func (s *fs) ModTime() time.Time { return time.Now() }
func (s *fs) Metadata() map[string]string { return nil }
func (s *fs) Open(ctx context.Context) (io.ReadCloser, error) { return os.Open(s.p) }
