package decoders

import (
	"context"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/memory"
	"github.com/apache/arrow/go/v14/parquet/file"
	"github.com/apache/arrow/go/v14/parquet/pqarrow"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// ParquetDecoder reads Parquet files and emits Arrow RecordBatches.
type ParquetDecoder struct {
	alloc memory.Allocator
}

// NewParquetDecoder creates a new Parquet decoder.
func NewParquetDecoder() *ParquetDecoder {
	return &ParquetDecoder{
		alloc: memory.DefaultAllocator,
	}
}

// Formats returns supported formats.
func (d *ParquetDecoder) Formats() []core.Format {
	return []core.Format{core.FormatParquet}
}

// Decode reads a Parquet file and emits Arrow batches.
func (d *ParquetDecoder) Decode(ctx context.Context, source core.Source, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	// For Parquet, we need a seekable reader
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to open source: %w", err)
	}

	// Get size for Parquet reader
	var size int64
	if seeker, ok := reader.(io.Seeker); ok {
		size, _ = seeker.Seek(0, io.SeekEnd)
		seeker.Seek(0, io.SeekStart)
	} else {
		size = source.Size()
	}

	// Create Parquet file reader
	pqReader, err := file.NewParquetReader(readerAtAdapter{reader, size})
	if err != nil {
		reader.Close()
		return nil, fmt.Errorf("failed to create parquet reader: %w", err)
	}

	// Create Arrow file reader
	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{
		Parallel:  true,
		BatchSize: int64(opts.BatchSize),
	}, d.alloc)
	if err != nil {
		pqReader.Close()
		reader.Close()
		return nil, fmt.Errorf("failed to create arrow reader: %w", err)
	}

	out := make(chan core.DecodedBatch, 4)

	go func() {
		defer close(out)
		defer reader.Close()
		defer pqReader.Close()

		schema, err := arrowReader.Schema()
		if err != nil {
			return
		}

		batchSize := opts.BatchSize
		if batchSize <= 0 {
			batchSize = 8192
		}

		// Read entire table
		table, err := arrowReader.ReadTable(ctx)
		if err != nil {
			return
		}
		defer table.Release()

		// Convert table to record batches
		tableReader := array.NewTableReader(table, int64(batchSize))
		defer tableReader.Release()

		batchIndex := 0
		for tableReader.Next() {
			select {
			case <-ctx.Done():
				return
			default:
			}

			record := tableReader.Record()
			record.Retain() // Keep alive for sending

			select {
			case out <- core.DecodedBatch{
				Batch: record,
				Index: batchIndex,
			}:
				batchIndex++
			case <-ctx.Done():
				record.Release()
				return
			}
		}

		// Mark last batch
		_ = schema
	}()

	return out, nil
}

// InferSchema returns the Parquet file schema.
func (d *ParquetDecoder) InferSchema(ctx context.Context, source core.Source, sampleSize int) (*arrow.Schema, error) {
	reader, err := source.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	var size int64
	if seeker, ok := reader.(io.Seeker); ok {
		size, _ = seeker.Seek(0, io.SeekEnd)
		seeker.Seek(0, io.SeekStart)
	} else {
		size = source.Size()
	}

	pqReader, err := file.NewParquetReader(readerAtAdapter{reader, size})
	if err != nil {
		return nil, err
	}
	defer pqReader.Close()

	arrowReader, err := pqarrow.NewFileReader(pqReader, pqarrow.ArrowReadProperties{}, d.alloc)
	if err != nil {
		return nil, err
	}

	return arrowReader.Schema()
}

// readerAtAdapter adapts io.Reader to parquet.ReaderAtSeeker
type readerAtAdapter struct {
	reader io.Reader
	size   int64
}

func (r readerAtAdapter) ReadAt(p []byte, off int64) (n int, err error) {
	if seeker, ok := r.reader.(io.Seeker); ok {
		_, err = seeker.Seek(off, io.SeekStart)
		if err != nil {
			return 0, err
		}
	}
	return r.reader.Read(p)
}

func (r readerAtAdapter) Read(p []byte) (n int, err error) {
	return r.reader.Read(p)
}

func (r readerAtAdapter) Seek(offset int64, whence int) (int64, error) {
	if seeker, ok := r.reader.(io.Seeker); ok {
		return seeker.Seek(offset, whence)
	}
	return 0, fmt.Errorf("not seekable")
}

func (r readerAtAdapter) Size() int64 {
	return r.size
}

func (r readerAtAdapter) Close() error {
	if closer, ok := r.reader.(io.Closer); ok {
		return closer.Close()
	}
	return nil
}

// DecodeFile decodes a Parquet file by path.
func (d *ParquetDecoder) DecodeFile(ctx context.Context, path string, opts core.DecodeOptions) (<-chan core.DecodedBatch, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	info, _ := f.Stat()

	source := &parquetFileSource{
		path:   path,
		file:   f,
		size:   info.Size(),
		format: core.FormatParquet,
	}

	return d.Decode(ctx, source, opts)
}

type parquetFileSource struct {
	path   string
	file   *os.File
	size   int64
	format core.Format
}

func (s *parquetFileSource) ID() string                  { return s.path }
func (s *parquetFileSource) Location() string            { return s.path }
func (s *parquetFileSource) Format() core.Format         { return s.format }
func (s *parquetFileSource) Size() int64                 { return s.size }
func (s *parquetFileSource) ModTime() time.Time          { return time.Now() }
func (s *parquetFileSource) Metadata() map[string]string { return nil }
func (s *parquetFileSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(s.path)
}

// Import array package for TableReader
var _ = array.NewTableReader

func init() {
	Register(NewParquetDecoder())
}
