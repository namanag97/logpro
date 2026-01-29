package core

import (
	"context"
	"io"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
)

// Sink writes Arrow RecordBatches to a destination.
type Sink interface {
	// Open prepares the sink for writing.
	Open(ctx context.Context, schema *arrow.Schema, opts SinkOptions) error

	// Write writes a batch to the sink.
	Write(ctx context.Context, batch arrow.Record) error

	// Flush forces any buffered data to be written.
	Flush(ctx context.Context) error

	// Close finalizes and closes the sink.
	Close(ctx context.Context) (*SinkResult, error)
}

// SinkOptions configures sink behavior.
type SinkOptions struct {
	// Path is the output location.
	Path string

	// Writer is an optional io.WriteCloser for streaming output.
	Writer io.WriteCloser

	// Compression algorithm.
	Compression Compression

	// CompressionLevel (codec-specific).
	CompressionLevel int

	// RowGroupSize for columnar formats.
	RowGroupSize int

	// PageSize for Parquet.
	PageSize int

	// DictionaryEncoding enables dictionary encoding.
	DictionaryEncoding bool

	// Statistics enables writing column statistics.
	Statistics bool

	// PartitionBy columns for partitioned output.
	PartitionBy []string

	// MaxRowsPerFile for splitting output.
	MaxRowsPerFile int64

	// TargetFileSize for rolling files (Iceberg).
	TargetFileSize int64

	// Metadata to include in output.
	Metadata map[string]string
}

// DefaultSinkOptions returns sensible defaults.
func DefaultSinkOptions() SinkOptions {
	return SinkOptions{
		Compression:        CompressionSnappy,
		CompressionLevel:   3,
		RowGroupSize:       10000,
		PageSize:           1024 * 1024,
		DictionaryEncoding: true,
		Statistics:         true,
	}
}

// SinkResult contains the outcome of sink operations.
type SinkResult struct {
	// Path of the output file(s).
	Path string

	// Paths of all output files (for multi-file output).
	Paths []string

	// RowsWritten total.
	RowsWritten int64

	// BytesWritten total.
	BytesWritten int64

	// FilesWritten count.
	FilesWritten int

	// Duration of write operations.
	Duration time.Duration

	// Checksum of output (if computed).
	Checksum uint64
}

// Compression represents compression algorithms.
type Compression uint8

const (
	CompressionNone   Compression = iota
	CompressionSnappy
	CompressionGzip
	CompressionLZ4
	CompressionZstd
	CompressionBrotli
)

func (c Compression) String() string {
	names := []string{"none", "snappy", "gzip", "lz4", "zstd", "brotli"}
	if int(c) < len(names) {
		return names[c]
	}
	return "unknown"
}

// ParseCompression parses a compression string.
func ParseCompression(s string) Compression {
	switch s {
	case "snappy":
		return CompressionSnappy
	case "gzip":
		return CompressionGzip
	case "lz4":
		return CompressionLZ4
	case "zstd":
		return CompressionZstd
	case "brotli":
		return CompressionBrotli
	case "none", "":
		return CompressionNone
	default:
		return CompressionSnappy
	}
}
