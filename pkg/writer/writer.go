// Package writer provides Arrow/Parquet output functionality.
package writer

import (
	"context"
	"io"

	"github.com/logflow/logflow/internal/model"
)

// Writer defines the interface for writing events to an output format.
type Writer interface {
	// Write writes a batch of events.
	Write(ctx context.Context, events <-chan *model.Event) error

	// Flush flushes any buffered data.
	Flush() error

	// Close closes the writer and releases resources.
	Close() error
}

// Config holds writer configuration.
type Config struct {
	// BatchSize is the number of events per record batch.
	BatchSize int

	// Compression type for Parquet output.
	Compression CompressionType

	// RowGroupSize is the number of rows per Parquet row group.
	RowGroupSize int64

	// Output is the destination writer.
	Output io.Writer
}

// CompressionType represents Parquet compression options.
type CompressionType uint8

const (
	CompressionNone CompressionType = iota
	CompressionSnappy
	CompressionGzip
	CompressionZstd
	CompressionLZ4
)

// String returns the compression type name.
func (c CompressionType) String() string {
	switch c {
	case CompressionSnappy:
		return "snappy"
	case CompressionGzip:
		return "gzip"
	case CompressionZstd:
		return "zstd"
	case CompressionLZ4:
		return "lz4"
	default:
		return "none"
	}
}

// ParseCompression parses a compression type string.
func ParseCompression(s string) CompressionType {
	switch s {
	case "snappy":
		return CompressionSnappy
	case "gzip":
		return CompressionGzip
	case "zstd":
		return CompressionZstd
	case "lz4":
		return CompressionLZ4
	default:
		return CompressionNone
	}
}

// DefaultConfig returns a Config with sensible defaults.
// RowGroupSize is set to 128MB as recommended by Parquet best practices.
// See: https://parquet.apache.org/docs/file-format/configurations/
func DefaultConfig() Config {
	return Config{
		BatchSize:    8192,                  // Larger batches for better throughput
		Compression:  CompressionSnappy,
		RowGroupSize: 128 * 1024 * 1024,     // 128MB row groups (Parquet default)
	}
}
