// Package core provides the fundamental abstractions for the ingestion pipeline.
package core

import (
	"context"
	"io"
	"time"
)

// Format represents the detected file format.
type Format uint8

const (
	FormatUnknown Format = iota
	FormatCSV
	FormatTSV
	FormatJSON
	FormatJSONL
	FormatXES
	FormatXML
	FormatXLSX
	FormatParquet
	FormatORC
	FormatAvro
	FormatGzip
)

func (f Format) String() string {
	names := []string{"unknown", "csv", "tsv", "json", "jsonl", "xes", "xml", "xlsx", "parquet", "orc", "avro", "gzip"}
	if int(f) < len(names) {
		return names[f]
	}
	return "unknown"
}

// Bucket returns the processing cost bucket for this format.
func (f Format) Bucket() SourceBucket {
	switch f {
	case FormatParquet, FormatORC, FormatAvro:
		return BucketNative
	case FormatCSV, FormatTSV, FormatJSONL:
		return BucketCheap
	case FormatJSON, FormatXML, FormatXES:
		return BucketMedium
	case FormatXLSX:
		return BucketExpensive
	default:
		return BucketMedium
	}
}

// SourceBucket categorizes sources by processing cost.
type SourceBucket int

const (
	BucketNative    SourceBucket = iota // Parquet, ORC, Avro
	BucketCheap                         // CSV, TSV, JSONL
	BucketMedium                        // JSON, XML
	BucketExpensive                     // Excel, PDF
)

// Source represents a data source for ingestion.
type Source interface {
	// ID returns a unique identifier for this source.
	ID() string

	// Location returns the source location (path, URL, etc.).
	Location() string

	// Format returns the detected format.
	Format() Format

	// Size returns the size in bytes, or -1 if unknown.
	Size() int64

	// ModTime returns the last modification time.
	ModTime() time.Time

	// Open returns a reader for the source content.
	Open(ctx context.Context) (io.ReadCloser, error)

	// Metadata returns source-specific metadata.
	Metadata() map[string]string
}

// SourceInfo contains basic source information.
type SourceInfo struct {
	ID_       string
	Location_ string
	Format_   Format
	Size_     int64
	ModTime_  time.Time
	Meta      map[string]string
}

func (s *SourceInfo) ID() string                  { return s.ID_ }
func (s *SourceInfo) Location() string            { return s.Location_ }
func (s *SourceInfo) Format() Format              { return s.Format_ }
func (s *SourceInfo) Size() int64                 { return s.Size_ }
func (s *SourceInfo) ModTime() time.Time          { return s.ModTime_ }
func (s *SourceInfo) Metadata() map[string]string { return s.Meta }

// MultiSource combines multiple sources.
type MultiSource interface {
	// Sources returns all sources.
	Sources() []Source

	// TotalSize returns total size across all sources.
	TotalSize() int64

	// Count returns number of sources.
	Count() int
}
