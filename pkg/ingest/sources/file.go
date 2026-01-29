// Package sources provides Source implementations for various input types.
package sources

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// FileSource implements Source for local files.
type FileSource struct {
	path    string
	info    os.FileInfo
	format  core.Format
	meta    map[string]string
}

// NewFileSource creates a new file source.
func NewFileSource(path string) (*FileSource, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}

	format := detectFormatFromPath(path)

	return &FileSource{
		path:   path,
		info:   info,
		format: format,
		meta:   make(map[string]string),
	}, nil
}

func (f *FileSource) ID() string       { return f.path }
func (f *FileSource) Location() string { return f.path }
func (f *FileSource) Format() core.Format   { return f.format }
func (f *FileSource) Size() int64      { return f.info.Size() }
func (f *FileSource) ModTime() time.Time    { return f.info.ModTime() }
func (f *FileSource) Metadata() map[string]string { return f.meta }

// Open returns a reader for the file.
func (f *FileSource) Open(ctx context.Context) (io.ReadCloser, error) {
	return os.Open(f.path)
}

// detectFormatFromPath guesses format from file extension.
func detectFormatFromPath(path string) core.Format {
	ext := strings.ToLower(filepath.Ext(path))

	// Handle .gz suffix
	if ext == ".gz" {
		base := strings.TrimSuffix(path, ".gz")
		ext = strings.ToLower(filepath.Ext(base))
		if ext == "" {
			return core.FormatGzip
		}
	}

	switch ext {
	case ".csv":
		return core.FormatCSV
	case ".tsv":
		return core.FormatTSV
	case ".json":
		return core.FormatJSON
	case ".jsonl", ".ndjson":
		return core.FormatJSONL
	case ".xes":
		return core.FormatXES
	case ".xml":
		return core.FormatXML
	case ".xlsx", ".xls":
		return core.FormatXLSX
	case ".parquet":
		return core.FormatParquet
	case ".orc":
		return core.FormatORC
	case ".avro":
		return core.FormatAvro
	default:
		return core.FormatUnknown
	}
}

// FileSourceOptions configures file source behavior.
type FileSourceOptions struct {
	// BufferSize for reading.
	BufferSize int

	// FollowSymlinks follows symbolic links.
	FollowSymlinks bool
}

// DefaultFileSourceOptions returns sensible defaults.
func DefaultFileSourceOptions() FileSourceOptions {
	return FileSourceOptions{
		BufferSize:     256 * 1024,
		FollowSymlinks: true,
	}
}
