// Package util provides utility functions for file operations.
package util

import (
	"compress/gzip"
	"io"
	"os"
	"path/filepath"
	"strings"
)

// OpenFile opens a file, automatically decompressing if it's gzip-compressed.
// Returns the reader, a cleanup function (to close resources), and any error.
// The caller must call the cleanup function when done reading.
func OpenFile(path string) (io.Reader, func() error, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, err
	}

	// Check if file is gzip compressed
	if IsGzipFile(path) {
		gzReader, err := gzip.NewReader(file)
		if err != nil {
			file.Close()
			return nil, nil, err
		}
		cleanup := func() error {
			gzReader.Close()
			return file.Close()
		}
		return gzReader, cleanup, nil
	}

	cleanup := func() error {
		return file.Close()
	}
	return file, cleanup, nil
}

// IsGzipFile returns true if the file path indicates gzip compression.
func IsGzipFile(path string) bool {
	return strings.HasSuffix(strings.ToLower(path), ".gz")
}

// StripCompression removes compression extensions (.gz) from a path.
func StripCompression(path string) string {
	lower := strings.ToLower(path)
	if strings.HasSuffix(lower, ".gz") {
		return path[:len(path)-3]
	}
	return path
}

// BaseFormat extracts the format extension after stripping compression.
// e.g., "file.xes.gz" -> ".xes", "file.csv" -> ".csv"
func BaseFormat(path string) string {
	stripped := StripCompression(path)
	return strings.ToLower(filepath.Ext(stripped))
}
