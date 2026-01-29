// Package storage provides unified access to local and cloud storage.
// Supports: local files, S3, GCS, Azure Blob, HTTP(S) URLs
package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// Storage provides a unified interface for reading/writing data.
type Storage interface {
	// Reader returns a reader for the given path.
	Reader(ctx context.Context, path string) (io.ReadCloser, int64, error)

	// Writer returns a writer for the given path.
	Writer(ctx context.Context, path string) (io.WriteCloser, error)

	// Stat returns file info.
	Stat(ctx context.Context, path string) (*FileInfo, error)

	// Scheme returns the storage scheme (file, s3, gs, az, http).
	Scheme() string
}

// FileInfo holds file metadata.
type FileInfo struct {
	Path    string
	Size    int64
	ModTime int64
	IsDir   bool
}

// Open returns appropriate storage for a path.
func Open(path string) (Storage, string, error) {
	u, err := url.Parse(path)
	if err != nil || u.Scheme == "" || len(u.Scheme) == 1 {
		// Local file (or Windows drive letter)
		return &LocalStorage{}, path, nil
	}

	switch u.Scheme {
	case "file":
		return &LocalStorage{}, u.Path, nil
	case "s3":
		return NewS3Storage(u.Host)
	case "gs":
		return NewGCSStorage(u.Host)
	case "az", "azure":
		return NewAzureStorage(u.Host)
	case "http", "https":
		return &HTTPStorage{}, path, nil
	default:
		return nil, "", fmt.Errorf("unsupported storage scheme: %s", u.Scheme)
	}
}

// ParsePath extracts bucket and key from cloud URLs.
func ParsePath(path string) (scheme, bucket, key string) {
	u, err := url.Parse(path)
	if err != nil {
		return "file", "", path
	}

	return u.Scheme, u.Host, strings.TrimPrefix(u.Path, "/")
}

// --- Local Storage ---

// LocalStorage handles local file operations.
type LocalStorage struct{}

func (s *LocalStorage) Scheme() string { return "file" }

func (s *LocalStorage) Reader(ctx context.Context, path string) (io.ReadCloser, int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, 0, err
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, 0, err
	}

	return f, info.Size(), nil
}

func (s *LocalStorage) Writer(ctx context.Context, path string) (io.WriteCloser, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, err
	}
	return os.Create(path)
}

func (s *LocalStorage) Stat(ctx context.Context, path string) (*FileInfo, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	return &FileInfo{
		Path:    path,
		Size:    info.Size(),
		ModTime: info.ModTime().Unix(),
		IsDir:   info.IsDir(),
	}, nil
}

// --- HTTP Storage (Read-Only) ---

// HTTPStorage handles HTTP/HTTPS URLs (read-only).
type HTTPStorage struct{}

func (s *HTTPStorage) Scheme() string { return "http" }

func (s *HTTPStorage) Reader(ctx context.Context, path string) (io.ReadCloser, int64, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", path, nil)
	if err != nil {
		return nil, 0, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, 0, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, 0, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return resp.Body, resp.ContentLength, nil
}

func (s *HTTPStorage) Writer(ctx context.Context, path string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("HTTP storage is read-only")
}

func (s *HTTPStorage) Stat(ctx context.Context, path string) (*FileInfo, error) {
	req, err := http.NewRequestWithContext(ctx, "HEAD", path, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, resp.Status)
	}

	return &FileInfo{
		Path: path,
		Size: resp.ContentLength,
	}, nil
}

// --- S3 Storage (Stub - implement with AWS SDK) ---

// S3Storage handles S3 operations.
type S3Storage struct {
	bucket string
}

func NewS3Storage(bucket string) (*S3Storage, string, error) {
	return &S3Storage{bucket: bucket}, "", nil
}

func (s *S3Storage) Scheme() string { return "s3" }

func (s *S3Storage) Reader(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	// TODO: Implement with AWS SDK
	// For now, use DuckDB's httpfs extension which supports S3
	return nil, 0, fmt.Errorf("S3 native reader not yet implemented - use DuckDB engine")
}

func (s *S3Storage) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("S3 native writer not yet implemented - use DuckDB engine")
}

func (s *S3Storage) Stat(ctx context.Context, key string) (*FileInfo, error) {
	return nil, fmt.Errorf("S3 stat not yet implemented")
}

// --- GCS Storage (Stub) ---

// GCSStorage handles Google Cloud Storage operations.
type GCSStorage struct {
	bucket string
}

func NewGCSStorage(bucket string) (*GCSStorage, string, error) {
	return &GCSStorage{bucket: bucket}, "", nil
}

func (s *GCSStorage) Scheme() string { return "gs" }

func (s *GCSStorage) Reader(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return nil, 0, fmt.Errorf("GCS not yet implemented")
}

func (s *GCSStorage) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("GCS not yet implemented")
}

func (s *GCSStorage) Stat(ctx context.Context, key string) (*FileInfo, error) {
	return nil, fmt.Errorf("GCS not yet implemented")
}

// --- Azure Storage (Stub) ---

// AzureStorage handles Azure Blob Storage operations.
type AzureStorage struct {
	container string
}

func NewAzureStorage(container string) (*AzureStorage, string, error) {
	return &AzureStorage{container: container}, "", nil
}

func (s *AzureStorage) Scheme() string { return "az" }

func (s *AzureStorage) Reader(ctx context.Context, key string) (io.ReadCloser, int64, error) {
	return nil, 0, fmt.Errorf("Azure not yet implemented")
}

func (s *AzureStorage) Writer(ctx context.Context, key string) (io.WriteCloser, error) {
	return nil, fmt.Errorf("Azure not yet implemented")
}

func (s *AzureStorage) Stat(ctx context.Context, key string) (*FileInfo, error) {
	return nil, fmt.Errorf("Azure not yet implemented")
}
