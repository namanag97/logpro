package interfaces

import (
	"context"
	"io"
	"time"
)

// ObjectStorage provides object storage operations for data files.
type ObjectStorage interface {
	// Basic operations
	Put(ctx context.Context, path string, data io.Reader, opts PutOptions) error
	Get(ctx context.Context, path string) (io.ReadCloser, error)
	Delete(ctx context.Context, path string) error
	Exists(ctx context.Context, path string) (bool, error)

	// Listing
	List(ctx context.Context, prefix string, opts ListOptions) ([]ObjectInfo, error)
	ListPaginated(ctx context.Context, prefix string, opts ListOptions) ObjectIterator

	// Metadata
	Head(ctx context.Context, path string) (ObjectInfo, error)

	// Bulk operations
	DeleteMany(ctx context.Context, paths []string) error
	Copy(ctx context.Context, src, dst string) error

	// Scheme returns the storage scheme (e.g., "file", "s3", "gs", "az").
	Scheme() string
}

// ObjectInfo contains metadata about a stored object.
type ObjectInfo struct {
	Path         string
	Size         int64
	LastModified time.Time
	ETag         string
	ContentType  string
	Metadata     map[string]string
	IsDir        bool
}

// PutOptions configures write operations.
type PutOptions struct {
	ContentType string
	Metadata    map[string]string
	// If set, the write will fail if the object already exists.
	IfNotExists bool
}

// ListOptions configures listing operations.
type ListOptions struct {
	// MaxKeys limits the number of results returned.
	MaxKeys int
	// Delimiter groups keys that contain the same string between prefix and delimiter.
	Delimiter string
	// StartAfter returns keys after this value (for pagination).
	StartAfter string
	// Recursive lists all objects recursively (ignores Delimiter).
	Recursive bool
}

// ObjectIterator provides paginated iteration over objects.
type ObjectIterator interface {
	// Next returns the next object, or io.EOF when done.
	Next() (ObjectInfo, error)
	// Close releases resources.
	Close() error
}

// ReadSeekCloser combines io.ReadSeeker and io.Closer.
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	io.Closer
}
