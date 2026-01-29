// Package object provides object storage implementations.
package object

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/logflow/logflow/pkg/interfaces"
)

// LocalStorage implements ObjectStorage for local filesystem.
type LocalStorage struct {
	root string
}

// NewLocalStorage creates a new local filesystem storage.
func NewLocalStorage(root string) (*LocalStorage, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("failed to resolve root path: %w", err)
	}

	if err := os.MkdirAll(absRoot, 0755); err != nil {
		return nil, fmt.Errorf("failed to create root directory: %w", err)
	}

	return &LocalStorage{root: absRoot}, nil
}

// Scheme returns "file".
func (s *LocalStorage) Scheme() string {
	return "file"
}

// Put writes data to a path.
func (s *LocalStorage) Put(ctx context.Context, path string, data io.Reader, opts interfaces.PutOptions) error {
	fullPath := s.fullPath(path)

	// Create parent directories
	if err := os.MkdirAll(filepath.Dir(fullPath), 0755); err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	// Check if exists when IfNotExists is set
	if opts.IfNotExists {
		if _, err := os.Stat(fullPath); err == nil {
			return fmt.Errorf("object already exists: %s", path)
		}
	}

	f, err := os.Create(fullPath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}
	defer f.Close()

	if _, err := io.Copy(f, data); err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}

	return nil
}

// Get returns a reader for the object.
func (s *LocalStorage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	fullPath := s.fullPath(path)
	f, err := os.Open(fullPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return f, nil
}

// Delete removes an object.
func (s *LocalStorage) Delete(ctx context.Context, path string) error {
	fullPath := s.fullPath(path)
	if err := os.Remove(fullPath); err != nil {
		return fmt.Errorf("failed to delete file: %w", err)
	}
	return nil
}

// Exists checks if an object exists.
func (s *LocalStorage) Exists(ctx context.Context, path string) (bool, error) {
	fullPath := s.fullPath(path)
	_, err := os.Stat(fullPath)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// List lists objects with a prefix.
func (s *LocalStorage) List(ctx context.Context, prefix string, opts interfaces.ListOptions) ([]interfaces.ObjectInfo, error) {
	var results []interfaces.ObjectInfo
	prefixPath := s.fullPath(prefix)

	err := filepath.Walk(s.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil // Skip errors
		}

		relPath, _ := filepath.Rel(s.root, path)
		if !strings.HasPrefix(relPath, prefix) && prefix != "" {
			return nil
		}

		// Apply delimiter filtering
		if opts.Delimiter != "" && !opts.Recursive {
			remaining := strings.TrimPrefix(relPath, prefix)
			if idx := strings.Index(remaining, opts.Delimiter); idx >= 0 {
				// This is a "directory" - skip if we've already seen it
				return nil
			}
		}

		results = append(results, interfaces.ObjectInfo{
			Path:         relPath,
			Size:         info.Size(),
			LastModified: info.ModTime(),
			IsDir:        info.IsDir(),
		})

		if opts.MaxKeys > 0 && len(results) >= opts.MaxKeys {
			return filepath.SkipAll
		}

		return nil
	})

	if err != nil && err != filepath.SkipAll {
		return nil, err
	}

	// Sort by path
	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})

	_ = prefixPath // Avoid unused variable
	return results, nil
}

// ListPaginated returns a paginated iterator.
func (s *LocalStorage) ListPaginated(ctx context.Context, prefix string, opts interfaces.ListOptions) interfaces.ObjectIterator {
	return &localIterator{
		storage: s,
		prefix:  prefix,
		opts:    opts,
	}
}

// Head returns object metadata.
func (s *LocalStorage) Head(ctx context.Context, path string) (interfaces.ObjectInfo, error) {
	fullPath := s.fullPath(path)
	info, err := os.Stat(fullPath)
	if err != nil {
		return interfaces.ObjectInfo{}, fmt.Errorf("failed to stat file: %w", err)
	}

	return interfaces.ObjectInfo{
		Path:         path,
		Size:         info.Size(),
		LastModified: info.ModTime(),
		IsDir:        info.IsDir(),
	}, nil
}

// DeleteMany deletes multiple objects.
func (s *LocalStorage) DeleteMany(ctx context.Context, paths []string) error {
	for _, path := range paths {
		if err := s.Delete(ctx, path); err != nil {
			return err
		}
	}
	return nil
}

// Copy copies an object.
func (s *LocalStorage) Copy(ctx context.Context, src, dst string) error {
	srcFile, err := s.Get(ctx, src)
	if err != nil {
		return err
	}
	defer srcFile.Close()

	return s.Put(ctx, dst, srcFile, interfaces.PutOptions{})
}

func (s *LocalStorage) fullPath(path string) string {
	return filepath.Join(s.root, path)
}

// localIterator implements ObjectIterator for local storage.
type localIterator struct {
	storage *LocalStorage
	prefix  string
	opts    interfaces.ListOptions
	items   []interfaces.ObjectInfo
	index   int
	loaded  bool
}

func (it *localIterator) Next() (interfaces.ObjectInfo, error) {
	if !it.loaded {
		items, err := it.storage.List(context.Background(), it.prefix, it.opts)
		if err != nil {
			return interfaces.ObjectInfo{}, err
		}
		it.items = items
		it.loaded = true
	}

	if it.index >= len(it.items) {
		return interfaces.ObjectInfo{}, io.EOF
	}

	item := it.items[it.index]
	it.index++
	return item, nil
}

func (it *localIterator) Close() error {
	return nil
}

// MemoryStorage implements ObjectStorage in memory (for testing).
type MemoryStorage struct {
	objects map[string][]byte
	meta    map[string]interfaces.ObjectInfo
}

// NewMemoryStorage creates a new in-memory storage.
func NewMemoryStorage() *MemoryStorage {
	return &MemoryStorage{
		objects: make(map[string][]byte),
		meta:    make(map[string]interfaces.ObjectInfo),
	}
}

// Scheme returns "memory".
func (s *MemoryStorage) Scheme() string {
	return "memory"
}

// Put stores data in memory.
func (s *MemoryStorage) Put(ctx context.Context, path string, data io.Reader, opts interfaces.PutOptions) error {
	if opts.IfNotExists {
		if _, ok := s.objects[path]; ok {
			return fmt.Errorf("object already exists: %s", path)
		}
	}

	bytes, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	s.objects[path] = bytes
	s.meta[path] = interfaces.ObjectInfo{
		Path:         path,
		Size:         int64(len(bytes)),
		LastModified: time.Now(),
		ContentType:  opts.ContentType,
		Metadata:     opts.Metadata,
	}
	return nil
}

// Get returns a reader for the object.
func (s *MemoryStorage) Get(ctx context.Context, path string) (io.ReadCloser, error) {
	data, ok := s.objects[path]
	if !ok {
		return nil, fmt.Errorf("object not found: %s", path)
	}
	return io.NopCloser(strings.NewReader(string(data))), nil
}

// Delete removes an object.
func (s *MemoryStorage) Delete(ctx context.Context, path string) error {
	delete(s.objects, path)
	delete(s.meta, path)
	return nil
}

// Exists checks if an object exists.
func (s *MemoryStorage) Exists(ctx context.Context, path string) (bool, error) {
	_, ok := s.objects[path]
	return ok, nil
}

// List lists objects with a prefix.
func (s *MemoryStorage) List(ctx context.Context, prefix string, opts interfaces.ListOptions) ([]interfaces.ObjectInfo, error) {
	var results []interfaces.ObjectInfo
	for path, info := range s.meta {
		if strings.HasPrefix(path, prefix) {
			results = append(results, info)
		}
	}
	sort.Slice(results, func(i, j int) bool {
		return results[i].Path < results[j].Path
	})
	return results, nil
}

// ListPaginated returns a paginated iterator.
func (s *MemoryStorage) ListPaginated(ctx context.Context, prefix string, opts interfaces.ListOptions) interfaces.ObjectIterator {
	items, _ := s.List(ctx, prefix, opts)
	return &memoryIterator{items: items}
}

// Head returns object metadata.
func (s *MemoryStorage) Head(ctx context.Context, path string) (interfaces.ObjectInfo, error) {
	info, ok := s.meta[path]
	if !ok {
		return interfaces.ObjectInfo{}, fmt.Errorf("object not found: %s", path)
	}
	return info, nil
}

// DeleteMany deletes multiple objects.
func (s *MemoryStorage) DeleteMany(ctx context.Context, paths []string) error {
	for _, path := range paths {
		s.Delete(ctx, path)
	}
	return nil
}

// Copy copies an object.
func (s *MemoryStorage) Copy(ctx context.Context, src, dst string) error {
	data, ok := s.objects[src]
	if !ok {
		return fmt.Errorf("source not found: %s", src)
	}
	s.objects[dst] = append([]byte{}, data...)
	s.meta[dst] = s.meta[src]
	s.meta[dst] = interfaces.ObjectInfo{
		Path:         dst,
		Size:         int64(len(data)),
		LastModified: time.Now(),
	}
	return nil
}

type memoryIterator struct {
	items []interfaces.ObjectInfo
	index int
}

func (it *memoryIterator) Next() (interfaces.ObjectInfo, error) {
	if it.index >= len(it.items) {
		return interfaces.ObjectInfo{}, io.EOF
	}
	item := it.items[it.index]
	it.index++
	return item, nil
}

func (it *memoryIterator) Close() error {
	return nil
}

// Verify interface compliance
var (
	_ interfaces.ObjectStorage = (*LocalStorage)(nil)
	_ interfaces.ObjectStorage = (*MemoryStorage)(nil)
)
