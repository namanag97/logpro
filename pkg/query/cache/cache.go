// Package cache provides query result caching.
package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
	"time"
)

// Cache caches query results.
type Cache struct {
	mu       sync.RWMutex
	entries  map[string]*Entry
	maxSize  int
	maxAge   time.Duration
	hits     int64
	misses   int64
}

// Entry represents a cached query result.
type Entry struct {
	Key       string
	Query     string
	Result    interface{}
	CreatedAt time.Time
	ExpiresAt time.Time
	Size      int64
	Hits      int64
}

// NewCache creates a new cache.
func NewCache(maxSize int, maxAge time.Duration) *Cache {
	return &Cache{
		entries: make(map[string]*Entry),
		maxSize: maxSize,
		maxAge:  maxAge,
	}
}

// Get retrieves a cached result.
func (c *Cache) Get(query string) (interface{}, bool) {
	key := c.keyFor(query)

	c.mu.RLock()
	entry, ok := c.entries[key]
	c.mu.RUnlock()

	if !ok {
		c.mu.Lock()
		c.misses++
		c.mu.Unlock()
		return nil, false
	}

	// Check expiration
	if time.Now().After(entry.ExpiresAt) {
		c.mu.Lock()
		delete(c.entries, key)
		c.misses++
		c.mu.Unlock()
		return nil, false
	}

	c.mu.Lock()
	entry.Hits++
	c.hits++
	c.mu.Unlock()

	return entry.Result, true
}

// Put stores a result in the cache.
func (c *Cache) Put(query string, result interface{}, size int64) {
	key := c.keyFor(query)

	c.mu.Lock()
	defer c.mu.Unlock()

	// Evict if at capacity
	if len(c.entries) >= c.maxSize {
		c.evictOldest()
	}

	c.entries[key] = &Entry{
		Key:       key,
		Query:     query,
		Result:    result,
		CreatedAt: time.Now(),
		ExpiresAt: time.Now().Add(c.maxAge),
		Size:      size,
	}
}

// Invalidate removes a specific entry.
func (c *Cache) Invalidate(query string) {
	key := c.keyFor(query)

	c.mu.Lock()
	delete(c.entries, key)
	c.mu.Unlock()
}

// InvalidateAll clears the cache.
func (c *Cache) InvalidateAll() {
	c.mu.Lock()
	c.entries = make(map[string]*Entry)
	c.mu.Unlock()
}

// Stats returns cache statistics.
func (c *Cache) Stats() Stats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var totalSize int64
	for _, e := range c.entries {
		totalSize += e.Size
	}

	return Stats{
		Entries:   len(c.entries),
		TotalSize: totalSize,
		Hits:      c.hits,
		Misses:    c.misses,
		HitRate:   c.hitRate(),
	}
}

func (c *Cache) hitRate() float64 {
	total := c.hits + c.misses
	if total == 0 {
		return 0
	}
	return float64(c.hits) / float64(total)
}

func (c *Cache) keyFor(query string) string {
	hash := sha256.Sum256([]byte(query))
	return hex.EncodeToString(hash[:])
}

func (c *Cache) evictOldest() {
	var oldest *Entry
	var oldestKey string

	for key, entry := range c.entries {
		if oldest == nil || entry.CreatedAt.Before(oldest.CreatedAt) {
			oldest = entry
			oldestKey = key
		}
	}

	if oldestKey != "" {
		delete(c.entries, oldestKey)
	}
}

// Stats contains cache statistics.
type Stats struct {
	Entries   int
	TotalSize int64
	Hits      int64
	Misses    int64
	HitRate   float64
}

// MetadataCache caches table metadata.
type MetadataCache struct {
	mu      sync.RWMutex
	schemas map[string]*SchemaCacheEntry
	stats   map[string]*TableStatsCacheEntry
	maxAge  time.Duration
}

// SchemaCacheEntry caches a table schema.
type SchemaCacheEntry struct {
	Columns   []string
	Types     []string
	CachedAt  time.Time
	ExpiresAt time.Time
}

// TableStatsCacheEntry caches table statistics.
type TableStatsCacheEntry struct {
	RowCount  int64
	SizeBytes int64
	CachedAt  time.Time
	ExpiresAt time.Time
}

// NewMetadataCache creates a new metadata cache.
func NewMetadataCache(maxAge time.Duration) *MetadataCache {
	return &MetadataCache{
		schemas: make(map[string]*SchemaCacheEntry),
		stats:   make(map[string]*TableStatsCacheEntry),
		maxAge:  maxAge,
	}
}

// GetSchema retrieves cached schema.
func (c *MetadataCache) GetSchema(table string) (*SchemaCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.schemas[table]
	if !ok || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry, true
}

// PutSchema caches a schema.
func (c *MetadataCache) PutSchema(table string, columns, types []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.schemas[table] = &SchemaCacheEntry{
		Columns:   columns,
		Types:     types,
		CachedAt:  time.Now(),
		ExpiresAt: time.Now().Add(c.maxAge),
	}
}

// GetStats retrieves cached table stats.
func (c *MetadataCache) GetStats(table string) (*TableStatsCacheEntry, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	entry, ok := c.stats[table]
	if !ok || time.Now().After(entry.ExpiresAt) {
		return nil, false
	}
	return entry, true
}

// PutStats caches table stats.
func (c *MetadataCache) PutStats(table string, rowCount, sizeBytes int64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.stats[table] = &TableStatsCacheEntry{
		RowCount:  rowCount,
		SizeBytes: sizeBytes,
		CachedAt:  time.Now(),
		ExpiresAt: time.Now().Add(c.maxAge),
	}
}

// InvalidateTable invalidates all cached data for a table.
func (c *MetadataCache) InvalidateTable(table string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.schemas, table)
	delete(c.stats, table)
}
