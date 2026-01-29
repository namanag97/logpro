// Package pipeline provides deduplication support for idempotent ingestion.
// Prevents duplicate records when re-running ingestion or recovering from failures.
package pipeline

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
)

// Deduplicator tracks seen records to prevent duplicates.
type Deduplicator struct {
	mu sync.RWMutex

	// Seen keys (in-memory)
	seen map[string]bool

	// Configuration
	keyColumns  []string
	maxEntries  int64
	strategy    DeduplicationStrategy

	// Stats
	totalSeen   int64
	duplicates  int64
}

// DeduplicationStrategy determines how to handle duplicates.
type DeduplicationStrategy int

const (
	// DeduplicationSkip silently skips duplicates
	DeduplicationSkip DeduplicationStrategy = iota
	// DeduplicationError returns an error on duplicate
	DeduplicationError
	// DeduplicationReplace replaces previous with new value
	DeduplicationReplace
	// DeduplicationKeepFirst keeps first occurrence only
	DeduplicationKeepFirst
)

func (s DeduplicationStrategy) String() string {
	switch s {
	case DeduplicationSkip:
		return "skip"
	case DeduplicationError:
		return "error"
	case DeduplicationReplace:
		return "replace"
	case DeduplicationKeepFirst:
		return "keep_first"
	default:
		return "unknown"
	}
}

// DeduplicationConfig configures deduplication.
type DeduplicationConfig struct {
	// KeyColumns are columns that form the unique key
	KeyColumns []string
	// MaxEntries is maximum entries to track (0 = unlimited, but watch memory)
	MaxEntries int64
	// Strategy determines how to handle duplicates
	Strategy DeduplicationStrategy
}

// NewDeduplicator creates a new deduplicator.
func NewDeduplicator(cfg DeduplicationConfig) *Deduplicator {
	return &Deduplicator{
		seen:       make(map[string]bool),
		keyColumns: cfg.KeyColumns,
		maxEntries: cfg.MaxEntries,
		strategy:   cfg.Strategy,
	}
}

// Check checks if a record is a duplicate.
// Returns (isDuplicate, error).
func (d *Deduplicator) Check(record map[string]interface{}) (bool, error) {
	key := d.computeKey(record)

	d.mu.RLock()
	isDuplicate := d.seen[key]
	d.mu.RUnlock()

	if isDuplicate {
		d.mu.Lock()
		d.duplicates++
		d.mu.Unlock()

		if d.strategy == DeduplicationError {
			return true, fmt.Errorf("duplicate record detected: key=%s", key)
		}
		return true, nil
	}

	return false, nil
}

// Add adds a record to the seen set.
// Returns true if record was added (not a duplicate), false if it was a duplicate.
func (d *Deduplicator) Add(record map[string]interface{}) bool {
	key := d.computeKey(record)

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.seen[key] {
		d.duplicates++
		if d.strategy == DeduplicationKeepFirst {
			return false // Keep existing, skip new
		}
		// For Replace strategy, we still return true to process new
		if d.strategy == DeduplicationReplace {
			return true
		}
		return false
	}

	// Check memory limit
	if d.maxEntries > 0 && int64(len(d.seen)) >= d.maxEntries {
		// Simple eviction: clear oldest half
		// In production, use LRU cache
		d.evict()
	}

	d.seen[key] = true
	d.totalSeen++
	return true
}

// CheckAndAdd combines Check and Add in one operation.
func (d *Deduplicator) CheckAndAdd(record map[string]interface{}) (isDuplicate bool, err error) {
	key := d.computeKey(record)

	d.mu.Lock()
	defer d.mu.Unlock()

	if d.seen[key] {
		d.duplicates++
		if d.strategy == DeduplicationError {
			return true, fmt.Errorf("duplicate record detected: key=%s", key)
		}
		if d.strategy == DeduplicationKeepFirst {
			return true, nil
		}
	}

	// Check memory limit
	if d.maxEntries > 0 && int64(len(d.seen)) >= d.maxEntries {
		d.evict()
	}

	d.seen[key] = true
	d.totalSeen++
	return false, nil
}

func (d *Deduplicator) computeKey(record map[string]interface{}) string {
	if len(d.keyColumns) == 0 {
		// Use all columns if no key columns specified
		return d.hashRecord(record)
	}

	// Build key from specified columns
	var keyParts []byte
	for _, col := range d.keyColumns {
		val := record[col]
		keyParts = append(keyParts, []byte(fmt.Sprintf("%v|", val))...)
	}

	// Hash for fixed-size key
	hash := sha256.Sum256(keyParts)
	return hex.EncodeToString(hash[:16]) // Use first 16 bytes
}

func (d *Deduplicator) hashRecord(record map[string]interface{}) string {
	var parts []byte
	for k, v := range record {
		parts = append(parts, []byte(fmt.Sprintf("%s=%v|", k, v))...)
	}
	hash := sha256.Sum256(parts)
	return hex.EncodeToString(hash[:16])
}

func (d *Deduplicator) evict() {
	// Simple eviction: clear all (in production, use LRU)
	// This is a trade-off between memory and duplicate detection accuracy
	d.seen = make(map[string]bool)
}

// Stats returns deduplication statistics.
type DeduplicationStats struct {
	TotalSeen      int64
	UniqueCount    int64
	DuplicateCount int64
	DuplicateRate  float64
	MemoryEntries  int64
}

// Stats returns current statistics.
func (d *Deduplicator) Stats() DeduplicationStats {
	d.mu.RLock()
	defer d.mu.RUnlock()

	uniqueCount := int64(len(d.seen))
	var rate float64
	if d.totalSeen > 0 {
		rate = float64(d.duplicates) / float64(d.totalSeen) * 100
	}

	return DeduplicationStats{
		TotalSeen:      d.totalSeen,
		UniqueCount:    uniqueCount,
		DuplicateCount: d.duplicates,
		DuplicateRate:  rate,
		MemoryEntries:  uniqueCount,
	}
}

// Reset clears all tracked records.
func (d *Deduplicator) Reset() {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.seen = make(map[string]bool)
	d.totalSeen = 0
	d.duplicates = 0
}

// Contains checks if a key has been seen without adding it.
func (d *Deduplicator) Contains(record map[string]interface{}) bool {
	key := d.computeKey(record)

	d.mu.RLock()
	defer d.mu.RUnlock()

	return d.seen[key]
}

// --- Batch Deduplication ---

// DeduplicateBatch removes duplicates from a batch of records.
// Returns deduplicated records and list of duplicate indices.
func DeduplicateBatch(records []map[string]interface{}, keyColumns []string) ([]map[string]interface{}, []int) {
	seen := make(map[string]int) // key -> first index
	var result []map[string]interface{}
	var duplicateIndices []int

	for i, record := range records {
		key := computeBatchKey(record, keyColumns)

		if _, exists := seen[key]; exists {
			duplicateIndices = append(duplicateIndices, i)
			continue
		}

		seen[key] = i
		result = append(result, record)
	}

	return result, duplicateIndices
}

func computeBatchKey(record map[string]interface{}, keyColumns []string) string {
	if len(keyColumns) == 0 {
		// Use all columns
		var parts []byte
		for k, v := range record {
			parts = append(parts, []byte(fmt.Sprintf("%s=%v|", k, v))...)
		}
		hash := sha256.Sum256(parts)
		return hex.EncodeToString(hash[:])
	}

	var keyParts []byte
	for _, col := range keyColumns {
		val := record[col]
		keyParts = append(keyParts, []byte(fmt.Sprintf("%v|", val))...)
	}

	hash := sha256.Sum256(keyParts)
	return hex.EncodeToString(hash[:])
}

// --- Bloom Filter for Memory-Efficient Deduplication ---

// BloomDeduplicator uses a bloom filter for memory-efficient deduplication.
// Trade-off: may have false positives (mark non-duplicate as duplicate) but never false negatives.
type BloomDeduplicator struct {
	mu sync.RWMutex

	filter     []uint64
	numBits    uint64
	numHashes  int
	keyColumns []string

	// Stats
	additions  int64
	queries    int64
	positives  int64
}

// NewBloomDeduplicator creates a bloom filter deduplicator.
// expectedItems is the expected number of unique items.
// falsePositiveRate is the acceptable false positive rate (e.g., 0.01 = 1%).
func NewBloomDeduplicator(expectedItems int64, falsePositiveRate float64, keyColumns []string) *BloomDeduplicator {
	// Calculate optimal parameters
	// m = -n * ln(p) / (ln(2)^2)
	// k = (m/n) * ln(2)
	numBits := uint64(float64(-expectedItems) * 2.303 / (0.480 * falsePositiveRate))
	numHashes := int(float64(numBits) / float64(expectedItems) * 0.693)

	if numHashes < 1 {
		numHashes = 1
	}
	if numHashes > 10 {
		numHashes = 10
	}

	// Round up to multiple of 64
	numBits = ((numBits + 63) / 64) * 64

	return &BloomDeduplicator{
		filter:     make([]uint64, numBits/64),
		numBits:    numBits,
		numHashes:  numHashes,
		keyColumns: keyColumns,
	}
}

// Add adds a record to the bloom filter.
func (b *BloomDeduplicator) Add(record map[string]interface{}) {
	key := computeBatchKey(record, b.keyColumns)
	hashes := b.computeHashes(key)

	b.mu.Lock()
	defer b.mu.Unlock()

	for _, h := range hashes {
		idx := h % b.numBits
		b.filter[idx/64] |= 1 << (idx % 64)
	}
	b.additions++
}

// MayContain returns true if the record might be in the set.
// False positives are possible, but false negatives are not.
func (b *BloomDeduplicator) MayContain(record map[string]interface{}) bool {
	key := computeBatchKey(record, b.keyColumns)
	hashes := b.computeHashes(key)

	b.mu.RLock()
	defer b.mu.RUnlock()

	b.queries++

	for _, h := range hashes {
		idx := h % b.numBits
		if b.filter[idx/64]&(1<<(idx%64)) == 0 {
			return false
		}
	}

	b.positives++
	return true
}

func (b *BloomDeduplicator) computeHashes(key string) []uint64 {
	data := []byte(key)
	hashes := make([]uint64, b.numHashes)

	// Use double hashing: h(i) = h1 + i*h2
	h1 := fnvHash(data)
	h2 := fnvHash(append(data, 0xFF))

	for i := 0; i < b.numHashes; i++ {
		hashes[i] = h1 + uint64(i)*h2
	}

	return hashes
}

// FNV-1a hash
func fnvHash(data []byte) uint64 {
	const (
		offset64 = 14695981039346656037
		prime64  = 1099511628211
	)

	hash := uint64(offset64)
	for _, b := range data {
		hash ^= uint64(b)
		hash *= prime64
	}
	return hash
}

// Stats returns bloom filter statistics.
func (b *BloomDeduplicator) Stats() map[string]interface{} {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return map[string]interface{}{
		"num_bits":         b.numBits,
		"num_hashes":       b.numHashes,
		"additions":        b.additions,
		"queries":          b.queries,
		"positives":        b.positives,
		"memory_bytes":     len(b.filter) * 8,
	}
}
