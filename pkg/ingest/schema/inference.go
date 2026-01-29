// Package schema provides schema inference, caching, and evolution.
package schema

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"

	"github.com/apache/arrow/go/v14/arrow"

	"github.com/logflow/logflow/pkg/ingest/core"
)

// Inference handles schema inference with caching.
type Inference struct {
	mu    sync.RWMutex
	cache map[string]*CachedSchema

	// Config
	SampleSize    int
	CacheEnabled  bool
	MaxCacheItems int
}

// CachedSchema stores inferred schema with metadata.
type CachedSchema struct {
	Schema     *arrow.Schema
	SourceHash string
	SampleRows int
	InferredAt int64
}

// NewInference creates a schema inference engine.
func NewInference() *Inference {
	return &Inference{
		cache:         make(map[string]*CachedSchema),
		SampleSize:    10000,
		CacheEnabled:  true,
		MaxCacheItems: 1000,
	}
}

// Infer infers schema from a source using the appropriate decoder.
func (i *Inference) Infer(ctx context.Context, source core.Source, decoder core.Decoder) (*arrow.Schema, error) {
	// Generate cache key
	cacheKey := i.cacheKey(source)

	// Check cache
	if i.CacheEnabled {
		i.mu.RLock()
		if cached, ok := i.cache[cacheKey]; ok {
			i.mu.RUnlock()
			return cached.Schema, nil
		}
		i.mu.RUnlock()
	}

	// Infer schema
	schema, err := decoder.InferSchema(ctx, source, i.SampleSize)
	if err != nil {
		return nil, fmt.Errorf("schema inference failed: %w", err)
	}

	// Cache result
	if i.CacheEnabled {
		i.mu.Lock()
		if len(i.cache) >= i.MaxCacheItems {
			// Simple eviction: clear half the cache
			for k := range i.cache {
				delete(i.cache, k)
				if len(i.cache) < i.MaxCacheItems/2 {
					break
				}
			}
		}
		i.cache[cacheKey] = &CachedSchema{
			Schema:     schema,
			SourceHash: cacheKey,
			SampleRows: i.SampleSize,
		}
		i.mu.Unlock()
	}

	return schema, nil
}

// InferFromSample infers schema from provided data.
func (i *Inference) InferFromSample(records []map[string]interface{}) (*arrow.Schema, error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("no records provided")
	}

	// Collect field types
	fieldTypes := make(map[string]map[arrow.Type]int)

	for _, record := range records {
		for key, value := range record {
			if fieldTypes[key] == nil {
				fieldTypes[key] = make(map[arrow.Type]int)
			}
			t := inferGoType(value)
			fieldTypes[key][t]++
		}
	}

	// Build schema
	var fields []arrow.Field
	for name, types := range fieldTypes {
		fields = append(fields, arrow.Field{
			Name:     name,
			Type:     selectBestType(types),
			Nullable: true,
		})
	}

	return arrow.NewSchema(fields, nil), nil
}

func inferGoType(value interface{}) arrow.Type {
	if value == nil {
		return arrow.NULL
	}

	switch value.(type) {
	case bool:
		return arrow.BOOL
	case int, int32, int64:
		return arrow.INT64
	case float32, float64:
		return arrow.FLOAT64
	case string:
		return arrow.STRING
	case []byte:
		return arrow.BINARY
	case []interface{}:
		return arrow.LIST
	case map[string]interface{}:
		return arrow.STRUCT
	default:
		return arrow.STRING
	}
}

func selectBestType(types map[arrow.Type]int) arrow.DataType {
	// Priority: more specific types win
	if types[arrow.LIST] > 0 {
		// Default list element type is string; callers needing a specific
		// element type should refine via InferListType.
		return arrow.ListOf(arrow.BinaryTypes.String)
	}
	if types[arrow.STRUCT] > 0 {
		// Bare struct without known fields; callers should refine.
		return arrow.StructOf()
	}
	if types[arrow.FLOAT64] > 0 {
		return arrow.PrimitiveTypes.Float64
	}
	if types[arrow.INT64] > 0 {
		return arrow.PrimitiveTypes.Int64
	}
	if types[arrow.BOOL] > 0 {
		return arrow.FixedWidthTypes.Boolean
	}
	if types[arrow.BINARY] > 0 {
		return arrow.BinaryTypes.Binary
	}
	return arrow.BinaryTypes.String
}

func (i *Inference) cacheKey(source core.Source) string {
	data := fmt.Sprintf("%s:%s:%d", source.Location(), source.Format(), source.Size())
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:8])
}

// ClearCache clears the schema cache.
func (i *Inference) ClearCache() {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.cache = make(map[string]*CachedSchema)
}

// GetCached returns a cached schema if available.
func (i *Inference) GetCached(source core.Source) *CachedSchema {
	i.mu.RLock()
	defer i.mu.RUnlock()
	return i.cache[i.cacheKey(source)]
}
