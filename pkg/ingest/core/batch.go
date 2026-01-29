package core

import (
	"sync"

	"github.com/apache/arrow/go/v14/arrow"
)

// RecordBatch wraps an Arrow RecordBatch with additional metadata.
type RecordBatch struct {
	// Record is the underlying Arrow record.
	Record arrow.Record

	// Metadata about this batch.
	Index      int
	RowOffset  int64
	ByteOffset int64

	// Quality metrics.
	NullCounts map[string]int64
	ErrorCount int
}

// NewRecordBatch creates a new batch wrapper.
func NewRecordBatch(record arrow.Record, index int, rowOffset int64) *RecordBatch {
	return &RecordBatch{
		Record:     record,
		Index:      index,
		RowOffset:  rowOffset,
		NullCounts: make(map[string]int64),
	}
}

// NumRows returns the number of rows in the batch.
func (b *RecordBatch) NumRows() int64 {
	if b.Record == nil {
		return 0
	}
	return b.Record.NumRows()
}

// NumCols returns the number of columns in the batch.
func (b *RecordBatch) NumCols() int {
	if b.Record == nil {
		return 0
	}
	return int(b.Record.NumCols())
}

// Schema returns the batch schema.
func (b *RecordBatch) Schema() *arrow.Schema {
	if b.Record == nil {
		return nil
	}
	return b.Record.Schema()
}

// Release releases the underlying Arrow memory.
func (b *RecordBatch) Release() {
	if b.Record != nil {
		b.Record.Release()
	}
}

// BatchAccumulator accumulates batches.
type BatchAccumulator struct {
	mu sync.Mutex

	schema    *arrow.Schema
	batchSize int
	batches   []*RecordBatch
}

// NewBatchAccumulator creates a new accumulator.
func NewBatchAccumulator(schema *arrow.Schema, batchSize int) *BatchAccumulator {
	return &BatchAccumulator{
		schema:    schema,
		batchSize: batchSize,
		batches:   make([]*RecordBatch, 0),
	}
}

// Schema returns the accumulator's schema.
func (a *BatchAccumulator) Schema() *arrow.Schema {
	return a.schema
}

// BatchCount returns number of completed batches.
func (a *BatchAccumulator) BatchCount() int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.batches)
}

// Add adds a batch.
func (a *BatchAccumulator) Add(batch *RecordBatch) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batches = append(a.batches, batch)
}

// Batches returns all batches.
func (a *BatchAccumulator) Batches() []*RecordBatch {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.batches
}

// Close releases resources.
func (a *BatchAccumulator) Close() {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.batches = nil
}

// BatchPool pools RecordBatch objects for reuse.
type BatchPool struct {
	pool sync.Pool
}

// NewBatchPool creates a new batch pool.
func NewBatchPool() *BatchPool {
	return &BatchPool{
		pool: sync.Pool{
			New: func() interface{} {
				return &RecordBatch{
					NullCounts: make(map[string]int64),
				}
			},
		},
	}
}

// Get retrieves a batch from the pool.
func (p *BatchPool) Get() *RecordBatch {
	return p.pool.Get().(*RecordBatch)
}

// Put returns a batch to the pool.
func (p *BatchPool) Put(b *RecordBatch) {
	b.Record = nil
	b.Index = 0
	b.RowOffset = 0
	b.ErrorCount = 0
	for k := range b.NullCounts {
		delete(b.NullCounts, k)
	}
	p.pool.Put(b)
}
