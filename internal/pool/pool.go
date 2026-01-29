// Package pool provides zero-allocation buffer management using sync.Pool.
package pool

import (
	"sync"

	"github.com/logflow/logflow/internal/model"
)

const (
	// DefaultBufferSize is the default size for byte buffers.
	DefaultBufferSize = 64 * 1024 // 64KB

	// DefaultEventBatchSize is the default number of events per batch.
	DefaultEventBatchSize = 1024
)

// ByteBuffer wraps a byte slice for pooled reuse.
type ByteBuffer struct {
	Data []byte
}

// Reset clears the buffer for reuse.
func (b *ByteBuffer) Reset() {
	b.Data = b.Data[:0]
}

// Grow ensures the buffer has at least n bytes of capacity.
func (b *ByteBuffer) Grow(n int) {
	if cap(b.Data) < n {
		b.Data = make([]byte, 0, n)
	}
}

// Write appends data to the buffer.
func (b *ByteBuffer) Write(p []byte) (int, error) {
	b.Data = append(b.Data, p...)
	return len(p), nil
}

// Len returns the current length of data in the buffer.
func (b *ByteBuffer) Len() int {
	return len(b.Data)
}

// Bytes returns the underlying byte slice.
func (b *ByteBuffer) Bytes() []byte {
	return b.Data
}

// BufferPool manages reusable byte buffers.
type BufferPool struct {
	pool sync.Pool
	size int
}

// NewBufferPool creates a new buffer pool with the specified buffer size.
func NewBufferPool(bufferSize int) *BufferPool {
	if bufferSize <= 0 {
		bufferSize = DefaultBufferSize
	}
	bp := &BufferPool{size: bufferSize}
	bp.pool.New = func() any {
		return &ByteBuffer{
			Data: make([]byte, 0, bufferSize),
		}
	}
	return bp
}

// Get retrieves a buffer from the pool.
func (p *BufferPool) Get() *ByteBuffer {
	return p.pool.Get().(*ByteBuffer)
}

// Put returns a buffer to the pool.
func (p *BufferPool) Put(buf *ByteBuffer) {
	buf.Reset()
	p.pool.Put(buf)
}

// EventPool manages reusable Event structs.
type EventPool struct {
	pool sync.Pool
}

// NewEventPool creates a new event pool.
func NewEventPool() *EventPool {
	ep := &EventPool{}
	ep.pool.New = func() any {
		return &model.Event{
			CaseID:     make([]byte, 0, 64),
			Activity:   make([]byte, 0, 128),
			Resource:   make([]byte, 0, 64),
			Attributes: make([]model.Attribute, 0, 8),
		}
	}
	return ep
}

// Get retrieves an event from the pool.
func (p *EventPool) Get() *model.Event {
	return p.pool.Get().(*model.Event)
}

// Put returns an event to the pool.
func (p *EventPool) Put(e *model.Event) {
	e.Reset()
	p.pool.Put(e)
}

// EventBatchPool manages reusable EventBatch structs.
type EventBatchPool struct {
	pool     sync.Pool
	batchLen int
}

// NewEventBatchPool creates a new event batch pool.
func NewEventBatchPool(batchSize int) *EventBatchPool {
	if batchSize <= 0 {
		batchSize = DefaultEventBatchSize
	}
	ebp := &EventBatchPool{batchLen: batchSize}
	ebp.pool.New = func() any {
		batch := &model.EventBatch{
			Events: make([]model.Event, batchSize),
		}
		// Pre-allocate slices for each event
		for i := range batch.Events {
			batch.Events[i].CaseID = make([]byte, 0, 64)
			batch.Events[i].Activity = make([]byte, 0, 128)
			batch.Events[i].Resource = make([]byte, 0, 64)
			batch.Events[i].Attributes = make([]model.Attribute, 0, 8)
		}
		return batch
	}
	return ebp
}

// Get retrieves an event batch from the pool.
func (p *EventBatchPool) Get() *model.EventBatch {
	return p.pool.Get().(*model.EventBatch)
}

// Put returns an event batch to the pool.
func (p *EventBatchPool) Put(b *model.EventBatch) {
	b.Reset()
	p.pool.Put(b)
}

// LineBuffer is optimized for line-by-line parsing.
type LineBuffer struct {
	data   []byte
	pos    int
	length int
}

// NewLineBuffer creates a line buffer wrapping the given byte slice.
func NewLineBuffer(data []byte) *LineBuffer {
	return &LineBuffer{
		data:   data,
		pos:    0,
		length: len(data),
	}
}

// Reset resets the line buffer with new data.
func (lb *LineBuffer) Reset(data []byte) {
	lb.data = data
	lb.pos = 0
	lb.length = len(data)
}

// NextLine returns the next line as a byte slice without allocation.
// Returns nil when EOF is reached.
func (lb *LineBuffer) NextLine() []byte {
	if lb.pos >= lb.length {
		return nil
	}

	start := lb.pos
	for lb.pos < lb.length {
		if lb.data[lb.pos] == '\n' {
			line := lb.data[start:lb.pos]
			lb.pos++
			// Handle \r\n line endings
			if len(line) > 0 && line[len(line)-1] == '\r' {
				line = line[:len(line)-1]
			}
			return line
		}
		lb.pos++
	}

	// Last line without newline
	if start < lb.length {
		return lb.data[start:lb.length]
	}
	return nil
}

// HasMore returns true if there's more data to read.
func (lb *LineBuffer) HasMore() bool {
	return lb.pos < lb.length
}
