package pmpt

import (
	"sync"

	"github.com/RoaringBitmap/roaring"

	"github.com/logflow/logflow/internal/model"
)

// Builder constructs a PMPT from an event stream.
type Builder struct {
	mu sync.Mutex

	// Active traces being built (case_id -> events)
	activeTraces map[string]*traceBuilder

	// Completed tree
	tree *Tree

	// Configuration
	cfg BuilderConfig
}

type traceBuilder struct {
	caseID     string
	activities []string
	timestamps []int64
}

// BuilderConfig configures the PMPT builder.
type BuilderConfig struct {
	// MaxActiveTraces limits memory usage by capping concurrent traces
	MaxActiveTraces int

	// IncludeTimestamps enables duration tracking
	IncludeTimestamps bool
}

// DefaultBuilderConfig returns sensible defaults.
func DefaultBuilderConfig() BuilderConfig {
	return BuilderConfig{
		MaxActiveTraces:   100000,
		IncludeTimestamps: true,
	}
}

// NewBuilder creates a new PMPT builder.
func NewBuilder(cfg BuilderConfig) *Builder {
	return &Builder{
		activeTraces: make(map[string]*traceBuilder),
		tree:         NewTree(),
		cfg:          cfg,
	}
}

// Add processes an event and updates the tree structure.
func (b *Builder) Add(event *model.Event) {
	b.mu.Lock()
	defer b.mu.Unlock()

	caseID := string(event.CaseID)
	activity := string(event.Activity)

	// Get or create trace builder
	tb, exists := b.activeTraces[caseID]
	if !exists {
		// Check if we need to evict old traces
		if len(b.activeTraces) >= b.cfg.MaxActiveTraces {
			b.flushOldestTrace()
		}

		tb = &traceBuilder{
			caseID:     caseID,
			activities: make([]string, 0, 16),
			timestamps: make([]int64, 0, 16),
		}
		b.activeTraces[caseID] = tb
	}

	// Add event to trace
	tb.activities = append(tb.activities, activity)
	if b.cfg.IncludeTimestamps {
		tb.timestamps = append(tb.timestamps, event.Timestamp)
	}
}

// flushOldestTrace flushes the oldest trace to make room for new ones.
func (b *Builder) flushOldestTrace() {
	// Simple eviction: flush first found (could be improved with LRU)
	for caseID, tb := range b.activeTraces {
		b.flushTrace(tb)
		delete(b.activeTraces, caseID)
		return
	}
}

// flushTrace adds a completed trace to the tree.
func (b *Builder) flushTrace(tb *traceBuilder) {
	if len(tb.activities) == 0 {
		return
	}

	trace := Trace{
		CaseID:     tb.caseID,
		Activities: tb.activities,
		Timestamps: tb.timestamps,
	}

	// Note: tree.AddTrace acquires its own lock, but we already hold b.mu
	// So we temporarily unlock
	b.mu.Unlock()
	b.tree.AddTrace(trace)
	b.mu.Lock()
}

// FlushAll finalizes all active traces and returns the completed tree.
func (b *Builder) FlushAll() *Tree {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, tb := range b.activeTraces {
		// Temporarily unlock for tree operations
		b.mu.Unlock()
		b.tree.AddTrace(Trace{
			CaseID:     tb.caseID,
			Activities: tb.activities,
			Timestamps: tb.timestamps,
		})
		b.mu.Lock()
	}

	b.activeTraces = make(map[string]*traceBuilder)
	return b.tree
}

// Tree returns the current tree (may be incomplete if traces are still active).
func (b *Builder) Tree() *Tree {
	return b.tree
}

// BuildFromEvents constructs a PMPT from an event channel.
func BuildFromEvents(events <-chan *model.Event) *Tree {
	builder := NewBuilder(DefaultBuilderConfig())

	for event := range events {
		builder.Add(event)
	}

	return builder.FlushAll()
}

// BuildFromTraces constructs a PMPT from a slice of traces.
func BuildFromTraces(traces []Trace) *Tree {
	tree := NewTree()
	for _, trace := range traces {
		tree.AddTrace(trace)
	}
	return tree
}
