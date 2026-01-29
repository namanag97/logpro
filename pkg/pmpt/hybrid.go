// Package pmpt provides a hybrid data structure combining Merkle Tree and Interval Tree
// for unified sequence + time indexing queries.
package pmpt

import (
	"fmt"
	"sort"
	"sync"

	"github.com/logflow/logflow/internal/model"
)

// HybridTree combines a Process Merkle Patricia Tree with an Interval Tree
// for unified sequence and temporal queries.
type HybridTree struct {
	mu sync.RWMutex

	// Merkle tree for sequence structure and O(1) comparison
	Merkle *Tree

	// Interval tree for O(log N) time-based queries
	Interval *IntervalTree

	// Statistics
	stats HybridStats
}

// HybridStats tracks hybrid tree statistics.
type HybridStats struct {
	TotalCases     int64
	TotalEvents    int64
	UniqueNodes    int64
	MaxDepth       int
	VariantCount   int64
	TimeRangeStart int64
	TimeRangeEnd   int64
}

// NewHybridTree creates an empty hybrid tree.
func NewHybridTree() *HybridTree {
	return &HybridTree{
		Merkle:   NewTree(),
		Interval: NewIntervalTree(),
	}
}

// HybridBuilder incrementally builds both trees from an event stream.
type HybridBuilder struct {
	mu sync.Mutex

	// Active traces being built (case_id -> events)
	activeTraces map[string]*hybridTraceBuilder

	// Output trees
	hybrid *HybridTree

	// Configuration
	cfg HybridBuilderConfig
}

type hybridTraceBuilder struct {
	caseID     string
	activities []string
	timestamps []int64
	nodes      []*ProcessNode // Track nodes for interval tree
}

// HybridBuilderConfig configures the hybrid builder.
type HybridBuilderConfig struct {
	// MaxActiveTraces limits memory usage by capping concurrent traces
	MaxActiveTraces int

	// RequireTimestamps fails if events lack timestamps
	RequireTimestamps bool

	// CalculateDurations computes activity durations
	CalculateDurations bool
}

// DefaultHybridBuilderConfig returns sensible defaults.
func DefaultHybridBuilderConfig() HybridBuilderConfig {
	return HybridBuilderConfig{
		MaxActiveTraces:    100000,
		RequireTimestamps:  false,
		CalculateDurations: true,
	}
}

// NewHybridBuilder creates a new hybrid tree builder.
func NewHybridBuilder(cfg HybridBuilderConfig) *HybridBuilder {
	return &HybridBuilder{
		activeTraces: make(map[string]*hybridTraceBuilder),
		hybrid:       NewHybridTree(),
		cfg:          cfg,
	}
}

// AddEvent processes an event and updates both trees.
func (b *HybridBuilder) AddEvent(event *model.Event) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	caseID := string(event.CaseID)
	activity := string(event.Activity)
	timestamp := event.Timestamp

	// Get or create trace builder
	tb, exists := b.activeTraces[caseID]
	if !exists {
		// Check if we need to evict old traces
		if len(b.activeTraces) >= b.cfg.MaxActiveTraces {
			b.flushOldestTraceLocked()
		}

		tb = &hybridTraceBuilder{
			caseID:     caseID,
			activities: make([]string, 0, 16),
			timestamps: make([]int64, 0, 16),
			nodes:      make([]*ProcessNode, 0, 16),
		}
		b.activeTraces[caseID] = tb
	}

	// Add event to trace
	tb.activities = append(tb.activities, activity)
	tb.timestamps = append(tb.timestamps, timestamp)

	return nil
}

// AddEventSimple adds an event with explicit parameters.
func (b *HybridBuilder) AddEventSimple(caseID, activity string, start, end int64) error {
	return b.AddEvent(&model.Event{
		CaseID:    []byte(caseID),
		Activity:  []byte(activity),
		Timestamp: start,
	})
}

// flushOldestTraceLocked flushes the oldest trace to make room for new ones.
func (b *HybridBuilder) flushOldestTraceLocked() {
	// Simple eviction: flush first found (could be improved with LRU)
	for caseID, tb := range b.activeTraces {
		b.flushTraceLocked(tb)
		delete(b.activeTraces, caseID)
		return
	}
}

// flushTraceLocked adds a completed trace to both trees.
func (b *HybridBuilder) flushTraceLocked(tb *hybridTraceBuilder) {
	if len(tb.activities) == 0 {
		return
	}

	// Build trace for Merkle tree
	trace := Trace{
		CaseID:     tb.caseID,
		Activities: tb.activities,
		Timestamps: tb.timestamps,
	}

	// Add to Merkle tree (temporarily unlock for tree operations)
	b.mu.Unlock()
	b.hybrid.Merkle.AddTrace(trace)
	b.mu.Lock()

	// Add to Interval tree with time data
	if len(tb.timestamps) > 0 && b.cfg.CalculateDurations {
		for i := 0; i < len(tb.activities); i++ {
			start := tb.timestamps[i]
			var end int64
			if i+1 < len(tb.timestamps) {
				end = tb.timestamps[i+1]
			} else {
				end = start // Last activity has no duration
			}

			// Find the corresponding node in Merkle tree
			nodes := b.hybrid.Merkle.FindActivity(tb.activities[i])
			if len(nodes) > 0 {
				// Use the most recently added node
				node := nodes[len(nodes)-1]
				b.hybrid.Interval.Add(node, start, end)
			}
		}

		// Update time range stats
		b.hybrid.stats.TimeRangeStart = min64(b.hybrid.stats.TimeRangeStart, tb.timestamps[0])
		b.hybrid.stats.TimeRangeEnd = max64(b.hybrid.stats.TimeRangeEnd, tb.timestamps[len(tb.timestamps)-1])
	}

	b.hybrid.stats.TotalEvents += int64(len(tb.activities))
}

// Build finalizes all active traces and returns the completed hybrid tree.
func (b *HybridBuilder) Build() *HybridTree {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, tb := range b.activeTraces {
		b.flushTraceLocked(tb)
	}
	b.activeTraces = make(map[string]*hybridTraceBuilder)

	// Update final stats from Merkle tree
	stats := b.hybrid.Merkle.Stats()
	b.hybrid.stats.TotalCases = stats.TotalCases
	b.hybrid.stats.UniqueNodes = stats.UniqueNodes
	b.hybrid.stats.MaxDepth = stats.MaxDepth
	b.hybrid.stats.VariantCount = stats.VariantCount

	return b.hybrid
}

// Tree returns the current hybrid tree (may be incomplete if traces are still active).
func (b *HybridBuilder) Tree() *HybridTree {
	return b.hybrid
}

// --- Combined Query Methods ---

// TimeRange represents a time interval for queries.
type TimeRange struct {
	Start int64
	End   int64
}

// FindBottlenecksInSequence finds bottlenecks within specific activities in a time range.
func (h *HybridTree) FindBottlenecksInSequence(activities []string, timeRange TimeRange) []SequenceBottleneck {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// First, verify the sequence exists in the Merkle tree
	if !h.Merkle.HasPath(activities) {
		return nil
	}

	// Find time-based bottlenecks for these specific activities
	var bottlenecks []SequenceBottleneck

	for _, activity := range activities {
		nodes := h.Interval.QueryActivity(activity)
		if len(nodes) == 0 {
			continue
		}

		// Filter by time range and aggregate
		var totalDuration int64
		var count int
		var maxDuration int64

		for _, node := range nodes {
			// Check time range overlap
			if node.TimeEnd < timeRange.Start || node.TimeStart > timeRange.End {
				continue
			}

			duration := node.TimeEnd - node.TimeStart
			totalDuration += duration
			count++
			if duration > maxDuration {
				maxDuration = duration
			}
		}

		if count > 0 {
			bottlenecks = append(bottlenecks, SequenceBottleneck{
				Activity:      activity,
				TotalDuration: totalDuration,
				Count:         count,
				AvgDuration:   totalDuration / int64(count),
				MaxDuration:   maxDuration,
			})
		}
	}

	// Sort by total duration (highest first)
	sort.Slice(bottlenecks, func(i, j int) bool {
		return bottlenecks[i].TotalDuration > bottlenecks[j].TotalDuration
	})

	return bottlenecks
}

// SequenceBottleneck represents a bottleneck within a specific sequence.
type SequenceBottleneck struct {
	Activity      string
	TotalDuration int64
	Count         int
	AvgDuration   int64
	MaxDuration   int64
}

// FindConcurrentActivities finds activities that occurred at the same time.
func (h *HybridTree) FindConcurrentActivities(timeRange TimeRange) []Overlap {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Get all activities in the time range
	nodes := h.Interval.QueryRange(timeRange.Start, timeRange.End)
	if len(nodes) < 2 {
		return nil
	}

	// Find overlapping pairs
	var overlaps []Overlap

	for i := 0; i < len(nodes); i++ {
		for j := i + 1; j < len(nodes); j++ {
			n1 := nodes[i]
			n2 := nodes[j]

			// Check for overlap
			overlapStart := max64(n1.TimeStart, n2.TimeStart)
			overlapEnd := min64(n1.TimeEnd, n2.TimeEnd)

			if overlapEnd > overlapStart {
				overlaps = append(overlaps, Overlap{
					Activity1:    n1.Activity,
					Activity2:    n2.Activity,
					OverlapStart: overlapStart,
					OverlapEnd:   overlapEnd,
					Duration:     overlapEnd - overlapStart,
				})
			}
		}
	}

	// Sort by duration (longest first)
	sort.Slice(overlaps, func(i, j int) bool {
		return overlaps[i].Duration > overlaps[j].Duration
	})

	return overlaps
}

// HybridDiff represents differences between two hybrid trees.
type HybridDiff struct {
	// Structural differences from Merkle tree
	*TreeDiff

	// Temporal differences
	TimeRangeChanged    bool
	NewTimeRangeStart   int64
	NewTimeRangeEnd     int64
	BottleneckChanges   []BottleneckChange
	NewConcurrentPairs  []string // "activity1 || activity2"
}

// BottleneckChange represents a change in bottleneck metrics.
type BottleneckChange struct {
	Activity       string
	OldAvgDuration int64
	NewAvgDuration int64
	DeltaPct       float64 // Percentage change
}

// Diff compares two hybrid trees and returns combined differences.
func (h *HybridTree) Diff(other *HybridTree) *HybridDiff {
	h.mu.RLock()
	other.mu.RLock()
	defer h.mu.RUnlock()
	defer other.mu.RUnlock()

	// Get Merkle tree diff
	merkle := Compare(h.Merkle, other.Merkle)

	diff := &HybridDiff{
		TreeDiff: merkle,
	}

	// Check time range changes
	if h.stats.TimeRangeStart != other.stats.TimeRangeStart ||
		h.stats.TimeRangeEnd != other.stats.TimeRangeEnd {
		diff.TimeRangeChanged = true
		diff.NewTimeRangeStart = other.stats.TimeRangeStart
		diff.NewTimeRangeEnd = other.stats.TimeRangeEnd
	}

	// Compare bottlenecks (simplified - compare top activities)
	oldBottlenecks := h.Interval.FindBottlenecks(h.stats.TimeRangeStart, h.stats.TimeRangeEnd, 10)
	newBottlenecks := other.Interval.FindBottlenecks(other.stats.TimeRangeStart, other.stats.TimeRangeEnd, 10)

	// Build map for comparison
	oldMap := make(map[string]Bottleneck)
	for _, b := range oldBottlenecks {
		oldMap[b.Activity] = b
	}

	for _, newB := range newBottlenecks {
		if oldB, exists := oldMap[newB.Activity]; exists {
			if oldB.AvgDuration > 0 {
				deltaPct := float64(newB.AvgDuration-oldB.AvgDuration) / float64(oldB.AvgDuration) * 100
				if abs(deltaPct) > 10 { // >10% change
					diff.BottleneckChanges = append(diff.BottleneckChanges, BottleneckChange{
						Activity:       newB.Activity,
						OldAvgDuration: oldB.AvgDuration,
						NewAvgDuration: newB.AvgDuration,
						DeltaPct:       deltaPct,
					})
				}
			}
		}
	}

	// Sort bottleneck changes by impact
	sort.Slice(diff.BottleneckChanges, func(i, j int) bool {
		return abs(diff.BottleneckChanges[i].DeltaPct) > abs(diff.BottleneckChanges[j].DeltaPct)
	})

	return diff
}

// Stats returns hybrid tree statistics.
func (h *HybridTree) Stats() HybridStats {
	h.mu.RLock()
	defer h.mu.RUnlock()

	// Update from Merkle tree
	merkleStats := h.Merkle.Stats()
	h.stats.TotalCases = merkleStats.TotalCases
	h.stats.TotalEvents = merkleStats.TotalEvents
	h.stats.UniqueNodes = merkleStats.UniqueNodes
	h.stats.MaxDepth = merkleStats.MaxDepth
	h.stats.VariantCount = merkleStats.VariantCount

	return h.stats
}

// Fingerprint returns the tree's unique fingerprint (from Merkle tree).
func (h *HybridTree) Fingerprint() Hash {
	return h.Merkle.Fingerprint()
}

// Equals compares two hybrid trees for structural equality.
func (h *HybridTree) Equals(other *HybridTree) bool {
	return h.Merkle.Equals(other.Merkle)
}

// --- Query Convenience Methods ---

// HasPath checks if a specific activity sequence exists.
func (h *HybridTree) HasPath(activities []string) bool {
	return h.Merkle.HasPath(activities)
}

// FindActivity returns all occurrences of an activity.
func (h *HybridTree) FindActivity(activity string) []*ProcessNode {
	return h.Merkle.FindActivity(activity)
}

// GetVariants returns all unique process variants.
func (h *HybridTree) GetVariants() []Variant {
	return h.Merkle.GetVariants()
}

// FindBottlenecks returns top bottleneck activities in a time range.
func (h *HybridTree) FindBottlenecks(timeRange TimeRange, limit int) []Bottleneck {
	return h.Interval.FindBottlenecks(timeRange.Start, timeRange.End, limit)
}

// QueryTimeRange returns all activities overlapping a time range.
func (h *HybridTree) QueryTimeRange(timeRange TimeRange) []*TimeNode {
	return h.Interval.QueryRange(timeRange.Start, timeRange.End)
}

// --- Factory Functions ---

// BuildHybridFromEvents constructs a hybrid tree from an event channel.
func BuildHybridFromEvents(events <-chan *model.Event) *HybridTree {
	builder := NewHybridBuilder(DefaultHybridBuilderConfig())

	for event := range events {
		builder.AddEvent(event)
	}

	return builder.Build()
}

// BuildHybridFromTraces constructs a hybrid tree from a slice of traces.
func BuildHybridFromTraces(traces []TraceWithTime) *HybridTree {
	hybrid := NewHybridTree()

	for _, trace := range traces {
		// Add to Merkle tree
		hybrid.Merkle.AddTrace(Trace{
			CaseID:     trace.CaseID,
			Activities: trace.Activities,
			Timestamps: trace.Timestamps,
		})

		// Add to Interval tree
		for i := 0; i < len(trace.Activities); i++ {
			start := trace.Timestamps[i]
			var end int64
			if i+1 < len(trace.Timestamps) {
				end = trace.Timestamps[i+1]
			} else {
				end = start
			}

			nodes := hybrid.Merkle.FindActivity(trace.Activities[i])
			if len(nodes) > 0 {
				hybrid.Interval.Add(nodes[len(nodes)-1], start, end)
			}
		}
	}

	return hybrid
}

// TraceWithTime is a trace with timestamp data.
type TraceWithTime struct {
	CaseID     string
	Activities []string
	Timestamps []int64
}

// String returns a human-readable summary of the hybrid tree.
func (h *HybridTree) String() string {
	stats := h.Stats()
	return fmt.Sprintf(
		"HybridTree{Cases: %d, Events: %d, Nodes: %d, Variants: %d, Depth: %d}",
		stats.TotalCases,
		stats.TotalEvents,
		stats.UniqueNodes,
		stats.VariantCount,
		stats.MaxDepth,
	)
}
