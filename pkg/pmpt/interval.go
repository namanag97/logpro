package pmpt

import (
	"sort"
	"sync"
)

// TimeNode extends ProcessNode with temporal intervals for O(log N) time queries.
type TimeNode struct {
	*ProcessNode

	// Temporal data
	TimeStart  int64 // Earliest timestamp at this node
	TimeEnd    int64 // Latest timestamp at this node
	MaxTimeEnd int64 // Max end time in subtree (for interval tree optimization)

	// Duration statistics
	MinDuration int64
	MaxDuration int64
	SumDuration int64
	DurationCount int64
}

// IntervalTree provides O(log N) time-based queries on process events.
type IntervalTree struct {
	mu sync.RWMutex

	// Nodes indexed by time interval
	nodes []*TimeNode

	// Activity -> TimeNodes for fast activity filtering
	activityIndex map[string][]*TimeNode

	// Sorted by start time for binary search
	sorted bool
}

// NewIntervalTree creates a new interval tree.
func NewIntervalTree() *IntervalTree {
	return &IntervalTree{
		nodes:         make([]*TimeNode, 0),
		activityIndex: make(map[string][]*TimeNode),
	}
}

// Add inserts a time-annotated node.
func (t *IntervalTree) Add(node *ProcessNode, start, end int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	duration := end - start
	if duration < 0 {
		duration = 0
	}

	timeNode := &TimeNode{
		ProcessNode:   node,
		TimeStart:     start,
		TimeEnd:       end,
		MaxTimeEnd:    end,
		MinDuration:   duration,
		MaxDuration:   duration,
		SumDuration:   duration,
		DurationCount: 1,
	}

	t.nodes = append(t.nodes, timeNode)
	t.activityIndex[node.Activity] = append(t.activityIndex[node.Activity], timeNode)
	t.sorted = false
}

// ensureSorted sorts nodes by start time for binary search.
func (t *IntervalTree) ensureSorted() {
	if t.sorted {
		return
	}
	sort.Slice(t.nodes, func(i, j int) bool {
		return t.nodes[i].TimeStart < t.nodes[j].TimeStart
	})

	// Update MaxTimeEnd for interval tree optimization
	maxEnd := int64(0)
	for i := len(t.nodes) - 1; i >= 0; i-- {
		if t.nodes[i].TimeEnd > maxEnd {
			maxEnd = t.nodes[i].TimeEnd
		}
		t.nodes[i].MaxTimeEnd = maxEnd
	}

	t.sorted = true
}

// QueryRange finds all nodes overlapping the given time range.
// This is O(log N + K) where K is the number of results.
func (t *IntervalTree) QueryRange(start, end int64) []*TimeNode {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.ensureSorted()

	results := make([]*TimeNode, 0)

	// Binary search for first node that might overlap
	idx := sort.Search(len(t.nodes), func(i int) bool {
		return t.nodes[i].MaxTimeEnd >= start
	})

	// Scan from that point
	for i := idx; i < len(t.nodes); i++ {
		node := t.nodes[i]

		// If this node starts after our range, we can stop
		if node.TimeStart > end {
			break
		}

		// Check for overlap
		if node.TimeStart <= end && node.TimeEnd >= start {
			results = append(results, node)
		}
	}

	return results
}

// QueryActivity finds all time intervals for a specific activity.
func (t *IntervalTree) QueryActivity(activity string) []*TimeNode {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.activityIndex[activity]
}

// FindOverlaps finds all cases where two activities occurred concurrently.
// This is the "bottleneck detection" feature.
func (t *IntervalTree) FindOverlaps(activity1, activity2 string) []Overlap {
	t.mu.RLock()
	defer t.mu.RUnlock()

	nodes1 := t.activityIndex[activity1]
	nodes2 := t.activityIndex[activity2]

	if len(nodes1) == 0 || len(nodes2) == 0 {
		return nil
	}

	// Sort both by start time
	sort.Slice(nodes1, func(i, j int) bool {
		return nodes1[i].TimeStart < nodes1[j].TimeStart
	})
	sort.Slice(nodes2, func(i, j int) bool {
		return nodes2[i].TimeStart < nodes2[j].TimeStart
	})

	// Sweep line algorithm for finding overlaps
	overlaps := make([]Overlap, 0)

	j := 0
	for _, n1 := range nodes1 {
		// Move j to first node2 that might overlap with n1
		for j < len(nodes2) && nodes2[j].TimeEnd < n1.TimeStart {
			j++
		}

		// Check all nodes2 that overlap with n1
		for k := j; k < len(nodes2) && nodes2[k].TimeStart <= n1.TimeEnd; k++ {
			n2 := nodes2[k]

			// Calculate overlap interval
			overlapStart := max64(n1.TimeStart, n2.TimeStart)
			overlapEnd := min64(n1.TimeEnd, n2.TimeEnd)

			if overlapEnd > overlapStart {
				overlaps = append(overlaps, Overlap{
					Activity1:    activity1,
					Activity2:    activity2,
					OverlapStart: overlapStart,
					OverlapEnd:   overlapEnd,
					Duration:     overlapEnd - overlapStart,
				})
			}
		}
	}

	return overlaps
}

// Overlap represents a time period where two activities occurred concurrently.
type Overlap struct {
	Activity1    string
	Activity2    string
	OverlapStart int64
	OverlapEnd   int64
	Duration     int64
}

// FindBottlenecks identifies activities with the longest durations in a time range.
func (t *IntervalTree) FindBottlenecks(start, end int64, limit int) []Bottleneck {
	nodes := t.QueryRange(start, end)

	// Aggregate by activity
	activityDurations := make(map[string]*durationStats)
	for _, node := range nodes {
		stats, ok := activityDurations[node.Activity]
		if !ok {
			stats = &durationStats{activity: node.Activity}
			activityDurations[node.Activity] = stats
		}
		stats.add(node.TimeEnd - node.TimeStart)
	}

	// Convert to slice and sort
	bottlenecks := make([]Bottleneck, 0, len(activityDurations))
	for _, stats := range activityDurations {
		bottlenecks = append(bottlenecks, Bottleneck{
			Activity:    stats.activity,
			TotalTime:   stats.total,
			Count:       stats.count,
			AvgDuration: stats.total / int64(stats.count),
			MaxDuration: stats.max,
		})
	}

	sort.Slice(bottlenecks, func(i, j int) bool {
		return bottlenecks[i].TotalTime > bottlenecks[j].TotalTime
	})

	if limit > 0 && len(bottlenecks) > limit {
		bottlenecks = bottlenecks[:limit]
	}

	return bottlenecks
}

// Bottleneck represents an activity consuming significant time.
type Bottleneck struct {
	Activity    string
	TotalTime   int64
	Count       int
	AvgDuration int64
	MaxDuration int64
}

type durationStats struct {
	activity string
	total    int64
	count    int
	max      int64
}

func (s *durationStats) add(duration int64) {
	s.total += duration
	s.count++
	if duration > s.max {
		s.max = duration
	}
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
