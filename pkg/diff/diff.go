// Package diff provides semantic comparison of process mining logs.
package diff

import (
	"context"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/logflow/logflow/internal/model"
)

// DiffReport contains the semantic differences between two logs.
type DiffReport struct {
	// Summary statistics
	LeftEventCount  int64
	RightEventCount int64
	LeftCaseCount   int64
	RightCaseCount  int64

	// Time range changes
	LeftTimeRange  TimeRange
	RightTimeRange TimeRange

	// Activity changes
	ActivityChanges []ActivityChange

	// Case duration changes
	AvgCaseDurationLeft  time.Duration
	AvgCaseDurationRight time.Duration
	CaseDurationDelta    float64 // Percentage change

	// Resource changes
	ResourceChanges []ResourceChange

	// Process drift indicators
	NewActivities     []string
	RemovedActivities []string
	NewResources      []string
	RemovedResources  []string
}

// TimeRange represents a time span.
type TimeRange struct {
	Min time.Time
	Max time.Time
}

// Duration returns the duration of the time range.
func (t TimeRange) Duration() time.Duration {
	return t.Max.Sub(t.Min)
}

// ActivityChange represents a change in activity frequency.
type ActivityChange struct {
	Activity       string
	LeftCount      int64
	RightCount     int64
	LeftPercent    float64
	RightPercent   float64
	PercentDelta   float64 // Positive means increased in right
	AbsoluteDelta  int64
	Significance   string // "increased", "decreased", "stable", "new", "removed"
}

// ResourceChange represents a change in resource allocation.
type ResourceChange struct {
	Resource      string
	LeftCount     int64
	RightCount    int64
	PercentDelta  float64
	Significance  string
}

// Analyzer performs semantic diff analysis.
type Analyzer struct {
	// Left log stats
	leftEvents         int64
	leftCases          map[string]struct{}
	leftActivities     map[string]int64
	leftResources      map[string]int64
	leftCaseDurations  map[string]*caseTiming
	leftMinTime        int64
	leftMaxTime        int64

	// Right log stats
	rightEvents        int64
	rightCases         map[string]struct{}
	rightActivities    map[string]int64
	rightResources     map[string]int64
	rightCaseDurations map[string]*caseTiming
	rightMinTime       int64
	rightMaxTime       int64
}

type caseTiming struct {
	minTime int64
	maxTime int64
}

// NewAnalyzer creates a new diff analyzer.
func NewAnalyzer() *Analyzer {
	return &Analyzer{
		leftCases:          make(map[string]struct{}),
		leftActivities:     make(map[string]int64),
		leftResources:      make(map[string]int64),
		leftCaseDurations:  make(map[string]*caseTiming),
		leftMinTime:        math.MaxInt64,
		leftMaxTime:        math.MinInt64,
		rightCases:         make(map[string]struct{}),
		rightActivities:    make(map[string]int64),
		rightResources:     make(map[string]int64),
		rightCaseDurations: make(map[string]*caseTiming),
		rightMinTime:       math.MaxInt64,
		rightMaxTime:       math.MinInt64,
	}
}

// AddLeft adds an event from the left (baseline) log.
func (a *Analyzer) AddLeft(event *model.Event) {
	a.leftEvents++

	caseID := string(event.CaseID)
	a.leftCases[caseID] = struct{}{}
	a.leftActivities[string(event.Activity)]++

	if len(event.Resource) > 0 {
		a.leftResources[string(event.Resource)]++
	}

	// Track timing
	if event.Timestamp > 0 {
		if event.Timestamp < a.leftMinTime {
			a.leftMinTime = event.Timestamp
		}
		if event.Timestamp > a.leftMaxTime {
			a.leftMaxTime = event.Timestamp
		}

		timing, ok := a.leftCaseDurations[caseID]
		if !ok {
			timing = &caseTiming{minTime: event.Timestamp, maxTime: event.Timestamp}
			a.leftCaseDurations[caseID] = timing
		} else {
			if event.Timestamp < timing.minTime {
				timing.minTime = event.Timestamp
			}
			if event.Timestamp > timing.maxTime {
				timing.maxTime = event.Timestamp
			}
		}
	}
}

// AddRight adds an event from the right (comparison) log.
func (a *Analyzer) AddRight(event *model.Event) {
	a.rightEvents++

	caseID := string(event.CaseID)
	a.rightCases[caseID] = struct{}{}
	a.rightActivities[string(event.Activity)]++

	if len(event.Resource) > 0 {
		a.rightResources[string(event.Resource)]++
	}

	// Track timing
	if event.Timestamp > 0 {
		if event.Timestamp < a.rightMinTime {
			a.rightMinTime = event.Timestamp
		}
		if event.Timestamp > a.rightMaxTime {
			a.rightMaxTime = event.Timestamp
		}

		timing, ok := a.rightCaseDurations[caseID]
		if !ok {
			timing = &caseTiming{minTime: event.Timestamp, maxTime: event.Timestamp}
			a.rightCaseDurations[caseID] = timing
		} else {
			if event.Timestamp < timing.minTime {
				timing.minTime = event.Timestamp
			}
			if event.Timestamp > timing.maxTime {
				timing.maxTime = event.Timestamp
			}
		}
	}
}

// Report generates the diff report.
func (a *Analyzer) Report() *DiffReport {
	report := &DiffReport{
		LeftEventCount:  a.leftEvents,
		RightEventCount: a.rightEvents,
		LeftCaseCount:   int64(len(a.leftCases)),
		RightCaseCount:  int64(len(a.rightCases)),
	}

	// Time ranges
	if a.leftMinTime != math.MaxInt64 {
		report.LeftTimeRange = TimeRange{
			Min: time.Unix(0, a.leftMinTime),
			Max: time.Unix(0, a.leftMaxTime),
		}
	}
	if a.rightMinTime != math.MaxInt64 {
		report.RightTimeRange = TimeRange{
			Min: time.Unix(0, a.rightMinTime),
			Max: time.Unix(0, a.rightMaxTime),
		}
	}

	// Calculate average case durations
	report.AvgCaseDurationLeft = a.avgCaseDuration(a.leftCaseDurations)
	report.AvgCaseDurationRight = a.avgCaseDuration(a.rightCaseDurations)
	if report.AvgCaseDurationLeft > 0 {
		report.CaseDurationDelta = float64(report.AvgCaseDurationRight-report.AvgCaseDurationLeft) / float64(report.AvgCaseDurationLeft) * 100
	}

	// Activity changes
	report.ActivityChanges = a.computeActivityChanges()

	// Resource changes
	report.ResourceChanges = a.computeResourceChanges()

	// Find new/removed activities
	for activity := range a.rightActivities {
		if _, ok := a.leftActivities[activity]; !ok {
			report.NewActivities = append(report.NewActivities, activity)
		}
	}
	for activity := range a.leftActivities {
		if _, ok := a.rightActivities[activity]; !ok {
			report.RemovedActivities = append(report.RemovedActivities, activity)
		}
	}

	// Find new/removed resources
	for resource := range a.rightResources {
		if _, ok := a.leftResources[resource]; !ok {
			report.NewResources = append(report.NewResources, resource)
		}
	}
	for resource := range a.leftResources {
		if _, ok := a.rightResources[resource]; !ok {
			report.RemovedResources = append(report.RemovedResources, resource)
		}
	}

	return report
}

func (a *Analyzer) avgCaseDuration(durations map[string]*caseTiming) time.Duration {
	if len(durations) == 0 {
		return 0
	}
	var total int64
	for _, timing := range durations {
		total += timing.maxTime - timing.minTime
	}
	return time.Duration(total / int64(len(durations)))
}

func (a *Analyzer) computeActivityChanges() []ActivityChange {
	// Collect all activities
	allActivities := make(map[string]struct{})
	for act := range a.leftActivities {
		allActivities[act] = struct{}{}
	}
	for act := range a.rightActivities {
		allActivities[act] = struct{}{}
	}

	changes := make([]ActivityChange, 0, len(allActivities))

	for activity := range allActivities {
		leftCount := a.leftActivities[activity]
		rightCount := a.rightActivities[activity]

		leftPercent := float64(0)
		if a.leftEvents > 0 {
			leftPercent = float64(leftCount) / float64(a.leftEvents) * 100
		}

		rightPercent := float64(0)
		if a.rightEvents > 0 {
			rightPercent = float64(rightCount) / float64(a.rightEvents) * 100
		}

		change := ActivityChange{
			Activity:      activity,
			LeftCount:     leftCount,
			RightCount:    rightCount,
			LeftPercent:   leftPercent,
			RightPercent:  rightPercent,
			PercentDelta:  rightPercent - leftPercent,
			AbsoluteDelta: rightCount - leftCount,
		}

		// Determine significance
		if leftCount == 0 {
			change.Significance = "new"
		} else if rightCount == 0 {
			change.Significance = "removed"
		} else if math.Abs(change.PercentDelta) < 1.0 {
			change.Significance = "stable"
		} else if change.PercentDelta > 0 {
			change.Significance = "increased"
		} else {
			change.Significance = "decreased"
		}

		changes = append(changes, change)
	}

	// Sort by absolute delta (most significant changes first)
	sort.Slice(changes, func(i, j int) bool {
		return math.Abs(changes[i].PercentDelta) > math.Abs(changes[j].PercentDelta)
	})

	return changes
}

func (a *Analyzer) computeResourceChanges() []ResourceChange {
	allResources := make(map[string]struct{})
	for res := range a.leftResources {
		allResources[res] = struct{}{}
	}
	for res := range a.rightResources {
		allResources[res] = struct{}{}
	}

	changes := make([]ResourceChange, 0, len(allResources))

	for resource := range allResources {
		leftCount := a.leftResources[resource]
		rightCount := a.rightResources[resource]

		percentDelta := float64(0)
		if leftCount > 0 {
			percentDelta = float64(rightCount-leftCount) / float64(leftCount) * 100
		}

		change := ResourceChange{
			Resource:     resource,
			LeftCount:    leftCount,
			RightCount:   rightCount,
			PercentDelta: percentDelta,
		}

		if leftCount == 0 {
			change.Significance = "new"
		} else if rightCount == 0 {
			change.Significance = "removed"
		} else if math.Abs(percentDelta) < 5.0 {
			change.Significance = "stable"
		} else if percentDelta > 0 {
			change.Significance = "increased"
		} else {
			change.Significance = "decreased"
		}

		changes = append(changes, change)
	}

	sort.Slice(changes, func(i, j int) bool {
		return math.Abs(changes[i].PercentDelta) > math.Abs(changes[j].PercentDelta)
	})

	return changes
}

// String returns a human-readable diff report.
func (r *DiffReport) String() string {
	s := "=== Process Log Diff Report ===\n\n"

	// Summary
	s += fmt.Sprintf("Events:  %d -> %d (%+d)\n", r.LeftEventCount, r.RightEventCount, r.RightEventCount-r.LeftEventCount)
	s += fmt.Sprintf("Cases:   %d -> %d (%+d)\n", r.LeftCaseCount, r.RightCaseCount, r.RightCaseCount-r.LeftCaseCount)

	// Time range
	if !r.LeftTimeRange.Min.IsZero() && !r.RightTimeRange.Min.IsZero() {
		s += fmt.Sprintf("\nTime Range:\n")
		s += fmt.Sprintf("  Left:  %s to %s (%s)\n",
			r.LeftTimeRange.Min.Format("2006-01-02"),
			r.LeftTimeRange.Max.Format("2006-01-02"),
			r.LeftTimeRange.Duration().Round(time.Hour))
		s += fmt.Sprintf("  Right: %s to %s (%s)\n",
			r.RightTimeRange.Min.Format("2006-01-02"),
			r.RightTimeRange.Max.Format("2006-01-02"),
			r.RightTimeRange.Duration().Round(time.Hour))
	}

	// Case duration
	if r.AvgCaseDurationLeft > 0 || r.AvgCaseDurationRight > 0 {
		s += fmt.Sprintf("\nAvg Case Duration: %s -> %s (%+.1f%%)\n",
			r.AvgCaseDurationLeft.Round(time.Minute),
			r.AvgCaseDurationRight.Round(time.Minute),
			r.CaseDurationDelta)
	}

	// Process drift
	if len(r.NewActivities) > 0 {
		s += fmt.Sprintf("\nNew Activities: %v\n", r.NewActivities)
	}
	if len(r.RemovedActivities) > 0 {
		s += fmt.Sprintf("Removed Activities: %v\n", r.RemovedActivities)
	}

	// Top activity changes
	s += "\nActivity Changes (top 10):\n"
	s += fmt.Sprintf("  %-30s %10s %10s %10s %12s\n", "Activity", "Left", "Right", "Delta", "Status")
	s += fmt.Sprintf("  %-30s %10s %10s %10s %12s\n", "--------", "----", "-----", "-----", "------")

	count := 0
	for _, change := range r.ActivityChanges {
		if count >= 10 {
			break
		}
		actName := change.Activity
		if len(actName) > 30 {
			actName = actName[:27] + "..."
		}
		s += fmt.Sprintf("  %-30s %10d %10d %+9.1f%% %12s\n",
			actName, change.LeftCount, change.RightCount, change.PercentDelta, change.Significance)
		count++
	}

	return s
}

// Compare performs semantic diff between two event streams.
func Compare(ctx context.Context, leftChan, rightChan <-chan *model.Event) (*DiffReport, error) {
	analyzer := NewAnalyzer()

	// Consume left channel
	for event := range leftChan {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			analyzer.AddLeft(event)
		}
	}

	// Consume right channel
	for event := range rightChan {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
			analyzer.AddRight(event)
		}
	}

	return analyzer.Report(), nil
}
