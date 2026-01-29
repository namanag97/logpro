package processors

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/logflow/logflow/pkg/pipeline"
)

// QualityInspector analyzes data quality without modifying events.
type QualityInspector struct {
	mu sync.Mutex

	// Counters
	totalEvents int64

	// Completeness
	missingCaseIDs    int64
	missingActivities int64
	missingTimestamps int64
	missingResources  int64

	// Time range
	minTimestamp int64
	maxTimestamp int64

	// Uniqueness
	cases      map[string]int64
	activities map[string]int64
	resources  map[string]int64

	// Duplicates
	eventHashes map[string]int64
	duplicates  int64

	// Ordering
	lastTimestamp   int64
	outOfOrderCount int64
}

// NewQualityInspector creates a new quality inspector.
func NewQualityInspector() *QualityInspector {
	return &QualityInspector{
		minTimestamp: math.MaxInt64,
		maxTimestamp: math.MinInt64,
		cases:        make(map[string]int64),
		activities:   make(map[string]int64),
		resources:    make(map[string]int64),
		eventHashes:  make(map[string]int64),
	}
}

// Name returns the inspector name.
func (i *QualityInspector) Name() string {
	return "quality"
}

// Inspect processes an event and updates statistics.
func (i *QualityInspector) Inspect(event *pipeline.Event) {
	i.mu.Lock()
	defer i.mu.Unlock()

	i.totalEvents++

	// Completeness
	if len(event.CaseID) == 0 {
		i.missingCaseIDs++
	} else {
		i.cases[string(event.CaseID)]++
	}

	if len(event.Activity) == 0 {
		i.missingActivities++
	} else {
		i.activities[string(event.Activity)]++
	}

	if event.Timestamp == 0 {
		i.missingTimestamps++
	} else {
		if event.Timestamp < i.minTimestamp {
			i.minTimestamp = event.Timestamp
		}
		if event.Timestamp > i.maxTimestamp {
			i.maxTimestamp = event.Timestamp
		}
		if event.Timestamp < i.lastTimestamp {
			i.outOfOrderCount++
		}
		i.lastTimestamp = event.Timestamp
	}

	if len(event.Resource) == 0 {
		i.missingResources++
	} else {
		i.resources[string(event.Resource)]++
	}

	// Duplicate detection
	hash := fmt.Sprintf("%s|%s|%d", event.CaseID, event.Activity, event.Timestamp)
	if i.eventHashes[hash] > 0 {
		i.duplicates++
	}
	i.eventHashes[hash]++
}

// Report returns the quality report.
func (i *QualityInspector) Report() interface{} {
	i.mu.Lock()
	defer i.mu.Unlock()

	report := &QualityReport{
		TotalEvents:     i.totalEvents,
		TotalCases:      int64(len(i.cases)),
		TotalActivities: int64(len(i.activities)),
	}

	// Time range
	if i.minTimestamp != math.MaxInt64 {
		report.MinTimestamp = time.Unix(0, i.minTimestamp).Format(time.RFC3339)
		report.MaxTimestamp = time.Unix(0, i.maxTimestamp).Format(time.RFC3339)
		duration := time.Duration(i.maxTimestamp - i.minTimestamp)
		report.TimeSpan = duration.String()
	}

	// Completeness
	if i.totalEvents > 0 {
		report.Completeness = CompletenessReport{
			CaseIDPct:    100.0 * float64(i.totalEvents-i.missingCaseIDs) / float64(i.totalEvents),
			ActivityPct:  100.0 * float64(i.totalEvents-i.missingActivities) / float64(i.totalEvents),
			TimestampPct: 100.0 * float64(i.totalEvents-i.missingTimestamps) / float64(i.totalEvents),
			ResourcePct:  100.0 * float64(i.totalEvents-i.missingResources) / float64(i.totalEvents),
		}
	}

	// Uniqueness
	report.Uniqueness = UniquenessReport{
		DistinctCases:      int64(len(i.cases)),
		DistinctActivities: int64(len(i.activities)),
		DistinctResources:  int64(len(i.resources)),
		DuplicateEvents:    i.duplicates,
	}

	// Distribution
	report.Distribution = i.calculateDistribution()

	// Issues
	report.Issues = i.detectIssues()

	return report
}

// QualityReport is the structured quality analysis output.
type QualityReport struct {
	TotalEvents     int64  `json:"total_events"`
	TotalCases      int64  `json:"total_cases"`
	TotalActivities int64  `json:"total_activities"`
	MinTimestamp    string `json:"min_timestamp,omitempty"`
	MaxTimestamp    string `json:"max_timestamp,omitempty"`
	TimeSpan        string `json:"time_span,omitempty"`

	Completeness CompletenessReport `json:"completeness"`
	Uniqueness   UniquenessReport   `json:"uniqueness"`
	Distribution DistributionReport `json:"distribution"`
	Issues       []Issue            `json:"issues,omitempty"`
}

// CompletenessReport shows missing value percentages.
type CompletenessReport struct {
	CaseIDPct    float64 `json:"case_id_pct"`
	ActivityPct  float64 `json:"activity_pct"`
	TimestampPct float64 `json:"timestamp_pct"`
	ResourcePct  float64 `json:"resource_pct"`
}

// UniquenessReport shows distinct value counts.
type UniquenessReport struct {
	DistinctCases      int64 `json:"distinct_cases"`
	DistinctActivities int64 `json:"distinct_activities"`
	DistinctResources  int64 `json:"distinct_resources"`
	DuplicateEvents    int64 `json:"duplicate_events"`
}

// DistributionReport shows data distribution.
type DistributionReport struct {
	AvgEventsPerCase    float64         `json:"avg_events_per_case"`
	MinEventsPerCase    int64           `json:"min_events_per_case"`
	MaxEventsPerCase    int64           `json:"max_events_per_case"`
	MedianEventsPerCase int64           `json:"median_events_per_case"`
	TopActivities       []ActivityCount `json:"top_activities,omitempty"`
}

// ActivityCount pairs activity with count.
type ActivityCount struct {
	Activity string `json:"activity"`
	Count    int64  `json:"count"`
}

// Issue describes a quality problem.
type Issue struct {
	Severity     string `json:"severity"`
	Category     string `json:"category"`
	Description  string `json:"description"`
	AffectedRows int64  `json:"affected_rows"`
}

func (i *QualityInspector) calculateDistribution() DistributionReport {
	dr := DistributionReport{}

	if len(i.cases) == 0 {
		return dr
	}

	// Events per case
	counts := make([]int64, 0, len(i.cases))
	var total int64
	for _, c := range i.cases {
		counts = append(counts, c)
		total += c
	}

	sort.Slice(counts, func(a, b int) bool { return counts[a] < counts[b] })

	dr.MinEventsPerCase = counts[0]
	dr.MaxEventsPerCase = counts[len(counts)-1]
	dr.AvgEventsPerCase = float64(total) / float64(len(i.cases))
	dr.MedianEventsPerCase = counts[len(counts)/2]

	// Top activities
	dr.TopActivities = i.topActivities(5)

	return dr
}

func (i *QualityInspector) topActivities(n int) []ActivityCount {
	type kv struct {
		k string
		v int64
	}

	var sorted []kv
	for k, v := range i.activities {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(a, b int) bool { return sorted[a].v > sorted[b].v })

	result := make([]ActivityCount, 0, n)
	for j := 0; j < n && j < len(sorted); j++ {
		result = append(result, ActivityCount{sorted[j].k, sorted[j].v})
	}
	return result
}

func (i *QualityInspector) detectIssues() []Issue {
	var issues []Issue

	if i.missingCaseIDs > 0 {
		issues = append(issues, Issue{
			Severity:     "error",
			Category:     "completeness",
			Description:  "Missing Case IDs",
			AffectedRows: i.missingCaseIDs,
		})
	}

	if i.missingTimestamps > 0 {
		issues = append(issues, Issue{
			Severity:     "error",
			Category:     "completeness",
			Description:  "Missing Timestamps",
			AffectedRows: i.missingTimestamps,
		})
	}

	if i.duplicates > 0 {
		issues = append(issues, Issue{
			Severity:     "warning",
			Category:     "consistency",
			Description:  "Duplicate events detected",
			AffectedRows: i.duplicates,
		})
	}

	if i.outOfOrderCount > 0 {
		issues = append(issues, Issue{
			Severity:     "warning",
			Category:     "consistency",
			Description:  "Out-of-order timestamps",
			AffectedRows: i.outOfOrderCount,
		})
	}

	return issues
}

// ToJSON returns the report as JSON.
func (r *QualityReport) ToJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// String returns a human-readable summary.
func (r *QualityReport) String() string {
	return fmt.Sprintf(`Quality Report
==============
Events: %d | Cases: %d | Activities: %d
Time: %s to %s (%s)
Completeness: CaseID=%.1f%% Activity=%.1f%% Timestamp=%.1f%% Resource=%.1f%%
Distribution: Avg=%.1f events/case, Min=%d, Max=%d
Issues: %d found
`,
		r.TotalEvents, r.TotalCases, r.TotalActivities,
		r.MinTimestamp, r.MaxTimestamp, r.TimeSpan,
		r.Completeness.CaseIDPct, r.Completeness.ActivityPct,
		r.Completeness.TimestampPct, r.Completeness.ResourcePct,
		r.Distribution.AvgEventsPerCase, r.Distribution.MinEventsPerCase,
		r.Distribution.MaxEventsPerCase, len(r.Issues),
	)
}
