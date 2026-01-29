// Package inspect provides data quality analysis and inspection utilities.
package inspect

import (
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/logflow/logflow/internal/model"
)

// QualityReport contains comprehensive data quality metrics.
type QualityReport struct {
	// Basic counts
	TotalEvents    int64 `json:"total_events"`
	TotalCases     int64 `json:"total_cases"`
	TotalActivities int64 `json:"total_activities"`

	// Time range
	MinTimestamp time.Time `json:"min_timestamp"`
	MaxTimestamp time.Time `json:"max_timestamp"`
	TimeSpan     string    `json:"time_span"`

	// Completeness metrics (% non-null)
	Completeness CompletenessMetrics `json:"completeness"`

	// Uniqueness metrics
	Uniqueness UniquenessMetrics `json:"uniqueness"`

	// Distribution metrics
	Distribution DistributionMetrics `json:"distribution"`

	// Quality issues
	Issues []QualityIssue `json:"issues"`

	// Warnings
	Warnings []string `json:"warnings"`
}

// CompletenessMetrics tracks null/missing values.
type CompletenessMetrics struct {
	CaseIDComplete    float64 `json:"case_id_complete_pct"`
	ActivityComplete  float64 `json:"activity_complete_pct"`
	TimestampComplete float64 `json:"timestamp_complete_pct"`
	ResourceComplete  float64 `json:"resource_complete_pct"`

	MissingCaseIDs    int64 `json:"missing_case_ids"`
	MissingActivities int64 `json:"missing_activities"`
	MissingTimestamps int64 `json:"missing_timestamps"`
	MissingResources  int64 `json:"missing_resources"`
}

// UniquenessMetrics tracks distinct values and duplicates.
type UniquenessMetrics struct {
	DistinctCases      int64 `json:"distinct_cases"`
	DistinctActivities int64 `json:"distinct_activities"`
	DistinctResources  int64 `json:"distinct_resources"`
	DuplicateEvents    int64 `json:"duplicate_events"`
}

// DistributionMetrics tracks data distribution.
type DistributionMetrics struct {
	AvgEventsPerCase float64 `json:"avg_events_per_case"`
	MinEventsPerCase int64   `json:"min_events_per_case"`
	MaxEventsPerCase int64   `json:"max_events_per_case"`
	MedianEventsPerCase int64 `json:"median_events_per_case"`

	// Top activities by frequency
	TopActivities []ActivityCount `json:"top_activities"`

	// Top resources by frequency
	TopResources []ResourceCount `json:"top_resources"`
}

// ActivityCount holds activity frequency.
type ActivityCount struct {
	Activity string `json:"activity"`
	Count    int64  `json:"count"`
}

// ResourceCount holds resource frequency.
type ResourceCount struct {
	Resource string `json:"resource"`
	Count    int64  `json:"count"`
}

// QualityIssue describes a specific data quality problem.
type QualityIssue struct {
	Severity    string `json:"severity"` // "error", "warning", "info"
	Category    string `json:"category"` // "completeness", "consistency", "validity"
	Description string `json:"description"`
	AffectedRows int64 `json:"affected_rows"`
}

// QualityAnalyzer performs streaming data quality analysis.
type QualityAnalyzer struct {
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

	// Distinct value tracking (using maps for uniqueness)
	cases      map[string]int64 // case_id -> event count
	activities map[string]int64 // activity -> count
	resources  map[string]int64 // resource -> count

	// Duplicate detection (hash of case_id + activity + timestamp)
	eventHashes map[string]int64
	duplicates  int64

	// Timestamp ordering
	lastTimestamp    int64
	outOfOrderCount  int64
}

// NewQualityAnalyzer creates a new analyzer.
func NewQualityAnalyzer() *QualityAnalyzer {
	return &QualityAnalyzer{
		minTimestamp: math.MaxInt64,
		maxTimestamp: math.MinInt64,
		cases:        make(map[string]int64),
		activities:   make(map[string]int64),
		resources:    make(map[string]int64),
		eventHashes:  make(map[string]int64),
	}
}

// Add processes an event for quality analysis.
func (a *QualityAnalyzer) Add(event *model.Event) {
	a.mu.Lock()
	defer a.mu.Unlock()

	a.totalEvents++

	// Completeness checks
	if len(event.CaseID) == 0 {
		a.missingCaseIDs++
	} else {
		a.cases[string(event.CaseID)]++
	}

	if len(event.Activity) == 0 {
		a.missingActivities++
	} else {
		a.activities[string(event.Activity)]++
	}

	if event.Timestamp == 0 {
		a.missingTimestamps++
	} else {
		if event.Timestamp < a.minTimestamp {
			a.minTimestamp = event.Timestamp
		}
		if event.Timestamp > a.maxTimestamp {
			a.maxTimestamp = event.Timestamp
		}

		// Check ordering
		if event.Timestamp < a.lastTimestamp {
			a.outOfOrderCount++
		}
		a.lastTimestamp = event.Timestamp
	}

	if len(event.Resource) == 0 {
		a.missingResources++
	} else {
		a.resources[string(event.Resource)]++
	}

	// Duplicate detection
	hash := fmt.Sprintf("%s|%s|%d", event.CaseID, event.Activity, event.Timestamp)
	if a.eventHashes[hash] > 0 {
		a.duplicates++
	}
	a.eventHashes[hash]++
}

// Report generates the quality report.
func (a *QualityAnalyzer) Report() *QualityReport {
	a.mu.Lock()
	defer a.mu.Unlock()

	report := &QualityReport{
		TotalEvents:     a.totalEvents,
		TotalCases:      int64(len(a.cases)),
		TotalActivities: int64(len(a.activities)),
	}

	// Time range
	if a.minTimestamp != math.MaxInt64 {
		report.MinTimestamp = time.Unix(0, a.minTimestamp)
		report.MaxTimestamp = time.Unix(0, a.maxTimestamp)
		duration := time.Duration(a.maxTimestamp - a.minTimestamp)
		report.TimeSpan = duration.String()
	}

	// Completeness
	if a.totalEvents > 0 {
		report.Completeness = CompletenessMetrics{
			CaseIDComplete:    100.0 * float64(a.totalEvents-a.missingCaseIDs) / float64(a.totalEvents),
			ActivityComplete:  100.0 * float64(a.totalEvents-a.missingActivities) / float64(a.totalEvents),
			TimestampComplete: 100.0 * float64(a.totalEvents-a.missingTimestamps) / float64(a.totalEvents),
			ResourceComplete:  100.0 * float64(a.totalEvents-a.missingResources) / float64(a.totalEvents),
			MissingCaseIDs:    a.missingCaseIDs,
			MissingActivities: a.missingActivities,
			MissingTimestamps: a.missingTimestamps,
			MissingResources:  a.missingResources,
		}
	}

	// Uniqueness
	report.Uniqueness = UniquenessMetrics{
		DistinctCases:      int64(len(a.cases)),
		DistinctActivities: int64(len(a.activities)),
		DistinctResources:  int64(len(a.resources)),
		DuplicateEvents:    a.duplicates,
	}

	// Distribution
	report.Distribution = a.calculateDistribution()

	// Issues
	report.Issues = a.detectIssues()

	// Warnings
	report.Warnings = a.generateWarnings()

	return report
}

// calculateDistribution computes distribution metrics.
func (a *QualityAnalyzer) calculateDistribution() DistributionMetrics {
	dm := DistributionMetrics{}

	if len(a.cases) == 0 {
		return dm
	}

	// Events per case statistics
	eventCounts := make([]int64, 0, len(a.cases))
	var total int64
	for _, count := range a.cases {
		eventCounts = append(eventCounts, count)
		total += count
	}

	sort.Slice(eventCounts, func(i, j int) bool { return eventCounts[i] < eventCounts[j] })

	dm.MinEventsPerCase = eventCounts[0]
	dm.MaxEventsPerCase = eventCounts[len(eventCounts)-1]
	dm.AvgEventsPerCase = float64(total) / float64(len(a.cases))
	dm.MedianEventsPerCase = eventCounts[len(eventCounts)/2]

	// Top activities
	dm.TopActivities = a.topN(a.activities, 10)

	// Top resources
	dm.TopResources = a.topNResources(a.resources, 10)

	return dm
}

// topN returns the top N activities by frequency.
func (a *QualityAnalyzer) topN(m map[string]int64, n int) []ActivityCount {
	type kv struct {
		key   string
		value int64
	}

	var sorted []kv
	for k, v := range m {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].value > sorted[j].value })

	result := make([]ActivityCount, 0, n)
	for i := 0; i < n && i < len(sorted); i++ {
		result = append(result, ActivityCount{
			Activity: sorted[i].key,
			Count:    sorted[i].value,
		})
	}
	return result
}

// topNResources returns the top N resources by frequency.
func (a *QualityAnalyzer) topNResources(m map[string]int64, n int) []ResourceCount {
	type kv struct {
		key   string
		value int64
	}

	var sorted []kv
	for k, v := range m {
		sorted = append(sorted, kv{k, v})
	}
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].value > sorted[j].value })

	result := make([]ResourceCount, 0, n)
	for i := 0; i < n && i < len(sorted); i++ {
		result = append(result, ResourceCount{
			Resource: sorted[i].key,
			Count:    sorted[i].value,
		})
	}
	return result
}

// detectIssues identifies quality problems.
func (a *QualityAnalyzer) detectIssues() []QualityIssue {
	var issues []QualityIssue

	// Missing critical fields
	if a.missingCaseIDs > 0 {
		issues = append(issues, QualityIssue{
			Severity:     "error",
			Category:     "completeness",
			Description:  "Missing Case IDs detected",
			AffectedRows: a.missingCaseIDs,
		})
	}

	if a.missingActivities > 0 {
		issues = append(issues, QualityIssue{
			Severity:     "error",
			Category:     "completeness",
			Description:  "Missing Activity names detected",
			AffectedRows: a.missingActivities,
		})
	}

	if a.missingTimestamps > 0 {
		issues = append(issues, QualityIssue{
			Severity:     "error",
			Category:     "completeness",
			Description:  "Missing Timestamps detected",
			AffectedRows: a.missingTimestamps,
		})
	}

	// Duplicates
	if a.duplicates > 0 {
		issues = append(issues, QualityIssue{
			Severity:     "warning",
			Category:     "consistency",
			Description:  "Duplicate events detected (same case, activity, timestamp)",
			AffectedRows: a.duplicates,
		})
	}

	// Out of order timestamps
	if a.outOfOrderCount > 0 {
		issues = append(issues, QualityIssue{
			Severity:     "warning",
			Category:     "consistency",
			Description:  "Timestamps not in chronological order",
			AffectedRows: a.outOfOrderCount,
		})
	}

	return issues
}

// generateWarnings creates warning messages.
func (a *QualityAnalyzer) generateWarnings() []string {
	var warnings []string

	// High missing resource rate
	if a.totalEvents > 0 {
		missingResourceRate := float64(a.missingResources) / float64(a.totalEvents)
		if missingResourceRate > 0.5 {
			warnings = append(warnings, fmt.Sprintf("%.1f%% of events have no resource assigned", missingResourceRate*100))
		}
	}

	// Low event count per case
	if len(a.cases) > 0 {
		avgEvents := float64(a.totalEvents) / float64(len(a.cases))
		if avgEvents < 2 {
			warnings = append(warnings, fmt.Sprintf("Average of %.1f events per case - consider verifying case ID mapping", avgEvents))
		}
	}

	// Single-event cases
	singleEventCases := 0
	for _, count := range a.cases {
		if count == 1 {
			singleEventCases++
		}
	}
	if len(a.cases) > 0 && float64(singleEventCases)/float64(len(a.cases)) > 0.3 {
		warnings = append(warnings, fmt.Sprintf("%.1f%% of cases have only one event", float64(singleEventCases)/float64(len(a.cases))*100))
	}

	return warnings
}

// ToJSON serializes the report to JSON.
func (r *QualityReport) ToJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// String returns a human-readable summary.
func (r *QualityReport) String() string {
	return fmt.Sprintf(`Data Quality Report
==================
Total Events:     %d
Total Cases:      %d
Total Activities: %d

Time Range:
  From: %s
  To:   %s
  Span: %s

Completeness:
  Case ID:    %.1f%% (%d missing)
  Activity:   %.1f%% (%d missing)
  Timestamp:  %.1f%% (%d missing)
  Resource:   %.1f%% (%d missing)

Distribution:
  Events per Case: min=%d, max=%d, avg=%.1f, median=%d
  Duplicate Events: %d

Issues Found: %d
Warnings: %d
`,
		r.TotalEvents, r.TotalCases, r.TotalActivities,
		r.MinTimestamp.Format(time.RFC3339),
		r.MaxTimestamp.Format(time.RFC3339),
		r.TimeSpan,
		r.Completeness.CaseIDComplete, r.Completeness.MissingCaseIDs,
		r.Completeness.ActivityComplete, r.Completeness.MissingActivities,
		r.Completeness.TimestampComplete, r.Completeness.MissingTimestamps,
		r.Completeness.ResourceComplete, r.Completeness.MissingResources,
		r.Distribution.MinEventsPerCase, r.Distribution.MaxEventsPerCase,
		r.Distribution.AvgEventsPerCase, r.Distribution.MedianEventsPerCase,
		r.Uniqueness.DuplicateEvents,
		len(r.Issues), len(r.Warnings),
	)
}
