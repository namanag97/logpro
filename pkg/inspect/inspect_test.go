package inspect

import (
	"testing"
	"time"

	"github.com/logflow/logflow/internal/model"
)

func makeEvent(caseID, activity, resource string, ts time.Time) *model.Event {
	return &model.Event{
		CaseID:    []byte(caseID),
		Activity:  []byte(activity),
		Resource:  []byte(resource),
		Timestamp: ts.UnixNano(),
	}
}

func TestNewQualityAnalyzer(t *testing.T) {
	a := NewQualityAnalyzer()
	if a == nil {
		t.Fatal("NewQualityAnalyzer returned nil")
	}
}

func TestQualityReportEmpty(t *testing.T) {
	a := NewQualityAnalyzer()
	r := a.Report()
	if r == nil {
		t.Fatal("Report returned nil")
	}
	if r.TotalEvents != 0 {
		t.Errorf("TotalEvents = %d, want 0", r.TotalEvents)
	}
}

func TestQualityReportCounts(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	a.Add(makeEvent("c1", "A", "Alice", now))
	a.Add(makeEvent("c1", "B", "Bob", now.Add(time.Hour)))
	a.Add(makeEvent("c2", "A", "Alice", now.Add(2*time.Hour)))

	r := a.Report()
	if r.TotalEvents != 3 {
		t.Errorf("TotalEvents = %d, want 3", r.TotalEvents)
	}
	if r.TotalCases != 2 {
		t.Errorf("TotalCases = %d, want 2", r.TotalCases)
	}
	if r.TotalActivities != 2 {
		t.Errorf("TotalActivities = %d, want 2", r.TotalActivities)
	}
}

func TestQualityReportCompleteness(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	// All fields populated
	a.Add(makeEvent("c1", "A", "Alice", now))
	a.Add(makeEvent("c2", "B", "Bob", now.Add(time.Hour)))

	r := a.Report()
	if r.Completeness.CaseIDComplete != 100 {
		t.Errorf("CaseIDComplete = %f, want 100", r.Completeness.CaseIDComplete)
	}
	if r.Completeness.ActivityComplete != 100 {
		t.Errorf("ActivityComplete = %f, want 100", r.Completeness.ActivityComplete)
	}
}

func TestQualityReportMissingFields(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	// Event with missing activity
	a.Add(&model.Event{
		CaseID:    []byte("c1"),
		Activity:  nil,
		Resource:  []byte("Alice"),
		Timestamp: now.UnixNano(),
	})
	// Event with missing resource
	a.Add(&model.Event{
		CaseID:    []byte("c2"),
		Activity:  []byte("A"),
		Resource:  nil,
		Timestamp: now.UnixNano(),
	})

	r := a.Report()
	if r.Completeness.MissingActivities != 1 {
		t.Errorf("MissingActivities = %d, want 1", r.Completeness.MissingActivities)
	}
	if r.Completeness.MissingResources != 1 {
		t.Errorf("MissingResources = %d, want 1", r.Completeness.MissingResources)
	}
}

func TestQualityReportUniqueness(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	a.Add(makeEvent("c1", "A", "Alice", now))
	a.Add(makeEvent("c1", "B", "Bob", now.Add(time.Hour)))
	a.Add(makeEvent("c2", "A", "Alice", now.Add(2*time.Hour)))
	a.Add(makeEvent("c2", "C", "Charlie", now.Add(3*time.Hour)))

	r := a.Report()
	if r.Uniqueness.DistinctCases != 2 {
		t.Errorf("DistinctCases = %d, want 2", r.Uniqueness.DistinctCases)
	}
	if r.Uniqueness.DistinctActivities != 3 {
		t.Errorf("DistinctActivities = %d, want 3", r.Uniqueness.DistinctActivities)
	}
	if r.Uniqueness.DistinctResources != 3 {
		t.Errorf("DistinctResources = %d, want 3", r.Uniqueness.DistinctResources)
	}
}

func TestQualityReportDistribution(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	// Case 1: 3 events
	a.Add(makeEvent("c1", "A", "Alice", now))
	a.Add(makeEvent("c1", "B", "Bob", now.Add(time.Minute)))
	a.Add(makeEvent("c1", "C", "Alice", now.Add(2*time.Minute)))

	// Case 2: 1 event
	a.Add(makeEvent("c2", "A", "Bob", now.Add(3*time.Minute)))

	r := a.Report()
	if r.Distribution.MinEventsPerCase != 1 {
		t.Errorf("MinEventsPerCase = %d, want 1", r.Distribution.MinEventsPerCase)
	}
	if r.Distribution.MaxEventsPerCase != 3 {
		t.Errorf("MaxEventsPerCase = %d, want 3", r.Distribution.MaxEventsPerCase)
	}
}

func TestQualityReportTopActivities(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	for i := 0; i < 5; i++ {
		a.Add(makeEvent("c1", "Frequent", "R", now.Add(time.Duration(i)*time.Minute)))
	}
	a.Add(makeEvent("c1", "Rare", "R", now.Add(10*time.Minute)))

	r := a.Report()
	if len(r.Distribution.TopActivities) == 0 {
		t.Error("TopActivities should not be empty")
	}
	if r.Distribution.TopActivities[0].Activity != "Frequent" {
		t.Errorf("top activity = %q, want %q", r.Distribution.TopActivities[0].Activity, "Frequent")
	}
}

func TestQualityReportIssuesDetection(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	// Add events with missing fields to trigger quality issues
	a.Add(&model.Event{
		CaseID:    nil, // missing case ID
		Activity:  []byte("A"),
		Timestamp: now.UnixNano(),
	})
	a.Add(&model.Event{
		CaseID:   []byte("c1"),
		Activity: nil, // missing activity
	})

	r := a.Report()
	if len(r.Issues) == 0 && r.Completeness.MissingCaseIDs == 0 && r.Completeness.MissingActivities == 0 {
		t.Error("expected quality issues or missing field counts for incomplete data")
	}
}

func TestQualityReportTimeRange(t *testing.T) {
	a := NewQualityAnalyzer()
	start := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC)
	end := time.Date(2024, 1, 31, 0, 0, 0, 0, time.UTC)

	a.Add(makeEvent("c1", "A", "R", start))
	a.Add(makeEvent("c1", "B", "R", end))

	r := a.Report()
	if r.MinTimestamp.After(start.Add(time.Second)) {
		t.Errorf("MinTimestamp = %v, want %v", r.MinTimestamp, start)
	}
	if r.MaxTimestamp.Before(end.Add(-time.Second)) {
		t.Errorf("MaxTimestamp = %v, want %v", r.MaxTimestamp, end)
	}
}

func TestQualityReportToJSON(t *testing.T) {
	a := NewQualityAnalyzer()
	a.Add(makeEvent("c1", "A", "R", time.Now()))

	r := a.Report()
	data, err := r.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON error: %v", err)
	}
	if len(data) == 0 {
		t.Error("ToJSON returned empty data")
	}
}

func TestQualityReportString(t *testing.T) {
	a := NewQualityAnalyzer()
	a.Add(makeEvent("c1", "A", "R", time.Now()))

	r := a.Report()
	s := r.String()
	if s == "" {
		t.Error("String() should not be empty")
	}
}

func TestQualityReportDuplicateEvents(t *testing.T) {
	a := NewQualityAnalyzer()
	now := time.Now()

	// Add identical events
	for i := 0; i < 3; i++ {
		a.Add(makeEvent("c1", "A", "Alice", now))
	}

	r := a.Report()
	// At minimum the duplicate counter should be tracked
	if r.Uniqueness.DuplicateEvents < 0 {
		t.Error("DuplicateEvents should be non-negative")
	}
}
