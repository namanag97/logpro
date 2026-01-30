package diff

import (
	"context"
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

func TestNewAnalyzer(t *testing.T) {
	a := NewAnalyzer()
	if a == nil {
		t.Fatal("NewAnalyzer returned nil")
	}
}

func TestAnalyzerEmptyReport(t *testing.T) {
	a := NewAnalyzer()
	r := a.Report()
	if r == nil {
		t.Fatal("Report returned nil")
	}
	if r.LeftEventCount != 0 || r.RightEventCount != 0 {
		t.Errorf("empty report counts: left=%d right=%d", r.LeftEventCount, r.RightEventCount)
	}
}

func TestAnalyzerAddLeftRight(t *testing.T) {
	a := NewAnalyzer()
	now := time.Now()

	a.AddLeft(makeEvent("c1", "A", "R1", now))
	a.AddLeft(makeEvent("c1", "B", "R2", now.Add(time.Hour)))
	a.AddRight(makeEvent("c1", "A", "R1", now))
	a.AddRight(makeEvent("c1", "C", "R3", now.Add(time.Hour)))
	a.AddRight(makeEvent("c2", "A", "R1", now))

	r := a.Report()
	if r.LeftEventCount != 2 {
		t.Errorf("LeftEventCount = %d, want 2", r.LeftEventCount)
	}
	if r.RightEventCount != 3 {
		t.Errorf("RightEventCount = %d, want 3", r.RightEventCount)
	}
}

func TestAnalyzerActivityChanges(t *testing.T) {
	a := NewAnalyzer()
	now := time.Now()

	// Left: 3 events of A, 1 of B
	a.AddLeft(makeEvent("c1", "A", "R1", now))
	a.AddLeft(makeEvent("c1", "A", "R1", now.Add(time.Minute)))
	a.AddLeft(makeEvent("c1", "A", "R1", now.Add(2*time.Minute)))
	a.AddLeft(makeEvent("c1", "B", "R1", now.Add(3*time.Minute)))

	// Right: 1 event of A, 3 of B
	a.AddRight(makeEvent("c1", "A", "R1", now))
	a.AddRight(makeEvent("c1", "B", "R1", now.Add(time.Minute)))
	a.AddRight(makeEvent("c1", "B", "R1", now.Add(2*time.Minute)))
	a.AddRight(makeEvent("c1", "B", "R1", now.Add(3*time.Minute)))

	r := a.Report()
	if len(r.ActivityChanges) == 0 {
		t.Error("expected activity changes")
	}
}

func TestAnalyzerNewAndRemovedActivities(t *testing.T) {
	a := NewAnalyzer()
	now := time.Now()

	a.AddLeft(makeEvent("c1", "A", "R1", now))
	a.AddLeft(makeEvent("c1", "B", "R1", now.Add(time.Minute)))

	a.AddRight(makeEvent("c1", "A", "R1", now))
	a.AddRight(makeEvent("c1", "C", "R1", now.Add(time.Minute)))

	r := a.Report()

	foundNew := false
	for _, act := range r.NewActivities {
		if act == "C" {
			foundNew = true
		}
	}
	if !foundNew {
		t.Error("expected 'C' in NewActivities")
	}

	foundRemoved := false
	for _, act := range r.RemovedActivities {
		if act == "B" {
			foundRemoved = true
		}
	}
	if !foundRemoved {
		t.Error("expected 'B' in RemovedActivities")
	}
}

func TestAnalyzerResourceChanges(t *testing.T) {
	a := NewAnalyzer()
	now := time.Now()

	a.AddLeft(makeEvent("c1", "A", "Alice", now))
	a.AddLeft(makeEvent("c1", "B", "Bob", now.Add(time.Minute)))

	a.AddRight(makeEvent("c1", "A", "Alice", now))
	a.AddRight(makeEvent("c1", "B", "Charlie", now.Add(time.Minute)))

	r := a.Report()
	if len(r.ResourceChanges) == 0 {
		t.Error("expected resource changes")
	}
}

func TestAnalyzerDurationDelta(t *testing.T) {
	a := NewAnalyzer()
	now := time.Now()

	// Left: case takes 1 hour
	a.AddLeft(makeEvent("c1", "A", "R1", now))
	a.AddLeft(makeEvent("c1", "B", "R1", now.Add(time.Hour)))

	// Right: case takes 2 hours
	a.AddRight(makeEvent("c1", "A", "R1", now))
	a.AddRight(makeEvent("c1", "B", "R1", now.Add(2*time.Hour)))

	r := a.Report()
	if r.AvgCaseDurationLeft == 0 {
		t.Error("AvgCaseDurationLeft should be non-zero")
	}
	if r.AvgCaseDurationRight == 0 {
		t.Error("AvgCaseDurationRight should be non-zero")
	}
}

func TestDiffReportString(t *testing.T) {
	a := NewAnalyzer()
	now := time.Now()
	a.AddLeft(makeEvent("c1", "A", "R1", now))
	a.AddRight(makeEvent("c1", "B", "R2", now))

	r := a.Report()
	s := r.String()
	if s == "" {
		t.Error("Report.String() should not be empty")
	}
}

func TestTimeRangeDuration(t *testing.T) {
	now := time.Now()
	tr := TimeRange{
		Min: now,
		Max: now.Add(24 * time.Hour),
	}
	d := tr.Duration()
	if d != 24*time.Hour {
		t.Errorf("Duration = %v, want 24h", d)
	}
}

func TestCompareFunction(t *testing.T) {
	now := time.Now()
	leftCh := make(chan *model.Event, 2)
	rightCh := make(chan *model.Event, 2)

	leftCh <- makeEvent("c1", "A", "R1", now)
	leftCh <- makeEvent("c1", "B", "R1", now.Add(time.Hour))
	close(leftCh)

	rightCh <- makeEvent("c1", "A", "R1", now)
	rightCh <- makeEvent("c1", "C", "R2", now.Add(time.Hour))
	close(rightCh)

	ctx := context.Background()
	r, err := Compare(ctx, leftCh, rightCh)
	if err != nil {
		t.Fatalf("Compare error: %v", err)
	}
	if r == nil {
		t.Fatal("Compare returned nil report")
	}
	if r.LeftEventCount != 2 || r.RightEventCount != 2 {
		t.Errorf("counts: left=%d right=%d, want 2/2", r.LeftEventCount, r.RightEventCount)
	}
}

func TestCompareContextCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	leftCh := make(chan *model.Event, 1)
	rightCh := make(chan *model.Event, 1)

	// Close channels so Compare doesn't block
	close(leftCh)
	close(rightCh)
	cancel()

	r, err := Compare(ctx, leftCh, rightCh)
	// Should handle closed channels gracefully
	_ = r
	_ = err
}
