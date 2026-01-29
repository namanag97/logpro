// Package scheduler provides default scheduler implementations.
package scheduler

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/logflow/logflow/pkg/interfaces"
)

// ErrJobNotFound is returned when a job is not found.
var ErrJobNotFound = errors.New("job not found")

// NoopScheduler executes jobs immediately without scheduling.
// Jobs run synchronously in the Submit call.
type NoopScheduler struct {
	mu    sync.RWMutex
	jobs  map[interfaces.JobID]*noopJob
	nextID int64
}

// NewNoopScheduler creates a new immediate-execution scheduler.
func NewNoopScheduler() *NoopScheduler {
	return &NoopScheduler{
		jobs: make(map[interfaces.JobID]*noopJob),
	}
}

// Submit executes the job immediately (synchronously).
// In this implementation, jobs complete before Submit returns.
func (s *NoopScheduler) Submit(ctx context.Context, spec interfaces.JobSpec) (interfaces.JobID, error) {
	id := interfaces.JobID(s.generateID())

	job := &noopJob{
		id:        id,
		spec:      spec,
		status:    interfaces.JobStatusRunning,
		startedAt: time.Now(),
	}

	s.mu.Lock()
	s.jobs[id] = job
	s.mu.Unlock()

	// Mark as succeeded immediately (noop execution)
	job.status = interfaces.JobStatusSucceeded
	job.completedAt = time.Now()

	return id, nil
}

// Cancel cancels a job.
func (s *NoopScheduler) Cancel(ctx context.Context, id interfaces.JobID) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	job, ok := s.jobs[id]
	if !ok {
		return ErrJobNotFound
	}

	if job.status == interfaces.JobStatusRunning || job.status == interfaces.JobStatusPending {
		job.status = interfaces.JobStatusCancelled
		job.completedAt = time.Now()
	}

	return nil
}

// Get retrieves a job by ID.
func (s *NoopScheduler) Get(ctx context.Context, id interfaces.JobID) (interfaces.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, ok := s.jobs[id]
	if !ok {
		return nil, ErrJobNotFound
	}

	return job, nil
}

// List lists jobs matching the filter.
func (s *NoopScheduler) List(ctx context.Context, filter interfaces.JobFilter) ([]interfaces.Job, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var result []interfaces.Job
	for _, job := range s.jobs {
		if matchesFilter(job, filter) {
			result = append(result, job)
		}
	}

	return result, nil
}

// Schedule creates a recurring job (noop - just submits once).
func (s *NoopScheduler) Schedule(ctx context.Context, spec interfaces.JobSpec, schedule interfaces.Schedule) (interfaces.JobID, error) {
	// In noop implementation, just submit once
	return s.Submit(ctx, spec)
}

// Unschedule removes a scheduled job.
func (s *NoopScheduler) Unschedule(ctx context.Context, id interfaces.JobID) error {
	return s.Cancel(ctx, id)
}

// Wait waits for a job to complete.
func (s *NoopScheduler) Wait(ctx context.Context, id interfaces.JobID) error {
	// Jobs complete immediately in noop scheduler
	return nil
}

func (s *NoopScheduler) generateID() string {
	return time.Now().Format("20060102150405") + "-" +
		string(rune('A'+atomic.AddInt64(&s.nextID, 1)%26))
}

func matchesFilter(job *noopJob, filter interfaces.JobFilter) bool {
	if len(filter.Status) > 0 {
		found := false
		for _, status := range filter.Status {
			if job.status == status {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if len(filter.Type) > 0 {
		found := false
		for _, t := range filter.Type {
			if job.spec.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	if !filter.Since.IsZero() && job.startedAt.Before(filter.Since) {
		return false
	}

	if !filter.Until.IsZero() && job.startedAt.After(filter.Until) {
		return false
	}

	return true
}

// noopJob implements interfaces.Job
type noopJob struct {
	id          interfaces.JobID
	spec        interfaces.JobSpec
	status      interfaces.JobStatus
	startedAt   time.Time
	completedAt time.Time
	err         error
	output      interface{}
}

func (j *noopJob) ID() interfaces.JobID           { return j.id }
func (j *noopJob) Spec() interfaces.JobSpec       { return j.spec }
func (j *noopJob) Status() interfaces.JobStatus   { return j.status }
func (j *noopJob) Progress() float64              {
	if j.status == interfaces.JobStatusSucceeded || j.status == interfaces.JobStatusFailed {
		return 1.0
	}
	return 0.0
}
func (j *noopJob) StartedAt() time.Time           { return j.startedAt }
func (j *noopJob) CompletedAt() time.Time         { return j.completedAt }
func (j *noopJob) Error() error                   { return j.err }
func (j *noopJob) Metrics() interfaces.JobMetrics { return interfaces.JobMetrics{} }
func (j *noopJob) Output() interface{}            { return j.output }

// Verify interface compliance.
var _ interfaces.Scheduler = (*NoopScheduler)(nil)
var _ interfaces.Job = (*noopJob)(nil)
