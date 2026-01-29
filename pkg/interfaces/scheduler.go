package interfaces

import (
	"context"
	"time"
)

// JobID uniquely identifies a job.
type JobID string

// Scheduler manages job scheduling and execution.
type Scheduler interface {
	// Job lifecycle
	Submit(ctx context.Context, spec JobSpec) (JobID, error)
	Cancel(ctx context.Context, id JobID) error
	Get(ctx context.Context, id JobID) (Job, error)

	// Listing
	List(ctx context.Context, filter JobFilter) ([]Job, error)

	// Scheduled jobs
	Schedule(ctx context.Context, spec JobSpec, schedule Schedule) (JobID, error)
	Unschedule(ctx context.Context, id JobID) error

	// Wait for job completion
	Wait(ctx context.Context, id JobID) error
}

// JobSpec defines a job to execute.
type JobSpec struct {
	Name      string
	Type      JobType
	Config    map[string]interface{}
	Priority  int
	Timeout   time.Duration
	Retries   int
	DependsOn []JobID
	Tags      map[string]string
}

// JobType categorizes different types of jobs.
type JobType string

const (
	JobTypeIngest     JobType = "ingest"
	JobTypeTransform  JobType = "transform"
	JobTypeExport     JobType = "export"
	JobTypeCompaction JobType = "compaction"
	JobTypeVacuum     JobType = "vacuum"
	JobTypeQuery      JobType = "query"
	JobTypeCustom     JobType = "custom"
)

// Job represents a running or completed job.
type Job interface {
	ID() JobID
	Spec() JobSpec
	Status() JobStatus
	Progress() float64 // 0.0 to 1.0
	StartedAt() time.Time
	CompletedAt() time.Time
	Error() error
	Metrics() JobMetrics
	Output() interface{}
}

// JobStatus represents the current state of a job.
type JobStatus string

const (
	JobStatusPending   JobStatus = "pending"
	JobStatusRunning   JobStatus = "running"
	JobStatusSucceeded JobStatus = "succeeded"
	JobStatusFailed    JobStatus = "failed"
	JobStatusCancelled JobStatus = "cancelled"
	JobStatusRetrying  JobStatus = "retrying"
)

// JobFilter specifies criteria for listing jobs.
type JobFilter struct {
	Status []JobStatus
	Type   []JobType
	Tags   map[string]string
	Since  time.Time
	Until  time.Time
	Limit  int
}

// JobMetrics contains execution metrics for a job.
type JobMetrics struct {
	RowsProcessed   int64
	BytesProcessed  int64
	RowsWritten     int64
	BytesWritten    int64
	ErrorCount      int64
	Duration        time.Duration
	CPUTime         time.Duration
	MemoryPeakBytes int64
}

// Schedule defines when a recurring job should run.
type Schedule struct {
	Type      ScheduleType
	Cron      string        // For cron type (e.g., "0 * * * *")
	Interval  time.Duration // For interval type
	StartTime time.Time
	EndTime   time.Time
	Timezone  string
	Paused    bool
}

// ScheduleType defines the type of schedule.
type ScheduleType int

const (
	ScheduleTypeOnce ScheduleType = iota
	ScheduleTypeCron
	ScheduleTypeInterval
)

func (s ScheduleType) String() string {
	switch s {
	case ScheduleTypeOnce:
		return "once"
	case ScheduleTypeCron:
		return "cron"
	case ScheduleTypeInterval:
		return "interval"
	default:
		return "unknown"
	}
}
