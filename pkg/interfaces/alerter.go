package interfaces

import (
	"context"
	"time"
)

// Alerter sends alerts when significant events occur.
type Alerter interface {
	// Alert sends an alert with the given level and message.
	Alert(ctx context.Context, alert Alert) error

	// Close releases resources.
	Close() error
}

// Alert represents an alert to be sent.
type Alert struct {
	// Level is the severity of the alert.
	Level AlertLevel

	// Title is a short summary of the alert.
	Title string

	// Message is the detailed alert message.
	Message string

	// Source identifies where the alert originated.
	Source string

	// Tags for filtering and routing.
	Tags map[string]string

	// Timestamp when the alert was generated.
	Timestamp time.Time

	// Metadata contains additional structured data.
	Metadata map[string]interface{}

	// Deduplication key for suppressing duplicate alerts.
	DedupeKey string

	// Error if this alert is related to an error.
	Error error
}

// AlertLevel represents the severity of an alert.
type AlertLevel int

const (
	AlertLevelInfo AlertLevel = iota
	AlertLevelWarning
	AlertLevelError
	AlertLevelCritical
)

func (l AlertLevel) String() string {
	switch l {
	case AlertLevelInfo:
		return "info"
	case AlertLevelWarning:
		return "warning"
	case AlertLevelError:
		return "error"
	case AlertLevelCritical:
		return "critical"
	default:
		return "unknown"
	}
}

// NewAlert creates a new alert with defaults.
func NewAlert(level AlertLevel, title, message string) Alert {
	return Alert{
		Level:     level,
		Title:     title,
		Message:   message,
		Timestamp: time.Now(),
		Tags:      make(map[string]string),
		Metadata:  make(map[string]interface{}),
	}
}

// WithSource sets the alert source.
func (a Alert) WithSource(source string) Alert {
	a.Source = source
	return a
}

// WithTags sets the alert tags.
func (a Alert) WithTags(tags map[string]string) Alert {
	a.Tags = tags
	return a
}

// WithMetadata sets the alert metadata.
func (a Alert) WithMetadata(metadata map[string]interface{}) Alert {
	a.Metadata = metadata
	return a
}

// WithError attaches an error to the alert.
func (a Alert) WithError(err error) Alert {
	a.Error = err
	return a
}

// WithDedupeKey sets the deduplication key.
func (a Alert) WithDedupeKey(key string) Alert {
	a.DedupeKey = key
	return a
}

// Common alert sources.
const (
	AlertSourceIngest  = "ingest"
	AlertSourceQuery   = "query"
	AlertSourceStorage = "storage"
	AlertSourceJob     = "job"
	AlertSourceSystem  = "system"
)
