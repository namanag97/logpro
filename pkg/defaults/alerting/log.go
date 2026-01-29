package alerting

import (
	"context"
	"fmt"
	"log"

	"github.com/logflow/logflow/pkg/interfaces"
)

// LogAlerter writes alerts to the standard logger.
// Useful for debugging and development.
type LogAlerter struct {
	prefix   string
	minLevel interfaces.AlertLevel
}

// LogAlerterOption configures LogAlerter.
type LogAlerterOption func(*LogAlerter)

// WithAlertPrefix sets the log prefix.
func WithAlertPrefix(prefix string) LogAlerterOption {
	return func(a *LogAlerter) {
		a.prefix = prefix
	}
}

// WithMinAlertLevel sets the minimum alert level to log.
func WithMinAlertLevel(level interfaces.AlertLevel) LogAlerterOption {
	return func(a *LogAlerter) {
		a.minLevel = level
	}
}

// NewLogAlerter creates a new log-based alerter.
func NewLogAlerter(opts ...LogAlerterOption) *LogAlerter {
	a := &LogAlerter{
		prefix:   "[alert]",
		minLevel: interfaces.AlertLevelInfo,
	}
	for _, opt := range opts {
		opt(a)
	}
	return a
}

// Alert logs the alert.
func (a *LogAlerter) Alert(ctx context.Context, alert interfaces.Alert) error {
	if alert.Level < a.minLevel {
		return nil
	}

	levelStr := alert.Level.String()
	msg := fmt.Sprintf("%s [%s] %s: %s", a.prefix, levelStr, alert.Title, alert.Message)

	if alert.Source != "" {
		msg += fmt.Sprintf(" (source: %s)", alert.Source)
	}

	if alert.Error != nil {
		msg += fmt.Sprintf(" error: %v", alert.Error)
	}

	switch alert.Level {
	case interfaces.AlertLevelCritical, interfaces.AlertLevelError:
		log.Printf("ERROR: %s", msg)
	case interfaces.AlertLevelWarning:
		log.Printf("WARN: %s", msg)
	default:
		log.Printf("INFO: %s", msg)
	}

	return nil
}

// Close does nothing.
func (a *LogAlerter) Close() error {
	return nil
}

// Verify interface compliance.
var _ interfaces.Alerter = (*LogAlerter)(nil)
