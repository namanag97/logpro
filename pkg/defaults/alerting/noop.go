// Package alerting provides default alerting implementations.
package alerting

import (
	"context"

	"github.com/logflow/logflow/pkg/interfaces"
)

// NoopAlerter discards all alerts.
// Use this when alerting is not needed.
type NoopAlerter struct{}

// NewNoopAlerter creates a new noop alerter.
func NewNoopAlerter() *NoopAlerter {
	return &NoopAlerter{}
}

// Alert does nothing.
func (n *NoopAlerter) Alert(ctx context.Context, alert interfaces.Alert) error {
	return nil
}

// Close does nothing.
func (n *NoopAlerter) Close() error {
	return nil
}

// Verify interface compliance.
var _ interfaces.Alerter = (*NoopAlerter)(nil)
