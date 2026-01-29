// Package server - Server-Sent Events for real-time progress streaming.
package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"
)

// SSEBroker manages Server-Sent Events connections.
type SSEBroker struct {
	mu          sync.RWMutex
	subscribers map[string]map[chan SSEEvent]struct{}
}

// SSEEvent represents an event to send to clients.
type SSEEvent struct {
	Event string      `json:"event"`
	Data  interface{} `json:"data"`
	ID    string      `json:"id,omitempty"`
}

// NewSSEBroker creates a new SSE broker.
func NewSSEBroker() *SSEBroker {
	return &SSEBroker{
		subscribers: make(map[string]map[chan SSEEvent]struct{}),
	}
}

// Subscribe creates a subscription for a job.
func (b *SSEBroker) Subscribe(jobID string) chan SSEEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan SSEEvent, 10)

	if b.subscribers[jobID] == nil {
		b.subscribers[jobID] = make(map[chan SSEEvent]struct{})
	}
	b.subscribers[jobID][ch] = struct{}{}

	return ch
}

// Unsubscribe removes a subscription.
func (b *SSEBroker) Unsubscribe(jobID string, ch chan SSEEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if subs, ok := b.subscribers[jobID]; ok {
		delete(subs, ch)
		close(ch)
		if len(subs) == 0 {
			delete(b.subscribers, jobID)
		}
	}
}

// Publish sends an event to all subscribers of a job.
func (b *SSEBroker) Publish(jobID string, event SSEEvent) {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if subs, ok := b.subscribers[jobID]; ok {
		for ch := range subs {
			select {
			case ch <- event:
			default:
				// Channel full, skip
			}
		}
	}
}

// PublishProgress sends a progress update.
func (b *SSEBroker) PublishProgress(jobID string, progress interface{}) {
	b.Publish(jobID, SSEEvent{
		Event: "progress",
		Data:  progress,
		ID:    fmt.Sprintf("%d", time.Now().UnixNano()),
	})
}

// PublishComplete sends a completion event.
func (b *SSEBroker) PublishComplete(jobID string, result interface{}) {
	b.Publish(jobID, SSEEvent{
		Event: "complete",
		Data:  result,
		ID:    fmt.Sprintf("%d", time.Now().UnixNano()),
	})
}

// PublishError sends an error event.
func (b *SSEBroker) PublishError(jobID string, err error) {
	b.Publish(jobID, SSEEvent{
		Event: "error",
		Data:  map[string]string{"error": err.Error()},
		ID:    fmt.Sprintf("%d", time.Now().UnixNano()),
	})
}

// HasSubscribers checks if a job has any subscribers.
func (b *SSEBroker) HasSubscribers(jobID string) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return len(b.subscribers[jobID]) > 0
}

// --- HTTP Handler ---

// SSEHandler creates an HTTP handler for SSE connections.
func (b *SSEBroker) SSEHandler(getJob func(string) interface{}) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		jobID := r.URL.Query().Get("job_id")
		if jobID == "" {
			http.Error(w, "job_id required", http.StatusBadRequest)
			return
		}

		// Set SSE headers
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.Header().Set("Access-Control-Allow-Origin", "*")

		// Get flusher
		flusher, ok := w.(http.Flusher)
		if !ok {
			http.Error(w, "SSE not supported", http.StatusInternalServerError)
			return
		}

		// Subscribe
		ch := b.Subscribe(jobID)
		defer b.Unsubscribe(jobID, ch)

		// Send initial state if job exists
		if getJob != nil {
			if job := getJob(jobID); job != nil {
				writeSSEEvent(w, SSEEvent{
					Event: "init",
					Data:  job,
				})
				flusher.Flush()
			}
		}

		// Handle client disconnect
		ctx := r.Context()

		// Stream events
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-ch:
				if !ok {
					return
				}
				writeSSEEvent(w, event)
				flusher.Flush()

				// Close on complete or error
				if event.Event == "complete" || event.Event == "error" {
					return
				}
			}
		}
	}
}

// writeSSEEvent writes an event in SSE format.
func writeSSEEvent(w http.ResponseWriter, event SSEEvent) {
	if event.ID != "" {
		fmt.Fprintf(w, "id: %s\n", event.ID)
	}
	fmt.Fprintf(w, "event: %s\n", event.Event)

	data, _ := json.Marshal(event.Data)
	fmt.Fprintf(w, "data: %s\n\n", data)
}

// --- Progress Tracker ---

// ProgressTracker tracks and broadcasts conversion progress.
type ProgressTracker struct {
	broker       *SSEBroker
	jobID        string
	totalBytes   int64
	startTime    time.Time
	lastUpdate   time.Time
	minInterval  time.Duration
	mu           sync.Mutex
}

// NewProgressTracker creates a new progress tracker.
func NewProgressTracker(broker *SSEBroker, jobID string, totalBytes int64) *ProgressTracker {
	return &ProgressTracker{
		broker:      broker,
		jobID:       jobID,
		totalBytes:  totalBytes,
		startTime:   time.Now(),
		lastUpdate:  time.Now(),
		minInterval: 100 * time.Millisecond,
	}
}

// Update sends a progress update (rate-limited).
func (t *ProgressTracker) Update(bytesRead, rowsWritten int64, phase string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	// Rate limit updates
	if time.Since(t.lastUpdate) < t.minInterval {
		return
	}
	t.lastUpdate = time.Now()

	elapsed := time.Since(t.startTime)
	var percent float64
	if t.totalBytes > 0 {
		percent = float64(bytesRead) * 100 / float64(t.totalBytes)
	}

	var eta time.Duration
	if percent > 0 {
		eta = time.Duration(float64(elapsed) * (100 - percent) / percent)
	}

	progress := map[string]interface{}{
		"phase":           phase,
		"percent":         percent,
		"bytes_read":      bytesRead,
		"rows_written":    rowsWritten,
		"bytes_per_sec":   float64(bytesRead) / elapsed.Seconds(),
		"rows_per_sec":    float64(rowsWritten) / elapsed.Seconds(),
		"elapsed_ms":      elapsed.Milliseconds(),
		"eta_ms":          eta.Milliseconds(),
	}

	t.broker.PublishProgress(t.jobID, progress)
}

// Complete sends completion event.
func (t *ProgressTracker) Complete(result interface{}) {
	t.broker.PublishComplete(t.jobID, result)
}

// Error sends error event.
func (t *ProgressTracker) Error(err error) {
	t.broker.PublishError(t.jobID, err)
}
