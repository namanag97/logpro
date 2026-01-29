// Package lifecycle provides graceful shutdown and lifecycle management.
// Ensures in-flight operations complete before shutdown.
package lifecycle

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

// ShutdownManager manages graceful shutdown of services.
type ShutdownManager struct {
	mu sync.Mutex

	// Configuration
	drainTimeout  time.Duration
	forceTimeout  time.Duration

	// State
	healthy    bool
	draining   bool
	shutdownAt time.Time

	// Callbacks
	onHealthChange func(healthy bool)
	onDrainStart   func()
	onShutdown     func()

	// In-flight tracking
	inFlight  sync.WaitGroup
	inFlightCount int64

	// Services to close
	closers []Closer

	// Channel to signal shutdown complete
	done chan struct{}
}

// Closer interface for services that need cleanup.
type Closer interface {
	Close() error
}

// ShutdownConfig configures the shutdown manager.
type ShutdownConfig struct {
	// DrainTimeout is how long to wait for in-flight requests to complete
	DrainTimeout time.Duration
	// ForceTimeout is how long to wait before forcing shutdown
	ForceTimeout time.Duration
	// OnHealthChange is called when health status changes
	OnHealthChange func(healthy bool)
	// OnDrainStart is called when drain begins
	OnDrainStart func()
	// OnShutdown is called when shutdown begins
	OnShutdown func()
}

// DefaultShutdownConfig returns sensible defaults.
func DefaultShutdownConfig() ShutdownConfig {
	return ShutdownConfig{
		DrainTimeout: 30 * time.Second,
		ForceTimeout: 60 * time.Second,
	}
}

// NewShutdownManager creates a new shutdown manager.
func NewShutdownManager(cfg ShutdownConfig) *ShutdownManager {
	if cfg.DrainTimeout == 0 {
		cfg.DrainTimeout = 30 * time.Second
	}
	if cfg.ForceTimeout == 0 {
		cfg.ForceTimeout = 60 * time.Second
	}

	return &ShutdownManager{
		drainTimeout:   cfg.DrainTimeout,
		forceTimeout:   cfg.ForceTimeout,
		onHealthChange: cfg.OnHealthChange,
		onDrainStart:   cfg.OnDrainStart,
		onShutdown:     cfg.OnShutdown,
		healthy:        true,
		done:           make(chan struct{}),
	}
}

// RegisterCloser adds a service to be closed during shutdown.
func (m *ShutdownManager) RegisterCloser(c Closer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closers = append(m.closers, c)
}

// StartRequest marks the start of an in-flight request.
// Returns false if we're draining and the request should be rejected.
func (m *ShutdownManager) StartRequest() bool {
	m.mu.Lock()
	if m.draining {
		m.mu.Unlock()
		return false
	}
	m.inFlightCount++
	m.mu.Unlock()

	m.inFlight.Add(1)
	return true
}

// EndRequest marks the end of an in-flight request.
func (m *ShutdownManager) EndRequest() {
	m.inFlight.Done()

	m.mu.Lock()
	m.inFlightCount--
	m.mu.Unlock()
}

// InFlightCount returns the number of in-flight requests.
func (m *ShutdownManager) InFlightCount() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.inFlightCount
}

// IsHealthy returns whether the service is healthy.
func (m *ShutdownManager) IsHealthy() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.healthy && !m.draining
}

// IsDraining returns whether the service is draining.
func (m *ShutdownManager) IsDraining() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.draining
}

// SetUnhealthy marks the service as unhealthy.
func (m *ShutdownManager) SetUnhealthy(reason string) {
	m.mu.Lock()
	wasHealthy := m.healthy
	m.healthy = false
	m.mu.Unlock()

	if wasHealthy && m.onHealthChange != nil {
		m.onHealthChange(false)
	}
}

// SetHealthy marks the service as healthy.
func (m *ShutdownManager) SetHealthy() {
	m.mu.Lock()
	wasHealthy := m.healthy
	m.healthy = true
	m.mu.Unlock()

	if !wasHealthy && m.onHealthChange != nil {
		m.onHealthChange(true)
	}
}

// Shutdown initiates graceful shutdown.
func (m *ShutdownManager) Shutdown(ctx context.Context) error {
	m.mu.Lock()
	if m.draining {
		m.mu.Unlock()
		return nil // Already shutting down
	}
	m.draining = true
	m.shutdownAt = time.Now()
	m.mu.Unlock()

	// Mark as unhealthy first (for load balancers)
	m.SetUnhealthy("shutting down")

	// Notify drain start
	if m.onDrainStart != nil {
		m.onDrainStart()
	}

	// Wait for in-flight requests to complete
	drainDone := make(chan struct{})
	go func() {
		m.inFlight.Wait()
		close(drainDone)
	}()

	select {
	case <-drainDone:
		// All requests completed
	case <-time.After(m.drainTimeout):
		// Drain timeout reached
		fmt.Printf("Drain timeout reached with %d in-flight requests\n", m.InFlightCount())
	case <-ctx.Done():
		// Context cancelled
	}

	// Notify shutdown
	if m.onShutdown != nil {
		m.onShutdown()
	}

	// Close all registered services
	var errs []error
	for _, c := range m.closers {
		if err := c.Close(); err != nil {
			errs = append(errs, err)
		}
	}

	close(m.done)

	if len(errs) > 0 {
		return fmt.Errorf("shutdown errors: %v", errs)
	}
	return nil
}

// Wait blocks until shutdown is complete.
func (m *ShutdownManager) Wait() {
	<-m.done
}

// HandleSignals sets up signal handling for graceful shutdown.
func (m *ShutdownManager) HandleSignals(ctx context.Context) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		select {
		case sig := <-sigChan:
			fmt.Printf("Received signal %v, initiating graceful shutdown...\n", sig)
			m.Shutdown(ctx)
		case <-ctx.Done():
			return
		}
	}()
}

// Status returns the current shutdown status.
type ShutdownStatus struct {
	Healthy       bool
	Draining      bool
	InFlightCount int64
	ShutdownAt    time.Time
	DrainTimeout  time.Duration
	ForceTimeout  time.Duration
}

// Status returns the current status.
func (m *ShutdownManager) Status() ShutdownStatus {
	m.mu.Lock()
	defer m.mu.Unlock()

	return ShutdownStatus{
		Healthy:       m.healthy,
		Draining:      m.draining,
		InFlightCount: m.inFlightCount,
		ShutdownAt:    m.shutdownAt,
		DrainTimeout:  m.drainTimeout,
		ForceTimeout:  m.forceTimeout,
	}
}

// --- HTTP Middleware ---

// ShutdownMiddleware wraps HTTP handlers with shutdown awareness.
type ShutdownMiddleware struct {
	manager *ShutdownManager
}

// NewShutdownMiddleware creates a new shutdown middleware.
func NewShutdownMiddleware(manager *ShutdownManager) *ShutdownMiddleware {
	return &ShutdownMiddleware{manager: manager}
}

// HealthHandler returns an HTTP handler for health checks.
// Returns 200 if healthy, 503 if draining/unhealthy.
func (m *ShutdownMiddleware) HealthHandler() func(w interface{ WriteHeader(int) }, r interface{}) {
	type ResponseWriter interface {
		WriteHeader(int)
		Write([]byte) (int, error)
	}

	return func(w interface{ WriteHeader(int) }, r interface{}) {
		if m.manager.IsHealthy() {
			w.WriteHeader(200)
		} else {
			w.WriteHeader(503)
		}
	}
}

// --- Convenience Functions ---

// GracefulShutdown runs a function with graceful shutdown handling.
func GracefulShutdown(ctx context.Context, cfg ShutdownConfig, fn func(ctx context.Context) error) error {
	manager := NewShutdownManager(cfg)
	manager.HandleSignals(ctx)

	// Run the main function
	errChan := make(chan error, 1)
	go func() {
		errChan <- fn(ctx)
	}()

	select {
	case err := <-errChan:
		return err
	case <-ctx.Done():
		manager.Shutdown(context.Background())
		return ctx.Err()
	}
}

// RunWithSignalHandling runs a function and handles shutdown signals.
func RunWithSignalHandling(fn func(ctx context.Context) error) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	errChan := make(chan error, 1)
	go func() {
		errChan <- fn(ctx)
	}()

	select {
	case err := <-errChan:
		return err
	case sig := <-sigChan:
		fmt.Printf("Received %v, shutting down...\n", sig)
		cancel()
		// Wait a bit for cleanup
		select {
		case err := <-errChan:
			return err
		case <-time.After(30 * time.Second):
			return fmt.Errorf("shutdown timeout")
		}
	}
}
