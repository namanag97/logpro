package schema

import (
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow/go/v14/arrow"
)

// TypeInferrer infers an Arrow type from string samples.
type TypeInferrer interface {
	Name() string
	Priority() int // Lower = checked first (higher precedence)
	Infer(samples []string) (arrow.DataType, float64) // type, confidence (0-1)
}

// InferrerRegistry manages type inferrers.
type InferrerRegistry struct {
	mu        sync.RWMutex
	inferrers []TypeInferrer
}

var defaultInferrerRegistry = NewInferrerRegistry()

// NewInferrerRegistry creates a registry pre-loaded with built-in inferrers.
func NewInferrerRegistry() *InferrerRegistry {
	r := &InferrerRegistry{}
	// Register built-in inferrers (sorted by priority in Register)
	r.Register(&BoolInferrer{})
	r.Register(&IntInferrer{})
	r.Register(&FloatInferrer{})
	r.Register(&TimestampInferrer{})
	r.Register(&StringInferrer{})
	return r
}

// Register adds a type inferrer and keeps the list sorted by priority.
func (r *InferrerRegistry) Register(inf TypeInferrer) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.inferrers = append(r.inferrers, inf)
	sort.SliceStable(r.inferrers, func(i, j int) bool {
		return r.inferrers[i].Priority() < r.inferrers[j].Priority()
	})
}

// InferType runs all registered inferrers and returns the best match.
func (r *InferrerRegistry) InferType(samples []string) (arrow.DataType, float64) {
	r.mu.RLock()
	inferrers := make([]TypeInferrer, len(r.inferrers))
	copy(inferrers, r.inferrers)
	r.mu.RUnlock()

	var bestType arrow.DataType
	var bestConf float64
	for _, inf := range inferrers {
		dt, conf := inf.Infer(samples)
		if conf > bestConf {
			bestType = dt
			bestConf = conf
		}
	}
	if bestType == nil {
		return arrow.BinaryTypes.String, 1.0
	}
	return bestType, bestConf
}

// List returns the names of all registered inferrers.
func (r *InferrerRegistry) List() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	names := make([]string, len(r.inferrers))
	for i, inf := range r.inferrers {
		names[i] = inf.Name()
	}
	return names
}

// Package-level convenience functions operating on the default registry.

// RegisterInferrer registers a type inferrer with the default registry.
func RegisterInferrer(inf TypeInferrer) { defaultInferrerRegistry.Register(inf) }

// InferTypeFromSamples infers an Arrow type from string samples using the default registry.
func InferTypeFromSamples(samples []string) (arrow.DataType, float64) {
	return defaultInferrerRegistry.InferType(samples)
}

// ListInferrers returns names of all inferrers in the default registry.
func ListInferrers() []string { return defaultInferrerRegistry.List() }

// --- Built-in inferrers ---

// BoolInferrer detects boolean values.
type BoolInferrer struct{}

func (BoolInferrer) Name() string  { return "bool" }
func (BoolInferrer) Priority() int { return 10 }
func (BoolInferrer) Infer(samples []string) (arrow.DataType, float64) {
	if len(samples) == 0 {
		return nil, 0
	}
	matches := 0
	bools := map[string]bool{
		"true": true, "false": true,
		"1": true, "0": true,
		"yes": true, "no": true,
		"t": true, "f": true,
	}
	for _, s := range samples {
		if s == "" {
			continue
		}
		if bools[strings.ToLower(s)] {
			matches++
		}
	}
	nonEmpty := countNonEmpty(samples)
	if nonEmpty == 0 {
		return nil, 0
	}
	ratio := float64(matches) / float64(nonEmpty)
	if ratio >= 0.9 {
		return arrow.FixedWidthTypes.Boolean, ratio
	}
	return nil, 0
}

// IntInferrer detects integer values.
type IntInferrer struct{}

func (IntInferrer) Name() string  { return "int64" }
func (IntInferrer) Priority() int { return 20 }
func (IntInferrer) Infer(samples []string) (arrow.DataType, float64) {
	if len(samples) == 0 {
		return nil, 0
	}
	matches := 0
	for _, s := range samples {
		if s == "" {
			continue
		}
		if _, err := strconv.ParseInt(s, 10, 64); err == nil {
			matches++
		}
	}
	nonEmpty := countNonEmpty(samples)
	if nonEmpty == 0 {
		return nil, 0
	}
	ratio := float64(matches) / float64(nonEmpty)
	if ratio >= 0.9 {
		return arrow.PrimitiveTypes.Int64, ratio
	}
	return nil, 0
}

// FloatInferrer detects floating-point values.
type FloatInferrer struct{}

func (FloatInferrer) Name() string  { return "float64" }
func (FloatInferrer) Priority() int { return 30 }
func (FloatInferrer) Infer(samples []string) (arrow.DataType, float64) {
	if len(samples) == 0 {
		return nil, 0
	}
	matches := 0
	hasDecimal := false
	for _, s := range samples {
		if s == "" {
			continue
		}
		if _, err := strconv.ParseFloat(s, 64); err == nil {
			matches++
			if strings.Contains(s, ".") {
				hasDecimal = true
			}
		}
	}
	nonEmpty := countNonEmpty(samples)
	if nonEmpty == 0 {
		return nil, 0
	}
	ratio := float64(matches) / float64(nonEmpty)
	// Only pick float if there are actual decimals (otherwise int wins)
	if ratio >= 0.9 && hasDecimal {
		return arrow.PrimitiveTypes.Float64, ratio
	}
	return nil, 0
}

// TimestampInferrer detects timestamp/date values.
type TimestampInferrer struct{}

func (TimestampInferrer) Name() string  { return "timestamp" }
func (TimestampInferrer) Priority() int { return 40 }
func (TimestampInferrer) Infer(samples []string) (arrow.DataType, float64) {
	if len(samples) == 0 {
		return nil, 0
	}
	formats := []string{
		time.RFC3339, "2006-01-02T15:04:05", "2006-01-02 15:04:05",
		"2006-01-02", "01/02/2006", "02-Jan-2006",
	}
	matches := 0
	for _, s := range samples {
		if s == "" {
			continue
		}
		for _, f := range formats {
			if _, err := time.Parse(f, s); err == nil {
				matches++
				break
			}
		}
	}
	nonEmpty := countNonEmpty(samples)
	if nonEmpty == 0 {
		return nil, 0
	}
	ratio := float64(matches) / float64(nonEmpty)
	if ratio >= 0.8 {
		// Return string but high confidence it's temporal
		return arrow.BinaryTypes.String, ratio * 0.95
	}
	return nil, 0
}

// StringInferrer is the fallback inferrer for string values.
type StringInferrer struct{}

func (StringInferrer) Name() string  { return "string" }
func (StringInferrer) Priority() int { return 100 } // Lowest priority - fallback
func (StringInferrer) Infer(samples []string) (arrow.DataType, float64) {
	return arrow.BinaryTypes.String, 0.5 // Always matches with low confidence
}

func countNonEmpty(samples []string) int {
	n := 0
	for _, s := range samples {
		if s != "" {
			n++
		}
	}
	return n
}
