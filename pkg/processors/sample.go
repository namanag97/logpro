package processors

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"github.com/logflow/logflow/internal/model"
	"github.com/logflow/logflow/pkg/pipeline"
)

// SampleProcessor implements various sampling strategies.
type SampleProcessor struct {
	strategy SamplingStrategy
}

// SamplingStrategy defines the sampling approach.
type SamplingStrategy interface {
	ShouldInclude(event *pipeline.Event) bool
	// For reservoir sampling, we need to finalize after all events
	Finalize() []*pipeline.Event
	IsStreaming() bool // True if results can be streamed, false if buffered
}

// RateSampling implements Bernoulli sampling at a fixed rate.
type RateSampling struct {
	rate float64
	rng  *rand.Rand
	mu   sync.Mutex
}

// NewRateSampling creates rate-based sampling.
func NewRateSampling(rate float64) *RateSampling {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return &RateSampling{
		rate: rate,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *RateSampling) ShouldInclude(event *pipeline.Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Float64() < s.rate
}

func (s *RateSampling) Finalize() []*pipeline.Event { return nil }
func (s *RateSampling) IsStreaming() bool           { return true }

// ReservoirSampling implements Algorithm R for fixed-size sampling.
type ReservoirSampling struct {
	k         int // Target sample size
	n         int // Items seen
	reservoir []*pipeline.Event
	rng       *rand.Rand
	mu        sync.Mutex
}

// NewReservoirSampling creates reservoir sampling for k items.
func NewReservoirSampling(k int) *ReservoirSampling {
	return &ReservoirSampling{
		k:         k,
		reservoir: make([]*pipeline.Event, 0, k),
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *ReservoirSampling) ShouldInclude(event *pipeline.Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.n++

	if s.n <= s.k {
		// Fill reservoir first
		s.reservoir = append(s.reservoir, copyEvent(event))
		return false // Don't stream yet
	}

	// Replace with probability k/n
	j := s.rng.Intn(s.n)
	if j < s.k {
		s.reservoir[j] = copyEvent(event)
	}
	return false // Reservoir sampling is not streaming
}

func (s *ReservoirSampling) Finalize() []*pipeline.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reservoir
}

func (s *ReservoirSampling) IsStreaming() bool { return false }

// StratifiedSampling samples within strata (e.g., by case ID).
type StratifiedSampling struct {
	perStratum int
	stratumKey string // Field to stratify by
	strata     map[string][]*pipeline.Event
	mu         sync.Mutex
}

// NewStratifiedSampling creates stratified sampling.
func NewStratifiedSampling(perStratum int, stratumKey string) *StratifiedSampling {
	return &StratifiedSampling{
		perStratum: perStratum,
		stratumKey: stratumKey,
		strata:     make(map[string][]*pipeline.Event),
	}
}

func (s *StratifiedSampling) ShouldInclude(event *pipeline.Event) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Get stratum key
	var key string
	switch s.stratumKey {
	case "case_id":
		key = string(event.CaseID)
	case "activity":
		key = string(event.Activity)
	case "resource":
		key = string(event.Resource)
	default:
		key = "default"
	}

	if _, ok := s.strata[key]; !ok {
		s.strata[key] = make([]*pipeline.Event, 0, s.perStratum)
	}

	if len(s.strata[key]) < s.perStratum {
		s.strata[key] = append(s.strata[key], copyEvent(event))
	}

	return false // Stratified sampling is not streaming
}

func (s *StratifiedSampling) Finalize() []*pipeline.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*pipeline.Event
	for _, events := range s.strata {
		result = append(result, events...)
	}
	return result
}

func (s *StratifiedSampling) IsStreaming() bool { return false }

// NewSampleProcessor creates a sampling processor.
func NewSampleProcessor(strategy SamplingStrategy) *SampleProcessor {
	return &SampleProcessor{strategy: strategy}
}

// Name returns the processor name.
func (p *SampleProcessor) Name() string {
	return "sample"
}

// Process implements Processor.Process.
func (p *SampleProcessor) Process(ctx context.Context, in <-chan *pipeline.Event, out chan<- *pipeline.Event) error {
	defer close(out)

	if p.strategy.IsStreaming() {
		// Stream results directly
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event, ok := <-in:
				if !ok {
					return nil
				}

				if p.strategy.ShouldInclude(event) {
					select {
					case out <- event:
					case <-ctx.Done():
						return ctx.Err()
					}
				}
			}
		}
	} else {
		// Buffer then emit (for reservoir/stratified)
		for {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event, ok := <-in:
				if !ok {
					// Input exhausted, emit buffered results
					for _, e := range p.strategy.Finalize() {
						select {
						case out <- e:
						case <-ctx.Done():
							return ctx.Err()
						}
					}
					return nil
				}
				p.strategy.ShouldInclude(event)
			}
		}
	}
}

// copyEvent creates a deep copy of an event.
func copyEvent(e *pipeline.Event) *pipeline.Event {
	cp := &model.Event{
		CaseID:     make([]byte, len(e.CaseID)),
		Activity:   make([]byte, len(e.Activity)),
		Timestamp:  e.Timestamp,
		Resource:   make([]byte, len(e.Resource)),
		Attributes: make([]model.Attribute, len(e.Attributes)),
	}
	copy(cp.CaseID, e.CaseID)
	copy(cp.Activity, e.Activity)
	copy(cp.Resource, e.Resource)
	for i, attr := range e.Attributes {
		cp.Attributes[i] = model.Attribute{
			Key:   make([]byte, len(attr.Key)),
			Value: make([]byte, len(attr.Value)),
			Type:  attr.Type,
		}
		copy(cp.Attributes[i].Key, attr.Key)
		copy(cp.Attributes[i].Value, attr.Value)
	}
	return cp
}

// SampleProcessorFactory creates a SampleProcessor from config.
func SampleProcessorFactory(cfg pipeline.Config) (pipeline.Processor, error) {
	var strategy SamplingStrategy

	if rate, ok := cfg.ProcessorOptions["sample_rate"].(float64); ok && rate > 0 {
		strategy = NewRateSampling(rate)
	} else if count, ok := cfg.ProcessorOptions["sample_count"].(int); ok && count > 0 {
		strategy = NewReservoirSampling(count)
	} else if stratumKey, ok := cfg.ProcessorOptions["stratify_by"].(string); ok {
		perStratum := 100 // Default
		if ps, ok := cfg.ProcessorOptions["per_stratum"].(int); ok {
			perStratum = ps
		}
		strategy = NewStratifiedSampling(perStratum, stratumKey)
	} else {
		// Default: 10% rate sampling
		strategy = NewRateSampling(0.1)
	}

	return NewSampleProcessor(strategy), nil
}
