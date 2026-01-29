package transform

import (
	"math/rand"
	"sync"
	"time"

	"github.com/logflow/logflow/internal/model"
)

// ReservoirSampler implements Algorithm R for streaming random sampling.
// It guarantees a mathematically uniform random sample in a single pass
// with O(k) memory where k is the sample size.
type ReservoirSampler struct {
	reservoir []*model.Event
	k         int // Target sample size
	n         int // Total items seen
	rng       *rand.Rand
	mu        sync.Mutex
}

// NewReservoirSampler creates a sampler for k items.
func NewReservoirSampler(k int) *ReservoirSampler {
	return &ReservoirSampler{
		reservoir: make([]*model.Event, 0, k),
		k:         k,
		n:         0,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Add processes an event for potential inclusion in the sample.
// Algorithm R: First k items go directly into reservoir.
// After that, item i replaces a random item with probability k/i.
func (s *ReservoirSampler) Add(event *model.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.n++

	if s.n <= s.k {
		// Fill reservoir first
		s.reservoir = append(s.reservoir, copyEvent(event))
		return
	}

	// Decide whether to include this item
	j := s.rng.Intn(s.n)
	if j < s.k {
		// Replace item j with the new event
		s.reservoir[j] = copyEvent(event)
	}
}

// Sample returns the current reservoir contents.
func (s *ReservoirSampler) Sample() []*model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.reservoir
}

// Count returns the total number of items seen.
func (s *ReservoirSampler) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.n
}

// Reset clears the sampler for reuse.
func (s *ReservoirSampler) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.reservoir = s.reservoir[:0]
	s.n = 0
}

// RateSampler samples at a fixed rate (e.g., 10% = 0.1).
// Uses Bernoulli sampling - each item has independent probability p of selection.
type RateSampler struct {
	rate float64
	rng  *rand.Rand
	mu   sync.Mutex
}

// NewRateSampler creates a sampler with the given rate (0.0 to 1.0).
func NewRateSampler(rate float64) *RateSampler {
	if rate < 0 {
		rate = 0
	}
	if rate > 1 {
		rate = 1
	}
	return &RateSampler{
		rate: rate,
		rng:  rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// ShouldInclude returns true if the item should be included.
func (s *RateSampler) ShouldInclude() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.rng.Float64() < s.rate
}

// StratifiedSampler samples within strata (e.g., by Case ID).
// Ensures each case has representation in the sample.
type StratifiedSampler struct {
	perStratum int
	strata     map[string][]*model.Event
	mu         sync.Mutex
}

// NewStratifiedSampler creates a sampler that keeps n events per stratum.
func NewStratifiedSampler(perStratum int) *StratifiedSampler {
	return &StratifiedSampler{
		perStratum: perStratum,
		strata:     make(map[string][]*model.Event),
	}
}

// Add adds an event to its stratum.
func (s *StratifiedSampler) Add(stratumKey string, event *model.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, ok := s.strata[stratumKey]; !ok {
		s.strata[stratumKey] = make([]*model.Event, 0, s.perStratum)
	}

	if len(s.strata[stratumKey]) < s.perStratum {
		s.strata[stratumKey] = append(s.strata[stratumKey], copyEvent(event))
	}
}

// Sample returns all sampled events across all strata.
func (s *StratifiedSampler) Sample() []*model.Event {
	s.mu.Lock()
	defer s.mu.Unlock()

	var result []*model.Event
	for _, events := range s.strata {
		result = append(result, events...)
	}
	return result
}

// StrataCount returns the number of unique strata.
func (s *StratifiedSampler) StrataCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.strata)
}

// copyEvent creates a deep copy of an event.
func copyEvent(e *model.Event) *model.Event {
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
