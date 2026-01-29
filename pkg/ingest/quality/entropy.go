package quality

import (
	"math"
	"sync"
)

// EntropySampler calculates Shannon entropy.
type EntropySampler struct {
	mu         sync.Mutex
	counts     map[string]int
	total      int
	sampleRate float64
	sampled    int
}

// NewEntropySampler creates an entropy calculator.
func NewEntropySampler(sampleRate float64) *EntropySampler {
	return &EntropySampler{
		counts:     make(map[string]int),
		sampleRate: sampleRate,
	}
}

// Add adds a value to the sampler.
func (e *EntropySampler) Add(value string) {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.total++

	// Sample
	if e.sampleRate < 1.0 && float64(e.total)*e.sampleRate < float64(e.sampled+1) {
		return
	}

	e.sampled++
	e.counts[value]++
}

// Entropy returns the Shannon entropy.
func (e *EntropySampler) Entropy() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.entropyLocked()
}

// entropyLocked computes Shannon entropy without acquiring the mutex.
// Caller must hold e.mu.
func (e *EntropySampler) entropyLocked() float64 {
	if e.sampled == 0 {
		return 0
	}

	entropy := 0.0
	n := float64(e.sampled)

	for _, count := range e.counts {
		if count > 0 {
			p := float64(count) / n
			entropy -= p * math.Log2(p)
		}
	}

	return entropy
}

// NormalizedEntropy returns entropy normalized to [0,1].
func (e *EntropySampler) NormalizedEntropy() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.sampled == 0 || len(e.counts) <= 1 {
		return 0
	}

	maxEntropy := math.Log2(float64(len(e.counts)))
	if maxEntropy == 0 {
		return 0
	}

	return e.entropyLocked() / maxEntropy
}

// UniqueRatio returns ratio of unique values.
func (e *EntropySampler) UniqueRatio() float64 {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.sampled == 0 {
		return 0
	}

	return float64(len(e.counts)) / float64(e.sampled)
}
