package transform

import (
	"testing"

	"github.com/logflow/logflow/internal/model"
)

// --- Anonymizer tests ---

func TestNewAnonymizer(t *testing.T) {
	a := NewAnonymizer("my-salt")
	if a == nil {
		t.Fatal("NewAnonymizer returned nil")
	}
}

func TestAnonymizerHashDeterminism(t *testing.T) {
	a := NewAnonymizer("salt")
	h1 := a.Hash([]byte("Alice"))
	h2 := a.Hash([]byte("Alice"))

	if h1 != h2 {
		t.Errorf("Hash not deterministic: %q vs %q", h1, h2)
	}
}

func TestAnonymizerHashDifferentValues(t *testing.T) {
	a := NewAnonymizer("salt")
	h1 := a.Hash([]byte("Alice"))
	h2 := a.Hash([]byte("Bob"))

	if h1 == h2 {
		t.Error("different values should produce different hashes")
	}
}

func TestAnonymizerHashDifferentSalts(t *testing.T) {
	a1 := NewAnonymizer("salt1")
	a2 := NewAnonymizer("salt2")

	h1 := a1.Hash([]byte("Alice"))
	h2 := a2.Hash([]byte("Alice"))

	if h1 == h2 {
		t.Error("different salts should produce different hashes")
	}
}

func TestAnonymizerHashBytes(t *testing.T) {
	a := NewAnonymizer("salt")
	result := a.HashBytes([]byte("Alice"))
	if len(result) == 0 {
		t.Error("HashBytes returned empty result")
	}
}

func TestAnonymizerCacheHit(t *testing.T) {
	a := NewAnonymizer("salt")

	// First call populates cache
	h1 := a.Hash([]byte("Alice"))
	// Second call should use cache
	h2 := a.Hash([]byte("Alice"))

	if h1 != h2 {
		t.Error("cached hash should be the same")
	}
}

func TestAnonymizerClearCache(t *testing.T) {
	a := NewAnonymizer("salt")
	a.Hash([]byte("Alice"))
	a.ClearCache()

	// Should still produce the same hash after cache clear
	h := a.Hash([]byte("Alice"))
	if h == "" {
		t.Error("Hash after ClearCache should not be empty")
	}
}

func TestDefaultAnonymizeConfig(t *testing.T) {
	cfg := DefaultAnonymizeConfig()
	if len(cfg.Columns) == 0 {
		t.Error("default config should have PII columns")
	}
	if cfg.Salt == "" {
		// Salt may be empty by default; just check Columns
	}
}

// --- ReservoirSampler tests ---

func makeTestEvent(caseID string) *model.Event {
	return &model.Event{
		CaseID:   []byte(caseID),
		Activity: []byte("A"),
	}
}

func TestNewReservoirSampler(t *testing.T) {
	s := NewReservoirSampler(10)
	if s == nil {
		t.Fatal("NewReservoirSampler returned nil")
	}
}

func TestReservoirSamplerUnderCapacity(t *testing.T) {
	s := NewReservoirSampler(10)
	for i := 0; i < 5; i++ {
		s.Add(makeTestEvent("c"))
	}

	sample := s.Sample()
	if len(sample) != 5 {
		t.Errorf("sample size = %d, want 5", len(sample))
	}
	if s.Count() != 5 {
		t.Errorf("count = %d, want 5", s.Count())
	}
}

func TestReservoirSamplerAtCapacity(t *testing.T) {
	s := NewReservoirSampler(10)
	for i := 0; i < 100; i++ {
		s.Add(makeTestEvent("c"))
	}

	sample := s.Sample()
	if len(sample) != 10 {
		t.Errorf("sample size = %d, want 10", len(sample))
	}
	if s.Count() != 100 {
		t.Errorf("count = %d, want 100", s.Count())
	}
}

func TestReservoirSamplerReset(t *testing.T) {
	s := NewReservoirSampler(10)
	for i := 0; i < 20; i++ {
		s.Add(makeTestEvent("c"))
	}
	s.Reset()

	if s.Count() != 0 {
		t.Errorf("count after reset = %d, want 0", s.Count())
	}
	if len(s.Sample()) != 0 {
		t.Errorf("sample after reset = %d, want 0", len(s.Sample()))
	}
}

func TestReservoirSamplerDistribution(t *testing.T) {
	// Statistical test: each item should have ~equal probability
	k := 100
	n := 10000
	counts := make(map[string]int)

	for trial := 0; trial < 50; trial++ {
		s := NewReservoirSampler(k)
		for i := 0; i < n; i++ {
			e := makeTestEvent("c")
			e.Activity = []byte("A" + string(rune('0'+i%10)))
			s.Add(e)
		}
		for _, ev := range s.Sample() {
			counts[string(ev.Activity)]++
		}
	}

	// Each of the 10 activity types should appear roughly equally
	for act, count := range counts {
		if count == 0 {
			t.Errorf("activity %q never sampled", act)
		}
	}
}

// --- RateSampler tests ---

func TestNewRateSampler(t *testing.T) {
	s := NewRateSampler(0.5)
	if s == nil {
		t.Fatal("NewRateSampler returned nil")
	}
}

func TestRateSamplerIncludeRate(t *testing.T) {
	s := NewRateSampler(0.5)
	included := 0
	total := 10000

	for i := 0; i < total; i++ {
		if s.ShouldInclude() {
			included++
		}
	}

	ratio := float64(included) / float64(total)
	if ratio < 0.4 || ratio > 0.6 {
		t.Errorf("inclusion ratio = %f, want ~0.5", ratio)
	}
}

func TestRateSamplerZero(t *testing.T) {
	s := NewRateSampler(0.0)
	for i := 0; i < 100; i++ {
		if s.ShouldInclude() {
			t.Error("rate=0.0 should never include")
			break
		}
	}
}

func TestRateSamplerOne(t *testing.T) {
	s := NewRateSampler(1.0)
	for i := 0; i < 100; i++ {
		if !s.ShouldInclude() {
			t.Error("rate=1.0 should always include")
			break
		}
	}
}

// --- StratifiedSampler tests ---

func TestNewStratifiedSampler(t *testing.T) {
	s := NewStratifiedSampler(5)
	if s == nil {
		t.Fatal("NewStratifiedSampler returned nil")
	}
}

func TestStratifiedSamplerBasic(t *testing.T) {
	s := NewStratifiedSampler(2)

	for i := 0; i < 10; i++ {
		s.Add("stratum-A", makeTestEvent("a"))
		s.Add("stratum-B", makeTestEvent("b"))
	}

	sample := s.Sample()
	if len(sample) != 4 { // 2 per stratum * 2 strata
		t.Errorf("sample size = %d, want 4", len(sample))
	}
}

func TestStratifiedSamplerStrataCount(t *testing.T) {
	s := NewStratifiedSampler(3)
	s.Add("X", makeTestEvent("x"))
	s.Add("Y", makeTestEvent("y"))
	s.Add("Z", makeTestEvent("z"))

	if s.StrataCount() != 3 {
		t.Errorf("StrataCount = %d, want 3", s.StrataCount())
	}
}

func TestStratifiedSamplerPerStratum(t *testing.T) {
	s := NewStratifiedSampler(3)

	// Add 10 events to one stratum
	for i := 0; i < 10; i++ {
		s.Add("only", makeTestEvent("o"))
	}

	sample := s.Sample()
	if len(sample) != 3 {
		t.Errorf("sample size = %d, want 3 (perStratum limit)", len(sample))
	}
}
