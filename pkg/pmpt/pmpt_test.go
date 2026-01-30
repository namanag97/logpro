package pmpt

import (
	"testing"
	"time"
)

// --- Tree tests ---

func TestNewTree(t *testing.T) {
	tree := NewTree()
	if tree == nil {
		t.Fatal("NewTree returned nil")
	}
	if tree.TotalCases != 0 {
		t.Errorf("TotalCases = %d, want 0", tree.TotalCases)
	}
}

func TestTreeAddTrace(t *testing.T) {
	tree := NewTree()
	trace := Trace{
		CaseID:     "case1",
		Activities: []string{"A", "B", "C"},
	}
	tree.AddTrace(trace)

	if tree.TotalCases != 1 {
		t.Errorf("TotalCases = %d, want 1", tree.TotalCases)
	}
	if tree.TotalEvents != 3 {
		t.Errorf("TotalEvents = %d, want 3", tree.TotalEvents)
	}
}

func TestTreeHasPath(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B", "C"}})

	if !tree.HasPath([]string{"A", "B", "C"}) {
		t.Error("HasPath should find A->B->C")
	}
	if tree.HasPath([]string{"X", "Y"}) {
		t.Error("HasPath should not find X->Y")
	}
}

func TestTreeFindActivity(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B", "A"}})

	nodes := tree.FindActivity("A")
	if len(nodes) == 0 {
		t.Error("FindActivity should find nodes for A")
	}
}

func TestTreeGetVariants(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})
	tree.AddTrace(Trace{CaseID: "c2", Activities: []string{"A", "B"}})
	tree.AddTrace(Trace{CaseID: "c3", Activities: []string{"A", "C"}})

	variants := tree.GetVariants()
	if len(variants) < 2 {
		t.Errorf("variants = %d, want >= 2", len(variants))
	}
}

func TestTreeStats(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B", "C"}})
	tree.AddTrace(Trace{CaseID: "c2", Activities: []string{"A", "D"}})

	stats := tree.Stats()
	if stats.TotalCases != 2 {
		t.Errorf("TotalCases = %d, want 2", stats.TotalCases)
	}
	if stats.TotalEvents != 5 {
		t.Errorf("TotalEvents = %d, want 5", stats.TotalEvents)
	}
}

func TestTreeFingerprint(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})

	fp := tree.Fingerprint()
	if fp.IsZero() {
		t.Error("fingerprint should not be zero")
	}
}

func TestTreeFingerprintDeterminism(t *testing.T) {
	build := func() Hash {
		tree := NewTree()
		tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B", "C"}})
		return tree.Fingerprint()
	}

	h1 := build()
	h2 := build()
	if h1 != h2 {
		t.Errorf("fingerprints differ: %s vs %s", h1.String(), h2.String())
	}
}

func TestTreeEquals(t *testing.T) {
	t1 := NewTree()
	t1.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})

	t2 := NewTree()
	t2.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})

	if !t1.Equals(t2) {
		t.Error("identical trees should be equal")
	}
}

func TestTreeNotEqual(t *testing.T) {
	t1 := NewTree()
	t1.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})

	t2 := NewTree()
	t2.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "C"}})

	if t1.Equals(t2) {
		t.Error("different trees should not be equal")
	}
}

// --- Compare tests ---

func TestCompareIdentical(t *testing.T) {
	t1 := NewTree()
	t1.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})
	t2 := NewTree()
	t2.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})

	diff := Compare(t1, t2)
	if !diff.Identical {
		t.Error("Compare identical trees should set Identical=true")
	}
}

func TestCompareDifferent(t *testing.T) {
	t1 := NewTree()
	t1.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})

	t2 := NewTree()
	t2.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B", "C"}})

	diff := Compare(t1, t2)
	if diff.Identical {
		t.Error("Compare different trees should set Identical=false")
	}
}

func TestTreeDiffString(t *testing.T) {
	t1 := NewTree()
	t1.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})
	t2 := NewTree()
	t2.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "C"}})

	diff := Compare(t1, t2)
	s := diff.String()
	if s == "" {
		t.Error("diff String() should not be empty")
	}
}

// --- IntervalTree tests ---

func TestNewIntervalTree(t *testing.T) {
	it := NewIntervalTree()
	if it == nil {
		t.Fatal("NewIntervalTree returned nil")
	}
}

func TestIntervalTreeAddAndQueryRange(t *testing.T) {
	it := NewIntervalTree()
	node := &ProcessNode{Activity: "A"}
	it.Add(node, 100, 200)

	results := it.QueryRange(50, 150)
	if len(results) == 0 {
		t.Error("QueryRange should find overlapping node")
	}

	results = it.QueryRange(300, 400)
	if len(results) != 0 {
		t.Error("QueryRange should not find non-overlapping node")
	}
}

func TestIntervalTreeQueryActivity(t *testing.T) {
	it := NewIntervalTree()
	it.Add(&ProcessNode{Activity: "A"}, 100, 200)
	it.Add(&ProcessNode{Activity: "B"}, 150, 300)

	nodes := it.QueryActivity("A")
	if len(nodes) != 1 {
		t.Errorf("QueryActivity(A) = %d nodes, want 1", len(nodes))
	}
}

func TestIntervalTreeFindOverlaps(t *testing.T) {
	it := NewIntervalTree()
	it.Add(&ProcessNode{Activity: "A"}, 100, 300)
	it.Add(&ProcessNode{Activity: "B"}, 200, 400)

	overlaps := it.FindOverlaps("A", "B")
	if len(overlaps) == 0 {
		t.Error("FindOverlaps should find overlap between A and B")
	}
}

func TestIntervalTreeFindBottlenecks(t *testing.T) {
	it := NewIntervalTree()
	it.Add(&ProcessNode{Activity: "A"}, 100, 500)
	it.Add(&ProcessNode{Activity: "B"}, 200, 250)

	bottlenecks := it.FindBottlenecks(0, 1000, 10)
	if len(bottlenecks) == 0 {
		t.Error("FindBottlenecks should find activities in range")
	}
}

// --- Serialization tests ---

func TestSerializeRoundTripJSON(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B", "C"}})
	tree.AddTrace(Trace{CaseID: "c2", Activities: []string{"A", "D"}})

	manifest := tree.ToManifest()
	data, err := manifest.ToJSON()
	if err != nil {
		t.Fatalf("ToJSON error: %v", err)
	}

	restored, err := ManifestFromJSON(data)
	if err != nil {
		t.Fatalf("ManifestFromJSON error: %v", err)
	}

	if restored.Stats.TotalCases != manifest.Stats.TotalCases {
		t.Errorf("TotalCases: got %d, want %d", restored.Stats.TotalCases, manifest.Stats.TotalCases)
	}
}

func TestSerializeRoundTripCompressedJSON(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})

	manifest := tree.ToManifest()
	data, err := manifest.ToCompressedJSON()
	if err != nil {
		t.Fatalf("ToCompressedJSON error: %v", err)
	}

	restored, err := ManifestFromCompressedJSON(data)
	if err != nil {
		t.Fatalf("ManifestFromCompressedJSON error: %v", err)
	}

	if restored.Stats.TotalCases != manifest.Stats.TotalCases {
		t.Errorf("TotalCases: got %d, want %d", restored.Stats.TotalCases, manifest.Stats.TotalCases)
	}
}

func TestSerializeRoundTripBinary(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B", "C"}})

	data, err := tree.ToBinary()
	if err != nil {
		t.Fatalf("ToBinary error: %v", err)
	}

	restored, err := TreeFromBinary(data)
	if err != nil {
		t.Fatalf("TreeFromBinary error: %v", err)
	}

	if restored.TotalCases != tree.TotalCases {
		t.Errorf("TotalCases: got %d, want %d", restored.TotalCases, tree.TotalCases)
	}
}

func TestCompareFingerprints(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A", "B"}})
	fp := tree.Fingerprint()

	if !CompareFingerprints(fp, fp) {
		t.Error("CompareFingerprints with same hash should return true")
	}

	var zero Hash
	if CompareFingerprints(fp, zero) {
		t.Error("CompareFingerprints with zero hash should return false")
	}
}

func TestHashString(t *testing.T) {
	tree := NewTree()
	tree.AddTrace(Trace{CaseID: "c1", Activities: []string{"A"}})
	fp := tree.Fingerprint()

	short := fp.String()
	full := fp.FullString()
	if short == "" || full == "" {
		t.Error("hash String/FullString should not be empty")
	}
	if len(full) < len(short) {
		t.Error("FullString should be >= String length")
	}
}

// --- HybridTree tests ---

func TestNewHybridTree(t *testing.T) {
	ht := NewHybridTree()
	if ht == nil {
		t.Fatal("NewHybridTree returned nil")
	}
}

func TestHybridBuilderAddEvent(t *testing.T) {
	builder := NewHybridBuilder(DefaultHybridBuilderConfig())

	now := time.Now().UnixNano()
	err := builder.AddEventSimple("case1", "A", now, now+1000)
	if err != nil {
		t.Fatalf("AddEventSimple error: %v", err)
	}
	err = builder.AddEventSimple("case1", "B", now+2000, now+3000)
	if err != nil {
		t.Fatalf("AddEventSimple error: %v", err)
	}

	ht := builder.Build()
	if ht == nil {
		t.Fatal("Build returned nil")
	}
}

func TestHybridTreeFingerprint(t *testing.T) {
	builder := NewHybridBuilder(DefaultHybridBuilderConfig())
	now := time.Now().UnixNano()
	_ = builder.AddEventSimple("c1", "A", now, now+100)
	_ = builder.AddEventSimple("c1", "B", now+200, now+300)
	ht := builder.Build()

	fp := ht.Fingerprint()
	if fp.IsZero() {
		t.Error("hybrid tree fingerprint should not be zero")
	}
}

func TestHybridTreeDiff(t *testing.T) {
	b1 := NewHybridBuilder(DefaultHybridBuilderConfig())
	now := time.Now().UnixNano()
	_ = b1.AddEventSimple("c1", "A", now, now+100)
	ht1 := b1.Build()

	b2 := NewHybridBuilder(DefaultHybridBuilderConfig())
	_ = b2.AddEventSimple("c1", "A", now, now+100)
	_ = b2.AddEventSimple("c1", "B", now+200, now+300)
	ht2 := b2.Build()

	diff := ht1.Diff(ht2)
	if diff == nil {
		t.Fatal("Diff returned nil")
	}
	if diff.Identical {
		t.Error("different hybrid trees should not be identical")
	}
}

func TestHybridTreeFindBottlenecks(t *testing.T) {
	builder := NewHybridBuilder(DefaultHybridBuilderConfig())
	now := time.Now().UnixNano()
	_ = builder.AddEventSimple("c1", "SlowTask", now, now+1_000_000_000) // 1 second
	_ = builder.AddEventSimple("c1", "FastTask", now+1_000_000_000, now+1_000_100_000)
	ht := builder.Build()

	bottlenecks := ht.FindBottlenecks(TimeRange{Start: 0, End: now + 2_000_000_000}, 10)
	if len(bottlenecks) == 0 {
		t.Error("FindBottlenecks should find activities")
	}
}

func TestBuildFromTraces(t *testing.T) {
	traces := []Trace{
		{CaseID: "c1", Activities: []string{"A", "B", "C"}},
		{CaseID: "c2", Activities: []string{"A", "D"}},
	}

	tree := BuildFromTraces(traces)
	if tree == nil {
		t.Fatal("BuildFromTraces returned nil")
	}
	if tree.TotalCases != 2 {
		t.Errorf("TotalCases = %d, want 2", tree.TotalCases)
	}
}

func TestVariantString(t *testing.T) {
	v := Variant{
		Path:  []string{"A", "B", "C"},
		Count: 5,
	}
	s := v.String()
	if s == "" {
		t.Error("Variant.String() should not be empty")
	}
}
