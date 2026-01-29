// Package pmpt implements a Process Merkle Patricia Tree (PMPT) for efficient
// process log storage, comparison, and drift detection.
//
// Key innovations:
//   - O(1) log comparison via root hash
//   - Deduplication of shared process prefixes
//   - Sub-linear variant search
//   - Git-like process versioning
package pmpt

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"sort"
	"sync"

	"github.com/RoaringBitmap/roaring"
)

// Hash represents a 32-byte SHA-256 hash.
type Hash [32]byte

// String returns the hex representation of the hash.
func (h Hash) String() string {
	return hex.EncodeToString(h[:])[:16] + "..." // Truncated for readability
}

// FullString returns the full hex representation.
func (h Hash) FullString() string {
	return hex.EncodeToString(h[:])
}

// IsZero returns true if the hash is all zeros.
func (h Hash) IsZero() bool {
	return h == Hash{}
}

// ProcessNode represents a node in the Process Merkle Patricia Tree.
// Each node is a unique activity step, with its hash derived from:
// - The activity name
// - The hashes of all parent nodes (for path uniqueness)
type ProcessNode struct {
	// NodeHash uniquely identifies this node in the tree
	// Hash = SHA256(Activity + sorted(ParentHashes))
	NodeHash Hash

	// Activity is the human-readable activity name
	Activity string

	// Parents are the preceding steps in the process
	// For most linear processes, this is 1 parent
	// For merging paths (e.g., after parallel execution), can be multiple
	Parents []*ProcessNode

	// Children are the subsequent steps
	Children []*ProcessNode

	// CaseCount is the number of cases that passed through this node
	CaseCount int64

	// Frequency is the relative frequency (CaseCount / TotalCases)
	Frequency float64

	// AvgDuration is the average time spent at this step (nanoseconds)
	AvgDuration int64

	// Depth is the distance from root (for visualization)
	Depth int

	// CaseBitmap tracks which case IDs passed through this node using a
	// roaring bitmap. Each case is assigned a sequential uint32 ID by the
	// builder. This enables O(1) set membership checks and efficient
	// intersection/union across nodes.
	CaseBitmap *roaring.Bitmap

	// ObjectCounts tracks the number of distinct OCEL objects per object type
	// that were involved in events passing through this node.
	ObjectCounts map[string]int64

	// ObjectBitmap tracks distinct object IDs per object type using roaring
	// bitmaps. Keys are object types; values are bitmaps of object IDs
	// (mapped to uint32 by the builder).
	ObjectBitmap map[string]*roaring.Bitmap
}

// OCELNode wraps a ProcessNode with additional object-centric analytics.
type OCELNode struct {
	*ProcessNode

	// ObjectTypes lists the distinct object types observed at this node.
	ObjectTypes []string

	// ObjectTypeFrequency maps object type -> fraction of events at this node
	// that involve at least one object of that type.
	ObjectTypeFrequency map[string]float64
}

// Tree represents a Process Merkle Patricia Tree.
type Tree struct {
	mu sync.RWMutex

	// Root node (represents process start)
	Root *ProcessNode

	// RootHash is the hash of the entire tree (for O(1) comparison)
	RootHash Hash

	// NodeIndex maps hash -> node for O(1) lookup
	NodeIndex map[Hash]*ProcessNode

	// ActivityIndex maps activity name -> nodes (for search)
	ActivityIndex map[string][]*ProcessNode

	// Stats
	TotalCases    int64
	TotalEvents   int64
	UniqueNodes   int64
	MaxDepth      int
	VariantCount  int64
}

// NewTree creates an empty PMPT.
func NewTree() *Tree {
	root := &ProcessNode{
		Activity: "__ROOT__",
		Children: make([]*ProcessNode, 0),
	}
	root.NodeHash = hashNode(root.Activity, nil)

	return &Tree{
		Root:          root,
		NodeIndex:     map[Hash]*ProcessNode{root.NodeHash: root},
		ActivityIndex: make(map[string][]*ProcessNode),
	}
}

// Trace represents a sequence of activities for a single case.
type Trace struct {
	CaseID     string
	Activities []string
	Timestamps []int64 // Optional: for duration calculation
}

// ObjectRef pairs an object ID with its type for OCEL traces.
type ObjectRef struct {
	ObjectID   string
	ObjectType string
}

// ObjectTrace extends Trace with per-step OCEL object references.
type ObjectTrace struct {
	Trace
	// Objects[i] contains the objects associated with Activities[i].
	Objects [][]ObjectRef
}

// AddTrace adds a process trace to the tree.
func (t *Tree) AddTrace(trace Trace) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if len(trace.Activities) == 0 {
		return
	}

	t.TotalCases++
	current := t.Root

	for i, activity := range trace.Activities {
		t.TotalEvents++

		// Compute expected hash for this step
		expectedHash := hashNode(activity, []*ProcessNode{current})

		// Check if node exists
		node, exists := t.NodeIndex[expectedHash]
		if !exists {
			// Create new node
			node = &ProcessNode{
				NodeHash: expectedHash,
				Activity: activity,
				Parents:  []*ProcessNode{current},
				Children: make([]*ProcessNode, 0),
				Depth:    current.Depth + 1,
			}
			t.NodeIndex[expectedHash] = node
			t.ActivityIndex[activity] = append(t.ActivityIndex[activity], node)
			t.UniqueNodes++

			// Add as child of current
			current.Children = append(current.Children, node)

			// Track max depth
			if node.Depth > t.MaxDepth {
				t.MaxDepth = node.Depth
			}
		}

		// Update stats
		node.CaseCount++

		// Calculate duration if timestamps available
		if len(trace.Timestamps) > i+1 {
			duration := trace.Timestamps[i+1] - trace.Timestamps[i]
			// Running average
			node.AvgDuration = (node.AvgDuration*(node.CaseCount-1) + duration) / node.CaseCount
		}

		current = node
	}

	// Recalculate root hash (tree fingerprint)
	t.RootHash = t.computeRootHash()
}

// computeRootHash computes the Merkle root hash of the entire tree.
func (t *Tree) computeRootHash() Hash {
	return t.hashSubtree(t.Root)
}

// hashSubtree recursively computes the hash of a subtree.
func (t *Tree) hashSubtree(node *ProcessNode) Hash {
	if len(node.Children) == 0 {
		return node.NodeHash
	}

	// Collect child hashes
	childHashes := make([]Hash, len(node.Children))
	for i, child := range node.Children {
		childHashes[i] = t.hashSubtree(child)
	}

	// Sort for deterministic ordering
	sort.Slice(childHashes, func(i, j int) bool {
		return bytes.Compare(childHashes[i][:], childHashes[j][:]) < 0
	})

	// Combine with node hash
	h := sha256.New()
	h.Write(node.NodeHash[:])
	for _, ch := range childHashes {
		h.Write(ch[:])
	}
	binary.Write(h, binary.LittleEndian, node.CaseCount)

	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

// hashNode computes the hash for a node given its activity and parents.
func hashNode(activity string, parents []*ProcessNode) Hash {
	h := sha256.New()
	h.Write([]byte(activity))

	if len(parents) > 0 {
		// Sort parent hashes for deterministic ordering
		parentHashes := make([]Hash, len(parents))
		for i, p := range parents {
			parentHashes[i] = p.NodeHash
		}
		sort.Slice(parentHashes, func(i, j int) bool {
			return bytes.Compare(parentHashes[i][:], parentHashes[j][:]) < 0
		})
		for _, ph := range parentHashes {
			h.Write(ph[:])
		}
	}

	var result Hash
	copy(result[:], h.Sum(nil))
	return result
}

// Compare compares two trees and returns structural differences.
func Compare(left, right *Tree) *TreeDiff {
	diff := &TreeDiff{
		LeftRootHash:  left.RootHash,
		RightRootHash: right.RootHash,
		Identical:     left.RootHash == right.RootHash,
	}

	if diff.Identical {
		return diff
	}

	// Find new nodes (in right but not left)
	for hash, node := range right.NodeIndex {
		if _, exists := left.NodeIndex[hash]; !exists {
			diff.NewNodes = append(diff.NewNodes, node)
		}
	}

	// Find removed nodes (in left but not right)
	for hash, node := range left.NodeIndex {
		if _, exists := right.NodeIndex[hash]; !exists {
			diff.RemovedNodes = append(diff.RemovedNodes, node)
		}
	}

	// Find frequency changes for shared nodes
	for hash, leftNode := range left.NodeIndex {
		if rightNode, exists := right.NodeIndex[hash]; exists {
			leftFreq := float64(leftNode.CaseCount) / float64(left.TotalCases)
			rightFreq := float64(rightNode.CaseCount) / float64(right.TotalCases)
			delta := rightFreq - leftFreq

			if abs(delta) > 0.01 { // >1% change
				diff.FrequencyChanges = append(diff.FrequencyChanges, FrequencyChange{
					Node:          leftNode,
					LeftFreq:      leftFreq,
					RightFreq:     rightFreq,
					Delta:         delta,
					LeftCount:     leftNode.CaseCount,
					RightCount:    rightNode.CaseCount,
				})
			}

			// Check duration changes
			if leftNode.AvgDuration > 0 && rightNode.AvgDuration > 0 {
				durationDelta := float64(rightNode.AvgDuration-leftNode.AvgDuration) / float64(leftNode.AvgDuration)
				if abs(durationDelta) > 0.1 { // >10% change
					diff.DurationChanges = append(diff.DurationChanges, DurationChange{
						Node:        leftNode,
						LeftDur:     leftNode.AvgDuration,
						RightDur:    rightNode.AvgDuration,
						DeltaPct:    durationDelta * 100,
					})
				}
			}
		}
	}

	// Sort by impact
	sort.Slice(diff.FrequencyChanges, func(i, j int) bool {
		return abs(diff.FrequencyChanges[i].Delta) > abs(diff.FrequencyChanges[j].Delta)
	})
	sort.Slice(diff.DurationChanges, func(i, j int) bool {
		return abs(diff.DurationChanges[i].DeltaPct) > abs(diff.DurationChanges[j].DeltaPct)
	})

	return diff
}

// TreeDiff represents differences between two process trees.
type TreeDiff struct {
	LeftRootHash     Hash
	RightRootHash    Hash
	Identical        bool
	NewNodes         []*ProcessNode
	RemovedNodes     []*ProcessNode
	FrequencyChanges []FrequencyChange
	DurationChanges  []DurationChange
}

// FrequencyChange represents a change in activity frequency.
type FrequencyChange struct {
	Node       *ProcessNode
	LeftFreq   float64
	RightFreq  float64
	Delta      float64
	LeftCount  int64
	RightCount int64
}

// DurationChange represents a change in activity duration.
type DurationChange struct {
	Node     *ProcessNode
	LeftDur  int64
	RightDur int64
	DeltaPct float64
}

// String returns a human-readable diff report.
func (d *TreeDiff) String() string {
	if d.Identical {
		return fmt.Sprintf("Trees are IDENTICAL\nRoot Hash: %s\n", d.LeftRootHash)
	}

	s := "=== Process Tree Diff ===\n\n"
	s += fmt.Sprintf("Left Root:  %s\n", d.LeftRootHash)
	s += fmt.Sprintf("Right Root: %s\n", d.RightRootHash)
	s += "\n"

	if len(d.NewNodes) > 0 {
		s += fmt.Sprintf("New Variants (+%d nodes):\n", len(d.NewNodes))
		for _, node := range d.NewNodes[:min(5, len(d.NewNodes))] {
			path := getPathString(node)
			s += fmt.Sprintf("  + %s (cases: %d)\n", path, node.CaseCount)
		}
		if len(d.NewNodes) > 5 {
			s += fmt.Sprintf("  ... and %d more\n", len(d.NewNodes)-5)
		}
		s += "\n"
	}

	if len(d.RemovedNodes) > 0 {
		s += fmt.Sprintf("Removed Variants (-%d nodes):\n", len(d.RemovedNodes))
		for _, node := range d.RemovedNodes[:min(5, len(d.RemovedNodes))] {
			path := getPathString(node)
			s += fmt.Sprintf("  - %s (was: %d cases)\n", path, node.CaseCount)
		}
		if len(d.RemovedNodes) > 5 {
			s += fmt.Sprintf("  ... and %d more\n", len(d.RemovedNodes)-5)
		}
		s += "\n"
	}

	if len(d.FrequencyChanges) > 0 {
		s += "Frequency Shifts (top 5):\n"
		for i, fc := range d.FrequencyChanges[:min(5, len(d.FrequencyChanges))] {
			sign := "+"
			if fc.Delta < 0 {
				sign = ""
			}
			s += fmt.Sprintf("  %d. %s: %.1f%% -> %.1f%% (%s%.1f%%)\n",
				i+1, fc.Node.Activity, fc.LeftFreq*100, fc.RightFreq*100, sign, fc.Delta*100)
		}
		s += "\n"
	}

	if len(d.DurationChanges) > 0 {
		s += "Duration Changes (top 5):\n"
		for i, dc := range d.DurationChanges[:min(5, len(d.DurationChanges))] {
			sign := "+"
			if dc.DeltaPct < 0 {
				sign = ""
			}
			s += fmt.Sprintf("  %d. %s: %s%.1f%%\n",
				i+1, dc.Node.Activity, sign, dc.DeltaPct)
		}
	}

	return s
}

// getPathString returns the path from root to this node.
func getPathString(node *ProcessNode) string {
	path := []string{}
	current := node
	for current != nil && current.Activity != "__ROOT__" {
		path = append([]string{current.Activity}, path...)
		if len(current.Parents) > 0 {
			current = current.Parents[0]
		} else {
			break
		}
	}
	result := ""
	for i, p := range path {
		if i > 0 {
			result += " -> "
		}
		result += p
	}
	return result
}

// HasPath checks if a specific activity sequence exists in the tree.
// This is O(path_length) instead of O(total_events).
func (t *Tree) HasPath(activities []string) bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	current := t.Root
	for _, activity := range activities {
		found := false
		for _, child := range current.Children {
			if child.Activity == activity {
				current = child
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}
	return true
}

// FindActivity returns all nodes with the given activity name.
func (t *Tree) FindActivity(activity string) []*ProcessNode {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.ActivityIndex[activity]
}

// GetVariants returns all unique end-to-end process variants.
func (t *Tree) GetVariants() []Variant {
	t.mu.RLock()
	defer t.mu.RUnlock()

	variants := make([]Variant, 0)
	t.collectVariants(t.Root, []string{}, &variants)

	// Sort by frequency
	sort.Slice(variants, func(i, j int) bool {
		return variants[i].Count > variants[j].Count
	})

	return variants
}

func (t *Tree) collectVariants(node *ProcessNode, path []string, variants *[]Variant) {
	if node.Activity != "__ROOT__" {
		path = append(path, node.Activity)
	}

	if len(node.Children) == 0 {
		// Leaf node - this is a complete variant
		if len(path) > 0 {
			*variants = append(*variants, Variant{
				Path:  append([]string{}, path...), // Copy
				Count: node.CaseCount,
			})
		}
		return
	}

	for _, child := range node.Children {
		t.collectVariants(child, path, variants)
	}
}

// Variant represents a unique end-to-end process path.
type Variant struct {
	Path  []string
	Count int64
}

// String returns the variant as an arrow-separated path.
func (v Variant) String() string {
	result := ""
	for i, p := range v.Path {
		if i > 0 {
			result += " -> "
		}
		result += p
	}
	return result
}

// Stats returns tree statistics.
func (t *Tree) Stats() TreeStats {
	t.mu.RLock()
	defer t.mu.RUnlock()

	variants := t.GetVariants()
	return TreeStats{
		TotalCases:   t.TotalCases,
		TotalEvents:  t.TotalEvents,
		UniqueNodes:  t.UniqueNodes,
		MaxDepth:     t.MaxDepth,
		VariantCount: int64(len(variants)),
		RootHash:     t.RootHash,
	}
}

// TreeStats contains tree statistics.
type TreeStats struct {
	TotalCases   int64
	TotalEvents  int64
	UniqueNodes  int64
	MaxDepth     int
	VariantCount int64
	RootHash     Hash
}

// Fingerprint returns the tree's unique fingerprint (root hash).
// Two trees with identical fingerprints have identical process structures.
func (t *Tree) Fingerprint() Hash {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.RootHash
}

// Equals compares two trees in O(1) time using root hashes.
func (t *Tree) Equals(other *Tree) bool {
	return t.Fingerprint() == other.Fingerprint()
}

func abs(x float64) float64 {
	if x < 0 {
		return -x
	}
	return x
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
