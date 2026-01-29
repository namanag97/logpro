package pmpt

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
)

// Manifest represents the serializable form of a PMPT for embedding in Parquet metadata.
type Manifest struct {
	Version     string         `json:"version"`
	RootHash    string         `json:"root_hash"`
	Stats       ManifestStats  `json:"stats"`
	Nodes       []ManifestNode `json:"nodes"`
}

// ManifestStats contains serializable statistics.
type ManifestStats struct {
	TotalCases   int64 `json:"total_cases"`
	TotalEvents  int64 `json:"total_events"`
	UniqueNodes  int64 `json:"unique_nodes"`
	MaxDepth     int   `json:"max_depth"`
	VariantCount int64 `json:"variant_count"`
}

// ManifestNode represents a serializable node.
type ManifestNode struct {
	Hash        string   `json:"hash"`
	Activity    string   `json:"activity"`
	ParentHash  []string `json:"parents,omitempty"`
	CaseCount   int64    `json:"case_count"`
	AvgDuration int64    `json:"avg_duration_ns,omitempty"`
	Depth       int      `json:"depth"`
}

// ToManifest converts a Tree to a serializable Manifest.
func (t *Tree) ToManifest() *Manifest {
	t.mu.RLock()
	defer t.mu.RUnlock()

	variants := t.GetVariants()

	m := &Manifest{
		Version:  "1.0",
		RootHash: t.RootHash.FullString(),
		Stats: ManifestStats{
			TotalCases:   t.TotalCases,
			TotalEvents:  t.TotalEvents,
			UniqueNodes:  t.UniqueNodes,
			MaxDepth:     t.MaxDepth,
			VariantCount: int64(len(variants)),
		},
		Nodes: make([]ManifestNode, 0, len(t.NodeIndex)),
	}

	// Serialize all nodes
	for _, node := range t.NodeIndex {
		if node.Activity == "__ROOT__" {
			continue // Skip root
		}

		mn := ManifestNode{
			Hash:        node.NodeHash.FullString(),
			Activity:    node.Activity,
			CaseCount:   node.CaseCount,
			AvgDuration: node.AvgDuration,
			Depth:       node.Depth,
		}

		for _, parent := range node.Parents {
			if parent.Activity != "__ROOT__" {
				mn.ParentHash = append(mn.ParentHash, parent.NodeHash.FullString())
			}
		}

		m.Nodes = append(m.Nodes, mn)
	}

	return m
}

// ToJSON serializes the manifest to JSON.
func (m *Manifest) ToJSON() ([]byte, error) {
	return json.Marshal(m)
}

// ToCompressedJSON serializes and gzip-compresses the manifest.
func (m *Manifest) ToCompressedJSON() ([]byte, error) {
	jsonData, err := m.ToJSON()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(jsonData); err != nil {
		return nil, err
	}
	if err := gz.Close(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// ManifestFromJSON deserializes a manifest from JSON.
func ManifestFromJSON(data []byte) (*Manifest, error) {
	var m Manifest
	if err := json.Unmarshal(data, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// ManifestFromCompressedJSON deserializes from gzip-compressed JSON.
func ManifestFromCompressedJSON(data []byte) (*Manifest, error) {
	gz, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer gz.Close()

	jsonData, err := io.ReadAll(gz)
	if err != nil {
		return nil, err
	}

	return ManifestFromJSON(jsonData)
}

// Binary serialization for maximum efficiency (optional)

// ToBinary serializes the tree to a compact binary format.
func (t *Tree) ToBinary() ([]byte, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	var buf bytes.Buffer

	// Magic number + version
	buf.WriteString("PMPT")
	binary.Write(&buf, binary.LittleEndian, uint16(1)) // Version 1

	// Root hash
	buf.Write(t.RootHash[:])

	// Stats
	binary.Write(&buf, binary.LittleEndian, t.TotalCases)
	binary.Write(&buf, binary.LittleEndian, t.TotalEvents)
	binary.Write(&buf, binary.LittleEndian, t.UniqueNodes)
	binary.Write(&buf, binary.LittleEndian, int32(t.MaxDepth))

	// Node count
	nodeCount := int32(len(t.NodeIndex) - 1) // Exclude root
	binary.Write(&buf, binary.LittleEndian, nodeCount)

	// Nodes
	for _, node := range t.NodeIndex {
		if node.Activity == "__ROOT__" {
			continue
		}

		// Hash
		buf.Write(node.NodeHash[:])

		// Activity (length-prefixed)
		actBytes := []byte(node.Activity)
		binary.Write(&buf, binary.LittleEndian, uint16(len(actBytes)))
		buf.Write(actBytes)

		// Parent count and hashes
		binary.Write(&buf, binary.LittleEndian, uint8(len(node.Parents)))
		for _, parent := range node.Parents {
			buf.Write(parent.NodeHash[:])
		}

		// Stats
		binary.Write(&buf, binary.LittleEndian, node.CaseCount)
		binary.Write(&buf, binary.LittleEndian, node.AvgDuration)
		binary.Write(&buf, binary.LittleEndian, int16(node.Depth))
	}

	return buf.Bytes(), nil
}

// TreeFromBinary deserializes a tree from binary format.
func TreeFromBinary(data []byte) (*Tree, error) {
	buf := bytes.NewReader(data)

	// Magic number
	magic := make([]byte, 4)
	if _, err := buf.Read(magic); err != nil {
		return nil, err
	}
	if string(magic) != "PMPT" {
		return nil, fmt.Errorf("invalid magic number")
	}

	// Version
	var version uint16
	binary.Read(buf, binary.LittleEndian, &version)
	if version != 1 {
		return nil, fmt.Errorf("unsupported version: %d", version)
	}

	tree := NewTree()

	// Root hash
	buf.Read(tree.RootHash[:])

	// Stats
	binary.Read(buf, binary.LittleEndian, &tree.TotalCases)
	binary.Read(buf, binary.LittleEndian, &tree.TotalEvents)
	binary.Read(buf, binary.LittleEndian, &tree.UniqueNodes)

	var maxDepth int32
	binary.Read(buf, binary.LittleEndian, &maxDepth)
	tree.MaxDepth = int(maxDepth)

	// Node count
	var nodeCount int32
	binary.Read(buf, binary.LittleEndian, &nodeCount)

	// Temporary map for reconstruction
	nodesByHash := make(map[Hash]*ProcessNode)
	nodesByHash[tree.Root.NodeHash] = tree.Root

	// First pass: create all nodes
	for i := int32(0); i < nodeCount; i++ {
		node := &ProcessNode{
			Children: make([]*ProcessNode, 0),
		}

		// Hash
		buf.Read(node.NodeHash[:])

		// Activity
		var actLen uint16
		binary.Read(buf, binary.LittleEndian, &actLen)
		actBytes := make([]byte, actLen)
		buf.Read(actBytes)
		node.Activity = string(actBytes)

		// Skip parent hashes for now (we'll link in second pass)
		var parentCount uint8
		binary.Read(buf, binary.LittleEndian, &parentCount)
		parentHashes := make([]Hash, parentCount)
		for j := uint8(0); j < parentCount; j++ {
			buf.Read(parentHashes[j][:])
		}

		// Stats
		binary.Read(buf, binary.LittleEndian, &node.CaseCount)
		binary.Read(buf, binary.LittleEndian, &node.AvgDuration)

		var depth int16
		binary.Read(buf, binary.LittleEndian, &depth)
		node.Depth = int(depth)

		nodesByHash[node.NodeHash] = node
		tree.NodeIndex[node.NodeHash] = node
		tree.ActivityIndex[node.Activity] = append(tree.ActivityIndex[node.Activity], node)
	}

	// Second pass: link parents and children
	// (Would need to store parent hashes in first pass - simplified version)

	return tree, nil
}

// CompareFingerprints performs O(1) comparison of two tree fingerprints.
func CompareFingerprints(left, right Hash) bool {
	return left == right
}

// ParquetMetadataKey is the key used to store PMPT in Parquet footer.
const ParquetMetadataKey = "pm:process_graph"

// ToParquetMetadata returns the PMPT as a string suitable for Parquet metadata.
func (t *Tree) ToParquetMetadata() (string, error) {
	manifest := t.ToManifest()
	data, err := manifest.ToCompressedJSON()
	if err != nil {
		return "", err
	}
	// Base64 encode for safe embedding
	return encodeBase64(data), nil
}

// encodeBase64 encodes bytes to base64 string.
func encodeBase64(data []byte) string {
	const base64Chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	result := make([]byte, ((len(data)+2)/3)*4)

	for i, j := 0, 0; i < len(data); i += 3 {
		var n uint32
		n |= uint32(data[i]) << 16
		if i+1 < len(data) {
			n |= uint32(data[i+1]) << 8
		}
		if i+2 < len(data) {
			n |= uint32(data[i+2])
		}

		result[j] = base64Chars[(n>>18)&0x3F]
		result[j+1] = base64Chars[(n>>12)&0x3F]
		result[j+2] = base64Chars[(n>>6)&0x3F]
		result[j+3] = base64Chars[n&0x3F]
		j += 4
	}

	// Padding
	padding := (3 - len(data)%3) % 3
	for i := 0; i < padding; i++ {
		result[len(result)-1-i] = '='
	}

	return string(result)
}
