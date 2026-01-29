package quality

import (
	"hash"
	"hash/fnv"
	"io"
	"os"
	"sync"
)

// Checksum computes FNV-1a checksums.
type Checksum struct {
	mu   sync.Mutex
	hash hash.Hash64
}

// NewChecksum creates a checksum calculator.
func NewChecksum() *Checksum {
	return &Checksum{
		hash: fnv.New64a(),
	}
}

// Update adds data to checksum.
func (c *Checksum) Update(data []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hash.Write(data)
}

// Sum returns the current checksum.
func (c *Checksum) Sum() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.hash.Sum64()
}

// Reset clears the checksum.
func (c *Checksum) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.hash.Reset()
}

// FileChecksum computes checksum of a file.
func FileChecksum(path string) (uint64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()

	h := fnv.New64a()
	buf := make([]byte, 256*1024)

	for {
		n, err := f.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}

	return h.Sum64(), nil
}

// StreamChecksum computes checksum from a reader.
func StreamChecksum(r io.Reader) (uint64, error) {
	h := fnv.New64a()
	buf := make([]byte, 256*1024)

	for {
		n, err := r.Read(buf)
		if n > 0 {
			h.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return 0, err
		}
	}

	return h.Sum64(), nil
}
