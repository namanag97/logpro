// Package transform provides data transformation utilities.
package transform

import (
	"crypto/sha256"
	"encoding/hex"
	"sync"
)

// Anonymizer provides GDPR-compliant data anonymization.
type Anonymizer struct {
	salt []byte
	mu   sync.RWMutex
	// Cache for repeated values (common in Case IDs)
	cache map[string]string
}

// NewAnonymizer creates an anonymizer with the given salt.
// Salt ensures hashes cannot be reversed via rainbow tables.
func NewAnonymizer(salt string) *Anonymizer {
	return &Anonymizer{
		salt:  []byte(salt),
		cache: make(map[string]string),
	}
}

// Hash anonymizes a value using SHA-256 with salt.
// Returns a hex-encoded hash truncated to 16 chars for readability.
func (a *Anonymizer) Hash(value []byte) string {
	if len(value) == 0 {
		return ""
	}

	// Check cache first
	key := string(value)
	a.mu.RLock()
	if cached, ok := a.cache[key]; ok {
		a.mu.RUnlock()
		return cached
	}
	a.mu.RUnlock()

	// Compute salted hash
	h := sha256.New()
	h.Write(a.salt)
	h.Write(value)
	fullHash := hex.EncodeToString(h.Sum(nil))

	// Truncate for readability (still 64 bits of entropy)
	result := fullHash[:16]

	// Cache result
	a.mu.Lock()
	a.cache[key] = result
	a.mu.Unlock()

	return result
}

// HashBytes anonymizes and returns as bytes.
func (a *Anonymizer) HashBytes(value []byte) []byte {
	return []byte(a.Hash(value))
}

// ClearCache clears the hash cache to free memory.
func (a *Anonymizer) ClearCache() {
	a.mu.Lock()
	a.cache = make(map[string]string)
	a.mu.Unlock()
}

// AnonymizeConfig specifies which columns to anonymize.
type AnonymizeConfig struct {
	// Columns to hash
	Columns []string

	// Salt for hashing (should be kept secret)
	Salt string

	// PreserveFormat attempts to keep the same length/pattern
	PreserveFormat bool
}

// DefaultAnonymizeConfig returns a config with common PII columns.
func DefaultAnonymizeConfig() AnonymizeConfig {
	return AnonymizeConfig{
		Columns: []string{
			"customer_name",
			"customer_id",
			"email",
			"phone",
			"address",
			"org:resource",
			"resource",
		},
		Salt: "logflow-default-salt-change-in-production",
	}
}
