package processors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sync"

	"github.com/logflow/logflow/pkg/pipeline"
)

// AnonymizeProcessor hashes specified fields for GDPR compliance.
type AnonymizeProcessor struct {
	columns map[string]bool // Fields to anonymize
	salt    []byte
	cache   map[string][]byte // Cache for repeated values
	mu      sync.RWMutex
}

// NewAnonymizeProcessor creates an anonymization processor.
func NewAnonymizeProcessor(columns []string, salt string) *AnonymizeProcessor {
	colMap := make(map[string]bool, len(columns))
	for _, col := range columns {
		colMap[col] = true
	}

	return &AnonymizeProcessor{
		columns: colMap,
		salt:    []byte(salt),
		cache:   make(map[string][]byte),
	}
}

// Name returns the processor name.
func (p *AnonymizeProcessor) Name() string {
	return "anonymize"
}

// Process implements Processor.Process.
func (p *AnonymizeProcessor) Process(ctx context.Context, in <-chan *pipeline.Event, out chan<- *pipeline.Event) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}

			p.anonymizeEvent(event)

			select {
			case out <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// anonymizeEvent hashes the specified fields in-place.
func (p *AnonymizeProcessor) anonymizeEvent(event *pipeline.Event) {
	// Standard fields
	if p.columns["case_id"] || p.columns["case:concept:name"] {
		event.CaseID = p.hash(event.CaseID)
	}
	if p.columns["activity"] || p.columns["concept:name"] {
		event.Activity = p.hash(event.Activity)
	}
	if p.columns["resource"] || p.columns["org:resource"] {
		event.Resource = p.hash(event.Resource)
	}

	// Attributes
	for i := range event.Attributes {
		keyStr := string(event.Attributes[i].Key)
		if p.columns[keyStr] {
			event.Attributes[i].Value = p.hash(event.Attributes[i].Value)
		}
	}
}

// hash computes a salted SHA-256 hash, truncated for readability.
func (p *AnonymizeProcessor) hash(value []byte) []byte {
	if len(value) == 0 {
		return value
	}

	// Check cache
	key := string(value)
	p.mu.RLock()
	if cached, ok := p.cache[key]; ok {
		p.mu.RUnlock()
		return cached
	}
	p.mu.RUnlock()

	// Compute hash
	h := sha256.New()
	h.Write(p.salt)
	h.Write(value)
	fullHash := hex.EncodeToString(h.Sum(nil))

	// Truncate to 16 chars (64 bits of entropy)
	result := []byte(fullHash[:16])

	// Cache
	p.mu.Lock()
	p.cache[key] = result
	p.mu.Unlock()

	return result
}

// ClearCache frees memory used by the hash cache.
func (p *AnonymizeProcessor) ClearCache() {
	p.mu.Lock()
	p.cache = make(map[string][]byte)
	p.mu.Unlock()
}

// AnonymizeProcessorFactory creates an AnonymizeProcessor from config.
func AnonymizeProcessorFactory(cfg pipeline.Config) (pipeline.Processor, error) {
	var columns []string
	if cols, ok := cfg.ProcessorOptions["anonymize_columns"].([]string); ok {
		columns = cols
	}

	salt := "logflow-default-salt"
	if s, ok := cfg.ProcessorOptions["anonymize_salt"].(string); ok && s != "" {
		salt = s
	}

	return NewAnonymizeProcessor(columns, salt), nil
}

// PseudonymizeProcessor replaces values with consistent pseudonyms.
// Unlike hashing, pseudonyms are readable but not reversible.
type PseudonymizeProcessor struct {
	columns   map[string]bool
	prefixes  map[string]string // Field -> prefix (e.g., "case_id" -> "C")
	counters  map[string]map[string]int64
	mu        sync.Mutex
}

// NewPseudonymizeProcessor creates a pseudonymization processor.
func NewPseudonymizeProcessor(columns []string, prefixes map[string]string) *PseudonymizeProcessor {
	colMap := make(map[string]bool, len(columns))
	for _, col := range columns {
		colMap[col] = true
	}

	if prefixes == nil {
		prefixes = map[string]string{
			"case_id":  "C",
			"resource": "R",
		}
	}

	return &PseudonymizeProcessor{
		columns:  colMap,
		prefixes: prefixes,
		counters: make(map[string]map[string]int64),
	}
}

// Name returns the processor name.
func (p *PseudonymizeProcessor) Name() string {
	return "pseudonymize"
}

// Process implements Processor.Process.
func (p *PseudonymizeProcessor) Process(ctx context.Context, in <-chan *pipeline.Event, out chan<- *pipeline.Event) error {
	defer close(out)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case event, ok := <-in:
			if !ok {
				return nil
			}

			p.pseudonymizeEvent(event)

			select {
			case out <- event:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}
}

// pseudonymizeEvent replaces fields with pseudonyms.
func (p *PseudonymizeProcessor) pseudonymizeEvent(event *pipeline.Event) {
	if p.columns["case_id"] {
		event.CaseID = p.getPseudonym("case_id", event.CaseID)
	}
	if p.columns["resource"] {
		event.Resource = p.getPseudonym("resource", event.Resource)
	}

	for i := range event.Attributes {
		keyStr := string(event.Attributes[i].Key)
		if p.columns[keyStr] {
			event.Attributes[i].Value = p.getPseudonym(keyStr, event.Attributes[i].Value)
		}
	}
}

// getPseudonym returns a consistent pseudonym for a value.
func (p *PseudonymizeProcessor) getPseudonym(field string, value []byte) []byte {
	if len(value) == 0 {
		return value
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	if p.counters[field] == nil {
		p.counters[field] = make(map[string]int64)
	}

	key := string(value)
	if _, ok := p.counters[field][key]; !ok {
		p.counters[field][key] = int64(len(p.counters[field]) + 1)
	}

	prefix := p.prefixes[field]
	if prefix == "" {
		prefix = "X"
	}

	return []byte(prefix + string(rune('0'+p.counters[field][key])))
}
