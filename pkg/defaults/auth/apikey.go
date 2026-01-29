package auth

import (
	"context"
	"errors"
	"sync"

	"github.com/logflow/logflow/pkg/interfaces"
)

// ErrInvalidAPIKey is returned when an API key is not found.
var ErrInvalidAPIKey = errors.New("invalid API key")

// APIKeyAuthenticator validates requests using API keys.
type APIKeyAuthenticator struct {
	mu   sync.RWMutex
	keys map[string]*APIKey
}

// APIKey represents an API key with associated identity.
type APIKey struct {
	Key        string
	ID         string
	Tenant     string
	Roles      []string
	Attributes map[string]string
	Enabled    bool
}

// NewAPIKeyAuthenticator creates a new API key authenticator.
func NewAPIKeyAuthenticator() *APIKeyAuthenticator {
	return &APIKeyAuthenticator{
		keys: make(map[string]*APIKey),
	}
}

// AddKey registers an API key.
func (a *APIKeyAuthenticator) AddKey(key *APIKey) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.keys[key.Key] = key
}

// RemoveKey removes an API key.
func (a *APIKeyAuthenticator) RemoveKey(keyValue string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	delete(a.keys, keyValue)
}

// Authenticate validates the API key and returns the identity.
func (a *APIKeyAuthenticator) Authenticate(ctx context.Context, token string) (interfaces.Identity, error) {
	a.mu.RLock()
	key, ok := a.keys[token]
	a.mu.RUnlock()

	if !ok {
		return nil, ErrInvalidAPIKey
	}

	if !key.Enabled {
		return nil, ErrInvalidAPIKey
	}

	return interfaces.NewBasicIdentity(
		key.ID,
		"api_key",
		key.Tenant,
		key.Roles,
		key.Attributes,
	), nil
}

// Type returns "apikey".
func (a *APIKeyAuthenticator) Type() string {
	return "apikey"
}

// Verify interface compliance.
var _ interfaces.Authenticator = (*APIKeyAuthenticator)(nil)
