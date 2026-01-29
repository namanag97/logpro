// Package auth provides default authentication implementations.
package auth

import (
	"context"

	"github.com/logflow/logflow/pkg/interfaces"
)

// NoopAuthenticator allows all requests without authentication.
// Use this for local development or trusted environments.
type NoopAuthenticator struct {
	defaultTenant string
}

// NewNoopAuthenticator creates a new noop authenticator.
func NewNoopAuthenticator() *NoopAuthenticator {
	return &NoopAuthenticator{
		defaultTenant: "default",
	}
}

// NewNoopAuthenticatorWithTenant creates a noop authenticator with a custom tenant.
func NewNoopAuthenticatorWithTenant(tenant string) *NoopAuthenticator {
	return &NoopAuthenticator{
		defaultTenant: tenant,
	}
}

// Authenticate returns an anonymous identity for any token.
func (n *NoopAuthenticator) Authenticate(ctx context.Context, token string) (interfaces.Identity, error) {
	return interfaces.NewAnonymousIdentity(n.defaultTenant), nil
}

// Type returns "noop".
func (n *NoopAuthenticator) Type() string {
	return "noop"
}

// NoopAuthorizer allows all authorization requests.
type NoopAuthorizer struct{}

// NewNoopAuthorizer creates a new noop authorizer.
func NewNoopAuthorizer() *NoopAuthorizer {
	return &NoopAuthorizer{}
}

// Authorize always returns true.
func (n *NoopAuthorizer) Authorize(ctx context.Context, identity interfaces.Identity, action string, resource string) (bool, error) {
	return true, nil
}

// AuthorizeData always allows access with no filter.
func (n *NoopAuthorizer) AuthorizeData(ctx context.Context, identity interfaces.Identity, table string, columns []string, filter string) (bool, string, error) {
	return true, filter, nil
}

// Verify interface compliance.
var (
	_ interfaces.Authenticator = (*NoopAuthenticator)(nil)
	_ interfaces.Authorizer    = (*NoopAuthorizer)(nil)
)
