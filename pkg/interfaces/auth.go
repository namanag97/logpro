// Package interfaces defines pluggable extension points for LogFlow.
// Users can implement these interfaces to integrate with their own
// authentication, storage, scheduling, and monitoring systems.
package interfaces

import "context"

// Identity represents an authenticated user or service.
type Identity interface {
	// ID returns the unique identifier for this identity.
	ID() string

	// Type returns the identity type: "user", "service", or "api_key".
	Type() string

	// Tenant returns the tenant/organization this identity belongs to.
	Tenant() string

	// Roles returns the roles assigned to this identity.
	Roles() []string

	// Attributes returns additional identity attributes.
	Attributes() map[string]string
}

// Authenticator validates credentials and returns an identity.
type Authenticator interface {
	// Authenticate validates the token and returns the associated identity.
	// Returns an error if authentication fails.
	Authenticate(ctx context.Context, token string) (Identity, error)

	// Type returns the authentication type (e.g., "jwt", "apikey", "oauth", "mtls").
	Type() string
}

// Authorizer checks if an identity can perform actions on resources.
type Authorizer interface {
	// Authorize checks if identity can perform action on resource.
	// Returns true if allowed, false otherwise.
	Authorize(ctx context.Context, identity Identity, action string, resource string) (bool, error)

	// AuthorizeData checks row/column level access for a table.
	// Returns whether access is allowed and any filter that should be applied.
	AuthorizeData(ctx context.Context, identity Identity, table string, columns []string, filter string) (allowed bool, appliedFilter string, err error)
}

// AnonymousIdentity represents an unauthenticated user.
type AnonymousIdentity struct {
	tenant string
}

// NewAnonymousIdentity creates a new anonymous identity.
func NewAnonymousIdentity(tenant string) *AnonymousIdentity {
	return &AnonymousIdentity{tenant: tenant}
}

func (a *AnonymousIdentity) ID() string                    { return "anonymous" }
func (a *AnonymousIdentity) Type() string                  { return "anonymous" }
func (a *AnonymousIdentity) Tenant() string                { return a.tenant }
func (a *AnonymousIdentity) Roles() []string               { return nil }
func (a *AnonymousIdentity) Attributes() map[string]string { return nil }

// BasicIdentity is a simple implementation of Identity.
type BasicIdentity struct {
	id         string
	idType     string
	tenant     string
	roles      []string
	attributes map[string]string
}

// NewBasicIdentity creates a new basic identity.
func NewBasicIdentity(id, idType, tenant string, roles []string, attributes map[string]string) *BasicIdentity {
	return &BasicIdentity{
		id:         id,
		idType:     idType,
		tenant:     tenant,
		roles:      roles,
		attributes: attributes,
	}
}

func (b *BasicIdentity) ID() string                    { return b.id }
func (b *BasicIdentity) Type() string                  { return b.idType }
func (b *BasicIdentity) Tenant() string                { return b.tenant }
func (b *BasicIdentity) Roles() []string               { return b.roles }
func (b *BasicIdentity) Attributes() map[string]string { return b.attributes }

// Common actions for authorization.
const (
	ActionRead   = "read"
	ActionWrite  = "write"
	ActionDelete = "delete"
	ActionAdmin  = "admin"
	ActionIngest = "ingest"
	ActionQuery  = "query"
)

// Common resource types.
const (
	ResourceTable    = "table"
	ResourceDatabase = "database"
	ResourceJob      = "job"
	ResourceSystem   = "system"
)
