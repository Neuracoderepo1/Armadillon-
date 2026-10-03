package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/lib/pq"
)

// PostgresStore is the durable Store implementation described in
// section 18/19 of the build directive. It is NOT on the synchronous
// risk-decision hot path (see internal/risk) — it backs tenant/key
// management only.
//
// All queries are parameterized ($1, $2, ...) to rule out SQL
// injection (section 16); no user input is ever interpolated into SQL
// text.
type PostgresStore struct {
	db *sql.DB
}

// OpenPostgresStore opens a connection pool and verifies connectivity
// with a Ping. It does NOT run migrations — see cmd/migrate (TODO) or
// apply migrations/*.sql manually / via your deploy pipeline, per
// section 18 ("migration system").
func OpenPostgresStore(ctx context.Context, dsn string) (*PostgresStore, error) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, fmt.Errorf("opening postgres connection: %w", err)
	}
	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(30 * time.Minute)

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := db.PingContext(pingCtx); err != nil {
		db.Close()
		return nil, fmt.Errorf("connecting to postgres: %w", err)
	}
	return &PostgresStore{db: db}, nil
}

func (p *PostgresStore) Close() error { return p.db.Close() }

// DB exposes the underlying connection pool so other durable components
// backed by the same Postgres database — currently
// reservation.PostgresRepository — can share it instead of opening a
// second pool (and a second set of SetMaxOpenConns/lifetime settings)
// against the same instance.
func (p *PostgresStore) DB() *sql.DB { return p.db }

func (p *PostgresStore) CreateTenant(ctx context.Context, name, slug string) (Tenant, error) {
	const q = `
		INSERT INTO tenants (name, slug)
		VALUES ($1, $2)
		RETURNING id, name, slug, status, created_at`
	var t Tenant
	err := p.db.QueryRowContext(ctx, q, name, slug).Scan(&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt)
	if isUniqueViolation(err) {
		return Tenant{}, ErrDuplicateSlug
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("inserting tenant: %w", err)
	}
	return t, nil
}

func (p *PostgresStore) GetTenant(ctx context.Context, id string) (Tenant, error) {
	const q = `SELECT id, name, slug, status, created_at FROM tenants WHERE id = $1`
	var t Tenant
	err := p.db.QueryRowContext(ctx, q, id).Scan(&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, ErrNotFound
	}
	// id is a UUID column; a caller-supplied id that isn't even
	// syntactically a valid UUID (e.g. "no-such-tenant") can never match
	// a row, so Postgres's "invalid input syntax for type uuid" (22P02)
	// is semantically equivalent to not-found here, not a real query
	// error. Without this, malformed IDs surface as an opaque 500-style
	// error instead of the same ErrNotFound a well-formed-but-absent
	// UUID produces.
	var pqErr *pq.Error
	if errors.As(err, &pqErr) && pqErr.Code == "22P02" {
		return Tenant{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, fmt.Errorf("querying tenant: %w", err)
	}
	return t, nil
}

func (p *PostgresStore) CreateAPIKey(ctx context.Context, tenantID string, scopes []string) (string, APIKey, error) {
	// Verify tenant exists first so we return ErrNotFound rather than
	// a raw foreign-key-violation error.
	if _, err := p.GetTenant(ctx, tenantID); err != nil {
		return "", APIKey{}, err
	}

	plaintext, hash, prefix, err := generatePlaintextKey()
	if err != nil {
		return "", APIKey{}, err
	}

	// pq.Array(nil) serializes to SQL NULL, not an empty array — which
	// violates the scopes NOT NULL constraint even though the column has
	// a DEFAULT '{}' (an explicit NULL in the INSERT overrides the
	// default). Normalize nil to an empty, non-nil slice so nil/legacy
	// scope lists (the common unscoped-key case — see httpapi.requireAuth's
	// legacy compatibility) insert cleanly as '{}'.
	if scopes == nil {
		scopes = []string{}
	}

	const q = `
		INSERT INTO api_keys (tenant_id, key_prefix, key_hash, scopes)
		VALUES ($1, $2, $3, $4)
		RETURNING id, tenant_id, key_prefix, key_hash, scopes, created_at`
	var k APIKey
	err = p.db.QueryRowContext(ctx, q, tenantID, prefix, hash, pq.Array(scopes)).
		Scan(&k.ID, &k.TenantID, &k.Prefix, &k.Hash, pq.Array(&k.Scopes), &k.CreatedAt)
	if err != nil {
		return "", APIKey{}, fmt.Errorf("inserting api key: %w", err)
	}
	return plaintext, k, nil
}

func (p *PostgresStore) Authenticate(ctx context.Context, plaintext string) (Tenant, APIKey, error) {
	hash := hashKey(plaintext)

	const q = `
		SELECT k.id, k.tenant_id, k.key_prefix, k.key_hash, k.scopes, k.created_at, k.last_used_at, k.revoked_at,
		       t.id, t.name, t.slug, t.status, t.created_at
		FROM api_keys k
		JOIN tenants t ON t.id = k.tenant_id
		WHERE k.key_hash = $1`

	var k APIKey
	var t Tenant
	err := p.db.QueryRowContext(ctx, q, hash).Scan(
		&k.ID, &k.TenantID, &k.Prefix, &k.Hash, pq.Array(&k.Scopes), &k.CreatedAt, &k.LastUsedAt, &k.RevokedAt,
		&t.ID, &t.Name, &t.Slug, &t.Status, &t.CreatedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return Tenant{}, APIKey{}, ErrNotFound
	}
	if err != nil {
		return Tenant{}, APIKey{}, fmt.Errorf("querying api key: %w", err)
	}
	if k.RevokedAt != nil {
		return Tenant{}, APIKey{}, ErrRevoked
	}

	// Best-effort last-used update; failure here must not fail auth.
	_, _ = p.db.ExecContext(ctx, `UPDATE api_keys SET last_used_at = now() WHERE id = $1`, k.ID)

	return t, k, nil
}

func (p *PostgresStore) RevokeAPIKey(ctx context.Context, keyID string) error {
	const q = `UPDATE api_keys SET revoked_at = now() WHERE id = $1 AND revoked_at IS NULL`
	res, err := p.db.ExecContext(ctx, q, keyID)
	if err != nil {
		// Same class of issue as GetTenant: keyID that isn't even a
		// syntactically valid UUID can never match a row, so treat it
		// as not-found rather than a query error.
		var pqErr *pq.Error
		if errors.As(err, &pqErr) && pqErr.Code == "22P02" {
			return ErrNotFound
		}
		return fmt.Errorf("revoking api key: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking revoke result: %w", err)
	}
	if n == 0 {
		return ErrNotFound
	}
	return nil
}

var _ Store = (*PostgresStore)(nil)

// isUniqueViolation reports whether err is a Postgres unique-constraint
// violation (SQLSTATE 23505 — see
// https://www.postgresql.org/docs/current/errcodes-appendix.html).
func isUniqueViolation(err error) bool {
	var pqErr *pq.Error
	if errors.As(err, &pqErr) {
		return pqErr.Code == "23505"
	}
	return false
}
