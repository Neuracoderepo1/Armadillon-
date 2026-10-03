package store

import (
	"context"
	"database/sql"
	"os"
	"testing"
)

// runStoreSuite exercises the Store contract against any implementation.
// It is called once for MemoryStore (always) and once for PostgresStore
// (only when VG_TEST_POSTGRES_DSN is set — this repo's sandbox has no
// live Postgres, so that path is honestly skipped here rather than
// faked; run it for real against docker-compose's postgres service).
func runStoreSuite(t *testing.T, s Store) {
	t.Helper()
	ctx := context.Background()

	t.Run("CreateTenant_then_GetTenant", func(t *testing.T) {
		tenant, err := s.CreateTenant(ctx, "Acme Corp", uniqueSlug(t))
		if err != nil {
			t.Fatalf("CreateTenant: %v", err)
		}
		if tenant.ID == "" {
			t.Fatal("expected non-empty tenant ID")
		}
		got, err := s.GetTenant(ctx, tenant.ID)
		if err != nil {
			t.Fatalf("GetTenant: %v", err)
		}
		if got.Name != "Acme Corp" {
			t.Fatalf("got name %q, want Acme Corp", got.Name)
		}
	})

	t.Run("CreateTenant_duplicate_slug_rejected", func(t *testing.T) {
		slug := uniqueSlug(t)
		if _, err := s.CreateTenant(ctx, "First", slug); err != nil {
			t.Fatalf("first CreateTenant: %v", err)
		}
		_, err := s.CreateTenant(ctx, "Second", slug)
		if err != ErrDuplicateSlug {
			t.Fatalf("got err %v, want ErrDuplicateSlug", err)
		}
	})

	t.Run("GetTenant_not_found", func(t *testing.T) {
		_, err := s.GetTenant(ctx, "does-not-exist")
		if err != ErrNotFound {
			t.Fatalf("got err %v, want ErrNotFound", err)
		}
	})

	t.Run("CreateAPIKey_then_Authenticate_succeeds", func(t *testing.T) {
		tenant, err := s.CreateTenant(ctx, "Key Co", uniqueSlug(t))
		if err != nil {
			t.Fatalf("CreateTenant: %v", err)
		}
		plaintext, key, err := s.CreateAPIKey(ctx, tenant.ID, []string{"proxy:write"})
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		if plaintext == "" || key.Hash == "" {
			t.Fatal("expected non-empty plaintext and hash")
		}
		if plaintext == key.Hash {
			t.Fatal("plaintext must never equal the stored hash")
		}

		gotTenant, gotKey, err := s.Authenticate(ctx, plaintext)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if gotTenant.ID != tenant.ID {
			t.Fatalf("authenticated tenant %q, want %q", gotTenant.ID, tenant.ID)
		}
		if gotKey.ID != key.ID {
			t.Fatalf("authenticated key %q, want %q", gotKey.ID, key.ID)
		}
	})

	t.Run("Authenticate_wrong_key_rejected", func(t *testing.T) {
		tenant, _ := s.CreateTenant(ctx, "Wrong Key Co", uniqueSlug(t))
		_, _, err := s.CreateAPIKey(ctx, tenant.ID, nil)
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		_, _, err = s.Authenticate(ctx, "vg_live_totally-wrong-key")
		if err != ErrNotFound {
			t.Fatalf("got err %v, want ErrNotFound", err)
		}
	})

	t.Run("RevokeAPIKey_then_Authenticate_rejected", func(t *testing.T) {
		tenant, _ := s.CreateTenant(ctx, "Revoke Co", uniqueSlug(t))
		plaintext, key, err := s.CreateAPIKey(ctx, tenant.ID, nil)
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		if err := s.RevokeAPIKey(ctx, key.ID); err != nil {
			t.Fatalf("RevokeAPIKey: %v", err)
		}
		_, _, err = s.Authenticate(ctx, plaintext)
		if err != ErrRevoked {
			t.Fatalf("got err %v, want ErrRevoked", err)
		}
	})

	t.Run("CreateAPIKey_unknown_tenant_rejected", func(t *testing.T) {
		_, _, err := s.CreateAPIKey(ctx, "no-such-tenant", nil)
		if err != ErrNotFound {
			t.Fatalf("got err %v, want ErrNotFound", err)
		}
	})

	// Regression test: against Postgres, an id that isn't even
	// syntactically a valid UUID must still come back as ErrNotFound,
	// not a raw driver/query error — a malformed id can never match a
	// row either way, so from the caller's perspective it's simply
	// not-found. (MemoryStore has no UUID typing to trip on this, but
	// runs the same assertion for parity.)
	t.Run("RevokeAPIKey_malformed_id_rejected", func(t *testing.T) {
		err := s.RevokeAPIKey(ctx, "not-a-valid-uuid")
		if err != ErrNotFound {
			t.Fatalf("got err %v, want ErrNotFound", err)
		}
	})

	t.Run("TenantIsolation_key_from_one_tenant_authenticates_only_that_tenant", func(t *testing.T) {
		tenantA, _ := s.CreateTenant(ctx, "Tenant A", uniqueSlug(t))
		tenantB, _ := s.CreateTenant(ctx, "Tenant B", uniqueSlug(t))
		plaintextA, _, err := s.CreateAPIKey(ctx, tenantA.ID, nil)
		if err != nil {
			t.Fatalf("CreateAPIKey: %v", err)
		}
		gotTenant, _, err := s.Authenticate(ctx, plaintextA)
		if err != nil {
			t.Fatalf("Authenticate: %v", err)
		}
		if gotTenant.ID == tenantB.ID {
			t.Fatal("tenant A's key must never authenticate as tenant B")
		}
		if gotTenant.ID != tenantA.ID {
			t.Fatalf("got tenant %q, want %q", gotTenant.ID, tenantA.ID)
		}
	})
}

var slugCounter int

func uniqueSlug(t *testing.T) string {
	t.Helper()
	slugCounter++
	return t.Name() + "-" + string(rune('a'+slugCounter%26))
}

func TestMemoryStore(t *testing.T) {
	runStoreSuite(t, NewMemoryStore())
}

// TestPostgresStore runs the identical suite against a real Postgres
// instance. It is SKIPPED unless VG_TEST_POSTGRES_DSN is set.
//
// This has now actually been run against a real local Postgres 16
// instance (not just CI's fresh-per-run container) — doing so surfaced
// a real bug: uniqueSlug() builds slugs from t.Name() plus an
// in-process counter, which is deterministic across separate `go test`
// invocations. Against CI's disposable per-run Postgres container that
// never mattered, but against any persistent Postgres (e.g. a
// developer's local instance run twice in a row) the second run hit
// unique-slug violations from the first run's leftover rows, so the
// suite silently only ever worked once per database lifetime. Fixed by
// truncating the tables this suite touches before each Postgres run, so
// it's idempotent regardless of what's already in the database.
//
//	Run it for real via: docker compose up -d postgres && \
//	  VG_TEST_POSTGRES_DSN="postgres://vg:vg@localhost:5432/vg?sslmode=disable" go test ./internal/store/...
func TestPostgresStore(t *testing.T) {
	dsn := os.Getenv("VG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VG_TEST_POSTGRES_DSN not set; skipping live Postgres store tests")
	}
	ctx := context.Background()

	cleanupDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("opening cleanup connection: %v", err)
	}
	defer cleanupDB.Close()
	// CASCADE reaches api_keys, budget_accounts, and reservations (all
	// FK-reference tenants), so this alone resets everything this test
	// file can have written.
	if _, err := cleanupDB.ExecContext(ctx, "TRUNCATE tenants CASCADE"); err != nil {
		t.Fatalf("truncating tables before test run: %v", err)
	}

	ps, err := OpenPostgresStore(ctx, dsn)
	if err != nil {
		t.Fatalf("OpenPostgresStore: %v", err)
	}
	defer ps.Close()
	runStoreSuite(t, ps)
}
