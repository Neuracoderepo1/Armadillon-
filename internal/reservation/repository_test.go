package reservation

import (
	"database/sql"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"velocityguard/internal/money"
)

// runRepositorySuite exercises the Repository contract identically against
// any implementation. It is run once against *Manager (always) and once
// against *PostgresRepository (only when VG_TEST_POSTGRES_DSN is set),
// proving the durable backend is a behaviorally-identical drop-in for the
// in-memory one, not just "implements the same method names."
func runRepositorySuite(t *testing.T, repo Repository, newTenant func(t *testing.T) string) {
	t.Helper()

	t.Run("Reserve_refuses_over_budget", func(t *testing.T) {
		tid := newTenant(t)
		repo.SetBudget(tid, money.FromFloat(10))
		if _, err := repo.Reserve(tid, money.FromFloat(11), time.Minute); err != ErrInsufficientBudget {
			t.Fatalf("expected ErrInsufficientBudget, got %v", err)
		}
		r, err := repo.Reserve(tid, money.FromFloat(10), time.Minute)
		if err != nil {
			t.Fatalf("expected the exact remaining budget to succeed: %v", err)
		}
		if r.State != StateReserved {
			t.Fatalf("expected RESERVED, got %s", r.State)
		}
		exp := repo.Exposure(tid)
		if exp.Available != 0 {
			t.Fatalf("expected $0 available after reserving the full budget, got %s", exp.Available)
		}
	})

	t.Run("Reserve_against_unconfigured_tenant_fails_closed", func(t *testing.T) {
		tid := newTenant(t) // SetBudget never called
		if _, err := repo.Reserve(tid, money.FromFloat(0.01), time.Minute); err != ErrInsufficientBudget {
			t.Fatalf("expected an unconfigured tenant to fail closed, got %v", err)
		}
	})

	t.Run("Release_is_idempotent_and_returns_budget", func(t *testing.T) {
		tid := newTenant(t)
		repo.SetBudget(tid, money.FromFloat(10))
		r, _ := repo.Reserve(tid, money.FromFloat(4), time.Minute)

		if err := repo.Release(r.ID); err != nil {
			t.Fatalf("Release: %v", err)
		}
		if exp := repo.Exposure(tid); exp.Available != money.FromFloat(10) {
			t.Fatalf("expected full budget back after release, got %s", exp.Available)
		}
		if err := repo.Release(r.ID); err != nil {
			t.Fatalf("second Release should be a no-op, not an error: %v", err)
		}
		if exp := repo.Exposure(tid); exp.Available != money.FromFloat(10) {
			t.Fatalf("expected no double-release, got %s", exp.Available)
		}
	})

	t.Run("Reconcile_idempotent_and_settles_actual_cost", func(t *testing.T) {
		tid := newTenant(t)
		repo.SetBudget(tid, money.FromFloat(10))
		r, _ := repo.Reserve(tid, money.FromFloat(4), time.Minute)

		res, err := repo.Reconcile(r.ID, money.FromFloat(3))
		if err != nil {
			t.Fatalf("Reconcile: %v", err)
		}
		if res.Released != money.FromFloat(1) {
			t.Fatalf("expected $1 released, got %s", res.Released)
		}
		exp := repo.Exposure(tid)
		if exp.Settled != money.FromFloat(3) || exp.Available != money.FromFloat(7) {
			t.Fatalf("expected settled=$3 available=$7, got settled=%s available=%s", exp.Settled, exp.Available)
		}

		res2, err := repo.Reconcile(r.ID, money.FromFloat(999))
		if err != nil {
			t.Fatalf("second Reconcile should be idempotent, got error: %v", err)
		}
		if res2.Actual != money.FromFloat(3) {
			t.Fatalf("idempotent Reconcile must return the ORIGINAL actual cost, got %s", res2.Actual)
		}
		if exp := repo.Exposure(tid); exp.Settled != money.FromFloat(3) {
			t.Fatalf("second Reconcile must not re-apply, settled should stay $3, got %s", exp.Settled)
		}
	})

	t.Run("ExpireStale_moves_to_UNKNOWN_without_returning_budget", func(t *testing.T) {
		tid := newTenant(t)
		repo.SetBudget(tid, money.FromFloat(10))
		r, err := repo.Reserve(tid, money.FromFloat(4), 10*time.Millisecond)
		if err != nil {
			t.Fatalf("Reserve: %v", err)
		}
		time.Sleep(50 * time.Millisecond)

		n := repo.ExpireStale()
		if n < 1 {
			t.Fatalf("expected at least 1 reservation to expire into UNKNOWN, got %d", n)
		}
		got, err := repo.Get(r.ID)
		if err != nil {
			t.Fatalf("Get: %v", err)
		}
		if got.State != StateUnknown {
			t.Fatalf("expected UNKNOWN, got %s", got.State)
		}
		if exp := repo.Exposure(tid); exp.Available != money.FromFloat(6) {
			t.Fatalf("THE critical invariant: budget must stay held through UNKNOWN. expected $6 available, got %s", exp.Available)
		}
	})

	t.Run("ResolveUnknownReleased_is_the_only_way_UNKNOWN_returns_budget", func(t *testing.T) {
		tid := newTenant(t)
		repo.SetBudget(tid, money.FromFloat(10))
		r, _ := repo.Reserve(tid, money.FromFloat(4), 10*time.Millisecond)
		time.Sleep(50 * time.Millisecond)
		repo.ExpireStale()

		if err := repo.ResolveUnknownReleased(r.ID); err != nil {
			t.Fatalf("ResolveUnknownReleased: %v", err)
		}
		if exp := repo.Exposure(tid); exp.Available != money.FromFloat(10) {
			t.Fatalf("expected full budget back, got %s", exp.Available)
		}
		// Idempotent.
		if err := repo.ResolveUnknownReleased(r.ID); err != nil {
			t.Fatalf("second call should be idempotent: %v", err)
		}
		// Cannot resolve a reservation that was never UNKNOWN.
		r2, _ := repo.Reserve(tid, money.FromFloat(1), time.Minute)
		if err := repo.ResolveUnknownReleased(r2.ID); err != ErrTerminalState {
			t.Fatalf("expected ErrTerminalState for a still-RESERVED reservation, got %v", err)
		}
	})

	t.Run("ResolveUnknownReconciled_settles_at_actual_cost", func(t *testing.T) {
		tid := newTenant(t)
		repo.SetBudget(tid, money.FromFloat(10))
		r, _ := repo.Reserve(tid, money.FromFloat(4), 10*time.Millisecond)
		time.Sleep(50 * time.Millisecond)
		repo.ExpireStale()

		res, err := repo.ResolveUnknownReconciled(r.ID, money.FromFloat(3.5))
		if err != nil {
			t.Fatalf("ResolveUnknownReconciled: %v", err)
		}
		if res.Released != money.FromFloat(0.5) {
			t.Fatalf("expected $0.50 released, got %s", res.Released)
		}
		if exp := repo.Exposure(tid); exp.Settled != money.FromFloat(3.5) {
			t.Fatalf("expected $3.50 settled, got %s", exp.Settled)
		}
	})

	t.Run("Get_unknown_id_returns_ErrNotFound", func(t *testing.T) {
		if _, err := repo.Get("does-not-exist"); err != ErrNotFound {
			t.Fatalf("expected ErrNotFound, got %v", err)
		}
	})

	// This is THE invariant the whole package exists to guarantee: under
	// concurrent load, granted reservations can never sum past the
	// tenant's limit. Run with -race.
	t.Run("Reserve_never_overcommits_under_concurrency", func(t *testing.T) {
		tid := newTenant(t)
		repo.SetBudget(tid, money.FromFloat(100))

		const attempts = 50
		const amount = 3.0 // 50 * $3 = $150 against a $100 budget: some MUST fail
		var wg sync.WaitGroup
		var mu sync.Mutex
		granted := 0
		for i := 0; i < attempts; i++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if _, err := repo.Reserve(tid, money.FromFloat(amount), time.Minute); err == nil {
					mu.Lock()
					granted++
					mu.Unlock()
				}
			}()
		}
		wg.Wait()

		const maxGrantable = 33 // floor(100 / 3) — budget is $100, each reservation is $3
		if granted > maxGrantable {
			t.Fatalf("OVERCOMMIT: granted %d reservations of $%.0f against a $100 budget (max possible: %d)", granted, amount, maxGrantable)
		}
		exp := repo.Exposure(tid)
		if exp.Available < 0 {
			t.Fatalf("OVERCOMMIT: available went negative: %s", exp.Available)
		}
	})
}

func TestManager_SatisfiesRepositorySuite(t *testing.T) {
	m := NewManager()
	counter := 0
	runRepositorySuite(t, m, func(t *testing.T) string {
		counter++
		return fmt.Sprintf("mem-tenant-%d", counter)
	})
}

// TestPostgresRepository_SatisfiesRepositorySuite runs the identical
// suite against a real Postgres instance. Skipped unless
// VG_TEST_POSTGRES_DSN is set.
func TestPostgresRepository_SatisfiesRepositorySuite(t *testing.T) {
	dsn := os.Getenv("VG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VG_TEST_POSTGRES_DSN not set; skipping live Postgres repository tests")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()

	// Same reasoning as store_test.go's TestPostgresStore: make this
	// idempotent against a persistent (non-CI) database rather than
	// assuming a fresh one every run.
	if _, err := db.Exec("TRUNCATE tenants CASCADE"); err != nil {
		t.Fatalf("truncating tables before test run: %v", err)
	}

	repo := NewPostgresRepository(db)
	counter := 0
	runRepositorySuite(t, repo, func(t *testing.T) string {
		counter++
		var id string
		slug := fmt.Sprintf("pg-repo-tenant-%d", counter)
		if err := db.QueryRow(
			`INSERT INTO tenants (name, slug) VALUES ($1, $2) RETURNING id`,
			slug, slug,
		).Scan(&id); err != nil {
			t.Fatalf("creating test tenant: %v", err)
		}
		return id
	})
}
