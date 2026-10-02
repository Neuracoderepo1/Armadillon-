package reservation

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velocityguard/internal/money"
)

// Test 3 (spec Golden Test Cases): Concurrent overspend.
// Remaining = $10, 1000 concurrent requests each try to reserve $1.
// Expected: authorized reservations sum <= $10 (exactly 10 succeed, since
// each request is exactly $1 and budget is exactly $10).
func TestConcurrentOverspend_NeverExceedsBudget(t *testing.T) {
	m := NewManager()
	m.SetBudget("tenant-a", money.FromFloat(10.00))

	const n = 1000
	var wg sync.WaitGroup
	var granted int64
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, err := m.Reserve("tenant-a", money.FromFloat(1.00), time.Minute)
			if err == nil {
				atomic.AddInt64(&granted, 1)
			}
		}()
	}
	wg.Wait()

	if granted != 10 {
		t.Fatalf("expected exactly 10 granted reservations for $10 budget @ $1 each, got %d", granted)
	}

	exp := m.Exposure("tenant-a")
	if exp.Reserved != money.FromFloat(10.00) {
		t.Fatalf("expected reserved == $10.00 exactly, got %s", exp.Reserved)
	}
	if exp.Available != 0 {
		t.Fatalf("expected $0 available after exhausting budget, got %s", exp.Available)
	}
}

// Same test but with uneven concurrent amounts and multiple tenants, to
// catch any cross-tenant leakage in addition to the overspend invariant.
func TestConcurrentOverspend_MultiTenantIsolation(t *testing.T) {
	m := NewManager()
	m.SetBudget("tenant-a", money.FromFloat(10.00))
	m.SetBudget("tenant-b", money.FromFloat(10.00))

	const perTenant = 500
	var wg sync.WaitGroup
	grantedA := int64(0)
	grantedB := int64(0)
	wg.Add(perTenant * 2)
	for i := 0; i < perTenant; i++ {
		go func() {
			defer wg.Done()
			if _, err := m.Reserve("tenant-a", money.FromFloat(1.00), time.Minute); err == nil {
				atomic.AddInt64(&grantedA, 1)
			}
		}()
		go func() {
			defer wg.Done()
			if _, err := m.Reserve("tenant-b", money.FromFloat(1.00), time.Minute); err == nil {
				atomic.AddInt64(&grantedB, 1)
			}
		}()
	}
	wg.Wait()

	if grantedA != 10 || grantedB != 10 {
		t.Fatalf("expected 10/10 granted per tenant, got a=%d b=%d", grantedA, grantedB)
	}
}

// Golden Test Case 6: Reconciliation releases the difference when actual < reserved.
func TestReconcile_ReleasesUnderage(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(100))
	r, err := m.Reserve("t1", money.FromFloat(1.00), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	res, err := m.Reconcile(r.ID, money.FromFloat(0.70))
	if err != nil {
		t.Fatal(err)
	}
	if res.Released != money.FromFloat(0.30) {
		t.Fatalf("expected $0.30 released, got %s", res.Released)
	}
	exp := m.Exposure("t1")
	if exp.Available != money.FromFloat(99.30) {
		t.Fatalf("expected $99.30 available, got %s", exp.Available)
	}
}

// Golden Test Case 7: Overage when actual > reserved.
func TestReconcile_RecordsOverage(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(100))
	r, err := m.Reserve("t1", money.FromFloat(1.00), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	res, err := m.Reconcile(r.ID, money.FromFloat(1.40))
	if err != nil {
		t.Fatal(err)
	}
	if res.Overage != money.FromFloat(0.40) {
		t.Fatalf("expected $0.40 overage, got %s", res.Overage)
	}
	exp := m.Exposure("t1")
	if exp.Available != money.FromFloat(98.60) {
		t.Fatalf("expected $98.60 available (100 - 1.40 settled), got %s", exp.Available)
	}
}

// Golden Test Case 8: Duplicate reconciliation must be harmless (idempotent).
func TestReconcile_IdempotentUnderConcurrency(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(100))
	r, err := m.Reserve("t1", money.FromFloat(1.00), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	const n = 50
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, _ = m.Reconcile(r.ID, money.FromFloat(0.70))
		}()
	}
	wg.Wait()

	exp := m.Exposure("t1")
	// Regardless of how many times Reconcile was called concurrently, the
	// financial effect must be applied exactly once.
	if exp.Available != money.FromFloat(99.30) {
		t.Fatalf("duplicate reconciliation was not idempotent: expected $99.30 available, got %s", exp.Available)
	}
}

// Release (e.g. request failed before execution) must return budget and
// must itself be idempotent / safe against a reservation already reconciled.
func TestRelease_ReturnsbudgetAndIsIdempotent(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(10))
	r, _ := m.Reserve("t1", money.FromFloat(5), time.Minute)

	if err := m.Release(r.ID); err != nil {
		t.Fatal(err)
	}
	if err := m.Release(r.ID); err != nil {
		t.Fatalf("second release should be a harmless no-op, got err: %v", err)
	}
	exp := m.Exposure("t1")
	if exp.Available != money.FromFloat(10) {
		t.Fatalf("expected full budget back after release, got %s", exp.Available)
	}

	// Reconcile after release must not be allowed to also apply a financial effect.
	_, err := m.Reconcile(r.ID, money.FromFloat(5))
	if err != ErrTerminalState {
		t.Fatalf("expected ErrTerminalState reconciling a released reservation, got %v", err)
	}
}

// Reservation expiry: a crashed/hung request must not hold budget hostage
// forever, BUT (the critical safety fix) TTL alone must never return
// budget either — see the State doc comment in reservation.go. Expiry
// moves the reservation to UNKNOWN and the amount stays held.
func TestExpireStale_MovesToUnknown_DoesNotReturnBudget(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(10))
	fixed := time.Now()
	m.now = func() time.Time { return fixed }

	r, err := m.Reserve("t1", money.FromFloat(4), time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Advance the clock past expiry.
	m.now = func() time.Time { return fixed.Add(2 * time.Second) }
	n := m.ExpireStale()
	if n != 1 {
		t.Fatalf("expected 1 reservation to move to UNKNOWN, got %d", n)
	}

	// The $4 must still be held — NOT returned — because we don't know
	// whether the upstream call executed and was billed.
	exp := m.Exposure("t1")
	if exp.Available != money.FromFloat(6) {
		t.Fatalf("expected $4 to remain held (available=$6) after expiry to UNKNOWN, got available=%s", exp.Available)
	}
	if exp.Reserved != money.FromFloat(4) {
		t.Fatalf("expected $4 still counted as reserved, got %s", exp.Reserved)
	}

	rec, _ := m.Get(r.ID)
	if rec.State != StateUnknown {
		t.Fatalf("expected state UNKNOWN, got %s", rec.State)
	}
}

// TestResolveUnknownReleased_ReturnsBudget proves the ONLY way an
// UNKNOWN reservation's budget comes back is an explicit recovery-worker
// call confirming the request never actually executed upstream.
func TestResolveUnknownReleased_ReturnsBudget(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(10))
	fixed := time.Now()
	m.now = func() time.Time { return fixed }

	r, _ := m.Reserve("t1", money.FromFloat(4), time.Second)
	m.now = func() time.Time { return fixed.Add(2 * time.Second) }
	m.ExpireStale()

	if err := m.ResolveUnknownReleased(r.ID); err != nil {
		t.Fatalf("ResolveUnknownReleased: %v", err)
	}
	exp := m.Exposure("t1")
	if exp.Available != money.FromFloat(10) {
		t.Fatalf("expected budget fully returned after confirmed non-execution, got %s", exp.Available)
	}
	rec, _ := m.Get(r.ID)
	if rec.State != StateReleased {
		t.Fatalf("expected state RELEASED, got %s", rec.State)
	}

	// Idempotent: calling it again must not double-release or error oddly.
	if err := m.ResolveUnknownReleased(r.ID); err != nil {
		t.Fatalf("second ResolveUnknownReleased should be idempotent, got %v", err)
	}
	if exp2 := m.Exposure("t1"); exp2.Available != money.FromFloat(10) {
		t.Fatalf("expected no double-release, available=%s", exp2.Available)
	}

	// Cannot resolve a reservation that was never in UNKNOWN.
	r2, _ := m.Reserve("t1", money.FromFloat(1), time.Minute)
	if err := m.ResolveUnknownReleased(r2.ID); err != ErrTerminalState {
		t.Fatalf("expected ErrTerminalState resolving a still-RESERVED reservation, got %v", err)
	}
}

// TestResolveUnknownReconciled_SettlesActualCost proves the other
// recovery outcome: the recovery worker confirms the call DID execute
// and was billed, so the held amount settles at the real cost — exactly
// like a normal Reconcile would have, had it arrived before the TTL.
func TestResolveUnknownReconciled_SettlesActualCost(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(10))
	fixed := time.Now()
	m.now = func() time.Time { return fixed }

	r, _ := m.Reserve("t1", money.FromFloat(4), time.Second)
	m.now = func() time.Time { return fixed.Add(2 * time.Second) }
	m.ExpireStale()

	res, err := m.ResolveUnknownReconciled(r.ID, money.FromFloat(3.50))
	if err != nil {
		t.Fatalf("ResolveUnknownReconciled: %v", err)
	}
	if res.Released != money.FromFloat(0.50) {
		t.Fatalf("expected $0.50 released (reserved $4 - actual $3.50), got %s", res.Released)
	}
	exp := m.Exposure("t1")
	if exp.Settled != money.FromFloat(3.50) {
		t.Fatalf("expected $3.50 settled, got %s", exp.Settled)
	}
	if exp.Available != money.FromFloat(6.50) {
		t.Fatalf("expected $6.50 available ($10 - $3.50 settled), got %s", exp.Available)
	}
	rec, _ := m.Get(r.ID)
	if rec.State != StateReconciled {
		t.Fatalf("expected state RECONCILED, got %s", rec.State)
	}

	// Idempotent.
	res2, err := m.ResolveUnknownReconciled(r.ID, money.FromFloat(999))
	if err != nil {
		t.Fatalf("second ResolveUnknownReconciled should be idempotent, got %v", err)
	}
	if res2.Actual != money.FromFloat(3.50) {
		t.Fatalf("expected idempotent call to return the original actual cost, got %s", res2.Actual)
	}
}

// Golden Test Case 1 & 2.
func TestReserve_AllowAndBlockOnBudget(t *testing.T) {
	m := NewManager()
	m.SetBudget("t1", money.FromFloat(100))
	if _, err := m.Reserve("t1", money.FromFloat(0.10), time.Minute); err != nil {
		t.Fatalf("expected allow for $0.10 against $100 budget, got %v", err)
	}

	m2 := NewManager()
	m2.SetBudget("t2", money.FromFloat(0.05))
	if _, err := m2.Reserve("t2", money.FromFloat(0.10), time.Minute); err != ErrInsufficientBudget {
		t.Fatalf("expected ErrInsufficientBudget, got %v", err)
	}
}
