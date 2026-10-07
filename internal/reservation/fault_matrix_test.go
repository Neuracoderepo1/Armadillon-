package reservation_test

// Fault-injection matrix for the money path (real PostgreSQL).
//
// Invariants asserted after every scenario:
//
//	I1  conservation: reserved + settled + available == limit (no budget appears or vanishes)
//	I3  a hold is never released when dispatch may have occurred
//	I4  recovery / expiry / resolution are idempotent
//
// "Crash" scenarios leave the durable rows exactly as a killed process would,
// then drive the same recovery entry points the maintenance worker uses.
//
//	C1  proven non-dispatch              -> RELEASED, budget restored
//	C2  died after reserve / before send -> stays RESERVED, TTL -> UNKNOWN (never RELEASED)
//	C3  post-send reset / timeout        -> UNKNOWN, hold kept
//	C4  provider billed, nothing durable -> same durable shape as C2: held, never released
//	C5  pending actual recorded, died    -> recovered exactly once, idempotent
//	C6  expired + pending actual         -> recovery settles it; expiry must not shadow it
//	R1  UNKNOWN backlog is listable, resolvable, and idempotent
//	R2  racing resolvers                 -> exactly one terminal outcome

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"testing"
	"time"

	"velocityguard/internal/money"
	"velocityguard/internal/provider"
	"velocityguard/internal/reservation"
	"velocityguard/internal/risk"
)

func assertConserved(t *testing.T, e reservation.Exposure) {
	t.Helper()
	if e.Reserved+e.Settled+e.Available != e.Limit {
		t.Fatalf("I1 violated: reserved %v + settled %v + available %v != limit %v", e.Reserved, e.Settled, e.Available, e.Limit)
	}
}

func assertState(t *testing.T, m *reservation.PGManager, id string, want reservation.State) {
	t.Helper()
	r, err := m.Get(id)
	if err != nil {
		t.Fatal(err)
	}
	if r.State != want {
		t.Fatalf("reservation %s: state %s, want %s", id, r.State, want)
	}
}

// forceExpiry makes a reservation's TTL elapse without sleeping.
func forceExpiry(t *testing.T, db *sql.DB, id string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE reservations SET expires_at = now() - interval '1 minute' WHERE id=$1`, id); err != nil {
		t.Fatal(err)
	}
}

func ambiguous(*testing.T) func(ctx context.Context) (provider.Response, provider.Usage, error) {
	return func(context.Context) (provider.Response, provider.Usage, error) {
		return provider.Response{}, provider.Usage{}, errors.New("read tcp: connection reset by peer")
	}
}

// onlyReservation returns the single reservation a tenant has (the gateway
// creates exactly one per request).
func onlyReservation(t *testing.T, db *sql.DB, tenant string) string {
	t.Helper()
	var id string
	if err := db.QueryRow(`SELECT id::text FROM reservations WHERE tenant_id=$1`, tenant).Scan(&id); err != nil {
		t.Fatal(err)
	}
	return id
}

// C1: a request proven never to have left is released and the budget is whole.
func TestFault_C1_ProvenNonDispatch_Releases(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	g := newGateway(m, fnProvider(func(context.Context) (provider.Response, provider.Usage, error) {
		return provider.Response{}, provider.Usage{}, provider.NotDispatched(errors.New("dial tcp: connection refused"))
	}), risk.DefaultPolicy())

	if res := handle(g, "c1", tn); res.Err == nil {
		t.Fatal("expected an error")
	}
	assertState(t, m, onlyReservation(t, db, tn), reservation.StateReleased)
	e := exposure(t, m, tn)
	if e.Reserved != 0 || e.Settled != 0 || e.Available != e.Limit {
		t.Fatalf("budget not restored: %+v", e)
	}
	assertConserved(t, e)
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}

// C3: a post-send reset proves nothing. The hold is kept, release is refused (I3).
func TestFault_C3_PostSendReset_HoldKept_NeverReleased(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	g := newGateway(m, fnProvider(ambiguous(t)), risk.DefaultPolicy())

	if res := handle(g, "c3", tn); res.Err == nil {
		t.Fatal("expected an error")
	}
	id := onlyReservation(t, db, tn)
	assertState(t, m, id, reservation.StateUnknown)
	e := exposure(t, m, tn)
	if e.Reserved == 0 || e.Available >= e.Limit {
		t.Fatalf("hold was returned on an ambiguous failure: %+v", e)
	}
	assertConserved(t, e)
	if err := m.Release(id); !errors.Is(err, reservation.ErrTerminalState) {
		t.Fatalf("I3: Release on UNKNOWN must be refused, got %v", err)
	}
	if after := exposure(t, m, tn); after != e {
		t.Fatalf("refused release changed exposure: %+v -> %+v", e, after)
	}
}

// C2/C4: the process died after reserving (possibly after the provider billed).
// Nothing durable says "billed", so the row stays RESERVED; expiry must move it
// to UNKNOWN and keep the hold, twice over without drift (I4).
func TestFault_C2C4_CrashWhileReserved_ExpiryGoesUnknown_NeverReleased(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	r, err := m.Reserve(tn, money.FromFloat(1), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	before := exposure(t, m, tn)
	if before.Reserved != money.FromFloat(1) {
		t.Fatalf("setup: %+v", before)
	}

	forceExpiry(t, db, r.ID)
	for i := 0; i < 2; i++ { // I4: a second pass is a no-op
		if _, err := m.ExpireStaleE(); err != nil {
			t.Fatal(err)
		}
		assertState(t, m, r.ID, reservation.StateUnknown)
		if got := exposure(t, m, tn); got != before {
			t.Fatalf("pass %d: expiry changed the hold: %+v -> %+v", i, before, got)
		}
		assertConserved(t, exposure(t, m, tn))
	}
	if err := m.Release(r.ID); !errors.Is(err, reservation.ErrTerminalState) {
		t.Fatalf("I3: expired hold must not be releasable by Release, got %v", err)
	}
}

// C5: pending actual durable, process died before settlement. Recovery settles
// it exactly once; running recovery again changes nothing (I4).
func TestFault_C5_PendingActual_RecoveredOnce_Idempotent(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	r, err := m.Reserve(tn, money.FromFloat(1), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	actual := money.FromFloat(0.4)
	if err := m.RecordPendingActual(r.ID, actual); err != nil {
		t.Fatal(err)
	}

	if _, err := m.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	assertState(t, m, r.ID, reservation.StateReconciled)
	first := exposure(t, m, tn)
	if first.Settled != actual || first.Reserved != 0 {
		t.Fatalf("settlement wrong: %+v", first)
	}
	if _, err := m.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	if second := exposure(t, m, tn); second != first {
		t.Fatalf("I4: second recovery drifted: %+v -> %+v", first, second)
	}
	assertConserved(t, first)
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}

// C6: an expired reservation that already holds a pending actual must be settled
// by recovery with the REAL cost, never swallowed into an estimate-sized UNKNOWN.
func TestFault_C6_ExpiredWithPendingActual_SettledNotShadowed(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	r, err := m.Reserve(tn, money.FromFloat(1), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	actual := money.FromFloat(0.7)
	if err := m.RecordPendingActual(r.ID, actual); err != nil {
		t.Fatal(err)
	}
	forceExpiry(t, db, r.ID)

	if _, err := m.ExpireStaleE(); err != nil {
		t.Fatal(err)
	}
	assertState(t, m, r.ID, reservation.StateReserved) // expiry leaves it for recovery
	if err := m.Release(r.ID); !errors.Is(err, reservation.ErrPendingActualExists) {
		t.Fatalf("I3: a billed call must not be releasable, got %v", err)
	}
	if _, err := m.RecoverPending(); err != nil {
		t.Fatal(err)
	}
	assertState(t, m, r.ID, reservation.StateReconciled)
	e := exposure(t, m, tn)
	if e.Settled != actual {
		t.Fatalf("settled %v, want real cost %v", e.Settled, actual)
	}
	assertConserved(t, e)
}

// R1: the UNKNOWN backlog is visible, resolvable by an operator, and every
// resolution is idempotent or cleanly refused.
func TestFault_R1_UnknownBacklog_ListAndResolve(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	rel, err := m.Reserve(tn, money.FromFloat(2), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	rec, err := m.Reserve(tn, money.FromFloat(3), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{rel.ID, rec.ID} {
		if err := m.MarkUnknown(id); err != nil {
			t.Fatal(err)
		}
	}

	rows, sum, err := m.ListUnknown(tn, 50)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || sum.Count != 2 || sum.Held != money.FromFloat(5) || sum.OldestAge < 0 {
		t.Fatalf("backlog wrong: rows=%d %+v", len(rows), sum)
	}
	if other, _, _ := m.ListUnknown("00000000-0000-0000-0000-000000000000", 50); len(other) != 0 {
		t.Fatalf("tenant filter leaked %d rows", len(other))
	}

	// Provider confirms the first call never billed: budget returns.
	if err := m.ResolveUnknownReleased(rel.ID); err != nil {
		t.Fatal(err)
	}
	if err := m.ResolveUnknownReleased(rel.ID); err != nil { // idempotent
		t.Fatalf("repeat release: %v", err)
	}
	// Provider invoice shows the second call billed less than the hold.
	if _, err := m.ResolveUnknownReconciled(rec.ID, money.FromFloat(1.25)); err != nil {
		t.Fatal(err)
	}
	// A resolved reservation cannot be resolved the other way.
	if _, err := m.ResolveUnknownReconciled(rel.ID, money.FromFloat(1)); !errors.Is(err, reservation.ErrTerminalState) {
		t.Fatalf("reconcile after release: %v", err)
	}
	if err := m.ResolveUnknownReleased(rec.ID); !errors.Is(err, reservation.ErrTerminalState) {
		t.Fatalf("release after reconcile: %v", err)
	}

	e := exposure(t, m, tn)
	if e.Reserved != 0 || e.Settled != money.FromFloat(1.25) {
		t.Fatalf("final exposure %+v", e)
	}
	assertConserved(t, e)
	if rows, sum, _ := m.ListUnknown(tn, 50); len(rows) != 0 || sum.Count != 0 {
		t.Fatalf("backlog not drained: %d %+v", len(rows), sum)
	}
}

// R2: two operators resolve the same UNKNOWN at once, one saying "released" and
// one "reconciled". Exactly one wins; the budget is never counted twice.
func TestFault_R2_RacingResolvers_ExactlyOneOutcome(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	for round := 0; round < 20; round++ {
		tn := tenantWithBudget(t, m, db, money.FromFloat(10))
		r, err := m.Reserve(tn, money.FromFloat(1), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		if err := m.MarkUnknown(r.ID); err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		errs := make([]error, 2)
		start := make(chan struct{})
		wg.Add(2)
		go func() { defer wg.Done(); <-start; errs[0] = m.ResolveUnknownReleased(r.ID) }()
		go func() {
			defer wg.Done()
			<-start
			_, errs[1] = m.ResolveUnknownReconciled(r.ID, money.FromFloat(0.5))
		}()
		close(start)
		wg.Wait()

		wins := 0
		for _, e := range errs {
			switch {
			case e == nil:
				wins++
			case errors.Is(e, reservation.ErrTerminalState):
			default:
				t.Fatalf("round %d: unexpected error %v", round, e)
			}
		}
		if wins != 1 {
			t.Fatalf("round %d: %d resolvers won (errs=%v), want exactly 1", round, wins, errs)
		}
		e := exposure(t, m, tn)
		if e.Reserved != 0 {
			t.Fatalf("round %d: hold left over %+v", round, e)
		}
		if e.Settled != 0 && e.Settled != money.FromFloat(0.5) {
			t.Fatalf("round %d: settled %v", round, e.Settled)
		}
		assertConserved(t, e)
	}
}

// C7: the database is unreachable when a request arrives. The gateway must fail
// closed: no reservation, and above all NO call to the provider.
func TestFault_C7_DatabaseDownAtRequestStart_NoDispatch(t *testing.T) {
	dsn := e2eDSN(t)
	m, db := openPG(t, dsn)
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))

	var calls int
	var mu sync.Mutex
	g := newGateway(m, fnProvider(func(ctx context.Context) (provider.Response, provider.Usage, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return okProv(ctx)
	}), risk.DefaultPolicy())

	// A second pool: the "outage" is the gateway's own pool going away.
	dead, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	deadM := reservation.NewPGManager(dead, quiet())
	deadM.SetOpTimeout(2 * time.Second)
	_ = dead.Close() // every operation on deadM now fails
	gDead := newGateway(deadM, fnProvider(func(ctx context.Context) (provider.Response, provider.Usage, error) {
		mu.Lock()
		calls++
		mu.Unlock()
		return okProv(ctx)
	}), risk.DefaultPolicy())

	res := handle(gDead, "c7", tn)
	if res.Executed {
		t.Fatal("request executed with the ledger down")
	}
	mu.Lock()
	n := calls
	mu.Unlock()
	if n != 0 {
		t.Fatalf("provider was called %d time(s) while the ledger was unavailable", n)
	}
	var held int
	if err := db.QueryRow(`SELECT count(*) FROM reservations WHERE tenant_id=$1`, tn).Scan(&held); err != nil || held != 0 {
		t.Fatalf("reservations created during outage: %d (%v)", held, err)
	}
	// The healthy gateway still works afterwards (no poisoned state).
	if r := handle(g, "c7-after", tn); r.Err != nil || !r.Executed {
		t.Fatalf("recovery after outage failed: %+v", r)
	}
	assertConserved(t, exposure(t, m, tn))
}

// failingSettle simulates the database failing after the provider has already
// answered: neither the pending actual nor the settlement can be written.
type failingSettle struct{ reservation.Service }

func (failingSettle) RecordPendingActual(string, money.Micros) error {
	return reservation.ErrBackendUnavailable
}
func (failingSettle) Reconcile(string, money.Micros) (reservation.ReconcileResult, error) {
	return reservation.ReconcileResult{}, reservation.ErrBackendUnavailable
}
func (failingSettle) ResolveUnknownReconciled(string, money.Micros) (reservation.ReconcileResult, error) {
	return reservation.ReconcileResult{}, reservation.ErrBackendUnavailable
}

// C8: provider billed, then BOTH durable writes fail. The hold must survive
// (never released), expiry must keep it as UNKNOWN, and an operator can then
// settle the real cost. This is the worst-case path end to end.
func TestFault_C8_ProviderBilled_DurableWritesFail_HoldSurvivesToOperator(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	g := newGateway(failingSettle{m}, fnProvider(okProv), risk.DefaultPolicy())

	res := handle(g, "c8", tn)
	if !res.Executed || res.Err == nil {
		t.Fatalf("expected executed-with-settlement-error, got %+v", res)
	}
	id := onlyReservation(t, db, tn)
	assertState(t, m, id, reservation.StateReserved) // not released, not lost
	held := exposure(t, m, tn)
	if held.Reserved == 0 {
		t.Fatalf("hold was returned although the provider billed: %+v", held)
	}
	assertConserved(t, held)
	// TTL passes with nobody having recorded the result: the hold becomes UNKNOWN.
	forceExpiry(t, db, id)
	if _, err := m.ExpireStaleE(); err != nil {
		t.Fatal(err)
	}
	assertState(t, m, id, reservation.StateUnknown)
	if got := exposure(t, m, tn); got.Reserved != held.Reserved {
		t.Fatalf("expiry changed the hold: %+v -> %+v", held, got)
	}
	// The operator, holding the provider's invoice, settles the real cost.
	if _, err := m.ResolveUnknownReconciled(id, res.ActualCost); err != nil {
		t.Fatal(err)
	}
	final := exposure(t, m, tn)
	if final.Reserved != 0 || final.Settled != res.ActualCost {
		t.Fatalf("final exposure %+v, want settled %v", final, res.ActualCost)
	}
	assertConserved(t, final)
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}
