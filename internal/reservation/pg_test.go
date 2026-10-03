package reservation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"math/rand"
	"net"
	"net/url"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lib/pq"

	"velocityguard/internal/money"
)

// ---------------------------------------------------------------------------
// Pure unit tests: exposure arithmetic (no database required).
// ---------------------------------------------------------------------------

func TestAvail_FailsClosedAndNeverOverflows(t *testing.T) {
	const max = math.MaxInt64
	cases := []struct {
		name                   string
		limit, reserved, settl int64
		want                   money.Micros
	}{
		{"all zero", 0, 0, 0, 0},
		{"normal", 100, 30, 20, 50},
		{"nothing used", 100, 0, 0, 100},
		{"reserved == limit", 100, 100, 0, 0},
		{"reserved > limit", 100, 101, 0, 0},
		{"settled == remaining", 100, 40, 60, 0},
		{"settled > remaining", 100, 40, 61, 0},
		{"settled alone > limit", 100, 0, 101, 0},
		{"negative limit", -1, 0, 0, 0},
		{"negative reserved", 100, -1, 0, 0},
		{"negative settled", 100, 0, -1, 0},
		{"negative reserved cannot inflate availability", 100, -50, 0, 0},
		{"all negative", -5, -5, -5, 0},
		{"max limit untouched", max, 0, 0, money.Micros(max)},
		{"max limit with usage", max, 1, 1, money.Micros(max - 2)},
		{"max reserved == max limit", max, max, 0, 0},
		{"max settled == max limit", max, 0, max, 0},
		{"max reserved and max settled", max, max, max, 0},
		{"min int64 reserved", 100, math.MinInt64, 0, 0},
		{"min int64 limit", math.MinInt64, 0, 0, 0},
		{"min int64 settled", 100, 0, math.MinInt64, 0},
		{"limit max, reserved min (overflow bait)", max, math.MinInt64, 0, 0},
	}
	for _, c := range cases {
		if got := avail(c.limit, c.reserved, c.settl); got != c.want {
			t.Errorf("%s: avail(%d,%d,%d)=%d want %d", c.name, c.limit, c.reserved, c.settl, got, c.want)
		}
	}
}

func TestCanCover(t *testing.T) {
	const max = math.MaxInt64
	cases := []struct {
		name                   string
		limit, reserved, settl int64
		amount                 money.Micros
		want                   bool
	}{
		{"fits exactly", 100, 30, 20, 50, true},
		{"one over", 100, 30, 20, 51, false},
		{"zero amount on healthy account", 100, 100, 0, 0, true},
		{"zero amount on corrupt account", 100, 101, 0, 0, false},
		{"negative amount", 100, 0, 0, -1, false},
		{"corrupt negative reserved", 100, -1, 0, 1, false},
		{"settled > remaining covers nothing", 100, 40, 61, 1, false},
		{"max amount on max limit", max, 0, 0, money.Micros(max), true},
		{"max amount with one used", max, 1, 0, money.Micros(max), false},
		{"overflow bait", max, math.MinInt64, 0, 1, false},
	}
	for _, c := range cases {
		if got := canCover(c.limit, c.reserved, c.settl, c.amount); got != c.want {
			t.Errorf("%s: canCover=%v want %v", c.name, got, c.want)
		}
	}
}

// ---------------------------------------------------------------------------
// Integration-test plumbing. All DB tests skip unless VG_TEST_POSTGRES_DSN is
// set, and expect migrations/*.sql to have been applied (CI does this).
// ---------------------------------------------------------------------------

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func testDSN(t *testing.T) string {
	t.Helper()
	dsn := os.Getenv("VG_TEST_POSTGRES_DSN")
	if dsn == "" {
		t.Skip("VG_TEST_POSTGRES_DSN not set; skipping live Postgres reservation tests")
	}
	return dsn
}

func openDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	db.SetMaxOpenConns(40)
	t.Cleanup(func() { _ = db.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		t.Fatalf("ping: %v", err)
	}
	return db
}

func newMgr(t *testing.T, dsn string) (*PGManager, *sql.DB) {
	t.Helper()
	db := openDB(t, dsn)
	m := NewPGManager(db, quietLog())
	m.SetOpTimeout(20 * time.Second)
	return m, db
}

var tenantSeq atomic.Int64

// newTenant inserts a tenant and removes it (cascading to its budgets,
// reservations and events) when the test ends.
func newTenant(t *testing.T, db *sql.DB) string {
	t.Helper()
	slug := fmt.Sprintf("rt-%d-%d", time.Now().UnixNano(), tenantSeq.Add(1))
	var id string
	if err := db.QueryRow(`INSERT INTO tenants (name, slug) VALUES ($1, $1) RETURNING id::text`, slug).Scan(&id); err != nil {
		t.Fatalf("create tenant: %v", err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, id) })
	return id
}

func newBudgetTenant(t *testing.T, m *PGManager, db *sql.DB, limit money.Micros) string {
	t.Helper()
	id := newTenant(t, db)
	if err := m.SetBudgetE(id, limit); err != nil {
		t.Fatalf("SetBudgetE: %v", err)
	}
	return id
}

func mustReserve(t *testing.T, m *PGManager, tenant string, amount money.Micros, ttl time.Duration) *Reservation {
	t.Helper()
	r, err := m.Reserve(tenant, amount, ttl)
	if err != nil {
		t.Fatalf("Reserve(%d): %v", amount, err)
	}
	return r
}

func expo(t *testing.T, m *PGManager, tenant string) Exposure {
	t.Helper()
	e, err := m.ExposureE(tenant)
	if err != nil {
		t.Fatalf("ExposureE: %v", err)
	}
	return e
}

func wantExposure(t *testing.T, m *PGManager, tenant string, reserved, settled money.Micros) {
	t.Helper()
	e := expo(t, m, tenant)
	if e.Reserved != reserved || e.Settled != settled {
		t.Fatalf("exposure reserved=%d settled=%d, want reserved=%d settled=%d", e.Reserved, e.Settled, reserved, settled)
	}
	if want := avail(int64(e.Limit), int64(reserved), int64(settled)); e.Available != want {
		t.Fatalf("available=%d want %d", e.Available, want)
	}
}

func wantState(t *testing.T, m *PGManager, id string, want State) *Reservation {
	t.Helper()
	r, err := m.Get(id)
	if err != nil {
		t.Fatalf("Get(%s): %v", id, err)
	}
	if r.State != want {
		t.Fatalf("reservation %s state=%s want %s", id, r.State, want)
	}
	return r
}

func countEvents(t *testing.T, db *sql.DB, id, toState string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM reservation_events WHERE reservation_id=$1 AND to_state=$2 AND from_state IS DISTINCT FROM to_state`, id, toState).Scan(&n); err != nil {
		t.Fatalf("count events: %v", err)
	}
	return n
}

func wantErr(t *testing.T, what string, got, want error) {
	t.Helper()
	if want == nil {
		if got != nil {
			t.Fatalf("%s: unexpected error: %v", what, got)
		}
		return
	}
	if !errors.Is(got, want) {
		t.Fatalf("%s: got %v, want %v", what, got, want)
	}
}

// auditScoped runs the full auditor and returns only violations that mention
// one of the needles (tenant or reservation ids), so a test is judged on its
// own data while the auditor itself still runs over the whole database.
func auditScoped(t *testing.T, db *sql.DB, needles ...string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	all, err := AuditPG(ctx, db)
	if err != nil {
		t.Fatalf("AuditPG failed to run: %v", err)
	}
	var out []string
	for _, v := range all {
		for _, n := range needles {
			if strings.Contains(v, n) {
				out = append(out, v)
				break
			}
		}
	}
	return out
}

func requireClean(t *testing.T, db *sql.DB, needles ...string) {
	t.Helper()
	if v := auditScoped(t, db, needles...); len(v) != 0 {
		t.Fatalf("AuditPG reported %d violation(s): %v", len(v), v)
	}
}

func execOrFatal(t *testing.T, db *sql.DB, q string, args ...any) {
	t.Helper()
	if _, err := db.Exec(q, args...); err != nil {
		t.Fatalf("exec %q: %v", q, err)
	}
}

func expectPQ(t *testing.T, what string, err error, code string) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s: expected SQLSTATE %s, got success (constraint missing)", what, code)
	}
	var pe *pq.Error
	if !errors.As(err, &pe) || string(pe.Code) != code {
		t.Fatalf("%s: expected SQLSTATE %s, got %v", what, code, err)
	}
}

const unit = money.Micros(1_000_000)

func usd(n int64) money.Micros { return money.Micros(n) * unit }

func shortTTLExpire(t *testing.T, m *PGManager) {
	t.Helper()
	time.Sleep(40 * time.Millisecond)
	if _, err := m.ExpireStaleE(); err != nil {
		t.Fatalf("ExpireStaleE: %v", err)
	}
}

// ---------------------------------------------------------------------------
// SetBudget semantics
// ---------------------------------------------------------------------------

func TestPG_SetBudget_RefusesToGoBelowExposure(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)

	tn := newBudgetTenant(t, m, db, usd(100))
	if err := m.SetBudgetE(tn, usd(150)); err != nil {
		t.Fatalf("increase: %v", err)
	}
	if err := m.SetBudgetE(tn, usd(150)); err != nil {
		t.Fatalf("same budget: %v", err)
	}
	if err := m.SetBudgetE(tn, usd(120)); err != nil {
		t.Fatalf("lower safely: %v", err)
	}
	if e := expo(t, m, tn); e.Limit != usd(120) {
		t.Fatalf("limit=%d want %d", e.Limit, usd(120))
	}

	// Below reserved.
	r := mustReserve(t, m, tn, usd(40), time.Hour)
	wantErr(t, "below reserved", m.SetBudgetE(tn, usd(39)), ErrBudgetBelowExposure)
	if err := m.SetBudgetE(tn, usd(40)); err != nil {
		t.Fatalf("limit == reserved must be allowed: %v", err)
	}
	if e := expo(t, m, tn); e.Limit != usd(40) || e.Reserved != usd(40) {
		t.Fatalf("rejected/accepted update changed the wrong thing: %+v", e)
	}

	// Below settled (+ reserved): reconcile 30 -> settled 30, then reserve nothing.
	if _, err := m.Reconcile(r.ID, usd(30)); err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	wantErr(t, "below settled", m.SetBudgetE(tn, usd(29)), ErrBudgetBelowExposure)
	if err := m.SetBudgetE(tn, usd(30)); err != nil {
		t.Fatalf("limit == settled must be allowed: %v", err)
	}
	wantExposure(t, m, tn, 0, usd(30))

	// Input validation.
	wantErr(t, "negative limit", m.SetBudgetE(tn, -1), ErrInvalidAmount)
	wantErr(t, "empty tenant", m.SetBudgetE("", usd(1)), ErrInvalidTenant)
	wantErr(t, "malformed tenant", m.SetBudgetE("not-a-uuid", usd(1)), ErrInvalidTenant)
	wantErr(t, "unknown tenant", m.SetBudgetE("00000000-0000-4000-8000-000000000000", usd(1)), ErrInvalidTenant)
	requireClean(t, db, tn)
}

func TestPG_SetBudget_ConcurrentWithReserve(t *testing.T) {
	dsn := testDSN(t)
	mA, db := newMgr(t, dsn)
	mB, _ := newMgr(t, dsn)
	tn := newBudgetTenant(t, mA, db, money.Micros(1000))

	var wg sync.WaitGroup
	var bad atomic.Int64
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			m := mA
			if i%2 == 1 {
				m = mB
			}
			if _, err := m.Reserve(tn, 10, time.Hour); err != nil && !errors.Is(err, ErrInsufficientBudget) {
				bad.Add(1)
				t.Errorf("reserve: %v", err)
			}
		}(i)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for limit := int64(950); limit >= 50; limit -= 25 {
			if err := mB.SetBudgetE(tn, money.Micros(limit)); err != nil && !errors.Is(err, ErrBudgetBelowExposure) {
				bad.Add(1)
				t.Errorf("setbudget: %v", err)
			}
		}
	}()
	wg.Wait()
	if bad.Load() != 0 {
		t.FailNow()
	}
	e := expo(t, mA, tn)
	if e.Reserved+e.Settled > e.Limit {
		t.Fatalf("limit %d fell below exposure %d", e.Limit, e.Reserved+e.Settled)
	}
	requireClean(t, db, tn)
}

// ---------------------------------------------------------------------------
// Expiration semantics (UNKNOWN model: expiry never returns budget)
// ---------------------------------------------------------------------------

func TestPG_ExpireStale_Semantics(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)
	tn := newBudgetTenant(t, m, db, usd(100))

	plain := mustReserve(t, m, tn, usd(10), time.Millisecond)
	withPending := mustReserve(t, m, tn, usd(20), time.Millisecond)
	late := mustReserve(t, m, tn, usd(30), time.Millisecond)
	if err := m.RecordPendingActual(withPending.ID, usd(15)); err != nil {
		t.Fatal(err)
	}
	shortTTLExpire(t, m)

	// 1. RESERVED, no pending actual -> UNKNOWN, and the hold is NOT returned.
	wantState(t, m, plain.ID, StateUnknown)
	wantState(t, m, late.ID, StateUnknown)
	if e := expo(t, m, tn); e.Reserved != usd(60) {
		t.Fatalf("expiry changed reserved to %d, want %d (budget must not be returned)", e.Reserved, usd(60))
	}
	if n := countEvents(t, db, plain.ID, "UNKNOWN"); n != 1 {
		t.Fatalf("expiry events=%d want 1", n)
	}
	// 2. RESERVED with a pending actual -> stays RESERVED.
	wantState(t, m, withPending.ID, StateReserved)

	// Expiry is idempotent.
	if n, err := m.ExpireStaleE(); err != nil || n != 0 {
		t.Fatalf("second ExpireStale n=%d err=%v", n, err)
	}
	if n := countEvents(t, db, plain.ID, "UNKNOWN"); n != 1 {
		t.Fatalf("duplicate expiry event: %d", n)
	}

	// 3. UNKNOWN + pending actual is recoverable (late reconciliation).
	if err := m.RecordPendingActual(late.ID, usd(25)); err != nil {
		t.Fatal(err)
	}
	// 4. Recovery moves both pending reservations into settled exposure.
	n, err := m.RecoverPending()
	if err != nil || n != 2 {
		t.Fatalf("RecoverPending n=%d err=%v, want 2", n, err)
	}
	r1 := wantState(t, m, withPending.ID, StateReconciled)
	r2 := wantState(t, m, late.ID, StateReconciled)
	if r1.ActualCost != usd(15) || r2.ActualCost != usd(25) {
		t.Fatalf("actuals %d/%d", r1.ActualCost, r2.ActualCost)
	}
	wantExposure(t, m, tn, usd(10), usd(40)) // `plain` is still held as UNKNOWN
	// 5. Repeated recovery is idempotent.
	if n, err := m.RecoverPending(); err != nil || n != 0 {
		t.Fatalf("repeat RecoverPending n=%d err=%v", n, err)
	}
	wantExposure(t, m, tn, usd(10), usd(40))
	for _, id := range []string{withPending.ID, late.ID} {
		if c := countEvents(t, db, id, "RECONCILED"); c != 1 {
			t.Fatalf("reservation %s has %d reconcile events", id, c)
		}
	}
	requireClean(t, db, tn)
}

// ---------------------------------------------------------------------------
// Reconciliation overflow and value edge cases
// ---------------------------------------------------------------------------

func TestPG_Reconcile_AmountEdges(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)

	t.Run("zero and exact and overage", func(t *testing.T) {
		tn := newBudgetTenant(t, m, db, usd(100))
		z := mustReserve(t, m, tn, usd(10), time.Hour)
		if _, err := m.Reconcile(z.ID, 0); err != nil {
			t.Fatal(err)
		}
		wantExposure(t, m, tn, 0, 0)
		e := mustReserve(t, m, tn, usd(10), time.Hour)
		res, err := m.Reconcile(e.ID, usd(10))
		if err != nil || res.Released != 0 || res.Overage != 0 {
			t.Fatalf("exact: %+v %v", res, err)
		}
		wantExposure(t, m, tn, 0, usd(10))
		o := mustReserve(t, m, tn, usd(10), time.Hour)
		res, err = m.Reconcile(o.ID, usd(14))
		if err != nil || res.Overage != usd(4) {
			t.Fatalf("overage: %+v %v", res, err)
		}
		wantExposure(t, m, tn, 0, usd(24))
		requireClean(t, db, tn)
	})

	t.Run("negative actual rejected", func(t *testing.T) {
		tn := newBudgetTenant(t, m, db, usd(100))
		r := mustReserve(t, m, tn, usd(10), time.Hour)
		_, err := m.Reconcile(r.ID, -1)
		wantErr(t, "negative", err, ErrInvalidAmount)
		wantErr(t, "negative pending", m.RecordPendingActual(r.ID, -1), ErrInvalidAmount)
		wantState(t, m, r.ID, StateReserved)
		wantExposure(t, m, tn, usd(10), 0)
	})

	t.Run("extremely large actual", func(t *testing.T) {
		tn := newBudgetTenant(t, m, db, usd(100))
		r := mustReserve(t, m, tn, usd(10), time.Hour)
		if _, err := m.Reconcile(r.ID, money.Micros(math.MaxInt64)); err != nil {
			t.Fatalf("max actual on empty settled must work: %v", err)
		}
		e := expo(t, m, tn)
		if e.Settled != money.Micros(math.MaxInt64) || e.Available != 0 {
			t.Fatalf("%+v", e)
		}
		// Over-committed account fails closed.
		if _, err := m.Reserve(tn, 1, time.Hour); !errors.Is(err, ErrInsufficientBudget) {
			t.Fatalf("reserve on blown account: %v", err)
		}
		requireClean(t, db, tn)
	})

	t.Run("settled overflow is refused and rolled back", func(t *testing.T) {
		tn := newBudgetTenant(t, m, db, money.Micros(math.MaxInt64))
		// Park settled just below the int64 ceiling (valid per CHECK constraints).
		execOrFatal(t, db, `UPDATE budget_accounts SET settled_minor_units=$2 WHERE tenant_id=$1`, tn, int64(math.MaxInt64-10))
		r := mustReserve(t, m, tn, 5, time.Hour)
		_, err := m.Reconcile(r.ID, 100)
		wantErr(t, "overflow", err, ErrAmountOverflow)
		wantState(t, m, r.ID, StateReserved)
		e := expo(t, m, tn)
		if e.Reserved != 5 || e.Settled != money.Micros(math.MaxInt64-10) {
			t.Fatalf("overflow attempt changed the ledger: %+v", e)
		}
		if n := countEvents(t, db, r.ID, "RECONCILED"); n != 0 {
			t.Fatalf("event written for refused reconcile")
		}
		if _, err := m.Reconcile(r.ID, 10); err != nil { // exactly reaches MaxInt64
			t.Fatalf("reaching MaxInt64 exactly must work: %v", err)
		}
		if e := expo(t, m, tn); e.Settled != money.Micros(math.MaxInt64) || e.Available != 0 {
			t.Fatalf("%+v", e)
		}
	})
}

// ---------------------------------------------------------------------------
// Terminal-state semantics
// ---------------------------------------------------------------------------

func TestPG_StateMachine_Matrix(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)

	type setup func(t *testing.T, tn string) string
	reserved := func(t *testing.T, tn string) string { return mustReserve(t, m, tn, usd(10), time.Hour).ID }
	reservedPending := func(t *testing.T, tn string) string {
		id := reserved(t, tn)
		if err := m.RecordPendingActual(id, usd(7)); err != nil {
			t.Fatal(err)
		}
		return id
	}
	unknown := func(t *testing.T, tn string) string {
		id := mustReserve(t, m, tn, usd(10), time.Millisecond).ID
		shortTTLExpire(t, m)
		wantState(t, m, id, StateUnknown)
		return id
	}
	released := func(t *testing.T, tn string) string {
		id := reserved(t, tn)
		if err := m.Release(id); err != nil {
			t.Fatal(err)
		}
		return id
	}
	reconciled := func(t *testing.T, tn string) string {
		id := reserved(t, tn)
		if _, err := m.Reconcile(id, usd(5)); err != nil {
			t.Fatal(err)
		}
		return id
	}

	rec := func(a int64) func(string) error {
		return func(id string) error { _, err := m.Reconcile(id, usd(a)); return err }
	}
	resRec := func(a int64) func(string) error {
		return func(id string) error { _, err := m.ResolveUnknownReconciled(id, usd(a)); return err }
	}
	pend := func(a int64) func(string) error {
		return func(id string) error { return m.RecordPendingActual(id, usd(a)) }
	}
	release := func(id string) error { return m.Release(id) }
	resRel := func(id string) error { return m.ResolveUnknownReleased(id) }

	cases := []struct {
		name string
		from setup
		op   func(string) error
		want error
	}{
		{"RESERVED Release", reserved, release, nil},
		{"RESERVED Reconcile", reserved, rec(5), nil},
		{"RESERVED ResolveUnknownReleased", reserved, resRel, ErrTerminalState},
		{"RESERVED ResolveUnknownReconciled", reserved, resRec(5), ErrTerminalState},
		{"RESERVED RecordPendingActual", reserved, pend(5), nil},

		{"RESERVED+pending Release", reservedPending, release, ErrPendingActualExists},
		{"RESERVED+pending Reconcile same", reservedPending, rec(7), nil},
		{"RESERVED+pending Reconcile different", reservedPending, rec(8), ErrConflictingReconcile},
		{"RESERVED+pending RecordPending same", reservedPending, pend(7), nil},
		{"RESERVED+pending RecordPending different", reservedPending, pend(8), ErrConflictingReconcile},

		{"UNKNOWN Release", unknown, release, ErrTerminalState},
		{"UNKNOWN Reconcile", unknown, rec(5), ErrTerminalState},
		{"UNKNOWN ResolveUnknownReleased", unknown, resRel, nil},
		{"UNKNOWN ResolveUnknownReconciled", unknown, resRec(5), nil},
		{"UNKNOWN RecordPendingActual", unknown, pend(5), nil},

		{"RELEASED Release", released, release, nil},
		{"RELEASED Reconcile", released, rec(5), ErrTerminalState},
		{"RELEASED ResolveUnknownReleased", released, resRel, nil},
		{"RELEASED ResolveUnknownReconciled", released, resRec(5), ErrTerminalState},
		{"RELEASED RecordPendingActual", released, pend(5), ErrTerminalState},

		{"RECONCILED Release", reconciled, release, ErrTerminalState},
		{"RECONCILED Reconcile same actual", reconciled, rec(5), nil},
		{"RECONCILED Reconcile different actual", reconciled, rec(6), ErrConflictingReconcile},
		{"RECONCILED ResolveUnknownReleased", reconciled, resRel, ErrTerminalState},
		{"RECONCILED ResolveUnknownReconciled same", reconciled, resRec(5), nil},
		{"RECONCILED ResolveUnknownReconciled different", reconciled, resRec(6), ErrConflictingReconcile},
		{"RECONCILED RecordPending same", reconciled, pend(5), nil},
		{"RECONCILED RecordPending different", reconciled, pend(6), ErrConflictingReconcile},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			tn := newBudgetTenant(t, m, db, usd(100))
			id := c.from(t, tn)
			before := expo(t, m, tn)
			err := c.op(id)
			wantErr(t, c.name, err, c.want)
			if c.want != nil {
				if after := expo(t, m, tn); after != before {
					t.Fatalf("refused operation changed the ledger: %+v -> %+v", before, after)
				}
			}
			if _, err := m.Get(id); err != nil {
				t.Fatal(err)
			}
			if _, err := m.Reconcile("not-a-uuid", 1); !errors.Is(err, ErrNotFound) {
				t.Fatalf("malformed id: %v", err)
			}
			requireClean(t, db, tn, id)
		})
	}
}

// ---------------------------------------------------------------------------
// Idempotency
// ---------------------------------------------------------------------------

func TestPG_Idempotency(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)
	m2, _ := newMgr(t, dsn)

	t.Run("retries return one reservation", func(t *testing.T) {
		tn := newBudgetTenant(t, m, db, usd(100))
		first, err := m.ReserveKeyed(tn, "k1", usd(10), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 5; i++ {
			again, err := m.ReserveKeyed(tn, "k1", usd(10), time.Hour)
			if err != nil || again.ID != first.ID || again.IdempotencyKey != "k1" {
				t.Fatalf("retry %d: %+v %v", i, again, err)
			}
		}
		wantExposure(t, m, tn, usd(10), 0)
		var n int
		execQ := db.QueryRow(`SELECT count(*) FROM reservations WHERE tenant_id=$1`, tn)
		if err := execQ.Scan(&n); err != nil || n != 1 {
			t.Fatalf("reservations=%d err=%v", n, err)
		}
		if c := countEventsFrom(t, db, first.ID); c != 1 {
			t.Fatalf("CREATED events=%d", c)
		}
		_, err = m.ReserveKeyed(tn, "k1", usd(11), time.Hour)
		wantErr(t, "conflicting amount", err, ErrIdempotencyConflict)
		wantExposure(t, m, tn, usd(10), 0)
		requireClean(t, db, tn)
	})

	t.Run("concurrent retries across instances", func(t *testing.T) {
		tn := newBudgetTenant(t, m, db, usd(100))
		ids := make([]string, 30)
		var wg sync.WaitGroup
		for i := range ids {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				mm := m
				if i%2 == 1 {
					mm = m2
				}
				r, err := mm.ReserveKeyed(tn, "dup", usd(10), time.Hour)
				if err != nil {
					t.Errorf("reserve: %v", err)
					return
				}
				ids[i] = r.ID
			}(i)
		}
		wg.Wait()
		if t.Failed() {
			return
		}
		for _, id := range ids {
			if id != ids[0] {
				t.Fatalf("different reservation ids for one key: %v", ids)
			}
		}
		wantExposure(t, m, tn, usd(10), 0)
		requireClean(t, db, tn)
	})

	t.Run("keys are tenant scoped; unkeyed never collide; bad keys", func(t *testing.T) {
		a := newBudgetTenant(t, m, db, usd(100))
		b := newBudgetTenant(t, m, db, usd(100))
		ra, err := m.ReserveKeyed(a, "shared", usd(10), time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		rb, err := m.ReserveKeyed(b, "shared", usd(20), time.Hour)
		if err != nil {
			t.Fatalf("different tenant must be independent: %v", err)
		}
		if ra.ID == rb.ID {
			t.Fatal("tenants share a reservation")
		}
		wantExposure(t, m, a, usd(10), 0)
		wantExposure(t, m, b, usd(20), 0)
		mustReserve(t, m, a, usd(1), time.Hour)
		mustReserve(t, m, a, usd(1), time.Hour)
		wantExposure(t, m, a, usd(12), 0)
		_, err = m.ReserveKeyed(a, "", usd(1), time.Hour)
		wantErr(t, "empty key", err, ErrInvalidKey)
		_, err = m.ReserveKeyed(a, strings.Repeat("x", 257), usd(1), time.Hour)
		wantErr(t, "oversized key", err, ErrInvalidKey)
		requireClean(t, db, a, b)
	})

	t.Run("input validation", func(t *testing.T) {
		tn := newBudgetTenant(t, m, db, usd(100))
		_, err := m.Reserve(tn, -1, time.Hour)
		wantErr(t, "negative amount", err, ErrInvalidAmount)
		_, err = m.Reserve(tn, 1, 0)
		wantErr(t, "zero ttl", err, ErrInvalidTTL)
		_, err = m.Reserve(tn, 1, 400*24*time.Hour)
		wantErr(t, "huge ttl", err, ErrInvalidTTL)
		_, err = m.Reserve("", 1, time.Hour)
		wantErr(t, "empty tenant", err, ErrInvalidTenant)
		_, err = m.Reserve("not-a-uuid", 1, time.Hour)
		wantErr(t, "malformed tenant", err, ErrInvalidTenant)
		_, err = m.Reserve("00000000-0000-4000-8000-000000000000", 1, time.Hour)
		wantErr(t, "tenant without budget", err, ErrInsufficientBudget)
		_, err = m.Reserve(tn, usd(101), time.Hour)
		wantErr(t, "over budget", err, ErrInsufficientBudget)
	})
}

func countEventsFrom(t *testing.T, db *sql.DB, id string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM reservation_events WHERE reservation_id=$1 AND from_state IS NULL AND to_state='RESERVED'`, id).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

// ---------------------------------------------------------------------------
// Crash / restart durability, concurrent recovery
// ---------------------------------------------------------------------------

func TestPG_CrashRestart_RecoversPendingActual(t *testing.T) {
	dsn := testDSN(t)

	// Process #1: reserve $100, provider ran, durable pending actual of $73, then "crash".
	db1, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	m1 := NewPGManager(db1, quietLog())
	admin := openDB(t, dsn)
	tn := newTenant(t, admin)
	if err := m1.SetBudgetE(tn, usd(100)); err != nil {
		t.Fatal(err)
	}
	r, err := m1.Reserve(tn, usd(100), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if err := m1.RecordPendingActual(r.ID, usd(73)); err != nil {
		t.Fatal(err)
	}
	_ = db1.Close() // process termination: no Reconcile ever ran
	wantStateAdmin(t, admin, r.ID, "RESERVED")

	// Process #2: a brand new manager over a brand new pool.
	m2, db2 := newMgr(t, dsn)
	n, err := m2.RecoverPending()
	if err != nil || n != 1 {
		t.Fatalf("RecoverPending n=%d err=%v, want 1", n, err)
	}
	got := wantState(t, m2, r.ID, StateReconciled)
	if got.ActualCost != usd(73) {
		t.Fatalf("actual=%d want %d", got.ActualCost, usd(73))
	}
	e := expo(t, m2, tn)
	if e.Reserved != 0 || e.Settled != usd(73) || e.Available != usd(27) {
		t.Fatalf("exposure after recovery: %+v", e)
	}
	if c := countEvents(t, db2, r.ID, "RECONCILED"); c != 1 {
		t.Fatalf("reconcile events=%d want exactly 1", c)
	}
	before := e

	// Recovery again: nothing changes.
	n, err = m2.RecoverPending()
	if err != nil || n != 0 {
		t.Fatalf("second RecoverPending n=%d err=%v", n, err)
	}
	if after := expo(t, m2, tn); after != before {
		t.Fatalf("second recovery changed state: %+v -> %+v", before, after)
	}
	if c := countEvents(t, db2, r.ID, "RECONCILED"); c != 1 {
		t.Fatalf("reconcile events=%d after repeat", c)
	}
	requireClean(t, db2, tn, r.ID)
}

func wantStateAdmin(t *testing.T, db *sql.DB, id, want string) {
	t.Helper()
	var st string
	if err := db.QueryRow(`SELECT state FROM reservations WHERE id=$1`, id).Scan(&st); err != nil || st != want {
		t.Fatalf("state=%q err=%v want %s", st, err, want)
	}
}

func TestPG_ConcurrentRecovery_SettlesExactlyOnce(t *testing.T) {
	dsn := testDSN(t)
	mA, db := newMgr(t, dsn)
	mB, _ := newMgr(t, dsn)

	for iter := 0; iter < 25; iter++ {
		tn := newBudgetTenant(t, mA, db, usd(100))
		r := mustReserve(t, mA, tn, usd(100), time.Hour)
		if err := mA.RecordPendingActual(r.ID, usd(73)); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		start := make(chan struct{})
		var total atomic.Int64
		for _, m := range []*PGManager{mA, mB} {
			wg.Add(1)
			go func(m *PGManager) {
				defer wg.Done()
				<-start
				n, err := m.RecoverPending()
				if err != nil {
					t.Errorf("RecoverPending: %v", err)
				}
				total.Add(int64(n))
			}(m)
		}
		close(start)
		wg.Wait()
		if total.Load() != 1 {
			t.Fatalf("iter %d: %d reconciliations performed, want exactly 1", iter, total.Load())
		}
		wantExposure(t, mA, tn, 0, usd(73))
		if c := countEvents(t, db, r.ID, "RECONCILED"); c != 1 {
			t.Fatalf("iter %d: reconcile events=%d", iter, c)
		}
		requireClean(t, db, tn, r.ID)
	}
}

func TestPG_RecoverPending_DrainsBacklogAcrossBatches(t *testing.T) {
	dsn := testDSN(t)
	managers := make([]*PGManager, 3)
	var db *sql.DB
	for i := range managers {
		m, d := newMgr(t, dsn)
		m.SetBatchSize(7) // far smaller than the backlog
		managers[i], db = m, d
	}
	tn := newBudgetTenant(t, managers[0], db, usd(10000))
	const backlog = 100
	ids := make([]string, backlog)
	for i := range ids {
		r := mustReserve(t, managers[0], tn, usd(10), time.Hour)
		ids[i] = r.ID
		if err := managers[0].RecordPendingActual(r.ID, usd(4)); err != nil {
			t.Fatal(err)
		}
	}
	if n, err := managers[0].PendingCount(context.Background()); err != nil || n != backlog {
		t.Fatalf("PendingCount=%d err=%v", n, err)
	}
	var wg sync.WaitGroup
	var total atomic.Int64
	for _, m := range managers {
		wg.Add(1)
		go func(m *PGManager) {
			defer wg.Done()
			n, err := m.RecoverPending()
			if err != nil {
				t.Errorf("RecoverPending: %v", err)
			}
			total.Add(int64(n))
		}(m)
	}
	wg.Wait()
	if total.Load() != backlog {
		t.Fatalf("recovered %d, want %d", total.Load(), backlog)
	}
	wantExposure(t, managers[0], tn, 0, usd(4*backlog))
	for _, id := range ids {
		if c := countEvents(t, db, id, "RECONCILED"); c != 1 {
			t.Fatalf("reservation %s has %d reconcile events", id, c)
		}
	}
	requireClean(t, db, tn)
}

func TestPG_RecoverPending_PoisonedRowDoesNotBlockOthers(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)

	poison := newBudgetTenant(t, m, db, money.Micros(math.MaxInt64))
	execOrFatal(t, db, `UPDATE budget_accounts SET settled_minor_units=$2 WHERE tenant_id=$1`, poison, int64(math.MaxInt64-10))
	bad := mustReserve(t, m, poison, 5, time.Hour)
	if err := m.RecordPendingActual(bad.ID, 100); err != nil {
		t.Fatal(err)
	}
	good := newBudgetTenant(t, m, db, usd(100))
	g := mustReserve(t, m, good, usd(10), time.Hour)
	if err := m.RecordPendingActual(g.ID, usd(6)); err != nil {
		t.Fatal(err)
	}

	n, err := m.RecoverPending()
	if !errors.Is(err, ErrAmountOverflow) {
		t.Fatalf("expected ErrAmountOverflow for the poisoned row, got %v", err)
	}
	if n < 1 {
		t.Fatalf("healthy row was not recovered (n=%d)", n)
	}
	wantState(t, m, g.ID, StateReconciled)
	wantState(t, m, bad.ID, StateReserved)
	if p, err := m.PendingCount(context.Background()); err != nil || p < 1 {
		t.Fatalf("poisoned row must stay visible as pending: p=%d err=%v", p, err)
	}
	requireClean(t, db, good)
}

// ---------------------------------------------------------------------------
// Expiration concurrency
// ---------------------------------------------------------------------------

func TestPG_ConcurrentExpiration_AndReconcile(t *testing.T) {
	dsn := testDSN(t)
	mA, db := newMgr(t, dsn)
	mB, _ := newMgr(t, dsn)
	mA.SetBatchSize(9)
	mB.SetBatchSize(9)
	tn := newBudgetTenant(t, mA, db, usd(100000))
	const n = 120
	ids := make([]string, n)
	for i := range ids {
		ids[i] = mustReserve(t, mA, tn, usd(10), time.Millisecond).ID
	}
	time.Sleep(40 * time.Millisecond)

	var wg sync.WaitGroup
	// Two expiry workers race each other and a reconciler for the same rows.
	for _, m := range []*PGManager{mA, mB} {
		wg.Add(1)
		go func(m *PGManager) {
			defer wg.Done()
			if _, err := m.ExpireStaleE(); err != nil {
				t.Errorf("expire: %v", err)
			}
		}(m)
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < n; i += 3 {
			_, err := mB.Reconcile(ids[i], usd(4))
			if err != nil && !errors.Is(err, ErrTerminalState) {
				t.Errorf("reconcile: %v", err)
				continue
			}
			if errors.Is(err, ErrTerminalState) { // lost the race to expiry: resolve the UNKNOWN row
				if _, err := mB.ResolveUnknownReconciled(ids[i], usd(4)); err != nil {
					t.Errorf("resolve unknown: %v", err)
				}
			}
		}
	}()
	wg.Wait()
	if t.Failed() {
		return
	}
	for i, id := range ids {
		want := StateUnknown
		if i%3 == 0 {
			want = StateReconciled
		}
		wantState(t, mA, id, want)
		if want == StateUnknown {
			if c := countEvents(t, db, id, "UNKNOWN"); c != 1 {
				t.Fatalf("reservation %s expiry events=%d", id, c)
			}
		}
	}
	requireClean(t, db, tn)
}

// ---------------------------------------------------------------------------
// Database outage and recovery (a TCP proxy in front of Postgres is cut and restored)
// ---------------------------------------------------------------------------

type tcpProxy struct {
	target string
	mu     sync.Mutex
	addr   string
	ln     net.Listener
	conns  []net.Conn
}

func newTCPProxy(t *testing.T, target string) *tcpProxy {
	p := &tcpProxy{target: target}
	p.start(t, "127.0.0.1:0")
	t.Cleanup(p.stop)
	return p
}

func (p *tcpProxy) start(t *testing.T, addr string) {
	var ln net.Listener
	var err error
	for i := 0; i < 50; i++ { // the port may linger briefly after close
		if ln, err = net.Listen("tcp", addr); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("proxy listen: %v", err)
	}
	p.mu.Lock()
	p.ln = ln
	p.addr = ln.Addr().String()
	p.mu.Unlock()
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			up, err := net.Dial("tcp", p.target)
			if err != nil {
				_ = c.Close()
				continue
			}
			p.mu.Lock()
			p.conns = append(p.conns, c, up)
			p.mu.Unlock()
			go func() { _, _ = io.Copy(up, c); _ = up.Close(); _ = c.Close() }()
			go func() { _, _ = io.Copy(c, up); _ = up.Close(); _ = c.Close() }()
		}
	}()
}

func (p *tcpProxy) stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ln != nil {
		_ = p.ln.Close()
	}
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.conns = nil
}

func (p *tcpProxy) address() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.addr
}

func TestPG_DatabaseDisconnectAndReconnect(t *testing.T) {
	dsn := testDSN(t)
	u, err := url.Parse(dsn)
	if err != nil || u.Host == "" {
		t.Skip("DSN is not URL-form with a TCP host; cannot interpose a proxy")
	}
	target := u.Host
	if u.Port() == "" {
		target = net.JoinHostPort(u.Hostname(), "5432")
	}
	proxy := newTCPProxy(t, target)
	pu := *u
	pu.Host = proxy.address()
	q := pu.Query()
	q.Set("connect_timeout", "2")
	pu.RawQuery = q.Encode()

	admin := openDB(t, dsn)
	proxied, err := sql.Open("postgres", pu.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = proxied.Close() })
	m := NewPGManager(proxied, quietLog())
	m.SetOpTimeout(3 * time.Second)

	tn := newTenant(t, admin)
	if err := m.SetBudgetE(tn, usd(100)); err != nil {
		t.Fatal(err)
	}
	r := mustReserve(t, m, tn, usd(10), time.Hour)

	proxy.stop() // ---- outage ----

	if e := m.Exposure(tn); e != (Exposure{}) {
		t.Fatalf("Exposure must fail closed during an outage, got %+v", e)
	}
	if _, err := m.ExposureE(tn); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("ExposureE: %v", err)
	}
	if _, err := m.Reserve(tn, usd(1), time.Hour); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("Reserve: %v", err)
	}
	if _, err := m.ReserveKeyed(tn, "k", usd(1), time.Hour); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("ReserveKeyed: %v", err)
	}
	if err := m.Release(r.ID); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("Release: %v", err)
	}
	if _, err := m.Reconcile(r.ID, usd(5)); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("Reconcile: %v", err)
	}
	if err := m.RecordPendingActual(r.ID, usd(5)); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("RecordPendingActual: %v", err)
	}
	if err := m.SetBudgetE(tn, usd(50)); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("SetBudgetE: %v", err)
	}
	if _, err := m.RecoverPending(); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("RecoverPending: %v", err)
	}
	if _, err := m.ExpireStaleE(); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("ExpireStale: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	if err := m.Ping(ctx); !errors.Is(err, ErrBackendUnavailable) {
		t.Fatalf("Ping: %v", err)
	}
	cancel()

	// Nothing the failed operations attempted may have been applied.
	wantExposure(t, &PGManager{db: admin, period: "monthly", opTimeout: 5 * time.Second, batchSize: 10, log: quietLog()}, tn, usd(10), 0)

	proxy.start(t, proxy.address()) // ---- Postgres is back ----

	deadline := time.Now().Add(15 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := m.Ping(ctx)
		cancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("manager never reconnected: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	r2 := mustReserve(t, m, tn, usd(5), time.Hour)
	if _, err := m.Reconcile(r.ID, usd(7)); err != nil {
		t.Fatalf("Reconcile after reconnect: %v", err)
	}
	if n, err := m.RecoverPending(); err != nil || n != 0 {
		t.Fatalf("RecoverPending after reconnect: n=%d err=%v", n, err)
	}
	wantExposure(t, m, tn, usd(5), usd(7))
	wantState(t, m, r2.ID, StateReserved)
	requireClean(t, admin, tn)
}

// ---------------------------------------------------------------------------
// Auditor
// ---------------------------------------------------------------------------

type auditFixture struct {
	tenant, reserved, reconciled, released string
}

func newAuditFixture(t *testing.T, m *PGManager, db *sql.DB) auditFixture {
	t.Helper()
	tn := newBudgetTenant(t, m, db, usd(1000))
	f := auditFixture{tenant: tn}
	f.reserved = mustReserve(t, m, tn, usd(100), time.Hour).ID
	r2 := mustReserve(t, m, tn, usd(100), time.Hour)
	if _, err := m.Reconcile(r2.ID, usd(60)); err != nil {
		t.Fatal(err)
	}
	f.reconciled = r2.ID
	r3 := mustReserve(t, m, tn, usd(100), time.Hour)
	if err := m.Release(r3.ID); err != nil {
		t.Fatal(err)
	}
	f.released = r3.ID
	return f
}

func (f auditFixture) needles() []string {
	return []string{f.tenant, f.reserved, f.reconciled, f.released}
}

func TestPG_Audit_CleanLedgerHasNoViolations(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)
	f := newAuditFixture(t, m, db)
	// Add UNKNOWN and pending-actual shapes so the clean case covers every state.
	u := mustReserve(t, m, f.tenant, usd(10), time.Millisecond)
	shortTTLExpire(t, m)
	wantState(t, m, u.ID, StateUnknown)
	p := mustReserve(t, m, f.tenant, usd(10), time.Hour)
	if err := m.RecordPendingActual(p.ID, usd(3)); err != nil {
		t.Fatal(err)
	}
	requireClean(t, db, append(f.needles(), u.ID, p.ID)...)
}

func TestPG_Audit_DetectsCorruption(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)

	cases := []struct {
		name    string
		corrupt func(f auditFixture)
		want    string // prefix of the expected violation
	}{
		{"reserved mismatch", func(f auditFixture) {
			execOrFatal(t, db, `UPDATE budget_accounts SET reserved_minor_units = reserved_minor_units + 5 WHERE tenant_id=$1`, f.tenant)
		}, "reserved_mismatch"},
		{"settled mismatch", func(f auditFixture) {
			execOrFatal(t, db, `UPDATE budget_accounts SET settled_minor_units = settled_minor_units + 5 WHERE tenant_id=$1`, f.tenant)
		}, "settled_mismatch"},
		{"reservation without budget", func(f auditFixture) {
			execOrFatal(t, db, `UPDATE reservations SET budget_account_id=NULL WHERE id=$1`, f.reserved)
		}, "reservation_without_budget"},
		{"missing CREATED event", func(f auditFixture) {
			execOrFatal(t, db, `DELETE FROM reservation_events WHERE reservation_id=$1 AND from_state IS NULL`, f.reserved)
		}, "missing_or_duplicate_created_event"},
		{"duplicate CREATED event", func(f auditFixture) {
			execOrFatal(t, db, `INSERT INTO reservation_events (reservation_id, from_state, to_state, reason, amount_minor_units) VALUES ($1, NULL, 'RESERVED', 'dup', $2)`, f.reserved, int64(usd(100)))
		}, "missing_or_duplicate_created_event"},
		{"duplicate settlement", func(f auditFixture) {
			execOrFatal(t, db, `INSERT INTO reservation_events (reservation_id, from_state, to_state, reason, amount_minor_units) VALUES ($1, 'RESERVED', 'RECONCILED', 'dup', $2)`, f.reconciled, int64(usd(60)))
		}, "duplicate_settlement"},
		{"reconciled reservation missing its event", func(f auditFixture) {
			execOrFatal(t, db, `DELETE FROM reservation_events WHERE reservation_id=$1 AND to_state='RECONCILED'`, f.reconciled)
		}, "reconciled_without_matching_event"},
		{"reconciliation event with wrong amount", func(f auditFixture) {
			execOrFatal(t, db, `UPDATE reservation_events SET amount_minor_units = amount_minor_units + 1 WHERE reservation_id=$1 AND to_state='RECONCILED'`, f.reconciled)
		}, "reconciled_without_matching_event"},
		{"event claims reconciliation but reservation is not reconciled", func(f auditFixture) {
			execOrFatal(t, db, `INSERT INTO reservation_events (reservation_id, from_state, to_state, reason, amount_minor_units) VALUES ($1, 'RESERVED', 'RECONCILED', 'forged', 1)`, f.reserved)
		}, "reconciliation_event_without_reconciled_state"},
		{"released reservation missing its event", func(f auditFixture) {
			execOrFatal(t, db, `DELETE FROM reservation_events WHERE reservation_id=$1 AND to_state='RELEASED'`, f.released)
		}, "released_state_event_mismatch"},
		{"release event amount differs from reservation amount", func(f auditFixture) {
			execOrFatal(t, db, `UPDATE reservation_events SET amount_minor_units = amount_minor_units - 1 WHERE reservation_id=$1 AND to_state='RELEASED'`, f.released)
		}, "released_state_event_mismatch"},
		{"unknown reservation without expiry event", func(f auditFixture) {
			execOrFatal(t, db, `UPDATE reservations SET state='UNKNOWN' WHERE id=$1`, f.reserved)
		}, "unknown_state_event_mismatch"},
		{"reservation both released and reconciled in the log", func(f auditFixture) {
			execOrFatal(t, db, `INSERT INTO reservation_events (reservation_id, from_state, to_state, reason, amount_minor_units) VALUES ($1, 'RESERVED', 'RELEASED', 'dup', $2)`, f.reconciled, int64(usd(100)))
		}, "conflicting_terminal_events"},
		{"event amount missing", func(f auditFixture) {
			execOrFatal(t, db, `UPDATE reservation_events SET amount_minor_units=NULL WHERE reservation_id=$1 AND from_state IS NULL`, f.reserved)
		}, "created_event_amount_mismatch"},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			f := newAuditFixture(t, m, db)
			requireClean(t, db, f.needles()...) // clean before corruption
			c.corrupt(f)
			got := auditScoped(t, db, f.needles()...)
			for _, v := range got {
				if strings.HasPrefix(v, c.want+":") {
					return
				}
			}
			t.Fatalf("auditor missed %q; reported: %v", c.want, got)
		})
	}

	t.Run("event without reservation (FK bypassed for the test)", func(t *testing.T) {
		ctx := context.Background()
		conn, err := db.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if _, err := conn.ExecContext(ctx, `SET session_replication_role = replica`); err != nil {
			t.Skipf("cannot disable FK triggers (need superuser): %v", err)
		}
		orphan := "11111111-2222-4333-8444-" + fmt.Sprintf("%012d", time.Now().UnixNano()%1_000_000_000_000)
		var evID int64
		err = conn.QueryRowContext(ctx, `INSERT INTO reservation_events (reservation_id, from_state, to_state, reason, amount_minor_units) VALUES ($1, NULL, 'RESERVED', 'orphan', 1) RETURNING id`, orphan).Scan(&evID)
		if err != nil {
			t.Fatalf("insert orphan: %v", err)
		}
		t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM reservation_events WHERE id=$1`, evID) })
		_, _ = conn.ExecContext(ctx, `SET session_replication_role = origin`)
		got := auditScoped(t, db, orphan)
		if len(got) == 0 || !strings.HasPrefix(got[0], "event_without_reservation:") {
			t.Fatalf("auditor missed the orphan event: %v", got)
		}
	})
}

// Corruptions the schema itself forbids: the constraint is the protection.
func TestPG_Schema_ConstraintsBlockImpossibleStates(t *testing.T) {
	dsn := testDSN(t)
	m, db := newMgr(t, dsn)
	f := newAuditFixture(t, m, db)

	expectPQ(t, "negative reserved balance", execErr(db, `UPDATE budget_accounts SET reserved_minor_units=-1 WHERE tenant_id=$1`, f.tenant), "23514")
	expectPQ(t, "negative settled balance", execErr(db, `UPDATE budget_accounts SET settled_minor_units=-1 WHERE tenant_id=$1`, f.tenant), "23514")
	expectPQ(t, "negative limit", execErr(db, `UPDATE budget_accounts SET limit_minor_units=-1 WHERE tenant_id=$1`, f.tenant), "23514")
	expectPQ(t, "negative reservation amount", execErr(db, `UPDATE reservations SET amount_minor_units=-1 WHERE id=$1`, f.reserved), "23514")
	expectPQ(t, "negative pending actual", execErr(db, `UPDATE reservations SET pending_actual_minor_units=-1 WHERE id=$1`, f.reserved), "23514")
	expectPQ(t, "negative actual cost", execErr(db, `UPDATE reservations SET actual_cost_minor_units=-1 WHERE id=$1`, f.reconciled), "23514")
	expectPQ(t, "negative event amount", execErr(db, `UPDATE reservation_events SET amount_minor_units=-1 WHERE reservation_id=$1`, f.reserved), "23514")
	expectPQ(t, "RESERVED with actual cost", execErr(db, `UPDATE reservations SET actual_cost_minor_units=1 WHERE id=$1`, f.reserved), "23514")
	expectPQ(t, "invalid state", execErr(db, `UPDATE reservations SET state='EXPIRED' WHERE id=$1`, f.reserved), "23514")
	expectPQ(t, "RECONCILED without terminal timestamp", execErr(db, `UPDATE reservations SET resolved_at=NULL WHERE id=$1`, f.reconciled), "23514")
	expectPQ(t, "RELEASED without terminal timestamp", execErr(db, `UPDATE reservations SET resolved_at=NULL WHERE id=$1`, f.released), "23514")
	expectPQ(t, "pending actual on RELEASED", execErr(db, `UPDATE reservations SET pending_actual_minor_units=1 WHERE id=$1`, f.released), "23514")
	expectPQ(t, "non-terminal with timestamp", execErr(db, `UPDATE reservations SET resolved_at=now() WHERE id=$1`, f.reserved), "23514")
	expectPQ(t, "invalid event state", execErr(db, `UPDATE reservation_events SET to_state='EXPIRED' WHERE reservation_id=$1 AND from_state IS NULL`, f.reserved), "23514")
	expectPQ(t, "reservation without tenant", execErr(db, `INSERT INTO reservations (tenant_id, amount_minor_units, state, expires_at) VALUES ('00000000-0000-4000-8000-000000000001', 1, 'RESERVED', now())`), "23503")
	expectPQ(t, "event without reservation", execErr(db, `INSERT INTO reservation_events (reservation_id, to_state, reason) VALUES ('00000000-0000-4000-8000-000000000001', 'RESERVED', 'x')`), "23503")
	expectPQ(t, "reservation with unknown budget account", execErr(db, `UPDATE reservations SET budget_account_id='00000000-0000-4000-8000-000000000001' WHERE id=$1`, f.reserved), "23503")

	// Tenant-scoped idempotency: duplicate within a tenant is refused...
	execOrFatal(t, db, `UPDATE reservations SET idempotency_key='ik' WHERE id=$1`, f.reserved)
	expectPQ(t, "duplicate idempotency key within a tenant", execErr(db, `UPDATE reservations SET idempotency_key='ik' WHERE id=$1`, f.reconciled), "23505")
	// ...but another tenant may reuse it, and NULL keys never collide.
	other := newTenant(t, db)
	var accID string
	if err := db.QueryRow(`INSERT INTO budget_accounts (tenant_id, period, limit_minor_units) VALUES ($1,'monthly',10) RETURNING id::text`, other).Scan(&accID); err != nil {
		t.Fatal(err)
	}
	execOrFatal(t, db, `INSERT INTO reservations (tenant_id, budget_account_id, amount_minor_units, state, idempotency_key, expires_at) VALUES ($1,$2,0,'RESERVED','ik',now()+interval '1 hour')`, other, accID)
	execOrFatal(t, db, `INSERT INTO reservations (tenant_id, budget_account_id, amount_minor_units, state, expires_at) VALUES ($1,$2,0,'RESERVED',now()+interval '1 hour')`, other, accID)
	execOrFatal(t, db, `INSERT INTO reservations (tenant_id, budget_account_id, amount_minor_units, state, expires_at) VALUES ($1,$2,0,'RESERVED',now()+interval '1 hour')`, other, accID)
	expectPQ(t, "empty idempotency key", execErr(db, `UPDATE reservations SET idempotency_key='' WHERE id=$1`, f.released), "23514")
}

func execErr(db *sql.DB, q string, args ...any) error {
	_, err := db.Exec(q, args...)
	return err
}

// ---------------------------------------------------------------------------
// Stress: many tenants, workers, instances, short TTLs, large values
// ---------------------------------------------------------------------------

func TestPG_Stress_LedgerStaysConsistent(t *testing.T) {
	dsn := testDSN(t)
	m1, db := newMgr(t, dsn)
	m2, _ := newMgr(t, dsn)
	m1.SetBatchSize(25)
	m2.SetBatchSize(25)
	managers := []*PGManager{m1, m2}

	type tenantCfg struct {
		id    string
		limit money.Micros
		scale money.Micros // reservation size multiplier
	}
	cfgs := []tenantCfg{
		{limit: 20_000, scale: 1},
		{limit: 20_000, scale: 1},
		{limit: 20_000, scale: 1},
		{limit: money.Micros(math.MaxInt64 / 2), scale: 1_000_000_000_000}, // large monetary values
	}
	var needles []string
	for i := range cfgs {
		cfgs[i].id = newBudgetTenant(t, m1, db, cfgs[i].limit)
		needles = append(needles, cfgs[i].id)
	}

	var failures atomic.Int64
	fail := func(format string, a ...any) {
		failures.Add(1)
		t.Errorf(format, a...)
	}
	tolerated := func(err error) bool {
		return err == nil || errors.Is(err, ErrTerminalState) || errors.Is(err, ErrPendingActualExists) ||
			errors.Is(err, ErrConflictingReconcile) || errors.Is(err, ErrInsufficientBudget)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// Background expiry + recovery workers on both instances (races with the traffic below).
	var bg sync.WaitGroup
	for _, m := range managers {
		bg.Add(1)
		go func(m *PGManager) {
			defer bg.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if _, err := m.ExpireStaleE(); err != nil {
					fail("expire: %v", err)
				}
				if _, err := m.RecoverPending(); err != nil {
					fail("recover: %v", err)
				}
				time.Sleep(5 * time.Millisecond)
			}
		}(m)
	}

	for w := 0; w < 12; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			rng := rand.New(rand.NewSource(int64(w) + time.Now().UnixNano()))
			m := managers[w%2]
			for i := 0; i < 40; i++ {
				cfg := cfgs[rng.Intn(len(cfgs))]
				amount := money.Micros(1+rng.Intn(50)) * cfg.scale
				ttl := time.Duration(1+rng.Intn(25)) * time.Millisecond
				var r *Reservation
				var err error
				if rng.Intn(3) == 0 {
					r, err = m.ReserveKeyed(cfg.id, fmt.Sprintf("w%d-i%d", w, i), amount, ttl)
					if err == nil { // duplicate request must resolve to the same reservation
						r2, err2 := m.ReserveKeyed(cfg.id, fmt.Sprintf("w%d-i%d", w, i), amount, ttl)
						if err2 != nil || r2.ID != r.ID {
							fail("keyed retry diverged: %v %v", err2, r2)
							return
						}
					}
				} else {
					r, err = m.Reserve(cfg.id, amount, ttl)
				}
				if errors.Is(err, ErrInsufficientBudget) {
					continue
				}
				if err != nil {
					fail("reserve: %v", err)
					return
				}
				actual := money.Micros(rng.Int63n(int64(amount) + 1)) // never above the reservation
				switch rng.Intn(5) {
				case 0:
					if err := m.Release(r.ID); !tolerated(err) {
						fail("release: %v", err)
					}
				case 1:
					_, err := m.Reconcile(r.ID, actual)
					if errors.Is(err, ErrTerminalState) {
						_, err = m.ResolveUnknownReconciled(r.ID, actual)
					}
					if !tolerated(err) {
						fail("reconcile: %v", err)
					}
				case 2:
					if err := m.RecordPendingActual(r.ID, actual); !tolerated(err) {
						fail("pending: %v", err)
					}
				case 3:
					time.Sleep(time.Duration(rng.Intn(30)) * time.Millisecond) // let it expire first
					if err := m.ResolveUnknownReleased(r.ID); !tolerated(err) {
						fail("resolve released: %v", err)
					}
				default: // abandon: TTL expiry takes over
				}
			}
		}(w)
	}
	wg.Wait()
	close(stop)
	bg.Wait()
	if failures.Load() != 0 {
		t.FailNow()
	}

	// Quiesce: expire everything, then recover every durable pending actual.
	time.Sleep(60 * time.Millisecond)
	if _, err := m1.ExpireStaleE(); err != nil {
		t.Fatal(err)
	}
	if _, err := m1.RecoverPending(); err != nil {
		t.Fatalf("final recovery: %v", err)
	}
	if n, err := m1.PendingCount(context.Background()); err != nil || n != 0 {
		var leftovers int
		_ = db.QueryRow(`SELECT count(*) FROM reservations WHERE pending_actual_minor_units IS NOT NULL AND state IN ('RESERVED','UNKNOWN') AND tenant_id = ANY($1::uuid[])`, pq.Array(needles)).Scan(&leftovers)
		if leftovers != 0 {
			t.Fatalf("%d pending actuals left unrecovered", leftovers)
		}
	}
	for _, c := range cfgs {
		e := expo(t, m1, c.id)
		if e.Reserved < 0 || e.Settled < 0 {
			t.Fatalf("negative balance: %+v", e)
		}
		if e.Reserved+e.Settled > e.Limit {
			t.Fatalf("tenant %s overspent: reserved %d + settled %d > limit %d", c.id, e.Reserved, e.Settled, e.Limit)
		}
	}
	// No double settlement anywhere, no cross-tenant leakage, ledger reconciles to the event log.
	var dupes int
	if err := db.QueryRow(`
		SELECT count(*) FROM (SELECT e.reservation_id FROM reservation_events e
		  JOIN reservations r ON r.id = e.reservation_id
		 WHERE r.tenant_id = ANY($1::uuid[]) AND e.to_state = 'RECONCILED'
		 GROUP BY e.reservation_id HAVING count(*) > 1) x`, pq.Array(needles)).Scan(&dupes); err != nil || dupes != 0 {
		t.Fatalf("duplicate settlements=%d err=%v", dupes, err)
	}
	requireClean(t, db, needles...)
}
