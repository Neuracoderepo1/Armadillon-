package reservation_test

// End-to-end durability tests: the real gateway + risk engine running on the
// durable PGManager, with deliberate failures injected at the points where
// money can be lost. They run in the isolated schema created by the internal
// package's TestMain and skip when VG_TEST_POSTGRES_DSN is unset.

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/lib/pq"

	"velocityguard/internal/gateway"
	"velocityguard/internal/ledger"
	"velocityguard/internal/money"
	"velocityguard/internal/pricing"
	"velocityguard/internal/provider"
	"velocityguard/internal/reservation"
	"velocityguard/internal/risk"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func e2eDSN(t *testing.T) string {
	t.Helper()
	dsn := reservation.IsolatedDSN()
	if dsn == "" {
		t.Skip("VG_TEST_POSTGRES_DSN not set; skipping live Postgres end-to-end tests")
	}
	return dsn
}

func openPG(t *testing.T, dsn string) (*reservation.PGManager, *sql.DB) {
	t.Helper()
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(30)
	t.Cleanup(func() { _ = db.Close() })
	m := reservation.NewPGManager(db, quiet())
	m.SetOpTimeout(15 * time.Second)
	return m, db
}

var seq atomic.Int64

func tenantWithBudget(t *testing.T, m *reservation.PGManager, db *sql.DB, limit money.Micros) string {
	t.Helper()
	slug := fmt.Sprintf("e2e-%d-%d", time.Now().UnixNano(), seq.Add(1))
	var id string
	if err := db.QueryRow(`INSERT INTO tenants (name, slug) VALUES ($1,$1) RETURNING id::text`, slug).Scan(&id); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Exec(`DELETE FROM tenants WHERE id=$1`, id) })
	if err := m.SetBudgetE(id, limit); err != nil {
		t.Fatal(err)
	}
	return id
}

func rateRegistry() *pricing.Registry {
	pr := pricing.NewRegistry()
	pr.AddRate(pricing.Rate{Provider: "p", Model: "m",
		InputPerUnit: money.FromFloat(0.0001), OutputPerUnit: money.FromFloat(0.0002),
		Version: 1, EffectiveFrom: time.Unix(0, 0)})
	return pr
}

type fnProvider func(ctx context.Context) (provider.Response, provider.Usage, error)

func (fnProvider) Name() string { return "p" }
func (f fnProvider) Execute(ctx context.Context, _ provider.ExecRequest) (provider.Response, provider.Usage, error) {
	return f(ctx)
}

var okUsage = provider.Usage{InputUnits: 100, OutputUnits: 100} // $0.03

func okProv(context.Context) (provider.Response, provider.Usage, error) {
	return provider.Response{StatusCode: 200}, okUsage, nil
}

func newGateway(svc reservation.Service, prov provider.Provider, policy risk.Policy) *gateway.Gateway {
	regs := provider.NewRegistry()
	regs.Register(prov)
	return gateway.New(risk.NewEngine(svc, policy), svc, rateRegistry(), regs, ledger.New())
}

var route = gateway.RouteConfig{Route: "/r", Provider: "p", Model: "m"}

func handle(g *gateway.Gateway, id, tenant string) gateway.RequestResult {
	return g.HandleRequest(context.Background(), id, tenant, route, provider.ExecRequest{Method: "POST"}, 100, 100, 1)
}

func audit(t *testing.T, db *sql.DB, tenant string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	all, err := reservation.AuditPG(ctx, db)
	if err != nil {
		t.Fatalf("AuditPG: %v", err)
	}
	var mine []string
	for _, v := range all {
		if strings.Contains(v, tenant) {
			mine = append(mine, v)
		}
	}
	return mine
}

func exposure(t *testing.T, m *reservation.PGManager, tenant string) reservation.Exposure {
	t.Helper()
	e, err := m.ExposureE(tenant)
	if err != nil {
		t.Fatal(err)
	}
	return e
}

// crashAfterProvider simulates the process dying after the provider answered and
// the pending actual was durably recorded, but before settlement.
type crashAfterProvider struct{ reservation.Service }

func (crashAfterProvider) Reconcile(string, money.Micros) (reservation.ReconcileResult, error) {
	return reservation.ReconcileResult{}, errors.New("process died")
}
func (crashAfterProvider) ResolveUnknownReconciled(string, money.Micros) (reservation.ReconcileResult, error) {
	return reservation.ReconcileResult{}, errors.New("process died")
}

func countEvents(t *testing.T, db *sql.DB, id, toState string) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM reservation_events WHERE reservation_id=$1 AND to_state=$2 AND from_state IS DISTINCT FROM to_state`, id, toState).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestE2E_HappyPath_SettlesDurably(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	g := newGateway(m, fnProvider(okProv), risk.DefaultPolicy())

	res := handle(g, "r1", tn)
	if res.Err != nil || !res.Executed {
		t.Fatalf("result %+v", res)
	}
	e := exposure(t, m, tn)
	if e.Reserved != 0 || e.Settled != res.ActualCost || res.ActualCost == 0 {
		t.Fatalf("exposure %+v actual %d", e, res.ActualCost)
	}
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}

func TestE2E_CrashAfterProviderSuccess_RecoveredOnRestart(t *testing.T) {
	dsn := e2eDSN(t)
	m1, db := openPG(t, dsn)
	tn := tenantWithBudget(t, m1, db, money.FromFloat(10))

	// Process #1: provider succeeds and bills, the pending actual is recorded, then it dies.
	g := newGateway(crashAfterProvider{m1}, fnProvider(okProv), risk.DefaultPolicy())
	res := handle(g, "crash-1", tn)
	if res.Err == nil || !res.Executed {
		t.Fatalf("expected an executed-but-unsettled result, got %+v", res)
	}
	id := res.Decision.ReservationID
	r, err := m1.Get(id)
	if err != nil || r.State != reservation.StateReserved || r.PendingActual == nil || *r.PendingActual != res.ActualCost {
		t.Fatalf("durable pending actual missing: %+v err=%v", r, err)
	}
	if e := exposure(t, m1, tn); e.Reserved != r.Amount || e.Settled != 0 {
		t.Fatalf("before recovery: %+v", e)
	}

	// Process #2: brand-new pool and manager; startup recovery runs before readiness.
	m2, db2 := openPG(t, dsn)
	mt := reservation.NewMaintenance(m2, quiet())
	if err := mt.Ready(context.Background()); err == nil {
		t.Fatal("service must not be ready before startup recovery completes")
	}
	if err := mt.RunOnce(context.Background()); err != nil {
		t.Fatalf("startup recovery: %v", err)
	}
	if err := mt.Ready(context.Background()); err != nil {
		t.Fatalf("ready after recovery: %v", err)
	}
	got, _ := m2.Get(id)
	if got.State != reservation.StateReconciled || got.ActualCost != res.ActualCost {
		t.Fatalf("not recovered: %+v", got)
	}
	e := exposure(t, m2, tn)
	if e.Reserved != 0 || e.Settled != res.ActualCost {
		t.Fatalf("after recovery: %+v", e)
	}
	if n := countEvents(t, db2, id, "RECONCILED"); n != 1 {
		t.Fatalf("reconcile events=%d", n)
	}
	// Idempotent: another pass changes nothing.
	if err := mt.RunOnce(context.Background()); err != nil {
		t.Fatal(err)
	}
	if e2 := exposure(t, m2, tn); e2 != e {
		t.Fatalf("second pass changed state: %+v -> %+v", e, e2)
	}
	if v := audit(t, db2, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}

func TestE2E_AmbiguousProviderFailure_NeverReturnsBudget(t *testing.T) {
	dsn := e2eDSN(t)
	m, db := openPG(t, dsn)
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	g := newGateway(m, fnProvider(func(context.Context) (provider.Response, provider.Usage, error) {
		return provider.Response{}, provider.Usage{}, errors.New("read tcp: i/o timeout") // sent; answer lost
	}), risk.DefaultPolicy())

	res := handle(g, "amb-1", tn)
	if res.Err == nil {
		t.Fatal("expected error")
	}
	id := res.Decision.ReservationID
	r, _ := m.Get(id)
	if r.State != reservation.StateUnknown {
		t.Fatalf("state=%s want UNKNOWN", r.State)
	}
	held := exposure(t, m, tn)
	if held.Reserved != r.Amount || held.Available >= held.Limit {
		t.Fatalf("hold was not kept: %+v", held)
	}
	// Maintenance must never release uncertain spend.
	mt := reservation.NewMaintenance(m, quiet())
	for i := 0; i < 3; i++ {
		if err := mt.RunOnce(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if e := exposure(t, m, tn); e != held {
		t.Fatalf("maintenance changed an UNKNOWN reservation: %+v -> %+v", held, e)
	}
	if r2, _ := m.Get(id); r2.State != reservation.StateUnknown {
		t.Fatalf("state=%s", r2.State)
	}
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}

func TestE2E_ProvenNotDispatched_ReleasesBudget(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	g := newGateway(m, fnProvider(func(context.Context) (provider.Response, provider.Usage, error) {
		return provider.Response{}, provider.Usage{}, provider.NotDispatched(errors.New("dial tcp: connection refused"))
	}), risk.DefaultPolicy())
	res := handle(g, "nd-1", tn)
	if res.Err == nil {
		t.Fatal("expected error")
	}
	r, _ := m.Get(res.Decision.ReservationID)
	if r.State != reservation.StateReleased {
		t.Fatalf("state=%s", r.State)
	}
	if e := exposure(t, m, tn); e.Reserved != 0 || e.Settled != 0 {
		t.Fatalf("%+v", e)
	}
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}

// A provider call that outlives the reservation TTL: expiry moves the hold to
// UNKNOWN mid-call; the completed call must still settle at its real cost.
func TestE2E_SlowSuccessOutlivesTTL_StillSettles(t *testing.T) {
	m, db := openPG(t, e2eDSN(t))
	tn := tenantWithBudget(t, m, db, money.FromFloat(10))
	policy := risk.DefaultPolicy()
	policy.ReservationTTL = 30 * time.Millisecond
	g := newGateway(m, fnProvider(func(context.Context) (provider.Response, provider.Usage, error) {
		time.Sleep(250 * time.Millisecond)
		return okProv(context.Background())
	}), policy)

	done := make(chan struct{})
	go func() { // the maintenance loop runs while the call is in flight
		defer close(done)
		for i := 0; i < 10; i++ {
			time.Sleep(40 * time.Millisecond)
			_, _ = m.ExpireStaleE()
		}
	}()
	res := handle(g, "slow-1", tn)
	<-done
	if res.Err != nil || !res.Executed {
		t.Fatalf("a completed call must settle: %+v", res)
	}
	r, _ := m.Get(res.Decision.ReservationID)
	if r.State != reservation.StateReconciled || r.ActualCost != res.ActualCost {
		t.Fatalf("%+v", r)
	}
	if e := exposure(t, m, tn); e.Reserved != 0 || e.Settled != res.ActualCost {
		t.Fatalf("%+v", e)
	}
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}

// Two gateway instances (two pools, two managers) hammering one small budget:
// the sum of settled + reserved can never exceed the limit, and the ledger audits clean.
func TestE2E_ConcurrentGateways_NeverOverspend(t *testing.T) {
	dsn := e2eDSN(t)
	m1, db := openPG(t, dsn)
	m2, _ := openPG(t, dsn)
	limit := money.FromFloat(0.50) // room for ~16 calls of $0.03
	tn := tenantWithBudget(t, m1, db, limit)
	pol := risk.DefaultPolicy()
	g1 := newGateway(m1, fnProvider(okProv), pol)
	g2 := newGateway(m2, fnProvider(okProv), pol)

	var wg sync.WaitGroup
	var executed atomic.Int64
	for i := 0; i < 80; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			g := g1
			if i%2 == 1 {
				g = g2
			}
			if r := handle(g, fmt.Sprintf("c-%d", i), tn); r.Executed && r.Err == nil {
				executed.Add(1)
			}
		}(i)
	}
	wg.Wait()
	e := exposure(t, m1, tn)
	if e.Reserved+e.Settled > limit {
		t.Fatalf("overspend: reserved %d + settled %d > limit %d", e.Reserved, e.Settled, limit)
	}
	if executed.Load() == 0 {
		t.Fatal("no request executed; the test proved nothing")
	}
	if v := audit(t, db, tn); len(v) != 0 {
		t.Fatalf("audit: %v", v)
	}
}
