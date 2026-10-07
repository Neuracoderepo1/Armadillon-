package gateway

import (
	"context"
	"errors"
	"testing"
	"time"

	"velocityguard/internal/ledger"
	"velocityguard/internal/money"
	"velocityguard/internal/pricing"
	"velocityguard/internal/provider"
	"velocityguard/internal/reservation"
	"velocityguard/internal/risk"
)

type fnProvider struct {
	fn func(ctx context.Context, req provider.ExecRequest) (provider.Response, provider.Usage, error)
}

func (p fnProvider) Name() string { return "demo-provider" }
func (p fnProvider) Execute(ctx context.Context, req provider.ExecRequest) (provider.Response, provider.Usage, error) {
	return p.fn(ctx, req)
}

const tenant = "t1"

func newGW(t *testing.T, svc reservation.Service, prov provider.Provider) (*Gateway, *ledger.Ledger) {
	t.Helper()
	svc.SetBudget(tenant, money.FromFloat(10))
	pr := pricing.NewRegistry()
	pr.AddRate(pricing.Rate{Provider: "demo-provider", Model: "m",
		InputPerUnit: money.FromFloat(0.0001), OutputPerUnit: money.FromFloat(0.0002),
		Version: 1, EffectiveFrom: time.Unix(0, 0)})
	regs := provider.NewRegistry()
	if prov != nil {
		regs.Register(prov)
	}
	l := ledger.New()
	return New(risk.NewEngine(svc, risk.DefaultPolicy()), svc, pr, regs, l), l
}

var route = RouteConfig{Route: "/r", Provider: "demo-provider", Model: "m"}

func call(g *Gateway) RequestResult {
	return g.HandleRequest(context.Background(), "req-1", tenant, route, provider.ExecRequest{Method: "POST"}, 100, 100, 1)
}

func hasEvent(l *ledger.Ledger, typ ledger.EventType) bool {
	for _, e := range l.Since(0) {
		if e.Type == typ {
			return true
		}
	}
	return false
}

func TestGateway_AmbiguousProviderFailure_KeepsHoldAndMarksUnknown(t *testing.T) {
	m := reservation.NewManager()
	g, l := newGW(t, m, fnProvider{func(context.Context, provider.ExecRequest) (provider.Response, provider.Usage, error) {
		return provider.Response{}, provider.Usage{}, errors.New("read tcp: i/o timeout") // sent, answer lost
	}})
	res := call(g)
	if res.Err == nil {
		t.Fatal("expected the provider error to surface")
	}
	r, err := m.Get(res.Decision.ReservationID)
	if err != nil {
		t.Fatal(err)
	}
	if r.State != reservation.StateUnknown {
		t.Fatalf("state=%s, want UNKNOWN (hold must be kept)", r.State)
	}
	if e := m.Exposure(tenant); e.Reserved != r.Amount || e.Reserved == 0 {
		t.Fatalf("budget was returned after an ambiguous failure: %+v", e)
	}
	if !hasEvent(l, ledger.ReservationUnknown) || hasEvent(l, ledger.ReservationReleased) {
		t.Fatal("ledger must record UNKNOWN and must not record a release")
	}
}

func TestGateway_ProvenNotDispatched_ReleasesHold(t *testing.T) {
	m := reservation.NewManager()
	g, l := newGW(t, m, fnProvider{func(context.Context, provider.ExecRequest) (provider.Response, provider.Usage, error) {
		return provider.Response{}, provider.Usage{}, provider.NotDispatched(errors.New("dial tcp: connection refused"))
	}})
	res := call(g)
	if res.Err == nil {
		t.Fatal("expected error")
	}
	r, _ := m.Get(res.Decision.ReservationID)
	if r.State != reservation.StateReleased {
		t.Fatalf("state=%s, want RELEASED", r.State)
	}
	if e := m.Exposure(tenant); e.Reserved != 0 {
		t.Fatalf("hold not returned: %+v", e)
	}
	if !hasEvent(l, ledger.ReservationReleased) {
		t.Fatal("missing release event")
	}
}

func TestGateway_UnregisteredProvider_ReleasesHold(t *testing.T) {
	m := reservation.NewManager()
	g, _ := newGW(t, m, nil)
	res := call(g)
	if res.Err == nil {
		t.Fatal("expected error")
	}
	if e := m.Exposure(tenant); e.Reserved != 0 {
		t.Fatalf("hold not returned: %+v", e)
	}
}

// Wraps a Service so the test can inject faults between provider success and settlement.
type faulty struct {
	reservation.Service
	beforeReconcile func(id string)
	failReconcile   bool
}

func (f faulty) Reconcile(id string, a money.Micros) (reservation.ReconcileResult, error) {
	if f.beforeReconcile != nil {
		f.beforeReconcile(id)
	}
	if f.failReconcile {
		return reservation.ReconcileResult{}, errors.New("database unavailable")
	}
	return f.Service.Reconcile(id, a)
}
func (f faulty) ResolveUnknownReconciled(id string, a money.Micros) (reservation.ReconcileResult, error) {
	if f.failReconcile {
		return reservation.ReconcileResult{}, errors.New("database unavailable")
	}
	return f.Service.ResolveUnknownReconciled(id, a)
}

var okProvider = fnProvider{func(context.Context, provider.ExecRequest) (provider.Response, provider.Usage, error) {
	return provider.Response{StatusCode: 200}, provider.Usage{InputUnits: 100, OutputUnits: 100}, nil
}}

func TestGateway_SlowSuccessAfterTTLExpiry_SettlesFromUnknown(t *testing.T) {
	m := reservation.NewManager()
	f := faulty{Service: m, beforeReconcile: func(id string) { _ = m.MarkUnknown(id) }} // TTL expiry raced the call
	g, l := newGW(t, f, okProvider)
	res := call(g)
	if res.Err != nil {
		t.Fatalf("a completed call must settle even if it outlived the TTL: %v", res.Err)
	}
	r, _ := m.Get(res.Decision.ReservationID)
	if r.State != reservation.StateReconciled {
		t.Fatalf("state=%s", r.State)
	}
	if e := m.Exposure(tenant); e.Reserved != 0 || e.Settled != res.ActualCost || e.Settled == 0 {
		t.Fatalf("exposure %+v actual %d", e, res.ActualCost)
	}
	if !hasEvent(l, ledger.CostReconciled) {
		t.Fatal("missing reconcile event")
	}
}

func TestGateway_ReconcileFailure_PendingActualSurvivesAndBlocksExpiryAndRelease(t *testing.T) {
	m := reservation.NewManager()
	g, _ := newGW(t, faulty{Service: m, failReconcile: true}, okProvider)
	res := call(g)
	if res.Err == nil || !res.Executed {
		t.Fatalf("expected an executed-but-unreconciled result, got %+v", res)
	}
	id := res.Decision.ReservationID
	r, _ := m.Get(id)
	if r.PendingActual == nil || *r.PendingActual != res.ActualCost {
		t.Fatalf("pending actual not recorded: %+v", r)
	}
	// Neither TTL expiry nor a release may discard the recorded provider result.
	time.Sleep(5 * time.Millisecond)
	_ = m.ExpireStale()
	if r, _ := m.Get(id); r.State != reservation.StateReserved {
		t.Fatalf("expiry touched a reservation with a pending actual: %s", r.State)
	}
	if err := m.Release(id); !errors.Is(err, reservation.ErrPendingActualExists) {
		t.Fatalf("release of a billed reservation must be refused, got %v", err)
	}
	// Recovery (here: a direct reconcile with the recorded value) settles it exactly once.
	if _, err := m.Reconcile(id, res.ActualCost); err != nil {
		t.Fatal(err)
	}
	if e := m.Exposure(tenant); e.Reserved != 0 || e.Settled != res.ActualCost {
		t.Fatalf("%+v", e)
	}
}
