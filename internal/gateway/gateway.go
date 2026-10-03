// Package gateway implements the core request pipeline (spec section 0 and
// 14): estimate -> reserve -> risk check -> allow/throttle/block ->
// execute -> reconcile -> emit telemetry.
package gateway

import (
	"context"
	"fmt"
	"time"

	"velocityguard/internal/ledger"
	"velocityguard/internal/money"
	"velocityguard/internal/pricing"
	"velocityguard/internal/provider"
	"velocityguard/internal/reservation"
	"velocityguard/internal/risk"
)

type RouteConfig struct {
	Route    string
	Provider string
	Model    string
}

type Gateway struct {
	Risk      *risk.Engine
	Reserv    reservation.Repository
	Pricing   *pricing.Registry
	Providers *provider.Registry
	Ledger    *ledger.Ledger
}

func New(riskEngine *risk.Engine, reserv reservation.Repository, pr *pricing.Registry, providers *provider.Registry, l *ledger.Ledger) *Gateway {
	return &Gateway{Risk: riskEngine, Reserv: reserv, Pricing: pr, Providers: providers, Ledger: l}
}

type RequestResult struct {
	RequestID  string
	Decision   risk.Decision
	Executed   bool
	Response   provider.Response
	Usage      provider.Usage
	ActualCost money.Micros
	Reconcile  reservation.ReconcileResult
	Err        error
}

// HandleRequest runs one request through the full pipeline. estimatedUnits
// is a rough pre-call estimate (e.g. from request size / historical
// average) used only for the pre-execution cost estimate and risk check;
// actual cost is always reconciled from the provider's reported usage.
func (g *Gateway) HandleRequest(ctx context.Context, requestID, tenantID string, route RouteConfig, execReq provider.ExecRequest, estimatedInputUnits, estimatedOutputUnits int64, concurrency int) RequestResult {
	now := time.Now()

	rate, err := g.Pricing.RateAt(route.Provider, route.Model, now)
	if err != nil {
		g.Ledger.Append(ledger.Event{Type: ledger.RequestFailed, TenantID: tenantID, RequestID: requestID, Route: route.Route,
			Metadata: map[string]string{"error": err.Error()}})
		return RequestResult{RequestID: requestID, Err: fmt.Errorf("pricing lookup failed: %w", err)}
	}
	estimatedCost := rate.Estimate(estimatedInputUnits, estimatedOutputUnits)

	g.Ledger.Append(ledger.Event{Type: ledger.RequestEstimated, TenantID: tenantID, RequestID: requestID,
		Route: route.Route, Provider: route.Provider, Amount: estimatedCost})

	decision := g.Risk.Evaluate(risk.Request{
		TenantID:      tenantID,
		Route:         route.Route,
		Provider:      route.Provider,
		EstimatedCost: estimatedCost,
		Concurrency:   concurrency,
	})

	switch decision.Action {
	case risk.ActionAllow, risk.ActionAllowWithLimit:
		g.Ledger.Append(ledger.Event{Type: ledger.ReservationCreated, TenantID: tenantID, RequestID: requestID,
			ReservationID: decision.ReservationID, Amount: estimatedCost})
		g.Ledger.Append(ledger.Event{Type: ledger.RequestAllowed, TenantID: tenantID, RequestID: requestID,
			Route: route.Route, Provider: route.Provider, Amount: estimatedCost,
			Metadata: map[string]string{"risk_score": itoa(decision.RiskScore)}})

	case risk.ActionThrottle:
		if decision.ReservationID != "" {
			g.Ledger.Append(ledger.Event{Type: ledger.ReservationCreated, TenantID: tenantID, RequestID: requestID,
				ReservationID: decision.ReservationID, Amount: estimatedCost})
		}
		g.Ledger.Append(ledger.Event{Type: ledger.RequestThrottled, TenantID: tenantID, RequestID: requestID,
			Route: route.Route, Provider: route.Provider,
			Metadata: map[string]string{"risk_score": itoa(decision.RiskScore)}})
		return RequestResult{RequestID: requestID, Decision: decision}

	default: // BLOCK, KILL_SWITCH
		g.Ledger.Append(ledger.Event{Type: ledger.RequestBlocked, TenantID: tenantID, RequestID: requestID,
			Route: route.Route, Provider: route.Provider,
			Metadata: map[string]string{"reason": decision.Reason, "rule": decision.RuleTriggered}})
		return RequestResult{RequestID: requestID, Decision: decision}
	}

	// --- Execute ---
	p, err := g.Providers.Get(route.Provider)
	if err != nil {
		g.Reserv.Release(decision.ReservationID)
		g.Ledger.Append(ledger.Event{Type: ledger.ReservationReleased, TenantID: tenantID, RequestID: requestID,
			ReservationID: decision.ReservationID, Metadata: map[string]string{"reason": "provider not found"}})
		return RequestResult{RequestID: requestID, Decision: decision, Err: err}
	}

	resp, usage, execErr := p.Execute(ctx, execReq)
	if execErr != nil {
		g.Reserv.Release(decision.ReservationID)
		g.Ledger.Append(ledger.Event{Type: ledger.ReservationReleased, TenantID: tenantID, RequestID: requestID,
			ReservationID: decision.ReservationID, Metadata: map[string]string{"reason": "execution failed"}})
		g.Ledger.Append(ledger.Event{Type: ledger.RequestFailed, TenantID: tenantID, RequestID: requestID,
			Metadata: map[string]string{"error": execErr.Error()}})
		return RequestResult{RequestID: requestID, Decision: decision, Err: execErr}
	}
	g.Ledger.Append(ledger.Event{Type: ledger.RequestExecuted, TenantID: tenantID, RequestID: requestID,
		Route: route.Route, Provider: route.Provider})

	// --- Reconcile ---
	actualCost := rate.Estimate(usage.InputUnits, usage.OutputUnits)
	g.Ledger.Append(ledger.Event{Type: ledger.UsageReported, TenantID: tenantID, RequestID: requestID, Amount: actualCost})

	recResult, err := g.Reserv.Reconcile(decision.ReservationID, actualCost)
	if err != nil {
		g.Ledger.Append(ledger.Event{Type: ledger.RequestFailed, TenantID: tenantID, RequestID: requestID,
			Metadata: map[string]string{"error": "reconciliation failed: " + err.Error()}})
		return RequestResult{RequestID: requestID, Decision: decision, Executed: true, Response: resp, Usage: usage, ActualCost: actualCost, Err: err}
	}
	g.Ledger.Append(ledger.Event{Type: ledger.CostReconciled, TenantID: tenantID, RequestID: requestID,
		ReservationID: decision.ReservationID, Amount: actualCost,
		Metadata: map[string]string{"released": recResult.Released.String(), "overage": recResult.Overage.String()}})

	// Feed actual cost back into the risk engine's velocity tracker so
	// subsequent decisions reflect real spend, not just estimates.
	g.Risk.RecordUsage(tenantID, actualCost)

	return RequestResult{
		RequestID: requestID, Decision: decision, Executed: true,
		Response: resp, Usage: usage, ActualCost: actualCost, Reconcile: recResult,
	}
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
