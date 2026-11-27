// Package gateway implements the core request pipeline (spec section 0 and
// 14): estimate -> reserve -> risk check -> allow/throttle/block ->
// execute -> reconcile -> emit telemetry.
package gateway

import (
	"context"
	"errors"
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
	Reserv    reservation.Service
	Pricing   *pricing.Registry
	Providers *provider.Registry
	Ledger    *ledger.Ledger
}

func New(riskEngine *risk.Engine, reserv reservation.Service, pr *pricing.Registry, providers *provider.Registry, l *ledger.Ledger) *Gateway {
	return &Gateway{Risk: riskEngine, Reserv: reserv, Pricing: pr, Providers: providers, Ledger: l}
}

type DispatchOutcome string

const (
	// DispatchOutcomeNotDispatched means the provider was not called or proved
	// that no request reached the upstream.
	DispatchOutcomeNotDispatched DispatchOutcome = "not_dispatched"
	// DispatchOutcomeUnknownAfterDispatch means the request may have reached
	// and been processed by the upstream, but no complete response was obtained.
	DispatchOutcomeUnknownAfterDispatch DispatchOutcome = "unknown_after_dispatch"
	// DispatchOutcomeExecuted means an upstream response was received and usage
	// was available for settlement (settlement itself may still be pending).
	DispatchOutcomeExecuted DispatchOutcome = "executed"
)

type RequestResult struct {
	RequestID       string
	Decision        risk.Decision
	DispatchOutcome DispatchOutcome
	SettlementState string
	Executed        bool
	Response        provider.Response
	Usage           provider.Usage
	ActualCost      money.Micros
	Reconcile       reservation.ReconcileResult
	Err             error
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
		return RequestResult{RequestID: requestID, DispatchOutcome: DispatchOutcomeNotDispatched, Err: fmt.Errorf("pricing lookup failed: %w", err)}
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
		// NOTE (policy, deliberately unchanged): the hold of a throttled request
		// is kept. The risk engine relies on it so that repeated throttled
		// retries consume budget and eventually escalate to BLOCK (see
		// risk.Engine THROTTLE case and TestFinalAcceptanceScenario). The
		// hold is never returned automatically; it ends as UNKNOWN after its
		// TTL. Whether to release it instead is a product decision.
		return RequestResult{RequestID: requestID, Decision: decision, DispatchOutcome: DispatchOutcomeNotDispatched}

	default: // BLOCK, KILL_SWITCH
		g.Ledger.Append(ledger.Event{Type: ledger.RequestBlocked, TenantID: tenantID, RequestID: requestID,
			Route: route.Route, Provider: route.Provider,
			Metadata: map[string]string{"reason": decision.Reason, "rule": decision.RuleTriggered}})
		return RequestResult{RequestID: requestID, Decision: decision, DispatchOutcome: DispatchOutcomeNotDispatched}
	}

	// --- Execute ---
	p, err := g.Providers.Get(route.Provider)
	if err != nil {
		// Nothing was sent: proven non-dispatch.
		g.releaseNotDispatched(tenantID, requestID, decision.ReservationID, "provider not found")
		return RequestResult{RequestID: requestID, Decision: decision, DispatchOutcome: DispatchOutcomeNotDispatched, Err: err}
	}

	resp, usage, execErr := p.Execute(ctx, execReq)
	if execErr != nil {
		outcome := DispatchOutcomeUnknownAfterDispatch
		settlementState := ""
		failureMeta := map[string]string{"error": execErr.Error()}
		if provider.IsNotDispatched(execErr) {
			// The provider proved the request never left: safe to return the hold.
			outcome = DispatchOutcomeNotDispatched
			g.releaseNotDispatched(tenantID, requestID, decision.ReservationID, "not dispatched: "+execErr.Error())
		} else {
			// AMBIGUOUS: a timeout, reset, cancellation or response-body read
			// failure after dispatch does not prove that the upstream did not
			// process (and bill) the call. Keep the hold and expose UNKNOWN rather
			// than serializing Executed=false to the client.
			meta := map[string]string{"reason": "ambiguous provider failure; hold kept"}
			if merr := g.Reserv.MarkUnknown(decision.ReservationID); merr != nil {
				// Do not claim UNKNOWN unless the transition was confirmed. The
				// RESERVED hold remains in place and TTL recovery can retry it.
				meta["mark_unknown_error"] = merr.Error()
				failureMeta["mark_unknown_error"] = merr.Error()
				settlementState = "pending"
			} else {
				settlementState = "unknown"
				g.Ledger.Append(ledger.Event{Type: ledger.ReservationUnknown, TenantID: tenantID, RequestID: requestID,
					ReservationID: decision.ReservationID, Metadata: meta})
			}
		}
		g.Ledger.Append(ledger.Event{Type: ledger.RequestFailed, TenantID: tenantID, RequestID: requestID,
			Metadata: failureMeta})
		return RequestResult{RequestID: requestID, Decision: decision, DispatchOutcome: outcome, SettlementState: settlementState, Err: execErr}
	}
	g.Ledger.Append(ledger.Event{Type: ledger.RequestExecuted, TenantID: tenantID, RequestID: requestID,
		Route: route.Route, Provider: route.Provider})

	// --- Reconcile ---
	actualCost := rate.Estimate(usage.InputUnits, usage.OutputUnits)
	g.Ledger.Append(ledger.Event{Type: ledger.UsageReported, TenantID: tenantID, RequestID: requestID, Amount: actualCost})

	// Durably record the provider result BEFORE reconciling. If the process
	// dies, the database is unreachable, or reconcile fails past this point,
	// the recovery worker settles the reservation from this record, so a
	// billed call can never silently revert to unused budget.
	if perr := g.Reserv.RecordPendingActual(decision.ReservationID, actualCost); perr != nil {
		g.Ledger.Append(ledger.Event{Type: ledger.RequestFailed, TenantID: tenantID, RequestID: requestID,
			Metadata: map[string]string{"error": "pending actual not recorded: " + perr.Error()}})
	}

	recResult, err := g.Reserv.Reconcile(decision.ReservationID, actualCost)
	if errors.Is(err, reservation.ErrTerminalState) {
		// A slow provider call outlived the reservation TTL, so expiry already
		// moved it to UNKNOWN. We hold the real usage: settle it from UNKNOWN.
		recResult, err = g.Reserv.ResolveUnknownReconciled(decision.ReservationID, actualCost)
	}
	if err != nil {
		g.Ledger.Append(ledger.Event{Type: ledger.RequestFailed, TenantID: tenantID, RequestID: requestID,
			Metadata: map[string]string{"error": "reconciliation failed: " + err.Error()}})
		// The cost is real even though the ledger write failed; keep the
		// velocity tracker honest. The durable pending actual (if it was
		// recorded) lets recovery finish the settlement.
		g.Risk.RecordUsage(tenantID, actualCost)
		return RequestResult{RequestID: requestID, Decision: decision, DispatchOutcome: DispatchOutcomeExecuted, Executed: true, Response: resp, Usage: usage, ActualCost: actualCost, Err: err}
	}
	g.Ledger.Append(ledger.Event{Type: ledger.CostReconciled, TenantID: tenantID, RequestID: requestID,
		ReservationID: decision.ReservationID, Amount: actualCost,
		Metadata: map[string]string{"released": recResult.Released.String(), "overage": recResult.Overage.String()}})

	// Feed actual cost back into the risk engine's velocity tracker so
	// subsequent decisions reflect real spend, not just estimates.
	g.Risk.RecordUsage(tenantID, actualCost)

	return RequestResult{
		RequestID: requestID, Decision: decision, DispatchOutcome: DispatchOutcomeExecuted, Executed: true,
		Response: resp, Usage: usage, ActualCost: actualCost, Reconcile: recResult,
	}
}

// releaseNotDispatched returns the hold of a request that is PROVEN never to have
// reached the provider. It must not be used for ambiguous failures.
func (g *Gateway) releaseNotDispatched(tenantID, requestID, reservationID, reason string) {
	if reservationID == "" {
		return
	}
	meta := map[string]string{"reason": reason}
	if err := g.Reserv.Release(reservationID); err != nil {
		meta["release_error"] = err.Error()
	}
	g.Ledger.Append(ledger.Event{Type: ledger.ReservationReleased, TenantID: tenantID, RequestID: requestID,
		ReservationID: reservationID, Metadata: meta})
}

func itoa(n int) string {
	return fmt.Sprintf("%d", n)
}
