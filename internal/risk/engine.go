// Package risk implements the deterministic (non-ML) financial risk engine:
// velocity/acceleration tracking, risk scoring, and the ALLOW/THROTTLE/BLOCK
// decision. It deliberately holds no Redis or Postgres dependency on the
// decision path (spec section 4) — all state is in-process.
package risk

import (
	"sync"
	"time"

	"velocityguard/internal/circuit"
	"velocityguard/internal/money"
	"velocityguard/internal/reservation"
)

type Action string

const (
	ActionAllow          Action = "ALLOW"
	ActionAllowWithLimit Action = "ALLOW_WITH_LIMIT"
	ActionThrottle       Action = "THROTTLE"
	ActionBlock          Action = "BLOCK"
	ActionKillSwitch     Action = "KILL_SWITCH"
)

type Level string

const (
	LevelLow      Level = "LOW"
	LevelMedium   Level = "MEDIUM"
	LevelHigh     Level = "HIGH"
	LevelCritical Level = "CRITICAL"
)

// Weights controls the (documented, configurable) contribution of each risk
// factor to the final 0-100 score. Spec section 8: "do not hard-code the
// exact weighting without documenting it."
type Weights struct {
	BudgetUtilization float64 // weight applied to (spend / limit) * 100
	Velocity          float64 // weight applied to min(velocityMultiplier/target, 1) * 100
	Acceleration      float64 // weight applied to normalized positive acceleration
	Concurrency       float64 // weight applied to (concurrent / maxConcurrency)
}

// DefaultWeights sum to 1.0 so the blended score stays in 0-100.
// Velocity is weighted most heavily because sudden spend acceleration is
// VelocityGuard's core differentiator (spec section 3) — a tenant can be
// under budget in absolute terms and still be actively dangerous.
var DefaultWeights = Weights{
	BudgetUtilization: 0.30,
	Velocity:          0.40,
	Acceleration:      0.20,
	Concurrency:       0.10,
}

type Policy struct {
	Weights            Weights
	VelocityTargetMult float64 // velocity multiplier that alone maps to a 100 velocity-subscore (e.g. 20x)
	MaxConcurrency     int     // used to normalize concurrency subscore
	WarnThreshold      int     // risk score >= this -> at least MEDIUM handling / alert
	BlockThreshold     int     // risk score >= this -> BLOCK
	ThrottleThreshold  int     // risk score >= this (and < BlockThreshold) -> THROTTLE
	ReservationTTL     time.Duration
	CircuitCooldown    time.Duration
	ProjectionHorizon  time.Duration

	// Rolling window durations for velocity/acceleration tracking (spec
	// section 9). Production should use the spec defaults; tests may
	// compress these so decay-based scenarios don't need real-time sleeps.
	ShortWindow  time.Duration
	MediumWindow time.Duration
	LongWindow   time.Duration
}

func DefaultPolicy() Policy {
	return Policy{
		Weights:            DefaultWeights,
		VelocityTargetMult: 20.0,
		MaxConcurrency:     50,
		WarnThreshold:      70,
		ThrottleThreshold:  70,
		BlockThreshold:     90,
		ReservationTTL:     30 * time.Second,
		CircuitCooldown:    15 * time.Second,
		ProjectionHorizon:  60 * time.Second,
		ShortWindow:        10 * time.Second,
		MediumWindow:       60 * time.Second,
		LongWindow:         5 * time.Minute,
	}
}

// Request is the input to a risk decision.
type Request struct {
	TenantID      string
	Route         string
	Provider      string
	EstimatedCost money.Micros
	Concurrency   int // current in-flight requests for this tenant, caller-supplied
}

// Decision is the full explainable output (spec section 7).
type Decision struct {
	Action            Action
	EstimatedCost     money.Micros
	CurrentExposure   money.Micros
	ReservedExposure  money.Micros
	RemainingBudget   money.Micros
	RiskScore         int
	RiskLevel         Level
	VelocityMult      float64
	Acceleration      float64
	ProjectedExposure money.Micros
	RuleTriggered     string
	ReservationID     string
	CircuitState      circuit.State
	Reason            string
}

type tenantState struct {
	tracker *MultiWindowTracker
	breaker *circuit.Breaker
}

// Engine ties together reservations, rolling-window velocity tracking, and
// the circuit breaker into the single Evaluate() decision described in
// spec section 7.
type Engine struct {
	mu           sync.Mutex
	tenants      map[string]*tenantState
	reserv       reservation.Service
	policy       Policy
	killAll      bool
	killTenant   map[string]bool
	killProvider map[string]bool
}

func NewEngine(reserv reservation.Service, policy Policy) *Engine {
	return &Engine{
		tenants:      make(map[string]*tenantState),
		reserv:       reserv,
		policy:       policy,
		killTenant:   make(map[string]bool),
		killProvider: make(map[string]bool),
	}
}

func (e *Engine) stateFor(tenantID string) *tenantState {
	e.mu.Lock()
	defer e.mu.Unlock()
	ts, ok := e.tenants[tenantID]
	if !ok {
		ts = &tenantState{
			tracker: NewMultiWindowTrackerWithWindows(e.policy.ShortWindow, e.policy.MediumWindow, e.policy.LongWindow),
			breaker: circuit.New(e.policy.CircuitCooldown),
		}
		e.tenants[tenantID] = ts
	}
	return ts
}

// --- Kill switch (spec section 22) ---

func (e *Engine) KillAll(on bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.killAll = on
}

func (e *Engine) KillTenant(tenantID string, on bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.killTenant[tenantID] = on
}

func (e *Engine) KillProvider(provider string, on bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.killProvider[provider] = on
}

func (e *Engine) killSwitchActive(tenantID, provider string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.killAll || e.killTenant[tenantID] || e.killProvider[provider]
}

// Evaluate is the core decision function: Estimate -> Reserve -> Risk Check
// -> Allow/Throttle/Block (spec section 0's central pipeline).
func (e *Engine) Evaluate(req Request) Decision {
	ts := e.stateFor(req.TenantID)

	// Kill switch short-circuits everything else (spec: "must propagate
	// rapidly" — here it's an in-process check, i.e. instant).
	if e.killSwitchActive(req.TenantID, req.Provider) {
		ts.breaker.Trip()
		return Decision{
			Action:        ActionKillSwitch,
			EstimatedCost: req.EstimatedCost,
			CircuitState:  ts.breaker.State(),
			RuleTriggered: "KILL_SWITCH_ACTIVE",
			Reason:        "kill switch is active for this tenant/provider",
		}
	}

	// Circuit breaker gate.
	if !ts.breaker.Allow() {
		exp := e.reserv.Exposure(req.TenantID)
		return Decision{
			Action:           ActionBlock,
			EstimatedCost:    req.EstimatedCost,
			CurrentExposure:  exp.Settled,
			ReservedExposure: exp.Reserved,
			RemainingBudget:  exp.Available,
			CircuitState:     ts.breaker.State(),
			RuleTriggered:    "CIRCUIT_OPEN",
			Reason:           "circuit breaker is open; rejecting without evaluating further",
		}
	}

	velocityMult := ts.tracker.VelocityMultiplier()
	accel := ts.tracker.Acceleration()
	exp := e.reserv.Exposure(req.TenantID)
	projected := ts.tracker.ProjectedExposure(exp.Settled+exp.Reserved, e.policy.ProjectionHorizon)

	score, rule := e.score(req, ts, exp, velocityMult, accel)
	level := levelFor(score)

	d := Decision{
		EstimatedCost:     req.EstimatedCost,
		CurrentExposure:   exp.Settled,
		ReservedExposure:  exp.Reserved,
		RemainingBudget:   exp.Available,
		RiskScore:         score,
		RiskLevel:         level,
		VelocityMult:      velocityMult,
		Acceleration:      accel,
		ProjectedExposure: projected,
		RuleTriggered:     rule,
		CircuitState:      ts.breaker.State(),
	}

	switch {
	case score >= e.policy.BlockThreshold:
		d.Action = ActionBlock
		d.Reason = "risk score at or above block threshold"
		ts.breaker.ReportFailure()
		if levelFor(score) == LevelCritical {
			ts.breaker.Trip()
			d.CircuitState = ts.breaker.State()
		}
		return d

	case score >= e.policy.ThrottleThreshold:
		// A throttled request is still permitted to proceed (just
		// rate-limited/deprioritized in a full implementation with an
		// actual token bucket) — so it still consumes reservation budget.
		// This matters: without it, a client that keeps retrying at a
		// steady THROTTLE-level risk score would loop forever without ever
		// escalating, even while continuing to spend. If budget can't
		// cover it, that's a real reason to escalate straight to BLOCK.
		r, err := e.reserv.Reserve(req.TenantID, req.EstimatedCost, e.policy.ReservationTTL)
		if err != nil {
			d.Action = ActionBlock
			d.RuleTriggered = "INSUFFICIENT_BUDGET"
			d.Reason = "throttled request could not be covered by remaining budget: " + err.Error()
			ts.breaker.ReportFailure()
			return d
		}
		d.Action = ActionThrottle
		d.Reason = "risk score at or above throttle threshold"
		d.ReservationID = r.ID
		ts.breaker.ReportSuccess() // request is throttled, not failed outright
		return d

	default:
		// Attempt the reservation now that risk allows proceeding.
		r, err := e.reserv.Reserve(req.TenantID, req.EstimatedCost, e.policy.ReservationTTL)
		if err != nil {
			d.Action = ActionBlock
			d.RuleTriggered = "INSUFFICIENT_BUDGET"
			d.Reason = err.Error()
			ts.breaker.ReportFailure()
			return d
		}
		d.Action = ActionAllow
		d.Reason = "within policy limits"
		d.ReservationID = r.ID
		ts.breaker.ReportSuccess()
		return d
	}
}

// RecordUsage feeds an actual (reconciled) cost back into the velocity
// tracker so future decisions reflect real spend, not just estimates.
func (e *Engine) RecordUsage(tenantID string, cost money.Micros) {
	ts := e.stateFor(tenantID)
	ts.tracker.Record(cost)
}

func levelFor(score int) Level {
	switch {
	case score >= 90:
		return LevelCritical
	case score >= 70:
		return LevelHigh
	case score >= 40:
		return LevelMedium
	default:
		return LevelLow
	}
}

func (e *Engine) score(req Request, ts *tenantState, exp reservation.Exposure, velocityMult, accel float64) (int, string) {
	w := e.policy.Weights

	// Budget utilization subscore: how much of the limit is already
	// committed (reserved+settled), including this request's estimate.
	var budgetSub float64
	if exp.Limit > 0 {
		used := exp.Reserved + exp.Settled + req.EstimatedCost
		budgetSub = clamp01(used.Float()/exp.Limit.Float()) * 100
	}

	// Velocity subscore: normalized against the configured target multiplier.
	velocitySub := clamp01(velocityMult/e.policy.VelocityTargetMult) * 100

	// Acceleration subscore: only positive acceleration (spend still
	// ramping) counts as risk; normalize against the baseline so it's
	// dimensionless.
	baseline := ts.tracker.Baseline()
	var accelSub float64
	if accel > 0 {
		denom := baseline
		if denom <= 0 {
			denom = 1
		}
		accelSub = clamp01(accel/denom) * 100
	}

	// Concurrency subscore.
	var concurrencySub float64
	if e.policy.MaxConcurrency > 0 {
		concurrencySub = clamp01(float64(req.Concurrency)/float64(e.policy.MaxConcurrency)) * 100
	}

	total := w.BudgetUtilization*budgetSub +
		w.Velocity*velocitySub +
		w.Acceleration*accelSub +
		w.Concurrency*concurrencySub

	score := int(total)
	if score > 100 {
		score = 100
	}
	if score < 0 {
		score = 0
	}

	rule := "COMPOSITE_SCORE"
	switch {
	case budgetSub >= 99:
		rule = "BUDGET_EXHAUSTED"
	case velocitySub >= 99:
		rule = "ABNORMAL_SPEND_ACCELERATION"
	case accelSub >= 80:
		rule = "SUSTAINED_ACCELERATION"
	}
	return score, rule
}

func clamp01(f float64) float64 {
	if f < 0 {
		return 0
	}
	if f > 1 {
		return 1
	}
	return f
}
