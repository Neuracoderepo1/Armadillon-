// Package reservation implements the financial reservation system that
// prevents concurrent requests from overspending a tenant's budget.
//
// Correctness invariant (tested under -race with 1000s of concurrent
// goroutines): at no point can total (reserved + settled) exceed the
// tenant's configured limit.
package reservation

import (
	"errors"
	"sync"
	"time"

	"velocityguard/internal/money"
)

// State is a reservation's position in its financial lifecycle.
//
// RESERVED  -- Reconcile(actual) --> RECONCILED   (definite: usage was reported)
// RESERVED  -- Release() before dispatch --> RELEASED   (definite: never sent upstream)
// RESERVED  -- TTL expiry (ExpireStale) --> UNKNOWN      (ambiguous: no reconciliation arrived in time)
// UNKNOWN   -- ResolveUnknownReleased() --> RELEASED   (recovery worker confirmed: never billed)
// UNKNOWN   -- ResolveUnknownReconciled(actual) --> RECONCILED (recovery worker confirmed: was billed, here's the cost)
//
// The critical safety rule (do not violate this): TTL expiry alone must
// NEVER return budget to the tenant. Once a reservation may have been
// dispatched to an upstream provider, a bare timeout does not tell us
// whether the call executed and was billed — only that reconciliation
// didn't arrive in time. Treating that ambiguity as "definitely not
// billed" (the old EXPIRED behavior) can let a tenant's budget silently
// refill while real spend is still outstanding upstream, defeating the
// entire purpose of reservations. UNKNOWN keeps the amount held
// (still counted in `reserved`, not returned to `available`) until a
// recovery worker resolves it by an out-of-band check (e.g. querying the
// provider for the actual outcome) — never by TTL alone.
type State string

const (
	StateReserved   State = "RESERVED"
	StateReconciled State = "RECONCILED"
	StateReleased   State = "RELEASED"
	// StateUnknown replaces the old StateExpired. See the State doc
	// comment above for why TTL expiry must land here, not on a state
	// that returns budget.
	StateUnknown State = "UNKNOWN"
)

var (
	ErrInsufficientBudget = errors.New("reservation: insufficient remaining budget")
	ErrNotFound           = errors.New("reservation: not found")
	ErrWrongTenant        = errors.New("reservation: tenant mismatch")
	ErrTerminalState      = errors.New("reservation: already in a terminal state")
)

type Reservation struct {
	ID        string
	TenantID  string
	Amount    money.Micros // originally reserved
	State     State
	CreatedAt time.Time
	ExpiresAt time.Time

	// Set once reconciled.
	ActualCost money.Micros
	Reconciled bool

	// Populated by the PostgreSQL implementation (PGManager). IdempotencyKey
	// is empty for unkeyed reservations. PendingActual is a provider result
	// that was durably recorded but not yet reconciled (see RecordPendingActual).
	IdempotencyKey string
	PendingActual  *money.Micros
}

// account holds the live financial state for one tenant. All mutation goes
// through the account mutex, which is the sole source of atomicity for the
// "never overcommit" invariant.
type account struct {
	mu       sync.Mutex
	limit    money.Micros // total budget for the accounting period
	reserved money.Micros // sum of amounts currently in RESERVED state
	settled  money.Micros // sum of RECONCILED actual costs (no longer "reserved", but spent)
}

func (a *account) available() money.Micros {
	return a.limit - a.reserved - a.settled
}

type Manager struct {
	mu           sync.RWMutex
	accounts     map[string]*account
	reservations map[string]*Reservation
	idCounter    uint64
	idMu         sync.Mutex
	now          func() time.Time // overridable for tests
}

func NewManager() *Manager {
	return &Manager{
		accounts:     make(map[string]*account),
		reservations: make(map[string]*Reservation),
		now:          time.Now,
	}
}

// SetBudget sets (or resets) a tenant's total budget for the current period.
// In a full system this would be driven by the budget_accounts table; here
// it's the in-memory source of truth the risk engine and gateway consult.
func (m *Manager) SetBudget(tenantID string, limit money.Micros) {
	m.mu.Lock()
	defer m.mu.Unlock()
	acc, ok := m.accounts[tenantID]
	if !ok {
		acc = &account{}
		m.accounts[tenantID] = acc
	}
	acc.mu.Lock()
	acc.limit = limit
	acc.mu.Unlock()
}

func (m *Manager) getAccount(tenantID string) *account {
	m.mu.RLock()
	acc, ok := m.accounts[tenantID]
	m.mu.RUnlock()
	if ok {
		return acc
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if acc, ok := m.accounts[tenantID]; ok {
		return acc
	}
	acc = &account{}
	m.accounts[tenantID] = acc
	return acc
}

func (m *Manager) nextID() string {
	m.idMu.Lock()
	defer m.idMu.Unlock()
	m.idCounter++
	return time.Now().UTC().Format("20060102T150405.000000000") + "-" + itoa(m.idCounter)
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	return string(buf[i:])
}

// Exposure reports the current financial state for a tenant.
type Exposure struct {
	Limit     money.Micros
	Reserved  money.Micros
	Settled   money.Micros
	Available money.Micros
}

func (m *Manager) Exposure(tenantID string) Exposure {
	acc := m.getAccount(tenantID)
	acc.mu.Lock()
	defer acc.mu.Unlock()
	return Exposure{
		Limit:     acc.limit,
		Reserved:  acc.reserved,
		Settled:   acc.settled,
		Available: acc.available(),
	}
}

// Reserve atomically checks and commits `amount` of exposure against the
// tenant's budget. This is the operation that must be race-free: under
// concurrent load, the sum of granted reservations + settled cost must
// never exceed the limit.
func (m *Manager) Reserve(tenantID string, amount money.Micros, ttl time.Duration) (*Reservation, error) {
	acc := m.getAccount(tenantID)

	acc.mu.Lock()
	if acc.available() < amount {
		acc.mu.Unlock()
		return nil, ErrInsufficientBudget
	}
	acc.reserved += amount
	acc.mu.Unlock()

	now := m.now()
	r := &Reservation{
		ID:        m.nextID(),
		TenantID:  tenantID,
		Amount:    amount,
		State:     StateReserved,
		CreatedAt: now,
		ExpiresAt: now.Add(ttl),
	}

	m.mu.Lock()
	m.reservations[r.ID] = r
	m.mu.Unlock()

	return r, nil
}

// Release fully releases a reservation without reconciling actual cost
// (e.g. the upstream request failed before execution / was blocked
// downstream / the client cancelled).
func (m *Manager) Release(reservationID string) error {
	m.mu.Lock()
	r, ok := m.reservations[reservationID]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	acc := m.getAccount(r.TenantID)
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if r.State != StateReserved {
		// Idempotent: releasing an already-terminal reservation is a no-op,
		// not an error, so retries from upstream failure handling are safe.
		return nil
	}
	if r.PendingActual != nil {
		return ErrPendingActualExists // the call completed; it must be reconciled, never released
	}
	acc.reserved -= r.Amount
	r.State = StateReleased
	return nil
}

// MarkUnknown moves a RESERVED reservation to UNKNOWN WITHOUT returning
// budget. The gateway calls it when an upstream call failed in a way that
// does not prove the request was never dispatched (timeout, connection
// reset, cancellation after send): the provider may have executed and
// billed it, so the hold must stay until a recovery path resolves it.
// UNKNOWN is idempotent; any other state is ErrTerminalState.
func (m *Manager) MarkUnknown(reservationID string) error {
	m.mu.Lock()
	r, ok := m.reservations[reservationID]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	acc := m.getAccount(r.TenantID)
	acc.mu.Lock()
	defer acc.mu.Unlock()
	switch r.State {
	case StateUnknown:
		return nil
	case StateReserved:
		r.State = StateUnknown
		return nil
	default:
		return ErrTerminalState
	}
}

// RecordPendingActual stores a provider result before reconciliation (the
// in-memory counterpart of the durable PGManager operation). While set,
// ExpireStale skips the reservation and Release refuses it.
func (m *Manager) RecordPendingActual(reservationID string, actual money.Micros) error {
	if actual < 0 {
		return ErrInvalidAmount
	}
	m.mu.Lock()
	r, ok := m.reservations[reservationID]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}
	acc := m.getAccount(r.TenantID)
	acc.mu.Lock()
	defer acc.mu.Unlock()
	switch r.State {
	case StateReconciled:
		if r.ActualCost != actual {
			return ErrConflictingReconcile
		}
		return nil
	case StateReleased:
		return ErrTerminalState
	}
	if r.PendingActual != nil {
		if *r.PendingActual != actual {
			return ErrConflictingReconcile
		}
		return nil
	}
	v := actual
	r.PendingActual = &v
	return nil
}

// ReconcileResult describes the financial effect of a reconciliation.
type ReconcileResult struct {
	Reserved money.Micros
	Actual   money.Micros
	Released money.Micros // positive: reserved amount returned to available budget
	Overage  money.Micros // positive: actual cost exceeded the reservation
}

// Reconcile records the actual provider-reported cost against a reservation.
// It is idempotent: calling it twice with the same reservationID has the
// financial effect applied exactly once; the second call returns the
// original result.
func (m *Manager) Reconcile(reservationID string, actualCost money.Micros) (ReconcileResult, error) {
	m.mu.Lock()
	r, ok := m.reservations[reservationID]
	m.mu.Unlock()
	if !ok {
		return ReconcileResult{}, ErrNotFound
	}

	acc := m.getAccount(r.TenantID)
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if r.Reconciled {
		// Already applied — return the same result, apply nothing further.
		return computeReconcileResult(r.Amount, r.ActualCost), nil
	}
	if r.State != StateReserved {
		return ReconcileResult{}, ErrTerminalState
	}
	if r.PendingActual != nil && *r.PendingActual != actualCost {
		return ReconcileResult{}, ErrConflictingReconcile
	}

	// Move the reserved amount out of "reserved" and into "settled" at the
	// actual cost. This is the single atomic step that prevents double
	// counting: reserved -= originalAmount, settled += actualCost.
	acc.reserved -= r.Amount
	acc.settled += actualCost

	r.ActualCost = actualCost
	r.Reconciled = true
	r.State = StateReconciled

	return computeReconcileResult(r.Amount, actualCost), nil
}

func computeReconcileResult(reserved, actual money.Micros) ReconcileResult {
	res := ReconcileResult{Reserved: reserved, Actual: actual}
	if actual < reserved {
		res.Released = reserved - actual
	} else if actual > reserved {
		res.Overage = actual - reserved
	}
	return res
}

// ExpireStale marks any RESERVED reservation whose TTL has passed as
// UNKNOWN — it does NOT return budget (see the State doc comment: TTL
// alone is not proof the upstream call never executed/was never billed).
// The amount stays held in `reserved` until a recovery worker calls
// ResolveUnknownReleased or ResolveUnknownReconciled based on an
// out-of-band check of what actually happened upstream.
func (m *Manager) ExpireStale() int {
	now := m.now()
	m.mu.Lock()
	var stale []*Reservation
	for _, r := range m.reservations {
		if r.State == StateReserved && r.PendingActual == nil && now.After(r.ExpiresAt) {
			stale = append(stale, r)
		}
	}
	m.mu.Unlock()

	for _, r := range stale {
		acc := m.getAccount(r.TenantID)
		acc.mu.Lock()
		if r.State == StateReserved && r.PendingActual == nil { // re-check under lock
			// Deliberately no acc.reserved change: the amount stays
			// held. This is the fix for the critical safety flaw where
			// TTL expiry silently returned budget for a reservation that
			// may already have been dispatched and billed upstream.
			r.State = StateUnknown
		}
		acc.mu.Unlock()
	}
	return len(stale)
}

// ResolveUnknownReleased is called by a recovery worker once it has
// confirmed, out-of-band, that an UNKNOWN reservation's request never
// actually executed upstream (e.g. the provider has no record of it) —
// so it's now safe to return the held budget. This is the ONLY path by
// which an UNKNOWN reservation returns budget; TTL alone can never do
// this (see ExpireStale).
func (m *Manager) ResolveUnknownReleased(reservationID string) error {
	m.mu.Lock()
	r, ok := m.reservations[reservationID]
	m.mu.Unlock()
	if !ok {
		return ErrNotFound
	}

	acc := m.getAccount(r.TenantID)
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if r.State == StateReleased {
		return nil // already resolved this way; idempotent
	}
	if r.State != StateUnknown {
		return ErrTerminalState
	}
	acc.reserved -= r.Amount
	r.State = StateReleased
	return nil
}

// ResolveUnknownReconciled is called by a recovery worker once it has
// confirmed, out-of-band, that an UNKNOWN reservation's request DID
// execute upstream and billed actualCost — so it's now safe to move the
// held amount from `reserved` to `settled` at the confirmed real cost,
// exactly as a normal Reconcile would have done if it had arrived in time.
func (m *Manager) ResolveUnknownReconciled(reservationID string, actualCost money.Micros) (ReconcileResult, error) {
	m.mu.Lock()
	r, ok := m.reservations[reservationID]
	m.mu.Unlock()
	if !ok {
		return ReconcileResult{}, ErrNotFound
	}

	acc := m.getAccount(r.TenantID)
	acc.mu.Lock()
	defer acc.mu.Unlock()

	if r.Reconciled {
		return computeReconcileResult(r.Amount, r.ActualCost), nil // idempotent
	}
	if r.State != StateUnknown {
		return ReconcileResult{}, ErrTerminalState
	}
	if r.PendingActual != nil && *r.PendingActual != actualCost {
		return ReconcileResult{}, ErrConflictingReconcile
	}

	acc.reserved -= r.Amount
	acc.settled += actualCost

	r.ActualCost = actualCost
	r.Reconciled = true
	r.State = StateReconciled

	return computeReconcileResult(r.Amount, actualCost), nil
}

func (m *Manager) Get(reservationID string) (*Reservation, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	r, ok := m.reservations[reservationID]
	if !ok {
		return nil, ErrNotFound
	}
	cp := *r
	return &cp, nil
}
