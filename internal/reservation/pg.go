package reservation

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"time"

	"github.com/lib/pq"

	"velocityguard/internal/money"
)

// Domain errors specific to the durable (PostgreSQL) implementation.
var (
	// ErrBackendUnavailable: the database could not be reached or the
	// statement failed for a non-domain reason. The operation had no
	// financial effect, except a connection lost *during COMMIT*, which is
	// genuinely ambiguous (see "Commit ambiguity" on PGManager).
	ErrBackendUnavailable = errors.New("reservation: backend unavailable")
	// ErrIdempotencyConflict: same tenant + same idempotency key, different amount.
	ErrIdempotencyConflict = errors.New("reservation: idempotency key reused with a different amount")
	// ErrConflictingReconcile: a different actual cost than the one already
	// recorded (pending or reconciled) was supplied.
	ErrConflictingReconcile = errors.New("reservation: conflicting actual cost")
	// ErrPendingActualExists: the reservation has a durably recorded provider
	// result, so it must be reconciled, never released.
	ErrPendingActualExists = errors.New("reservation: a pending actual cost exists; reconcile instead")
	ErrInvalidAmount       = errors.New("reservation: invalid amount")
	ErrInvalidTenant       = errors.New("reservation: invalid tenant")
	ErrInvalidTTL          = errors.New("reservation: invalid ttl")
	ErrInvalidKey          = errors.New("reservation: invalid idempotency key")
	// ErrBudgetBelowExposure: refusing to set a limit below reserved + settled.
	ErrBudgetBelowExposure = errors.New("reservation: budget limit is below current exposure")
	// ErrAmountOverflow: the operation would exceed the int64 monetary range.
	ErrAmountOverflow = errors.New("reservation: monetary amount overflow")
	// ErrLedgerCorrupt: stored balances contradict the reservation being
	// operated on. The transaction is rolled back; run AuditPG.
	ErrLedgerCorrupt = errors.New("reservation: ledger inconsistency detected")
)

// maxTTL bounds a reservation lifetime so the SQL interval arithmetic can
// never overflow.
const maxTTL = 365 * 24 * time.Hour

// PGManager is the durable, authoritative reservation ledger.
//
// Schema (migrations 0001-0003): budgets live in budget_accounts, one row per
// (tenant, period); a PGManager operates on one period (default "monthly").
// Amounts are stored as money.Micros as-is in *_minor_units columns.
//
// State machine (TTL expiry NEVER returns budget; see State in reservation.go):
//
//	RESERVED -> RELEASED     Release (refused while a pending actual exists)
//	RESERVED -> UNKNOWN      ExpireStale (skipped while a pending actual exists)
//	RESERVED -> RECONCILED   Reconcile / RecoverPending
//	UNKNOWN  -> RELEASED     ResolveUnknownReleased (refused if pending actual)
//	UNKNOWN  -> RECONCILED   ResolveUnknownReconciled / RecoverPending
//
// Every transition is one transaction that locks the rows it changes, so the
// ledger survives process death and is safe with multiple instances.
// Lock order: Reserve and SetBudget lock the budget row; Release, Reconcile,
// Resolve*, RecordPendingActual lock the reservation row and then its budget
// row; ExpireStale/RecoverPending lock reservation rows with SKIP LOCKED.
//
// Commit ambiguity: a connection lost during COMMIT leaves the caller unable
// to know if the operation applied. Use ReserveKeyed for externally initiated
// reservations: retrying with the same key returns the same reservation.
// Reserve (unkeyed) is for internal callers; a leaked hold stays visible and
// is moved to UNKNOWN by its TTL for operator/recovery resolution.
type PGManager struct {
	db        *sql.DB
	period    string
	opTimeout time.Duration
	batchSize int
	log       *slog.Logger
}

// NewPGManager returns a manager for the "monthly" budget period.
func NewPGManager(db *sql.DB, log *slog.Logger) *PGManager {
	return NewPGManagerPeriod(db, "monthly", log)
}

// NewPGManagerPeriod returns a manager bound to one budget period
// ("hourly", "daily" or "monthly").
func NewPGManagerPeriod(db *sql.DB, period string, log *slog.Logger) *PGManager {
	if log == nil {
		log = slog.Default()
	}
	return &PGManager{db: db, period: period, opTimeout: 5 * time.Second, batchSize: 200, log: log}
}

// SetOpTimeout bounds every database operation.
func (p *PGManager) SetOpTimeout(d time.Duration) { p.opTimeout = d }

// SetBatchSize bounds how many rows one recovery/expiry transaction handles.
func (p *PGManager) SetBatchSize(n int) {
	if n > 0 {
		p.batchSize = n
	}
}

func (p *PGManager) ctx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), p.opTimeout)
}

// Ping reports whether the authoritative store is reachable (for readiness).
func (p *PGManager) Ping(ctx context.Context) error {
	if err := p.db.PingContext(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrBackendUnavailable, err)
	}
	return nil
}

func pqCode(err error) pq.ErrorCode {
	var e *pq.Error
	if errors.As(err, &e) {
		return e.Code
	}
	return ""
}

var domainErrors = []error{
	ErrInsufficientBudget, ErrNotFound, ErrWrongTenant, ErrTerminalState, ErrBackendUnavailable,
	ErrIdempotencyConflict, ErrConflictingReconcile, ErrPendingActualExists, ErrInvalidAmount,
	ErrInvalidTenant, ErrInvalidTTL, ErrInvalidKey, ErrBudgetBelowExposure, ErrAmountOverflow,
	ErrLedgerCorrupt,
}

// classify turns a driver error into a domain error; domain errors pass through.
func (p *PGManager) classify(err error) error {
	if err == nil {
		return nil
	}
	for _, d := range domainErrors {
		if errors.Is(err, d) {
			return err
		}
	}
	switch pqCode(err) {
	case "22P02": // malformed UUID can never match a row
		return ErrNotFound
	case "22003": // bigint out of range: rolled back, no effect
		return ErrAmountOverflow
	case "23514": // a CHECK constraint refused the write: never silently corrupt
		p.log.Error("reservation ledger check violation", "err", err)
		return fmt.Errorf("%w: %v", ErrLedgerCorrupt, err)
	}
	p.log.Error("reservation database error", "err", err)
	return fmt.Errorf("%w: %v", ErrBackendUnavailable, err)
}

func retryable(err error) bool {
	c := pqCode(err)
	return c == "40001" || c == "40P01"
}

func (p *PGManager) withTx(fn func(ctx context.Context, tx *sql.Tx) error) error {
	var last error
	for attempt := 0; attempt < 4; attempt++ {
		ctx, cancel := p.ctx()
		tx, err := p.db.BeginTx(ctx, nil)
		if err != nil {
			cancel()
			return p.classify(err)
		}
		err = fn(ctx, tx)
		if err == nil {
			err = tx.Commit()
		} else {
			_ = tx.Rollback()
		}
		cancel()
		if err == nil {
			return nil
		}
		if retryable(err) {
			last = err
			time.Sleep(time.Duration(attempt+1) * 5 * time.Millisecond)
			continue
		}
		return p.classify(err)
	}
	return p.classify(last)
}

func mustOneRow(res sql.Result, what string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return fmt.Errorf("%w: %s affected %d rows, expected 1", ErrLedgerCorrupt, what, n)
	}
	return nil
}

// ---- exposure arithmetic ----

// avail computes limit - reserved - settled and FAILS CLOSED: any negative
// input, reserved > limit, or settled > limit-reserved yields 0. Every
// subtraction happens only after validation, on non-negative operands with
// minuend >= subtrahend, so it cannot overflow.
func avail(limit, reserved, settled int64) money.Micros {
	if limit < 0 || reserved < 0 || settled < 0 {
		return 0
	}
	if reserved > limit {
		return 0
	}
	remaining := limit - reserved
	if settled > remaining {
		return 0
	}
	return money.Micros(remaining - settled)
}

// validExposure reports whether the three balances are mutually consistent.
func validExposure(limit, reserved, settled int64) bool {
	if limit < 0 || reserved < 0 || settled < 0 || reserved > limit {
		return false
	}
	return settled <= limit-reserved
}

// canCover reports whether amount fits in the remaining budget. Corrupt or
// over-committed state covers nothing, not even a zero amount.
func canCover(limit, reserved, settled int64, amount money.Micros) bool {
	if amount < 0 || !validExposure(limit, reserved, settled) {
		return false
	}
	return int64(amount) <= int64(avail(limit, reserved, settled))
}

// ---- budgets ----

// SetBudgetE sets a tenant's limit for this manager's period. A limit below
// the current exposure (reserved + settled) is refused with
// ErrBudgetBelowExposure, atomically, via a guarded upsert: the WHERE clause
// is evaluated against the row version locked by the update, so it is safe
// against concurrent reservations.
func (p *PGManager) SetBudgetE(tenantID string, limit money.Micros) error {
	if tenantID == "" {
		return ErrInvalidTenant
	}
	if limit < 0 {
		return ErrInvalidAmount
	}
	ctx, cancel := p.ctx()
	defer cancel()
	res, err := p.db.ExecContext(ctx, `
		INSERT INTO budget_accounts (tenant_id, period, limit_minor_units)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, period) DO UPDATE
		   SET limit_minor_units = EXCLUDED.limit_minor_units, updated_at = now()
		 WHERE budget_accounts.reserved_minor_units::numeric + budget_accounts.settled_minor_units::numeric
		       <= EXCLUDED.limit_minor_units::numeric`,
		tenantID, p.period, money.ToMinorUnits(limit))
	switch pqCode(err) {
	case "22P02", "23503":
		return ErrInvalidTenant
	}
	if err != nil {
		return p.classify(err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return p.classify(err)
	}
	if n == 0 {
		return ErrBudgetBelowExposure
	}
	return nil
}

// SetBudget is the fire-and-forget form used by the in-memory API shape.
// Prefer SetBudgetE; failures are logged.
func (p *PGManager) SetBudget(tenantID string, limit money.Micros) {
	if err := p.SetBudgetE(tenantID, limit); err != nil {
		p.log.Error("SetBudget failed", "tenant_id", tenantID, "err", err)
	}
}

// ExposureE reads a tenant's financial position. A missing budget reads as zero.
func (p *PGManager) ExposureE(tenantID string) (Exposure, error) {
	ctx, cancel := p.ctx()
	defer cancel()
	var l, r, s int64
	err := p.db.QueryRowContext(ctx, `
		SELECT limit_minor_units, reserved_minor_units, settled_minor_units
		  FROM budget_accounts WHERE tenant_id=$1 AND period=$2`, tenantID, p.period).Scan(&l, &r, &s)
	if errors.Is(err, sql.ErrNoRows) || pqCode(err) == "22P02" {
		return Exposure{}, nil
	}
	if err != nil {
		return Exposure{}, p.classify(err)
	}
	return Exposure{
		Limit:     money.FromMinorUnits(l),
		Reserved:  money.FromMinorUnits(r),
		Settled:   money.FromMinorUnits(s),
		Available: avail(l, r, s),
	}, nil
}

// Exposure fails CLOSED: if the store is unreachable it reports a zero budget,
// so nothing is granted.
func (p *PGManager) Exposure(tenantID string) Exposure {
	e, err := p.ExposureE(tenantID)
	if err != nil {
		return Exposure{}
	}
	return e
}

// ---- reservations ----

const reservationCols = `id::text, tenant_id::text, amount_minor_units, state, COALESCE(idempotency_key,''),
	pending_actual_minor_units, actual_cost_minor_units, created_at, expires_at`

type rowScanner interface{ Scan(...any) error }

func scanReservation(row rowScanner) (*Reservation, error) {
	var r Reservation
	var amount int64
	var state string
	var pending, actual sql.NullInt64
	if err := row.Scan(&r.ID, &r.TenantID, &amount, &state, &r.IdempotencyKey, &pending, &actual, &r.CreatedAt, &r.ExpiresAt); err != nil {
		return nil, err
	}
	r.Amount = money.FromMinorUnits(amount)
	r.State = State(state)
	if pending.Valid {
		v := money.FromMinorUnits(pending.Int64)
		r.PendingActual = &v
	}
	if actual.Valid {
		r.ActualCost = money.FromMinorUnits(actual.Int64)
		r.Reconciled = true
	}
	return &r, nil
}

// Reserve creates an unkeyed reservation. See "Commit ambiguity" on PGManager:
// external callers should use ReserveKeyed.
func (p *PGManager) Reserve(tenantID string, amount money.Micros, ttl time.Duration) (*Reservation, error) {
	return p.reserve(tenantID, "", amount, ttl)
}

// ReserveKeyed is the production path for externally initiated reservations.
// Same tenant + same key + same amount returns the original reservation;
// same tenant + same key + different amount returns ErrIdempotencyConflict.
func (p *PGManager) ReserveKeyed(tenantID, key string, amount money.Micros, ttl time.Duration) (*Reservation, error) {
	if key == "" || len(key) > 256 {
		return nil, ErrInvalidKey
	}
	return p.reserve(tenantID, key, amount, ttl)
}

func (p *PGManager) reserve(tenantID, key string, amount money.Micros, ttl time.Duration) (*Reservation, error) {
	if tenantID == "" {
		return nil, ErrInvalidTenant
	}
	if ttl <= 0 || ttl > maxTTL {
		return nil, ErrInvalidTTL
	}
	if amount < 0 {
		return nil, ErrInvalidAmount
	}
	var out *Reservation
	err := p.withTx(func(ctx context.Context, tx *sql.Tx) error {
		out = nil
		// The budget row lock serialises every reservation (and SetBudget)
		// for this tenant: two requests never see the same remaining budget.
		var accID string
		var limit, reserved, settled int64
		err := tx.QueryRowContext(ctx, `
			SELECT id::text, limit_minor_units, reserved_minor_units, settled_minor_units
			  FROM budget_accounts WHERE tenant_id=$1 AND period=$2 FOR UPDATE`, tenantID, p.period).
			Scan(&accID, &limit, &reserved, &settled)
		if errors.Is(err, sql.ErrNoRows) {
			return ErrInsufficientBudget // no budget configured: nothing may be reserved
		}
		if pqCode(err) == "22P02" {
			return ErrInvalidTenant
		}
		if err != nil {
			return err
		}
		if key != "" {
			existing, err := scanReservation(tx.QueryRowContext(ctx,
				`SELECT `+reservationCols+` FROM reservations WHERE tenant_id=$1 AND idempotency_key=$2`, tenantID, key))
			if err == nil {
				if existing.Amount != amount {
					return ErrIdempotencyConflict
				}
				out = existing
				return nil
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
		if !canCover(limit, reserved, settled, amount) {
			return ErrInsufficientBudget
		}
		res, err := tx.ExecContext(ctx, `
			UPDATE budget_accounts SET reserved_minor_units = reserved_minor_units + $2, updated_at = now()
			 WHERE id = $1`, accID, money.ToMinorUnits(amount))
		if err != nil {
			return err
		}
		if err := mustOneRow(res, "reserve budget update"); err != nil {
			return err
		}
		var keyArg any
		if key != "" {
			keyArg = key
		}
		r, err := scanReservation(tx.QueryRowContext(ctx, `
			INSERT INTO reservations (tenant_id, budget_account_id, amount_minor_units, state, idempotency_key, expires_at)
			VALUES ($1, $2, $3, 'RESERVED', $4, now() + ($5::bigint * interval '1 microsecond'))
			RETURNING `+reservationCols, tenantID, accID, money.ToMinorUnits(amount), keyArg, ttl.Microseconds()))
		if err != nil {
			return err
		}
		if err := addEvent(ctx, tx, r.ID, "", "RESERVED", "created", money.ToMinorUnits(amount)); err != nil {
			return err
		}
		out = r
		return nil
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Get returns a reservation by id.
func (p *PGManager) Get(id string) (*Reservation, error) {
	ctx, cancel := p.ctx()
	defer cancel()
	r, err := scanReservation(p.db.QueryRowContext(ctx, `SELECT `+reservationCols+` FROM reservations WHERE id=$1`, id))
	if errors.Is(err, sql.ErrNoRows) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, p.classify(err)
	}
	return r, nil
}

// ---- locked-row helpers ----

type lockedRes struct {
	budgetID string
	amount   int64
	state    State
	actual   sql.NullInt64
	pending  sql.NullInt64
}

func lockReservation(ctx context.Context, tx *sql.Tx, id string) (lockedRes, error) {
	var l lockedRes
	var st string
	var budget sql.NullString
	err := tx.QueryRowContext(ctx, `
		SELECT budget_account_id::text, amount_minor_units, state, actual_cost_minor_units, pending_actual_minor_units
		  FROM reservations WHERE id=$1 FOR UPDATE`, id).Scan(&budget, &l.amount, &st, &l.actual, &l.pending)
	if errors.Is(err, sql.ErrNoRows) || pqCode(err) == "22P02" {
		return l, ErrNotFound
	}
	if err != nil {
		return l, err
	}
	if !budget.Valid {
		return l, fmt.Errorf("%w: reservation %s has no budget account", ErrLedgerCorrupt, id)
	}
	l.budgetID = budget.String
	l.state = State(st)
	return l, nil
}

func addEvent(ctx context.Context, tx *sql.Tx, id, from, to, reason string, amount int64) error {
	var fromArg any
	if from != "" {
		fromArg = from
	}
	_, err := tx.ExecContext(ctx, `
		INSERT INTO reservation_events (reservation_id, from_state, to_state, reason, amount_minor_units)
		VALUES ($1, $2, $3, $4, $5)`, id, fromArg, to, reason, amount)
	return err
}

// releaseLocked moves a locked, unresolved reservation to RELEASED and returns
// its hold to the budget. The guarded update refuses to drive reserved negative.
func releaseLocked(ctx context.Context, tx *sql.Tx, id string, l lockedRes, reason string) error {
	res, err := tx.ExecContext(ctx, `
		UPDATE budget_accounts SET reserved_minor_units = reserved_minor_units - $2, updated_at = now()
		 WHERE id = $1 AND reserved_minor_units >= $2`, l.budgetID, l.amount)
	if err != nil {
		return err
	}
	if err := mustOneRow(res, "release budget update"); err != nil {
		return err
	}
	res, err = tx.ExecContext(ctx, `UPDATE reservations SET state='RELEASED', resolved_at=now() WHERE id=$1 AND state=$2`, id, string(l.state))
	if err != nil {
		return err
	}
	if err := mustOneRow(res, "release state update"); err != nil {
		return err
	}
	return addEvent(ctx, tx, id, string(l.state), "RELEASED", reason, l.amount)
}

// reconcileLocked moves a locked, unresolved reservation to RECONCILED:
// reserved -= amount, settled += actual, with explicit overflow protection.
func reconcileLocked(ctx context.Context, tx *sql.Tx, id string, l lockedRes, actual int64, reason string) error {
	var reserved, settled int64
	err := tx.QueryRowContext(ctx, `
		SELECT reserved_minor_units, settled_minor_units FROM budget_accounts WHERE id=$1 FOR UPDATE`, l.budgetID).
		Scan(&reserved, &settled)
	if err != nil {
		return err
	}
	if reserved < l.amount || settled < 0 {
		return fmt.Errorf("%w: budget %s reserved=%d settled=%d cannot absorb reservation %s amount=%d",
			ErrLedgerCorrupt, l.budgetID, reserved, settled, id, l.amount)
	}
	if actual > math.MaxInt64-settled {
		return ErrAmountOverflow
	}
	res, err := tx.ExecContext(ctx, `
		UPDATE budget_accounts
		   SET reserved_minor_units = reserved_minor_units - $2,
		       settled_minor_units  = settled_minor_units + $3,
		       updated_at = now()
		 WHERE id = $1 AND reserved_minor_units >= $2`, l.budgetID, l.amount, actual)
	if err != nil {
		return err
	}
	if err := mustOneRow(res, "reconcile budget update"); err != nil {
		return err
	}
	res, err = tx.ExecContext(ctx, `
		UPDATE reservations SET state='RECONCILED', actual_cost_minor_units=$2, resolved_at=now()
		 WHERE id=$1 AND state=$3`, id, actual, string(l.state))
	if err != nil {
		return err
	}
	if err := mustOneRow(res, "reconcile state update"); err != nil {
		return err
	}
	return addEvent(ctx, tx, id, string(l.state), "RECONCILED", reason, actual)
}

// ---- transitions ----

// Release returns an unused hold to the budget. RESERVED -> RELEASED only.
// Repeating Release on a RELEASED reservation is a no-op (identical result).
// A reservation with a pending actual cannot be released (ErrPendingActualExists);
// UNKNOWN and RECONCILED reservations return ErrTerminalState.
func (p *PGManager) Release(id string) error {
	return p.withTx(func(ctx context.Context, tx *sql.Tx) error {
		l, err := lockReservation(ctx, tx, id)
		if err != nil {
			return err
		}
		switch l.state {
		case StateReleased:
			return nil
		case StateReserved:
			if l.pending.Valid {
				return ErrPendingActualExists
			}
			return releaseLocked(ctx, tx, id, l, "released")
		default:
			return ErrTerminalState
		}
	})
}

// Reconcile records the provider-reported actual cost. RESERVED -> RECONCILED.
// Repeating with the same actual is a no-op; a different actual returns
// ErrConflictingReconcile; RELEASED and UNKNOWN return ErrTerminalState (an
// UNKNOWN reservation is resolved with ResolveUnknownReconciled).
func (p *PGManager) Reconcile(id string, actual money.Micros) (ReconcileResult, error) {
	return p.reconcile(id, actual, false)
}

// ResolveUnknownReconciled settles an UNKNOWN reservation that the recovery
// worker confirmed was billed. UNKNOWN -> RECONCILED.
func (p *PGManager) ResolveUnknownReconciled(id string, actual money.Micros) (ReconcileResult, error) {
	return p.reconcile(id, actual, true)
}

func (p *PGManager) reconcile(id string, actual money.Micros, fromUnknown bool) (ReconcileResult, error) {
	if actual < 0 {
		return ReconcileResult{}, ErrInvalidAmount
	}
	var out ReconcileResult
	err := p.withTx(func(ctx context.Context, tx *sql.Tx) error {
		l, err := lockReservation(ctx, tx, id)
		if err != nil {
			return err
		}
		out = computeReconcileResult(money.FromMinorUnits(l.amount), actual)
		a := money.ToMinorUnits(actual)
		if l.state == StateReconciled {
			if l.actual.Int64 != a {
				return ErrConflictingReconcile
			}
			return nil
		}
		want := StateReserved
		reason := "reconciled"
		if fromUnknown {
			want = StateUnknown
			reason = "unknown_resolved_reconciled"
		}
		if l.state != want {
			return ErrTerminalState
		}
		if l.pending.Valid && l.pending.Int64 != a {
			return ErrConflictingReconcile
		}
		return reconcileLocked(ctx, tx, id, l, a, reason)
	})
	if err != nil {
		return ReconcileResult{}, err
	}
	return out, nil
}

// ResolveUnknownReleased returns the hold of an UNKNOWN reservation that the
// recovery worker confirmed was never billed. UNKNOWN -> RELEASED.
func (p *PGManager) ResolveUnknownReleased(id string) error {
	return p.withTx(func(ctx context.Context, tx *sql.Tx) error {
		l, err := lockReservation(ctx, tx, id)
		if err != nil {
			return err
		}
		switch l.state {
		case StateReleased:
			return nil
		case StateUnknown:
			if l.pending.Valid {
				return ErrPendingActualExists
			}
			return releaseLocked(ctx, tx, id, l, "unknown_resolved_released")
		default:
			return ErrTerminalState
		}
	})
}

// RecordPendingActual durably stores a provider result before reconciliation,
// so it survives process death. Valid in RESERVED and UNKNOWN. Repeating with
// the same value is a no-op; a different value is ErrConflictingReconcile; on
// RECONCILED it must equal the reconciled actual; on RELEASED it is refused.
func (p *PGManager) RecordPendingActual(id string, actual money.Micros) error {
	if actual < 0 {
		return ErrInvalidAmount
	}
	a := money.ToMinorUnits(actual)
	return p.withTx(func(ctx context.Context, tx *sql.Tx) error {
		l, err := lockReservation(ctx, tx, id)
		if err != nil {
			return err
		}
		switch l.state {
		case StateReconciled:
			if l.actual.Int64 != a {
				return ErrConflictingReconcile
			}
			return nil
		case StateReleased:
			return ErrTerminalState
		}
		if l.pending.Valid {
			if l.pending.Int64 != a {
				return ErrConflictingReconcile
			}
			return nil
		}
		res, err := tx.ExecContext(ctx, `UPDATE reservations SET pending_actual_minor_units=$2 WHERE id=$1`, id, a)
		if err != nil {
			return err
		}
		if err := mustOneRow(res, "pending actual update"); err != nil {
			return err
		}
		return addEvent(ctx, tx, id, string(l.state), string(l.state), "pending_actual_recorded", a)
	})
}

// PendingCount returns how many reservations hold an unreconciled pending
// actual. Readiness should stay unhealthy while this is non-zero after startup.
func (p *PGManager) PendingCount(ctx context.Context) (int, error) {
	var n int
	err := p.db.QueryRowContext(ctx, `
		SELECT count(*) FROM reservations
		 WHERE pending_actual_minor_units IS NOT NULL AND state IN ('RESERVED','UNKNOWN')`).Scan(&n)
	if err != nil {
		return 0, p.classify(err)
	}
	return n, nil
}

// RecoverPending settles every reservation whose provider result was durably
// recorded but never reconciled (e.g. the process died in between). It drains
// the backlog in bounded transactions, claiming rows with FOR UPDATE SKIP
// LOCKED so concurrent instances never settle the same row twice. It returns
// the number of reservations this call reconciled. Rows that cannot be
// reconciled (e.g. overflow) are skipped, remain pending, and are reported in
// the returned error; they never stop the drain.
func (p *PGManager) RecoverPending() (int, error) {
	total := 0
	last := "00000000-0000-0000-0000-000000000000"
	var failures []error
	for {
		n, next, done, errs, err := p.recoverBatch(last)
		total += n
		if err != nil {
			return total, err
		}
		failures = append(failures, errs...)
		if done {
			break
		}
		last = next
	}
	if len(failures) > 0 {
		return total, fmt.Errorf("recovery left %d reservation(s) pending: %w", len(failures), errors.Join(failures...))
	}
	return total, nil
}

func (p *PGManager) recoverBatch(after string) (n int, next string, done bool, failures []error, err error) {
	err = p.withTx(func(ctx context.Context, tx *sql.Tx) error {
		n, next, done, failures = 0, after, false, nil
		rows, qerr := tx.QueryContext(ctx, `
			SELECT id::text FROM reservations
			 WHERE state IN ('RESERVED','UNKNOWN') AND pending_actual_minor_units IS NOT NULL AND id > $1::uuid
			 ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED`, after, p.batchSize)
		if qerr != nil {
			return qerr
		}
		var ids []string
		for rows.Next() {
			var id string
			if serr := rows.Scan(&id); serr != nil {
				rows.Close()
				return serr
			}
			ids = append(ids, id)
		}
		if rerr := rows.Err(); rerr != nil {
			rows.Close()
			return rerr
		}
		rows.Close()
		if len(ids) == 0 {
			done = true
			return nil
		}
		for _, id := range ids {
			next = id
			l, lerr := lockReservation(ctx, tx, id)
			if lerr != nil {
				return lerr
			}
			if (l.state != StateReserved && l.state != StateUnknown) || !l.pending.Valid {
				continue
			}
			reason := "recovered_pending_actual"
			// Logical failures are detected before any statement runs, so the
			// transaction stays usable and the row is simply skipped.
			if cerr := preflightReconcile(ctx, tx, l, l.pending.Int64); cerr != nil {
				failures = append(failures, fmt.Errorf("reservation %s: %w", id, cerr))
				continue
			}
			if rerr := reconcileLocked(ctx, tx, id, l, l.pending.Int64, reason); rerr != nil {
				return rerr
			}
			n++
		}
		if len(ids) < p.batchSize {
			done = true
		}
		return nil
	})
	if err != nil {
		return 0, after, false, nil, err
	}
	return n, next, done, failures, nil
}

// preflightReconcile performs the read-only validity checks of reconcileLocked
// so a poisoned row can be skipped without aborting the surrounding batch.
func preflightReconcile(ctx context.Context, tx *sql.Tx, l lockedRes, actual int64) error {
	var reserved, settled int64
	err := tx.QueryRowContext(ctx, `
		SELECT reserved_minor_units, settled_minor_units FROM budget_accounts WHERE id=$1`, l.budgetID).
		Scan(&reserved, &settled)
	if err != nil {
		return err
	}
	if reserved < l.amount || settled < 0 {
		return ErrLedgerCorrupt
	}
	if actual > math.MaxInt64-settled {
		return ErrAmountOverflow
	}
	return nil
}

// ExpireStale moves RESERVED reservations past their TTL to UNKNOWN. It never
// touches budget balances (TTL alone is not proof the upstream call was not
// billed) and skips reservations that hold a pending actual, which stay
// RESERVED until RecoverPending reconciles them. Safe with concurrent workers.
func (p *PGManager) ExpireStale() int {
	n, err := p.ExpireStaleE()
	if err != nil {
		p.log.Error("ExpireStale failed", "err", err)
	}
	return n
}

// ExpireStaleE is ExpireStale returning the error.
func (p *PGManager) ExpireStaleE() (int, error) {
	total := 0
	for {
		n, err := p.expireBatch()
		total += n
		if err != nil {
			return total, err
		}
		if n < p.batchSize {
			return total, nil
		}
	}
}

func (p *PGManager) expireBatch() (int, error) {
	n := 0
	err := p.withTx(func(ctx context.Context, tx *sql.Tx) error {
		n = 0
		rows, err := tx.QueryContext(ctx, `
			SELECT id::text FROM reservations
			 WHERE state='RESERVED' AND pending_actual_minor_units IS NULL AND expires_at < now()
			 ORDER BY expires_at, id LIMIT $1 FOR UPDATE SKIP LOCKED`, p.batchSize)
		if err != nil {
			return err
		}
		var ids []string
		for rows.Next() {
			var id string
			if err := rows.Scan(&id); err != nil {
				rows.Close()
				return err
			}
			ids = append(ids, id)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return err
		}
		rows.Close()
		if len(ids) == 0 {
			return nil
		}
		res, err := tx.ExecContext(ctx, `UPDATE reservations SET state='UNKNOWN' WHERE id = ANY($1::uuid[]) AND state='RESERVED'`, pq.Array(ids))
		if err != nil {
			return err
		}
		got, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if int(got) != len(ids) {
			return fmt.Errorf("%w: expiry updated %d of %d locked rows", ErrLedgerCorrupt, got, len(ids))
		}
		if _, err := tx.ExecContext(ctx, `
			INSERT INTO reservation_events (reservation_id, from_state, to_state, reason, amount_minor_units)
			SELECT id, 'RESERVED', 'UNKNOWN', 'ttl_expiry', amount_minor_units
			  FROM reservations WHERE id = ANY($1::uuid[])`, pq.Array(ids)); err != nil {
			return err
		}
		n = len(ids)
		return nil
	})
	return n, err
}
