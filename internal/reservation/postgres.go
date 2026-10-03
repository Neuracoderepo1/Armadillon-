package reservation

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/lib/pq"

	"velocityguard/internal/money"
)

// isInvalidIDSyntax reports whether err is Postgres rejecting a
// reservationID that isn't even well-formed UUID syntax (SQLSTATE 22P02).
// Manager.Get/Release/Reconcile treat any unrecognized ID the same way —
// ErrNotFound — because a Go map lookup can't distinguish "malformed" from
// "well-formed but absent". Without this check, PostgresRepository would
// leak a raw driver error for malformed IDs while returning ErrNotFound
// for well-formed-but-absent ones, breaking behavioral equivalence with
// Manager for callers coded against the shared Repository interface
// (exactly what repository_test.go's shared suite caught).
func isInvalidIDSyntax(err error) bool {
	var pqErr *pq.Error
	return errors.As(err, &pqErr) && pqErr.Code == "22P02"
}

// defaultPeriod is the only budget_accounts.period value this repository
// uses. The control-plane schema (migrations/0001) supports hourly/daily/
// monthly periods with independent limits per tenant, but neither this
// repository nor the in-memory Manager it mirrors implements multi-period
// budgets or period rollover (resetting reserved/settled at a period
// boundary) — Manager has always had exactly one flat limit per tenant,
// and this repository is built to be its durable, behaviorally-identical
// equivalent, not a superset. Multi-period budgeting is a real future
// requirement but a distinct one from reservation durability; it is not
// addressed here.
const defaultPeriod = "daily"

// PostgresRepository is the durable counterpart to Manager: it implements
// the exact same Repository interface and the exact same state machine
// (see the State doc comment in reservation.go), but reservations and
// running budget totals survive a process restart because they live in
// Postgres (migrations/0002_financial_control.sql) instead of a Go map.
//
// Concurrency safety ("never overcommit") is achieved the same way
// Manager achieves it — serializing access to one tenant's budget state —
// but via `SELECT ... FOR UPDATE` row locks on that tenant's
// budget_accounts row instead of an in-process mutex, so it holds under
// concurrent access from multiple processes, not just multiple goroutines
// in one process.
type PostgresRepository struct {
	db  *sql.DB
	now func() time.Time // overridable for tests, mirrors Manager.now
}

func NewPostgresRepository(db *sql.DB) *PostgresRepository {
	return &PostgresRepository{db: db, now: time.Now}
}

var _ Repository = (*PostgresRepository)(nil)

func (p *PostgresRepository) SetBudget(tenantID string, limit money.Micros) {
	// Matches Manager.SetBudget's signature, which has no error return —
	// callers (main.go's demo bootstrap, tests) have never handled one.
	// A real failure here (bad DSN, connection dropped) will surface on
	// the next Reserve/Exposure call against the same tenant, which does
	// return an error.
	_, _ = p.db.ExecContext(context.Background(), `
		INSERT INTO budget_accounts (tenant_id, period, limit_minor_units)
		VALUES ($1, $2, $3)
		ON CONFLICT (tenant_id, period)
		DO UPDATE SET limit_minor_units = excluded.limit_minor_units, updated_at = now()
	`, tenantID, defaultPeriod, money.ToMinorUnits(limit))
}

func (p *PostgresRepository) Exposure(tenantID string) Exposure {
	var limitU, reservedU, settledU int64
	err := p.db.QueryRowContext(context.Background(), `
		SELECT limit_minor_units, reserved_minor_units, settled_minor_units
		FROM budget_accounts WHERE tenant_id = $1 AND period = $2
	`, tenantID, defaultPeriod).Scan(&limitU, &reservedU, &settledU)
	if err != nil {
		// No budget_accounts row yet (SetBudget never called for this
		// tenant) is the overwhelmingly common reason this fails — treat
		// it the same way Manager treats an unseen tenant: an all-zero
		// account, so Available() is 0 and Reserve() safely refuses any
		// positive amount. A real connectivity error degrades the same
		// way (fail closed, not fail open), which matches this system's
		// stated default.
		return Exposure{}
	}
	limit, reserved, settled := money.FromMinorUnits(limitU), money.FromMinorUnits(reservedU), money.FromMinorUnits(settledU)
	return Exposure{Limit: limit, Reserved: reserved, Settled: settled, Available: limit - reserved - settled}
}

func (p *PostgresRepository) Reserve(tenantID string, amount money.Micros, ttl time.Duration) (*Reservation, error) {
	ctx := context.Background()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck // no-op once committed

	var limitU, reservedU, settledU int64
	err = tx.QueryRowContext(ctx, `
		SELECT limit_minor_units, reserved_minor_units, settled_minor_units
		FROM budget_accounts WHERE tenant_id = $1 AND period = $2
		FOR UPDATE
	`, tenantID, defaultPeriod).Scan(&limitU, &reservedU, &settledU)
	if errors.Is(err, sql.ErrNoRows) {
		// No budget configured for this tenant at all: available is 0,
		// so any positive amount is refused. Nothing to lock, nothing
		// to race against.
		return nil, ErrInsufficientBudget
	}
	if err != nil {
		return nil, err
	}

	available := money.FromMinorUnits(limitU) - money.FromMinorUnits(reservedU) - money.FromMinorUnits(settledU)
	if available < amount {
		return nil, ErrInsufficientBudget
	}

	now := p.now()
	expiresAt := now.Add(ttl)

	var id string
	var createdAt time.Time
	err = tx.QueryRowContext(ctx, `
		INSERT INTO reservations (tenant_id, amount_minor_units, state, created_at, expires_at)
		VALUES ($1, $2, 'RESERVED', $3, $4)
		RETURNING id, created_at
	`, tenantID, money.ToMinorUnits(amount), now, expiresAt).Scan(&id, &createdAt)
	if err != nil {
		return nil, err
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE budget_accounts SET reserved_minor_units = reserved_minor_units + $1, updated_at = now()
		WHERE tenant_id = $2 AND period = $3
	`, money.ToMinorUnits(amount), tenantID, defaultPeriod); err != nil {
		return nil, err
	}

	if err := tx.Commit(); err != nil {
		return nil, err
	}

	return &Reservation{
		ID: id, TenantID: tenantID, Amount: amount, State: StateReserved,
		CreatedAt: createdAt, ExpiresAt: expiresAt,
	}, nil
}

// lockReservation reads a reservation's current state under FOR UPDATE
// inside an already-open transaction, so the caller's subsequent
// state-dependent decision and write are atomic against concurrent
// resolvers of the same reservation.
func (p *PostgresRepository) lockReservation(ctx context.Context, tx *sql.Tx, id string) (tenantID string, amount money.Micros, state State, actualCost sql.NullInt64, err error) {
	var amountU int64
	var stateStr string
	err = tx.QueryRowContext(ctx, `
		SELECT tenant_id, amount_minor_units, state, actual_cost_minor_units
		FROM reservations WHERE id = $1 FOR UPDATE
	`, id).Scan(&tenantID, &amountU, &stateStr, &actualCost)
	if errors.Is(err, sql.ErrNoRows) || isInvalidIDSyntax(err) {
		return "", 0, "", actualCost, ErrNotFound
	}
	if err != nil {
		return "", 0, "", actualCost, err
	}
	return tenantID, money.FromMinorUnits(amountU), State(stateStr), actualCost, nil
}

func (p *PostgresRepository) Release(reservationID string) error {
	ctx := context.Background()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	tenantID, amount, state, _, err := p.lockReservation(ctx, tx, reservationID)
	if err != nil {
		return err
	}
	if state != StateReserved {
		// Idempotent / safe no-op, matching Manager.Release exactly:
		// releasing anything already terminal (RELEASED, RECONCILED, or
		// UNKNOWN) does nothing and is not an error.
		return tx.Commit()
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE reservations SET state = 'RELEASED', resolved_at = now() WHERE id = $1
	`, reservationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE budget_accounts SET reserved_minor_units = reserved_minor_units - $1, updated_at = now()
		WHERE tenant_id = $2 AND period = $3
	`, money.ToMinorUnits(amount), tenantID, defaultPeriod); err != nil {
		return err
	}
	return tx.Commit()
}

func (p *PostgresRepository) Reconcile(reservationID string, actualCost money.Micros) (ReconcileResult, error) {
	return p.settle(reservationID, actualCost, StateReserved)
}

func (p *PostgresRepository) ResolveUnknownReconciled(reservationID string, actualCost money.Micros) (ReconcileResult, error) {
	return p.settle(reservationID, actualCost, StateUnknown)
}

// settle is the shared implementation behind Reconcile (fromState =
// RESERVED) and ResolveUnknownReconciled (fromState = UNKNOWN) — both
// move a reservation to RECONCILED at actualCost; they differ only in
// which state they're allowed to start from. This mirrors how Manager's
// Reconcile and ResolveUnknownReconciled are near-identical except for
// that one check, kept here as one function instead of two copies of the
// same five SQL statements.
func (p *PostgresRepository) settle(reservationID string, actualCost money.Micros, fromState State) (ReconcileResult, error) {
	ctx := context.Background()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return ReconcileResult{}, err
	}
	defer tx.Rollback() //nolint:errcheck

	tenantID, amount, state, existingActual, err := p.lockReservation(ctx, tx, reservationID)
	if err != nil {
		return ReconcileResult{}, err
	}
	if state == StateReconciled {
		// Idempotent: already settled, return the original result,
		// write nothing further — matches Manager's Reconciled check.
		actual := money.FromMinorUnits(existingActual.Int64)
		return computeReconcileResult(amount, actual), tx.Commit()
	}
	if state != fromState {
		return ReconcileResult{}, ErrTerminalState
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE reservations SET state = 'RECONCILED', actual_cost_minor_units = $1, resolved_at = now()
		WHERE id = $2
	`, money.ToMinorUnits(actualCost), reservationID); err != nil {
		return ReconcileResult{}, err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE budget_accounts
		SET reserved_minor_units = reserved_minor_units - $1, settled_minor_units = settled_minor_units + $2, updated_at = now()
		WHERE tenant_id = $3 AND period = $4
	`, money.ToMinorUnits(amount), money.ToMinorUnits(actualCost), tenantID, defaultPeriod); err != nil {
		return ReconcileResult{}, err
	}
	if err := tx.Commit(); err != nil {
		return ReconcileResult{}, err
	}
	return computeReconcileResult(amount, actualCost), nil
}

func (p *PostgresRepository) ResolveUnknownReleased(reservationID string) error {
	ctx := context.Background()
	tx, err := p.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback() //nolint:errcheck

	tenantID, amount, state, _, err := p.lockReservation(ctx, tx, reservationID)
	if err != nil {
		return err
	}
	if state == StateReleased {
		return tx.Commit() // idempotent
	}
	if state != StateUnknown {
		return ErrTerminalState
	}

	if _, err := tx.ExecContext(ctx, `
		UPDATE reservations SET state = 'RELEASED', resolved_at = now() WHERE id = $1
	`, reservationID); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE budget_accounts SET reserved_minor_units = reserved_minor_units - $1, updated_at = now()
		WHERE tenant_id = $2 AND period = $3
	`, money.ToMinorUnits(amount), tenantID, defaultPeriod); err != nil {
		return err
	}
	return tx.Commit()
}

// ExpireStale is a single statement across all tenants — Postgres's own
// atomicity guarantees the "never return budget" invariant here, so
// unlike the per-row loop in Manager.ExpireStale (needed there because
// Go has no equivalent of a set-based UPDATE), one UPDATE does the whole
// sweep.
func (p *PostgresRepository) ExpireStale() int {
	res, err := p.db.ExecContext(context.Background(), `
		UPDATE reservations SET state = 'UNKNOWN'
		WHERE state = 'RESERVED' AND expires_at < $1
	`, p.now())
	if err != nil {
		return 0
	}
	n, _ := res.RowsAffected()
	return int(n)
}

func (p *PostgresRepository) Get(reservationID string) (*Reservation, error) {
	var r Reservation
	var stateStr string
	var amountU int64
	var actualCost sql.NullInt64
	err := p.db.QueryRowContext(context.Background(), `
		SELECT id, tenant_id, amount_minor_units, state, created_at, expires_at, actual_cost_minor_units
		FROM reservations WHERE id = $1
	`, reservationID).Scan(&r.ID, &r.TenantID, &amountU, &stateStr, &r.CreatedAt, &r.ExpiresAt, &actualCost)
	if errors.Is(err, sql.ErrNoRows) || isInvalidIDSyntax(err) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	r.Amount = money.FromMinorUnits(amountU)
	r.State = State(stateStr)
	if actualCost.Valid {
		r.ActualCost = money.FromMinorUnits(actualCost.Int64)
		r.Reconciled = r.State == StateReconciled
	}
	return &r, nil
}
