-- 0003_reservation_ledger_hardening.sql
-- Forward-only, additive, idempotent. Run after 0002_financial_control.sql.
--
-- Monetary unit: every *_minor_units column stores the Go money.Micros
-- integer as-is (see internal/money: ToMinorUnits/FromMinorUnits). No unit
-- conversion happens at the persistence boundary and none is introduced here.
--
-- State model (unchanged from 0002): RESERVED, UNKNOWN, RELEASED, RECONCILED.
-- TTL expiry moves RESERVED -> UNKNOWN and never returns budget.
--
-- This migration adds, without rewriting or reinterpreting any existing row:
--   * durable pending-actual storage (survives process death)
--   * tenant-scoped idempotency keys (NULL = unkeyed, any number allowed)
--   * the link from a reservation to the budget account it is held against
--   * an amount on every lifecycle event (needed for ledger auditing)
--   * additional CHECK constraints. Constraints on pre-existing tables are
--     added NOT VALID: they are enforced for every new/updated row, and any
--     historical row that violates them is surfaced by AuditPG instead of
--     blocking the deploy. Operators may VALIDATE them once audited clean.
--
-- Note: reserved + settled MAY legitimately exceed limit (a provider can bill
-- more than was reserved), so there is deliberately no such CHECK.

ALTER TABLE reservations
    ADD COLUMN IF NOT EXISTS idempotency_key            TEXT
        CHECK (idempotency_key IS NULL OR length(idempotency_key) BETWEEN 1 AND 256),
    ADD COLUMN IF NOT EXISTS pending_actual_minor_units BIGINT
        CHECK (pending_actual_minor_units >= 0),
    ADD COLUMN IF NOT EXISTS budget_account_id          UUID
        REFERENCES budget_accounts(id);

-- Same tenant + same key => same logical reservation. Different tenants may
-- reuse a key. Unkeyed (NULL) reservations are never constrained.
CREATE UNIQUE INDEX IF NOT EXISTS ux_reservations_tenant_idempotency
    ON reservations (tenant_id, idempotency_key)
    WHERE idempotency_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_reservations_budget_account_id
    ON reservations (budget_account_id);

-- Recovery worker: rows holding a durably-recorded provider result that has
-- not been reconciled yet.
CREATE INDEX IF NOT EXISTS idx_reservations_pending_actual
    ON reservations (id)
    WHERE pending_actual_minor_units IS NOT NULL AND state IN ('RESERVED', 'UNKNOWN');

ALTER TABLE reservation_events
    ADD COLUMN IF NOT EXISTS amount_minor_units BIGINT
        CHECK (amount_minor_units >= 0);

DO $$
BEGIN
    -- A pending actual only makes sense while the reservation is unresolved,
    -- or (kept for traceability) after reconciliation to that same amount.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'reservations_pending_shape' AND conrelid = 'reservations'::regclass) THEN
        ALTER TABLE reservations ADD CONSTRAINT reservations_pending_shape CHECK (
            pending_actual_minor_units IS NULL
            OR state IN ('RESERVED', 'UNKNOWN')
            OR (state = 'RECONCILED' AND pending_actual_minor_units = actual_cost_minor_units)
        ) NOT VALID;
    END IF;

    -- Terminal states carry a resolution timestamp; non-terminal ones do not.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'reservations_resolved_shape' AND conrelid = 'reservations'::regclass) THEN
        ALTER TABLE reservations ADD CONSTRAINT reservations_resolved_shape CHECK (
            (state IN ('RESERVED', 'UNKNOWN') AND resolved_at IS NULL)
            OR (state IN ('RELEASED', 'RECONCILED') AND resolved_at IS NOT NULL)
        ) NOT VALID;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM pg_constraint WHERE conname = 'reservation_events_states_valid' AND conrelid = 'reservation_events'::regclass) THEN
        ALTER TABLE reservation_events ADD CONSTRAINT reservation_events_states_valid CHECK (
            to_state IN ('RESERVED', 'UNKNOWN', 'RELEASED', 'RECONCILED')
            AND (from_state IS NULL OR from_state IN ('RESERVED', 'UNKNOWN', 'RELEASED', 'RECONCILED'))
        ) NOT VALID;
    END IF;
END
$$;
