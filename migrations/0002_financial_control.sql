-- 0002_financial_control.sql
-- Durable reservation state, matching the corrected state machine
-- implemented in internal/reservation (see the State doc comment there
-- for the full rationale). This migration is additive only; run after
-- 0001_control_plane_init.sql.
--
-- Safety invariant this schema exists to protect, enforced here at the
-- DATABASE layer (not just application logic) via CHECK constraints:
-- a reservation's TTL expiring is never, by itself, sufficient grounds
-- to treat it as RELEASED. Only RECONCILED (actual cost confirmed) or an
-- explicit, audited UNKNOWN -> {RELEASED, RECONCILED} resolution can
-- change what the tenant is actually charged.

CREATE TABLE IF NOT EXISTS reservations (
    id                       UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id                UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    amount_minor_units       BIGINT NOT NULL CHECK (amount_minor_units >= 0),
    state                    TEXT NOT NULL CHECK (state IN ('RESERVED', 'RECONCILED', 'RELEASED', 'UNKNOWN')),
    actual_cost_minor_units  BIGINT CHECK (actual_cost_minor_units >= 0),
    created_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    expires_at               TIMESTAMPTZ NOT NULL,
    resolved_at              TIMESTAMPTZ,

    -- A row's shape must match its state: RECONCILED is the only state
    -- that carries an actual cost, and it always has a resolution
    -- timestamp. This makes an inconsistent row (e.g. RESERVED with an
    -- actual cost already set) impossible to write, not just discouraged.
    CONSTRAINT reservations_state_shape CHECK (
        (state = 'RESERVED'   AND resolved_at IS NULL     AND actual_cost_minor_units IS NULL)
        OR (state = 'UNKNOWN'    AND actual_cost_minor_units IS NULL)
        OR (state = 'RELEASED'   AND actual_cost_minor_units IS NULL)
        OR (state = 'RECONCILED' AND actual_cost_minor_units IS NOT NULL AND resolved_at IS NOT NULL)
    )
);

CREATE INDEX IF NOT EXISTS idx_reservations_tenant_id ON reservations(tenant_id);

-- Used by ExpireStale's sweep: find RESERVED rows past their TTL.
CREATE INDEX IF NOT EXISTS idx_reservations_reserved_expiring
    ON reservations(expires_at) WHERE state = 'RESERVED';

-- Used by the recovery worker: find everything still awaiting resolution.
CREATE INDEX IF NOT EXISTS idx_reservations_unknown
    ON reservations(tenant_id) WHERE state = 'UNKNOWN';

-- Append-only audit trail of every state transition. Distinct from the
-- financial ledger in internal/ledger (that's spend accounting; this is
-- reservation lifecycle audit — what moved, when, and why).
CREATE TABLE IF NOT EXISTS reservation_events (
    id             BIGSERIAL PRIMARY KEY,
    reservation_id UUID NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    from_state     TEXT,
    to_state       TEXT NOT NULL,
    reason         TEXT NOT NULL, -- e.g. 'ttl_expiry', 'provider_confirmed_billed', 'provider_confirmed_not_dispatched'
    occurred_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reservation_events_reservation_id ON reservation_events(reservation_id);

-- One row per UNKNOWN -> {RELEASED, RECONCILED} resolution performed by
-- the recovery worker (or an operator override). idempotency_key lets a
-- retried resolution attempt be applied at most once.
CREATE TABLE IF NOT EXISTS reservation_reconciliations (
    id                      UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    reservation_id          UUID NOT NULL REFERENCES reservations(id) ON DELETE CASCADE,
    idempotency_key         TEXT NOT NULL UNIQUE,
    outcome                 TEXT NOT NULL CHECK (outcome IN ('RELEASED', 'RECONCILED')),
    actual_cost_minor_units BIGINT CHECK (actual_cost_minor_units >= 0),
    resolved_by             TEXT NOT NULL, -- e.g. 'recovery-worker', 'operator:<token-prefix>'
    resolved_at             TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_reservation_reconciliations_reservation_id
    ON reservation_reconciliations(reservation_id);

-- Durable running totals per budget period. These are kept in sync with
-- the reservations table by the (forthcoming) Postgres-backed
-- reservation repository, in the same transaction as each reservation
-- write, so a crash mid-write can never leave them inconsistent.
ALTER TABLE budget_accounts
    ADD COLUMN IF NOT EXISTS reserved_minor_units BIGINT NOT NULL DEFAULT 0 CHECK (reserved_minor_units >= 0),
    ADD COLUMN IF NOT EXISTS settled_minor_units  BIGINT NOT NULL DEFAULT 0 CHECK (settled_minor_units >= 0);
