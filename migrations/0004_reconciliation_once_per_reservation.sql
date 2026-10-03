-- 0004_reconciliation_once_per_reservation.sql
-- Forward-only, idempotent. Run after 0003.
--
-- reservation_reconciliations records how an UNKNOWN reservation was resolved
-- (RELEASED or RECONCILED). Two problems with the 0002 definition:
--   1. idempotency_key was UNIQUE across the whole table, so one tenant's key
--      could block (and reveal the existence of) another tenant's resolution.
--   2. Nothing stopped one reservation from being resolved twice with
--      different keys, i.e. two conflicting resolution records.
--
-- New contract: at most ONE resolution record per reservation. A reservation
-- already belongs to exactly one tenant, so this is tenant-scoped by
-- construction, and a retried resolution (same reservation) is detected by the
-- unique index regardless of the key it carries. The global key uniqueness is
-- therefore redundant and is dropped; the column is kept (NOT NULL) for callers.
--
-- No data is deleted or rewritten. If historical duplicates exist the migration
-- stops with a clear error so they can be reviewed by a human instead of being
-- silently discarded.

DO $$
DECLARE dupes BIGINT;
BEGIN
    SELECT count(*) INTO dupes FROM (
        SELECT reservation_id FROM reservation_reconciliations
         GROUP BY reservation_id HAVING count(*) > 1
    ) d;
    IF dupes > 0 THEN
        RAISE EXCEPTION '0004: % reservation(s) have more than one resolution record; review and resolve manually before migrating', dupes;
    END IF;
END
$$;

CREATE UNIQUE INDEX IF NOT EXISTS ux_reservation_reconciliations_reservation_id
    ON reservation_reconciliations (reservation_id);

DO $$
BEGIN
    IF EXISTS (SELECT 1 FROM pg_constraint
                WHERE conname = 'reservation_reconciliations_idempotency_key_key'
                  AND conrelid = 'reservation_reconciliations'::regclass) THEN
        ALTER TABLE reservation_reconciliations
            DROP CONSTRAINT reservation_reconciliations_idempotency_key_key;
    END IF;

    -- A RECONCILED resolution must carry its actual cost; a RELEASED one must not.
    -- NOT VALID: enforced for new rows; historical rows are reported by AuditPG.
    IF NOT EXISTS (SELECT 1 FROM pg_constraint
                    WHERE conname = 'reservation_reconciliations_outcome_shape'
                      AND conrelid = 'reservation_reconciliations'::regclass) THEN
        ALTER TABLE reservation_reconciliations ADD CONSTRAINT reservation_reconciliations_outcome_shape CHECK (
            (outcome = 'RECONCILED' AND actual_cost_minor_units IS NOT NULL)
            OR (outcome = 'RELEASED' AND actual_cost_minor_units IS NULL)
        ) NOT VALID;
    END IF;
END
$$;
