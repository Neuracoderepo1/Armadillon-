-- Maintenance (NOT auto-applied: CI and deploys glob migrations/*.sql only).
--
-- Migrations 0003/0004 added four CHECK constraints as NOT VALID so they could
-- ship without scanning historical rows. They are enforced for every new or
-- updated row already; this script certifies the OLD rows and flips them to
-- validated. VALIDATE CONSTRAINT takes SHARE UPDATE EXCLUSIVE, so it does not
-- block normal reads or writes.
--
-- Run order:
--   1. Run the PREFLIGHT block. Every count must be 0.
--   2. If any count is non-zero, STOP: those rows are historical ledger
--      anomalies to be explained (see reservation.AuditPG), not silently fixed.
--   3. Run the VALIDATE block.

-- ===== PREFLIGHT (read-only) =====
SELECT 'reservations_pending_shape' AS constraint_name, count(*) AS violations
  FROM reservations
 WHERE NOT (pending_actual_minor_units IS NULL
            OR state IN ('RESERVED', 'UNKNOWN')
            OR (state = 'RECONCILED' AND pending_actual_minor_units = actual_cost_minor_units))
UNION ALL
SELECT 'reservations_resolved_shape', count(*)
  FROM reservations
 WHERE NOT ((state IN ('RESERVED', 'UNKNOWN') AND resolved_at IS NULL)
            OR (state IN ('RELEASED', 'RECONCILED') AND resolved_at IS NOT NULL))
UNION ALL
SELECT 'reservation_events_states_valid', count(*)
  FROM reservation_events
 WHERE NOT (to_state IN ('RESERVED', 'UNKNOWN', 'RELEASED', 'RECONCILED')
            AND (from_state IS NULL OR from_state IN ('RESERVED', 'UNKNOWN', 'RELEASED', 'RECONCILED')))
UNION ALL
SELECT 'reservation_reconciliations_outcome_shape', count(*)
  FROM reservation_reconciliations
 WHERE NOT ((outcome = 'RECONCILED' AND actual_cost_minor_units IS NOT NULL)
            OR (outcome = 'RELEASED' AND actual_cost_minor_units IS NULL));

-- ===== VALIDATE (only after every preflight count is 0) =====
ALTER TABLE reservations                VALIDATE CONSTRAINT reservations_pending_shape;
ALTER TABLE reservations                VALIDATE CONSTRAINT reservations_resolved_shape;
ALTER TABLE reservation_events          VALIDATE CONSTRAINT reservation_events_states_valid;
ALTER TABLE reservation_reconciliations VALIDATE CONSTRAINT reservation_reconciliations_outcome_shape;
