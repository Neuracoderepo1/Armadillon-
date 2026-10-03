package reservation

import (
	"context"
	"database/sql"
	"fmt"
)

// auditCheck is one read-only SQL check. Every row it returns is one
// violation; the single text column describes it and always names the
// tenant and/or reservation involved.
type auditCheck struct{ name, query string }

// auditChecks is written against the real schema (migrations 0001-0003):
// budget_accounts, reservations, reservation_events(from_state, to_state,
// reason, amount_minor_units). Event semantics:
//
//	CREATED     from_state IS NULL, to_state='RESERVED', amount = reservation amount
//	EXPIRED     to_state='UNKNOWN',                       amount = reservation amount
//	RELEASED    to_state='RELEASED',                      amount = reservation amount
//	RECONCILED  to_state='RECONCILED',                    amount = actual cost
//
// A reservation's reserved-balance contribution is its amount while RESERVED or
// UNKNOWN (expiry never returns budget); its settled contribution is its actual
// cost once RECONCILED.
//
// Deliberately NOT a violation: reserved + settled > limit. A provider can bill
// more than was reserved, so overage is a legitimate (if undesirable) state.
//
// Not applicable to this schema: "event with an invalid tenant relationship"
// (events carry no tenant_id; the tenant is reached through the reservation),
// and a separate "duplicate idempotency key" detector is kept only as a
// belt-and-braces check because ux_reservations_tenant_idempotency already
// makes it unwritable.
var auditChecks = []auditCheck{
	// ---- account integrity ----
	{"negative_balance", `
		SELECT b.id::text || ' tenant=' || b.tenant_id::text || ' limit=' || b.limit_minor_units
		       || ' reserved=' || b.reserved_minor_units || ' settled=' || b.settled_minor_units
		  FROM budget_accounts b
		 WHERE b.limit_minor_units < 0 OR b.reserved_minor_units < 0 OR b.settled_minor_units < 0`},
	{"negative_reservation_amount", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text
		  FROM reservations r
		 WHERE r.amount_minor_units < 0 OR r.actual_cost_minor_units < 0 OR r.pending_actual_minor_units < 0`},
	{"reserved_mismatch", `
		SELECT b.id::text || ' tenant=' || b.tenant_id::text || ' account_reserved=' || b.reserved_minor_units
		       || ' recomputed=' || COALESCE(r.s, 0)
		  FROM budget_accounts b
		  LEFT JOIN (SELECT budget_account_id, SUM(amount_minor_units) s
		               FROM reservations WHERE state IN ('RESERVED','UNKNOWN') GROUP BY budget_account_id) r
		         ON r.budget_account_id = b.id
		 WHERE b.reserved_minor_units::numeric <> COALESCE(r.s, 0)`},
	{"settled_mismatch", `
		SELECT b.id::text || ' tenant=' || b.tenant_id::text || ' account_settled=' || b.settled_minor_units
		       || ' recomputed=' || COALESCE(r.s, 0)
		  FROM budget_accounts b
		  LEFT JOIN (SELECT budget_account_id, SUM(actual_cost_minor_units) s
		               FROM reservations WHERE state = 'RECONCILED' GROUP BY budget_account_id) r
		         ON r.budget_account_id = b.id
		 WHERE b.settled_minor_units::numeric <> COALESCE(r.s, 0)`},

	// ---- referential integrity ----
	{"reservation_without_budget", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text
		  FROM reservations r LEFT JOIN budget_accounts b ON b.id = r.budget_account_id
		 WHERE r.budget_account_id IS NULL OR b.id IS NULL`},
	{"reservation_budget_tenant_mismatch", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' budget_tenant=' || b.tenant_id::text
		  FROM reservations r JOIN budget_accounts b ON b.id = r.budget_account_id
		 WHERE b.tenant_id <> r.tenant_id`},
	{"reservation_without_tenant", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text
		  FROM reservations r LEFT JOIN tenants t ON t.id = r.tenant_id
		 WHERE t.id IS NULL`},
	{"event_without_reservation", `
		SELECT 'event=' || e.id || ' reservation=' || e.reservation_id::text
		  FROM reservation_events e LEFT JOIN reservations r ON r.id = e.reservation_id
		 WHERE r.id IS NULL`},

	// ---- reservation invariants ----
	{"invalid_state", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state
		  FROM reservations r WHERE r.state NOT IN ('RESERVED','UNKNOWN','RELEASED','RECONCILED')`},
	{"actual_cost_on_non_reconciled", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state
		  FROM reservations r WHERE r.state <> 'RECONCILED' AND r.actual_cost_minor_units IS NOT NULL`},
	{"reconciled_without_actual_cost", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text
		  FROM reservations r WHERE r.state = 'RECONCILED' AND r.actual_cost_minor_units IS NULL`},
	{"terminal_without_timestamp", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state
		  FROM reservations r WHERE r.state IN ('RELEASED','RECONCILED') AND r.resolved_at IS NULL`},
	{"nonterminal_with_timestamp", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state
		  FROM reservations r WHERE r.state IN ('RESERVED','UNKNOWN') AND r.resolved_at IS NOT NULL`},
	{"invalid_pending_actual_state", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state
		  FROM reservations r
		 WHERE r.pending_actual_minor_units IS NOT NULL
		   AND (r.state = 'RELEASED'
		        OR (r.state = 'RECONCILED' AND r.pending_actual_minor_units IS DISTINCT FROM r.actual_cost_minor_units))`},

	// ---- event invariants ----
	{"missing_or_duplicate_created_event", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' created_events=' || c.n
		  FROM reservations r
		 CROSS JOIN LATERAL (SELECT count(*) n FROM reservation_events e
		                      WHERE e.reservation_id = r.id AND e.from_state IS NULL AND e.to_state = 'RESERVED') c
		 WHERE c.n <> 1`},
	{"created_event_amount_mismatch", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' event_amount=' || COALESCE(e.amount_minor_units::text, 'NULL')
		       || ' reservation_amount=' || r.amount_minor_units
		  FROM reservations r
		  JOIN reservation_events e ON e.reservation_id = r.id AND e.from_state IS NULL AND e.to_state = 'RESERVED'
		 WHERE e.amount_minor_units IS DISTINCT FROM r.amount_minor_units`},
	{"duplicate_settlement", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' settlements=' || c.n
		  FROM reservations r
		 CROSS JOIN LATERAL (SELECT count(*) n FROM reservation_events e
		                      WHERE e.reservation_id = r.id AND e.to_state = 'RECONCILED') c
		 WHERE c.n > 1`},
	{"reconciled_without_matching_event", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' actual=' || COALESCE(r.actual_cost_minor_units::text, 'NULL')
		  FROM reservations r
		 WHERE r.state = 'RECONCILED'
		   AND NOT EXISTS (SELECT 1 FROM reservation_events e
		                    WHERE e.reservation_id = r.id AND e.to_state = 'RECONCILED'
		                      AND e.amount_minor_units IS NOT DISTINCT FROM r.actual_cost_minor_units)`},
	{"reconciliation_event_without_reconciled_state", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state
		  FROM reservations r
		 WHERE r.state <> 'RECONCILED'
		   AND EXISTS (SELECT 1 FROM reservation_events e WHERE e.reservation_id = r.id AND e.to_state = 'RECONCILED')`},
	{"released_state_event_mismatch", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state || ' released_events=' || c.n
		  FROM reservations r
		 CROSS JOIN LATERAL (SELECT count(*) n FROM reservation_events e
		                      WHERE e.reservation_id = r.id AND e.to_state = 'RELEASED'
		                        AND e.amount_minor_units IS NOT DISTINCT FROM r.amount_minor_units) c
		 WHERE (r.state = 'RELEASED' AND c.n <> 1)
		    OR (r.state <> 'RELEASED' AND EXISTS (SELECT 1 FROM reservation_events e
		                                           WHERE e.reservation_id = r.id AND e.to_state = 'RELEASED'))`},
	{"unknown_state_event_mismatch", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text || ' state=' || r.state || ' expiry_events=' || c.n
		  FROM reservations r
		 CROSS JOIN LATERAL (SELECT count(*) n FROM reservation_events e
		                      WHERE e.reservation_id = r.id AND e.to_state = 'UNKNOWN'
		                        AND e.from_state = 'RESERVED'
		                        AND e.amount_minor_units IS NOT DISTINCT FROM r.amount_minor_units) c
		 WHERE (r.state = 'UNKNOWN' AND c.n <> 1)
		    OR (r.state = 'RESERVED' AND EXISTS (SELECT 1 FROM reservation_events e
		                                          WHERE e.reservation_id = r.id AND e.to_state = 'UNKNOWN'))
		    OR (r.state IN ('RELEASED','RECONCILED') AND c.n > 1)`},
	{"conflicting_terminal_events", `
		SELECT r.id::text || ' tenant=' || r.tenant_id::text
		  FROM reservations r
		 WHERE EXISTS (SELECT 1 FROM reservation_events e WHERE e.reservation_id = r.id AND e.to_state = 'RELEASED')
		   AND EXISTS (SELECT 1 FROM reservation_events e WHERE e.reservation_id = r.id AND e.to_state = 'RECONCILED')`},

	// ---- idempotency integrity ----
	{"duplicate_idempotency_key", `
		SELECT 'tenant=' || r.tenant_id::text || ' key=' || r.idempotency_key || ' reservations=' || count(*)
		  FROM reservations r WHERE r.idempotency_key IS NOT NULL
		 GROUP BY r.tenant_id, r.idempotency_key HAVING count(*) > 1`},
}

// AuditPG is the read-only ledger consistency auditor. It recomputes every
// account from the reservation rows and event log and returns one string per
// violation ("<check>: <detail>"). The result is empty ONLY when the ledger is
// internally consistent. All checks run in a single REPEATABLE READ read-only
// transaction, so concurrent writers cannot produce false violations.
//
// A nil error with violations means "the audit ran, the books are wrong" and
// must be treated as a failure by callers. A non-nil error means the audit
// itself could not complete and proves nothing.
func AuditPG(ctx context.Context, db *sql.DB) ([]string, error) {
	tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return nil, fmt.Errorf("audit: begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	var violations []string
	for _, c := range auditChecks {
		rows, err := tx.QueryContext(ctx, c.query)
		if err != nil {
			return violations, fmt.Errorf("audit %s: %w", c.name, err)
		}
		for rows.Next() {
			var detail string
			if err := rows.Scan(&detail); err != nil {
				rows.Close()
				return violations, fmt.Errorf("audit %s: scan: %w", c.name, err)
			}
			violations = append(violations, c.name+": "+detail)
		}
		if err := rows.Err(); err != nil {
			rows.Close()
			return violations, fmt.Errorf("audit %s: %w", c.name, err)
		}
		rows.Close()
	}
	return violations, nil
}
