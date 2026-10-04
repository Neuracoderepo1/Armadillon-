package reservation

import (
	"time"

	"velocityguard/internal/money"
)

// Service is the reservation surface the gateway, risk engine and HTTP API
// depend on. *Manager (in-memory, dev/test only: nothing survives a restart)
// and *PGManager (durable, authoritative) both implement it.
//
// The ambiguity-safe contract the gateway relies on:
//
//   - Release returns budget and is only for requests PROVEN never to have
//     reached the provider.
//   - MarkUnknown keeps the hold but flags the reservation for recovery, for
//     failures that do not prove the request was never dispatched.
//   - RecordPendingActual stores a completed provider result before
//     Reconcile, so a crash in between cannot lose it.
//   - ResolveUnknownReconciled settles a reservation that is already UNKNOWN
//     (e.g. TTL expiry raced a slow but successful provider call).
type Service interface {
	Reserve(tenantID string, amount money.Micros, ttl time.Duration) (*Reservation, error)
	Release(reservationID string) error
	MarkUnknown(reservationID string) error
	RecordPendingActual(reservationID string, actual money.Micros) error
	Reconcile(reservationID string, actual money.Micros) (ReconcileResult, error)
	ResolveUnknownReconciled(reservationID string, actual money.Micros) (ReconcileResult, error)
	Exposure(tenantID string) Exposure
	SetBudget(tenantID string, limit money.Micros)
}

var (
	_ Service = (*Manager)(nil)
	_ Service = (*PGManager)(nil)
)
