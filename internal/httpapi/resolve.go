package httpapi

// Operator-only resolution of UNKNOWN reservations.
//
// An UNKNOWN reservation is a hold whose real-world outcome is not proven (an
// ambiguous provider failure, or a TTL expiry). The budget stays held until a
// human or a verified provider reconciliation decides what happened. Without
// this surface the only exit was a direct database write.
//
//	GET  /v1/reservations/unknown[?tenant=<id>&limit=<n>]   backlog + summary
//	POST /v1/reservations/{id}/resolve                      resolve one
//
// Both require the operator token (never a tenant API key). Every resolution
// requires written evidence and is recorded in the ledger.

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"velocityguard/internal/ledger"
	"velocityguard/internal/money"
	"velocityguard/internal/reservation"
)

// UnknownResolver is the durable-ledger surface the resolver needs. *PGManager
// implements it; the in-memory dev manager does not, so the routes answer 404
// there rather than pretending to work.
type UnknownResolver interface {
	ListUnknown(tenantID string, limit int) ([]*reservation.Reservation, reservation.UnknownSummary, error)
	ResolveUnknownReleased(id string) error
	ResolveUnknownReconciled(id string, actual money.Micros) (reservation.ReconcileResult, error)
}

// SetUnknownResolver enables the UNKNOWN resolution routes.
func (s *Server) SetUnknownResolver(r UnknownResolver) { s.Resolver = r }

const maxEvidenceLen = 1000

type unknownView struct {
	ID            string `json:"id"`
	TenantID      string `json:"tenant_id"`
	AmountMicros  int64  `json:"amount_micros"`
	PendingMicros *int64 `json:"pending_actual_micros,omitempty"`
	CreatedAt     string `json:"created_at"`
	ExpiresAt     string `json:"expires_at"`
}

func (s *Server) handleListUnknown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "GET only", http.StatusMethodNotAllowed)
		return
	}
	if s.Resolver == nil {
		http.NotFound(w, r)
		return
	}
	limit := 100
	if v := r.URL.Query().Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			http.Error(w, "limit must be a positive integer", http.StatusBadRequest)
			return
		}
		limit = n
	}
	rows, sum, err := s.Resolver.ListUnknown(r.URL.Query().Get("tenant"), limit)
	if err != nil {
		writeResolveErr(w, err)
		return
	}
	out := make([]unknownView, 0, len(rows))
	for _, rv := range rows {
		v := unknownView{ID: rv.ID, TenantID: rv.TenantID, AmountMicros: int64(rv.Amount),
			CreatedAt: rv.CreatedAt.UTC().Format("2006-01-02T15:04:05Z"), ExpiresAt: rv.ExpiresAt.UTC().Format("2006-01-02T15:04:05Z")}
		if rv.PendingActual != nil {
			p := int64(*rv.PendingActual)
			v.PendingMicros = &p
		}
		out = append(out, v)
	}
	writeJSON(w, http.StatusOK, map[string]interface{}{
		"summary": map[string]interface{}{
			"count":              sum.Count,
			"held_micros":        int64(sum.Held),
			"oldest_age_seconds": int64(sum.OldestAge.Seconds()),
		},
		"reservations": out,
	})
}

// handleReservationAction serves /v1/reservations/{id}/resolve.
func (s *Server) handleReservationAction(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/v1/reservations/")
	parts := strings.Split(rest, "/")
	if len(parts) != 2 || parts[0] == "" || parts[1] != "resolve" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if s.Resolver == nil {
		http.NotFound(w, r)
		return
	}
	id := parts[0]
	var body struct {
		Outcome      string `json:"outcome"`       // "released" | "reconciled"
		ActualMicros *int64 `json:"actual_micros"` // required for "reconciled"
		Evidence     string `json:"evidence"`      // required: what proves the outcome
	}
	dec := json.NewDecoder(http.MaxBytesReader(w, r.Body, 1<<14))
	dec.DisallowUnknownFields()
	if err := dec.Decode(&body); err != nil {
		http.Error(w, "invalid JSON body: "+err.Error(), http.StatusBadRequest)
		return
	}
	ev := strings.TrimSpace(body.Evidence)
	if ev == "" || len(ev) > maxEvidenceLen {
		http.Error(w, "evidence is required (1-1000 chars): state what proves the outcome", http.StatusBadRequest)
		return
	}

	meta := map[string]string{"outcome": body.Outcome, "evidence": ev}
	var err error
	switch body.Outcome {
	case "released":
		if body.ActualMicros != nil {
			http.Error(w, "actual_micros is not allowed with outcome=released", http.StatusBadRequest)
			return
		}
		err = s.Resolver.ResolveUnknownReleased(id)
	case "reconciled":
		if body.ActualMicros == nil || *body.ActualMicros < 0 {
			http.Error(w, "actual_micros (>= 0) is required with outcome=reconciled", http.StatusBadRequest)
			return
		}
		meta["actual_micros"] = strconv.FormatInt(*body.ActualMicros, 10)
		_, err = s.Resolver.ResolveUnknownReconciled(id, money.Micros(*body.ActualMicros))
	default:
		http.Error(w, "outcome must be released|reconciled", http.StatusBadRequest)
		return
	}
	if err != nil {
		writeResolveErr(w, err)
		return
	}
	// Audit trail. The operator token is never logged, only the decision.
	s.Ledger.Append(ledger.Event{Type: ledger.ReservationResolved, ReservationID: id, Metadata: meta})
	writeJSON(w, http.StatusOK, map[string]interface{}{"id": id, "outcome": body.Outcome})
}

func writeResolveErr(w http.ResponseWriter, err error) {
	switch {
	case errors.Is(err, reservation.ErrNotFound):
		http.Error(w, "reservation not found", http.StatusNotFound)
	case errors.Is(err, reservation.ErrTerminalState),
		errors.Is(err, reservation.ErrPendingActualExists),
		errors.Is(err, reservation.ErrConflictingReconcile):
		// Not UNKNOWN anymore, or a durable pending actual contradicts the request.
		http.Error(w, err.Error(), http.StatusConflict)
	case errors.Is(err, reservation.ErrInvalidAmount):
		http.Error(w, err.Error(), http.StatusBadRequest)
	case errors.Is(err, reservation.ErrBackendUnavailable):
		http.Error(w, "ledger unavailable; retry", http.StatusServiceUnavailable)
	default:
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
}
