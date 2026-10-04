// Package ledger implements the append-only financial event log (spec
// section 17). In this MVP it is in-memory; a durable Postgres-backed
// implementation would satisfy the same Ledger interface without changing
// any caller.
package ledger

import (
	"sync"
	"time"

	"velocityguard/internal/money"
)

type EventType string

const (
	RequestEstimated    EventType = "REQUEST_ESTIMATED"
	ReservationCreated  EventType = "RESERVATION_CREATED"
	RequestAllowed      EventType = "REQUEST_ALLOWED"
	RequestThrottled    EventType = "REQUEST_THROTTLED"
	RequestBlocked      EventType = "REQUEST_BLOCKED"
	RequestExecuted     EventType = "REQUEST_EXECUTED"
	RequestFailed       EventType = "REQUEST_FAILED"
	UsageReported       EventType = "USAGE_REPORTED"
	CostReconciled      EventType = "COST_RECONCILED"
	ReservationReleased EventType = "RESERVATION_RELEASED"
	ReservationExpired  EventType = "RESERVATION_EXPIRED"
	// ReservationUnknown: an upstream failure did not prove the request was never
	// dispatched, so the hold was kept (not released) for recovery.
	ReservationUnknown EventType = "RESERVATION_UNKNOWN"
	CircuitOpened      EventType = "CIRCUIT_OPENED"
	CircuitClosed      EventType = "CIRCUIT_CLOSED"
	KillSwitchOn       EventType = "KILL_SWITCH_ACTIVATED"
	KillSwitchOff      EventType = "KILL_SWITCH_DEACTIVATED"

	// LegacyUnscopedKeyUsed fires whenever a key with no Scopes set
	// passes a scope-gated request purely via the legacy compatibility
	// path in httpapi.requireAuth. An unscoped key acts as a skeleton
	// key across every scope, so this event exists to make that
	// otherwise-silent fact visible in the audit trail — see
	// requireAuth's doc comment for why the compatibility path exists.
	LegacyUnscopedKeyUsed EventType = "LEGACY_UNSCOPED_KEY_USED"
)

type Event struct {
	EventID       string
	Type          EventType
	TenantID      string
	Timestamp     time.Time
	RequestID     string
	ReservationID string
	Amount        money.Micros
	Provider      string
	Route         string
	Metadata      map[string]string
}

type Ledger struct {
	mu     sync.Mutex
	events []Event
	seq    uint64
}

func New() *Ledger {
	return &Ledger{}
}

func (l *Ledger) Append(e Event) Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.seq++
	if e.EventID == "" {
		e.EventID = e.Type.String() + "-" + itoa(l.seq)
	}
	if e.Timestamp.IsZero() {
		e.Timestamp = time.Now()
	}
	l.events = append(l.events, e)
	return e
}

// Since returns all events after the given sequence-derived cursor (simple
// approach: return all events with index >= from). Used by the live event
// stream / SSE feed.
func (l *Ledger) Since(from int) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	if from < 0 {
		from = 0
	}
	if from >= len(l.events) {
		return nil
	}
	out := make([]Event, len(l.events)-from)
	copy(out, l.events[from:])
	return out
}

func (l *Ledger) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.events)
}

func (l *Ledger) ForTenant(tenantID string) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	var out []Event
	for _, e := range l.events {
		if e.TenantID == tenantID {
			out = append(out, e)
		}
	}
	return out
}

func (t EventType) String() string { return string(t) }

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
