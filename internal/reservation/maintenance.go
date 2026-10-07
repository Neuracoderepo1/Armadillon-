package reservation

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"
)

// Maintainer is the durable-ledger surface the maintenance loop drives.
// *PGManager implements it.
type Maintainer interface {
	ExpireStaleE() (int, error)
	RecoverPending() (int, error)
	PendingCount(ctx context.Context) (int, error)
	Ping(ctx context.Context) error
}

var _ Maintainer = (*PGManager)(nil)

// Maintenance runs reservation expiry and pending-actual recovery. Both are
// safe to run on every instance concurrently (SKIP LOCKED claims) and safe to
// retry. Neither ever releases uncertain spend: expiry moves RESERVED to
// UNKNOWN (hold kept) and recovery only settles durably recorded provider
// results.
//
// Readiness: the service reports ready only after one full successful pass
// (startup recovery) and while the latest pass had no unrecoverable rows and
// the database is reachable.
type Maintenance struct {
	m   Maintainer
	log *slog.Logger

	mu       sync.Mutex
	started  bool // at least one full pass completed without error
	lastErr  error
	lastRun  time.Time
	pending  int
	recovery int64 // total reservations recovered by this instance
	expired  int64 // total reservations expired by this instance
}

func NewMaintenance(m Maintainer, log *slog.Logger) *Maintenance {
	if log == nil {
		log = slog.Default()
	}
	return &Maintenance{m: m, log: log}
}

// RunOnce performs one expiry + recovery pass and records the outcome.
func (mt *Maintenance) RunOnce(ctx context.Context) error {
	var errs []error
	exp, err := mt.m.ExpireStaleE()
	if err != nil {
		errs = append(errs, fmt.Errorf("expire: %w", err))
	}
	rec, err := mt.m.RecoverPending()
	if err != nil {
		errs = append(errs, fmt.Errorf("recover: %w", err))
	}
	pend, perr := mt.m.PendingCount(ctx)
	if perr != nil {
		errs = append(errs, fmt.Errorf("pending count: %w", perr))
	}
	err = errors.Join(errs...)

	mt.mu.Lock()
	mt.lastRun = time.Now()
	mt.lastErr = err
	mt.pending = pend
	mt.recovery += int64(rec)
	mt.expired += int64(exp)
	if err == nil {
		mt.started = true
	}
	mt.mu.Unlock()

	if err != nil {
		mt.log.Error("reservation maintenance pass failed", "err", err)
	} else if exp > 0 || rec > 0 {
		mt.log.Info("reservation maintenance", "expired", exp, "recovered", rec, "pending", pend)
	}
	return err
}

// Run executes RunOnce immediately and then every interval until ctx ends.
func (mt *Maintenance) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = 5 * time.Second
	}
	_ = mt.RunOnce(ctx)
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			_ = mt.RunOnce(ctx)
		}
	}
}

// Ready returns nil only when the ledger's control loop is healthy: the
// database answers, startup recovery completed, and the last pass left no
// unrecoverable pending work.
func (mt *Maintenance) Ready(ctx context.Context) error {
	if err := mt.m.Ping(ctx); err != nil {
		return err
	}
	mt.mu.Lock()
	defer mt.mu.Unlock()
	if !mt.started {
		return errors.New("startup recovery has not completed")
	}
	if mt.lastErr != nil {
		return fmt.Errorf("last maintenance pass failed: %w", mt.lastErr)
	}
	return nil
}

// Stats reports counters for observability.
func (mt *Maintenance) Stats() (recovered, expired int64, pending int, lastRun time.Time) {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	return mt.recovery, mt.expired, mt.pending, mt.lastRun
}
