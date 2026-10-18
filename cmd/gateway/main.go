// Command gateway starts the VelocityGuard gateway HTTP server with an
// in-memory demo tenant, provider, and pricing configuration.
//
// This is the MVP slice described in the build directive: it proves the
// full request lifecycle (estimate -> reserve -> risk check -> allow /
// throttle / block -> execute -> reconcile) over real HTTP, using a mock
// provider so it runs with zero external dependencies (no Postgres, no
// Redis, no real upstream API needed).
package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	"velocityguard/internal/config"
	"velocityguard/internal/gateway"
	"velocityguard/internal/httpapi"
	"velocityguard/internal/ledger"
	"velocityguard/internal/pricing"
	"velocityguard/internal/provider"
	"velocityguard/internal/reservation"
	"velocityguard/internal/risk"
	"velocityguard/internal/store"

	"velocityguard/internal/money"
)

// defaultShutdownTimeout bounds how long in-flight requests get to finish
// after SIGTERM / Ctrl-C. It can be overridden with VG_SHUTDOWN_TIMEOUT
// (a Go duration such as "30s"). Orchestrators typically send SIGKILL after
// a fixed grace period (Kubernetes: 30s, Render: 30s), so keep this below it.
const defaultShutdownTimeout = 15 * time.Second

// workerStopWait bounds how long shutdown waits for background workers to
// exit before releasing database handles.
const workerStopWait = 5 * time.Second

func main() {
	// run owns every resource and returns instead of calling log.Fatal so
	// that deferred cleanup (worker stop, database Close) always executes.
	if err := run(); err != nil {
		log.Printf("gateway exiting with error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err) // fail fast, per section 42
	}
	shutdownTimeout, err := shutdownTimeoutFromEnv()
	if err != nil {
		return err
	}

	ctx := context.Background()
	var st store.Store
	switch cfg.StoreMode {
	case config.StoreModePostgres:
		ps, err := store.OpenPostgresStore(ctx, cfg.PostgresDSN)
		if err != nil {
			return fmt.Errorf("connecting to postgres control plane: %w", err)
		}
		st = ps
		// Closed only after the HTTP server has drained (defers run LIFO and
		// run() does not return until runServer has).
		defer func() {
			if err := ps.Close(); err != nil {
				log.Printf("closing control-plane database: %v", err)
			}
		}()
	default:
		st = store.NewMemoryStore()
	}

	// The reservation ledger. In postgres mode it is the DURABLE PGManager:
	// reservations, pending actuals and settlements survive restarts and are
	// shared by every instance. In memory mode it is the in-process Manager,
	// which is for demos/tests only: nothing survives a restart.
	runCtx, stopRun := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stopRun()
	var rm reservation.Service
	var maint *reservation.Maintenance
	stopWorkers := func(time.Duration) bool { return true }
	if cfg.StoreMode == config.StoreModePostgres {
		db, err := sql.Open("postgres", cfg.PostgresDSN)
		if err != nil {
			return fmt.Errorf("opening reservation ledger database: %w", err)
		}
		defer func() {
			if err := db.Close(); err != nil {
				log.Printf("closing reservation ledger database: %v", err)
			}
		}()
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(30 * time.Minute)
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := db.PingContext(pctx); err != nil {
			cancel()
			return fmt.Errorf("reservation ledger database unreachable: %w", err)
		}
		cancel()
		pg := reservation.NewPGManager(db, slog.Default())
		rm = pg
		maint = reservation.NewMaintenance(pg, slog.Default())

		// Startup recovery: settle every durably recorded provider result left by
		// a previous crash BEFORE advertising readiness. Failures do not abort
		// startup; /readyz stays 503 and the loop below keeps retrying.
		for attempt := 1; attempt <= 3; attempt++ {
			if err := maint.RunOnce(runCtx); err == nil {
				break
			}
			if attempt < 3 {
				select {
				case <-time.After(2 * time.Second):
				case <-runCtx.Done():
				}
			}
		}
		// The worker deliberately outlives signal cancellation so expiry and
		// recovery keep running while in-flight requests drain; it is stopped
		// (and waited for) explicitly after the HTTP server has shut down and
		// before the database handles above are closed.
		stopWorkers = startWorker(runCtx, func(wctx context.Context) { maint.Run(wctx, 5*time.Second) })
		defer func() {
			if !stopWorkers(workerStopWait) {
				log.Printf("maintenance worker did not stop within %s", workerStopWait)
			}
		}()
		log.Printf("reservation ledger: durable PostgreSQL manager active (expiry + recovery worker running)")
	} else {
		rm = reservation.NewManager()
		log.Printf("WARNING: in-memory reservation ledger (VG_STORE_MODE=memory): holds and settlements are lost on restart; use postgres mode for real spend control")
	}

	// The auto-created demo tenant (with its plaintext key printed to
	// stdout) only makes sense in memory/dev mode, where the process
	// starts with an empty store every time and there's no other way to
	// get a working key. In postgres mode this is a real, persistent
	// control plane — printing a fresh plaintext key to logs on every
	// boot would put a live credential into whatever aggregates this
	// process's stdout. Production tenants/keys should be provisioned
	// out-of-band (there is no admin API for this yet — see README
	// "Status"; this is scaffolding, not a complete control plane).
	var plaintextKey string
	if cfg.StoreMode == config.StoreModePostgres {
		log.Printf("store mode is postgres: skipping demo tenant bootstrap")
		log.Printf("provision tenants, API keys, and budgets via the store directly (no admin API yet) before routing real traffic")
	} else {
		tenant, err := st.CreateTenant(ctx, "Demo Corp", cfg.DemoTenantSlug)
		if err != nil {
			return fmt.Errorf("bootstrapping demo tenant: %w", err)
		}
		plaintextKey, _, err = st.CreateAPIKey(ctx, tenant.ID, []string{"proxy:write", "read:exposure"})
		if err != nil {
			return fmt.Errorf("bootstrapping demo API key: %w", err)
		}
		rm.SetBudget(tenant.ID, money.FromFloat(float64(cfg.DemoBudgetMinor)/100.0))
		log.Printf("demo tenant %q created; API key (shown once, never stored in plaintext):", tenant.Name)
		log.Printf("  %s", plaintextKey)
		log.Printf("try: curl -X POST %s/proxy/x -H 'Authorization: Bearer %s'", cfg.Addr, plaintextKey)
	}

	re := risk.NewEngine(rm, risk.DefaultPolicy())

	pr := pricing.NewRegistry()
	pr.AddRate(pricing.Rate{
		Provider: "demo-provider", Model: "demo-model",
		InputPerUnit: money.FromFloat(0.0001), OutputPerUnit: money.FromFloat(0.0002),
		Version: 1, EffectiveFrom: time.Unix(0, 0),
	})

	mock := provider.NewMockProvider("demo-provider", nil) // default usage: ~$0.03/call
	providers := provider.NewRegistry()

	// When an upstream is configured, route real traffic to it instead
	// of the mock — the demo tenant/route stays the same, but /proxy/*
	// now actually forwards. AllowedHosts is derived from the configured
	// URL itself so there is exactly one reachable destination.
	if cfg.UpstreamURL != "" {
		u, err := url.Parse(cfg.UpstreamURL)
		if err != nil {
			return fmt.Errorf("invalid VG_UPSTREAM_URL: %w", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			return fmt.Errorf("VG_UPSTREAM_URL must be http or https, got %q", u.Scheme)
		}
		generic := provider.NewGenericHTTP("demo-provider", cfg.UpstreamURL, []string{u.Host})
		providers.Register(generic)
		log.Printf("proxying /proxy/* to upstream %s (allowlisted host: %s)", cfg.UpstreamURL, u.Host)
	} else {
		providers.Register(mock)
		log.Printf("VG_UPSTREAM_URL not set; /proxy/* uses the built-in mock provider (demo mode)")
	}

	l := ledger.New()
	gw := gateway.New(re, rm, pr, providers, l)

	route := gateway.RouteConfig{Route: "/agent/execute", Provider: "demo-provider", Model: "demo-model"}
	srv := httpapi.NewServer(gw, rm, re, l, st, route, cfg.OperatorToken)
	if maint != nil {
		srv.SetReadiness(maint.Ready)
	}
	if res, ok := rm.(httpapi.UnknownResolver); ok {
		srv.SetUnknownResolver(res)
	}
	srv.SetRateLimits(cfg.RateLimitIPPerSec, cfg.RateLimitIPBurst, cfg.RateLimitTenantPerSec, cfg.RateLimitTenantBurst)

	log.Printf("VelocityGuard gateway listening on %s (store mode: %s)", cfg.Addr, cfg.StoreMode)
	log.Printf("rate limits — per-IP: %.0f req/s (burst %.0f), per-tenant: %.0f req/s (burst %.0f)",
		cfg.RateLimitIPPerSec, cfg.RateLimitIPBurst, cfg.RateLimitTenantPerSec, cfg.RateLimitTenantBurst)
	if cfg.OperatorToken == "" {
		log.Printf("VG_OPERATOR_TOKEN not set: /v1/kill-switch is disabled (404), not open")
	}

	// A bare http.ListenAndServe has no timeouts at all, which leaves the
	// process open to slow-loris style connection exhaustion (a client
	// that opens a connection and trickles bytes, or never closes an idle
	// keep-alive). Every field below closes one such gap; see config.go.
	httpSrv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           srv,
		ReadHeaderTimeout: cfg.ReadHeaderTimeout,
		ReadTimeout:       cfg.ReadTimeout,
		WriteTimeout:      cfg.WriteTimeout,
		IdleTimeout:       cfg.IdleTimeout,
	}
	ln, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("listening on %s: %w", cfg.Addr, err)
	}
	return runServer(runCtx, httpSrv, ln, shutdownTimeout)
}

// shutdownTimeoutFromEnv returns the graceful-shutdown budget.
func shutdownTimeoutFromEnv() (time.Duration, error) {
	v := os.Getenv("VG_SHUTDOWN_TIMEOUT")
	if v == "" {
		return defaultShutdownTimeout, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("VG_SHUTDOWN_TIMEOUT: %w", err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("VG_SHUTDOWN_TIMEOUT must be positive, got %s", v)
	}
	return d, nil
}

// runServer serves on ln until ctx is cancelled (SIGTERM / Ctrl-C) or serving
// fails. On cancellation it stops accepting new connections and lets in-flight
// requests finish for up to grace; if they do not, it force-closes the
// remaining connections and reports both outcomes. http.ErrServerClosed is an
// expected result and is never returned as an error.
func runServer(ctx context.Context, srv *http.Server, ln net.Listener, grace time.Duration) error {
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()

	select {
	case err := <-serveErr:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return fmt.Errorf("http serve: %w", err)
	case <-ctx.Done():
	}

	log.Printf("shutdown signal received; draining in-flight requests (timeout %s)", grace)
	shCtx, cancel := context.WithTimeout(context.Background(), grace)
	defer cancel()

	var result error
	if err := srv.Shutdown(shCtx); err != nil {
		log.Printf("graceful shutdown did not complete within %s: %v; forcing close", grace, err)
		result = fmt.Errorf("graceful shutdown did not complete within %s: %w", grace, err)
		if cerr := srv.Close(); cerr != nil {
			log.Printf("forced close failed: %v", cerr)
			result = errors.Join(result, fmt.Errorf("forced close: %w", cerr))
		} else {
			log.Printf("forced close completed; remaining connections dropped")
		}
	} else {
		log.Printf("graceful shutdown complete")
	}

	// Serve returns as soon as Shutdown/Close is called; collect it so the
	// serving goroutine never outlives this function.
	if err := <-serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		result = errors.Join(result, fmt.Errorf("http serve: %w", err))
	}
	return result
}

// startWorker runs fn on a context that is NOT cancelled by parent's
// cancellation (so background work continues while requests drain) and is
// instead cancelled by the returned stop function. stop waits up to wait for
// fn to return and reports whether it did.
func startWorker(parent context.Context, fn func(context.Context)) (stop func(wait time.Duration) bool) {
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	done := make(chan struct{})
	go func() {
		defer close(done)
		fn(ctx)
	}()
	return func(wait time.Duration) bool {
		cancel()
		select {
		case <-done:
			return true
		case <-time.After(wait):
			return false
		}
	}
}
