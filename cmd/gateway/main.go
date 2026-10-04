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
	"log"
	"log/slog"
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

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("invalid configuration: %v", err) // fail fast, per section 42
	}

	ctx := context.Background()
	var st store.Store
	switch cfg.StoreMode {
	case config.StoreModePostgres:
		ps, err := store.OpenPostgresStore(ctx, cfg.PostgresDSN)
		if err != nil {
			log.Fatalf("connecting to postgres control plane: %v", err)
		}
		st = ps
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
	if cfg.StoreMode == config.StoreModePostgres {
		db, err := sql.Open("postgres", cfg.PostgresDSN)
		if err != nil {
			log.Fatalf("opening reservation ledger database: %v", err)
		}
		db.SetMaxOpenConns(25)
		db.SetMaxIdleConns(10)
		db.SetConnMaxLifetime(30 * time.Minute)
		pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		if err := db.PingContext(pctx); err != nil {
			cancel()
			log.Fatalf("reservation ledger database unreachable: %v", err)
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
				time.Sleep(2 * time.Second)
			}
		}
		go maint.Run(runCtx, 5*time.Second)
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
			log.Fatalf("bootstrapping demo tenant: %v", err)
		}
		plaintextKey, _, err = st.CreateAPIKey(ctx, tenant.ID, []string{"proxy:write", "read:exposure"})
		if err != nil {
			log.Fatalf("bootstrapping demo API key: %v", err)
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
			log.Fatalf("invalid VG_UPSTREAM_URL: %v", err)
		}
		if u.Scheme != "http" && u.Scheme != "https" {
			log.Fatalf("VG_UPSTREAM_URL must be http or https, got %q", u.Scheme)
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
	log.Fatal(httpSrv.ListenAndServe())
}
