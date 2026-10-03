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
	"log"
	"net/http"
	"net/url"
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
	var rm reservation.Repository
	switch cfg.StoreMode {
	case config.StoreModePostgres:
		ps, err := store.OpenPostgresStore(ctx, cfg.PostgresDSN)
		if err != nil {
			log.Fatalf("connecting to postgres control plane: %v", err)
		}
		st = ps
		// Durable reservations: shares ps's connection pool rather than
		// opening a second one. This is what actually makes "financial
		// firewall" true across restarts — see
		// migrations/0002_financial_control.sql and the State doc
		// comment in internal/reservation/reservation.go for the
		// corrected UNKNOWN-state safety rule this backend enforces at
		// the database layer, not just in Go.
		rm = reservation.NewPostgresRepository(ps.DB())
		log.Printf("reservation backend: postgres (durable across restarts)")
	default:
		st = store.NewMemoryStore()
		rm = reservation.NewManager()
		log.Printf("reservation backend: in-memory (lost on restart — use VG_STORE_MODE=postgres for production)")
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
