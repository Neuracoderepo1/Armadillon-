# VelocityGuard

VelocityGuard is a cost-and-risk gateway for metered API/provider spend
(e.g. LLM API calls). It sits in front of upstream providers and enforces a
deterministic control-plane pipeline on every request:

```
estimate -> reserve -> risk check -> allow / throttle / block -> execute -> reconcile -> telemetry
```

## Why

Metered API spend (LLM tokens, per-call billing, etc.) can spike fast enough
that after-the-fact billing alerts are too slow to prevent damage.
VelocityGuard reserves estimated cost *before* a call executes, scores
velocity/acceleration of spend in real time, and can throttle or block a
tenant/route/provider before an incident becomes expensive — then
reconciles the reservation against actual usage once the call completes.

## Core components

- **`internal/risk`** — deterministic (non-ML) risk engine: velocity and
  acceleration tracking, scoring, and ALLOW / ALLOW_WITH_LIMIT / THROTTLE /
  BLOCK decisions. No external dependency on the decision hot path.
- **`internal/circuit`** — CLOSED → OPEN → HALF_OPEN → CLOSED circuit
  breaker that cuts traffic to a tenant/provider/route after a violation
  and cautiously probes recovery.
- **`internal/reservation`** — pre-execution cost reservation and
  post-execution reconciliation against actual usage.
- **`internal/ledger`** — spend accounting.
- **`internal/pricing`** — provider/model pricing registry.
- **`internal/provider`** — upstream provider abstraction.
- **`internal/store`** — persistence, with in-memory and Postgres
  (`internal/store/postgres.go`) backends.
- **`internal/gateway`** — orchestrates the full request pipeline.
- **`internal/httpapi`** — HTTP surface.
- **`internal/config`** — configuration.
- **`internal/money`** — fixed-point money handling (micros).
- **`migrations/`** — SQL migrations for the control-plane schema.
- **`cmd/gateway`** — MVP entrypoint: runs the full pipeline over real HTTP
  with an in-memory demo tenant and a mock provider, so it starts with zero
  external dependencies (no Postgres/Redis required).
- **`tests/integration`** — end-to-end acceptance tests.

## Getting started

```bash
go build ./...
go test ./...
go run ./cmd/gateway
```

## Status

Core pipeline, risk engine, and circuit breaker are implemented and
covered by unit and integration tests. The HTTP API enforces API key
scopes, has a secured kill switch and upstream proxy, per-IP/per-tenant
rate limiting, and request timeouts; CI runs the full suite with
`-race` against a live Postgres service container.

**Reservation durability.** The reservation state machine is corrected
and now durable. A reservation whose TTL expires moves to `UNKNOWN`
rather than silently returning budget (see
`internal/reservation/reservation.go`'s `State` doc comment) — only an
explicit, audited resolution (`ResolveUnknownReleased` /
`ResolveUnknownReconciled`), never a bare timeout, can determine what
actually happened. `internal/reservation.Repository` is the interface
the rest of the app depends on; `Manager` (in-memory) and
`PostgresRepository` (`migrations/0002_financial_control.sql`) both
implement it and are proven behaviorally identical by a shared test
suite run against both, including the "never overcommit under
concurrency" invariant run with `-race` against a real Postgres
instance. Set `VG_STORE_MODE=postgres` to use it — `cmd/gateway`
selects the durable backend automatically and logs which one it's
using. Verified end-to-end over real HTTP against a live Postgres
instance (reservation row and budget totals confirmed in the
database, not just in the HTTP response).

**Not yet done:** there is no periodic sweep calling `ExpireStale` in
`cmd/gateway` — a reservation only moves to `UNKNOWN` if something
calls it, and nothing does yet. The OpenAI-specific usage-parsing
adapter, the recovery worker that resolves `UNKNOWN` reservations, and
admin HTTP routes (tenant/key/budget provisioning — there is currently
no way to do this except direct SQL or writing a one-off Go program
against the `store`/`reservation` packages) are still queued.
`internal/provider` (GenericHTTP forwarding, SSRF allowlist) still has
no test coverage. `budget_accounts` supports hourly/daily/monthly
periods in its schema, but neither `Manager` nor `PostgresRepository`
implements multi-period budgets or period rollover — both use a single
flat limit per tenant, pinned to the `daily` period value.
