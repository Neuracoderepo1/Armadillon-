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

**Reservation durability.** With `VG_STORE_MODE=postgres` the gateway runs
on `reservation.PGManager`, the durable ledger (`internal/reservation/pg.go`,
migrations `0002`-`0004`). Every transition is one transaction, so reserved
and settled balances, idempotency keys and recorded provider results survive
restarts and are shared by all instances. `memory` mode keeps the in-process
`Manager`, which is for demos and tests only: nothing survives a restart, and
the gateway logs a warning at startup.

State machine: `RESERVED` becomes `RELEASED`, `UNKNOWN` or `RECONCILED`;
`UNKNOWN` becomes `RELEASED` or `RECONCILED`. TTL expiry only moves a hold to
`UNKNOWN` and never returns budget.

Failure handling in the gateway (the money-critical part):

- A hold is **released** only when the provider proves the request never left
  (`provider.NotDispatchedError`: connection refused, allowlist rejection,
  oversized body, cancelled before send).
- Any other provider failure (timeout, reset, cancellation after send) is
  **ambiguous**: the hold is kept and the reservation is marked `UNKNOWN`,
  because the upstream may have executed and billed the call.
- After a provider success the actual cost is durably recorded **before**
  reconciliation. A reservation holding a recorded result is never expired or
  released, and a crash between the two steps is finished by recovery.
- A background worker (`reservation.Maintenance`) expires stale holds and
  settles recorded results; multiple instances can run it safely
  (`FOR UPDATE SKIP LOCKED`). It runs once at startup, and `/readyz` returns
  503 until that first pass succeeds and while the latest pass is failing or
  the database is unreachable. `/health` remains a plain liveness probe.

`AuditPG` is a read-only auditor with 27 checks (balances recomputed from
reservation rows and the event log, referential integrity, event and
resolution-record consistency, idempotency); any violation is a failure.

**Not done yet:** a way to resolve `UNKNOWN` reservations out-of-band (the
manager methods `ResolveUnknownReleased` / `ResolveUnknownReconciled` exist,
but no admin route or provider-side lookup calls them, so UNKNOWN holds stay
until an operator resolves them); tenant, API-key and budget provisioning in
postgres mode (there is no admin API; budgets are set through
`PGManager.SetBudgetE`); the OpenAI-specific usage-parsing adapter; and
`staticcheck` / `govulncheck` runs. The `NOT VALID` constraints added in
`0003`/`0004` are not yet validated. A throttled request keeps its hold by
design (the risk engine relies on it to escalate repeated throttled retries).
