# Armadillon

**A budget-enforcing gateway for LLM API traffic.** Per-tenant spend limits, authenticated access, and a ledger that guarantees AI costs can't run away.

> Status: the PostgreSQL reservation core has passing CI evidence. The real-upstream path is **not production-certified**: GenericHTTP reports synthetic usage, HTTP idempotency is not durable, and upstream responses are not forwarded transparently. See [Known limitations](#known-limitations) and [P0 issue #11](https://github.com/Neuracoderepo1/Armadillon-/issues/11).

## The problem

Teams put paid LLM APIs behind their products and discover the bill after the fact. A bug, a retry storm, or a leaked key can burn a month's budget in minutes. Armadillon sits between your applications and your model providers and refuses traffic that would exceed a tenant's budget, before the money is spent.

## What it does

Armadillon is a **Go budget-enforcement / cost-control gateway** for LLM API traffic. It sits between client applications and downstream model providers, reserves an estimated amount against a tenant's budget, forwards the request, and settles using the usage returned by the selected provider adapter. Settlement accuracy depends on that adapter: the currently wired GenericHTTP adapter does not parse provider usage and returns placeholder units. The real-upstream path therefore does not currently reconcile actual provider charges and remains blocked from production traffic.

**Verified reservation properties:**
- PostgreSQL reservation writes serialize on the tenant budget row, preventing concurrent reservations from overcommitting the configured estimate.
- The reservation test suite exercises concurrent requests, fault injection, ambiguous outcomes, and recovery.
- In PostgreSQL mode, reservation state is durable across restarts and a startup/background maintenance worker expires stale holds and retries recovery.

These guarantees do **not** establish actual provider-billing reconciliation, durable HTTP idempotency, or multi-replica risk-control state. A provider charge can differ from the estimate; see [P0 issue #11](https://github.com/Neuracoderepo1/Armadillon-/issues/11).

## Request flow

Every request follows a **reserve-then-settle** pattern:

1. **Authenticate** — A Bearer API key is presented. The key is stored only as a SHA-256 hash; tenant identity is derived from the key, never from client-supplied headers or query parameters.
2. **Reserve** — The risk engine estimates the cost using the pricing registry and reserves that amount against the tenant's budget.
3. **Forward** — The request is sent to the downstream provider through a provider abstraction, behind a circuit breaker.
4. **Settle** — The gateway calculates a settlement amount from the usage returned by the provider adapter and reconciles the reservation. This is only as trustworthy as the adapter's usage data. GenericHTTP currently returns synthetic usage, so this is not actual provider-invoice reconciliation.

**Handling ambiguous failures:** If a provider call times out or is reset mid-response, the gateway cannot prove the request was *not* dispatched. In those cases the gateway keeps the budget hold (marking the settlement as `UNKNOWN`) rather than prematurely releasing it, preventing budget from being spent twice. A reconciliation loop later resolves these pending entries.

## Persistence modes

Armadillon supports two persistence back-ends for its control plane:

- **In-memory** — Suitable for single-replica development and testing.
- **PostgreSQL** — The durable reservation-storage path. SQL migrations are in `migrations/`. The gateway runs startup recovery and a background maintenance loop for stale reservations and unknown settlements. `/readyz` remains 503 until startup recovery succeeds. This is necessary but not sufficient for production certification: the separate event ledger and risk-engine state are still process-local, and the P0 upstream-accounting/idempotency gaps remain open.

Tests that require a live database skip gracefully unless the `VG_TEST_POSTGRES_DSN` environment variable is set.

## Runtime provider wiring

The actual entry point is `cmd/gateway/main.go`. It registers only these runtime providers:

| Runtime provider | Selection condition |
|---|---|
| `GenericHTTP` | `VG_UPSTREAM_URL` is set; the configured destination host is allowlisted |
| `MockProvider` | `VG_UPSTREAM_URL` is not set; demo/test mode |

Provider package names or directories are not proof that a provider is registered in the running gateway. The OpenAI, Google, and Unstructured entries in older documentation are not runtime targets wired by the current `cmd/gateway/main.go`.

GenericHTTP currently returns placeholder usage (`InputUnits: 1`, `OutputUnits: 0`) rather than parsing provider-reported token or billing usage. Do not treat its settlement as an actual provider charge. Real upstream traffic remains blocked pending provider-specific usage parsing, versioned pricing, durable request idempotency, and response forwarding (see [P0 issue #11](https://github.com/Neuracoderepo1/Armadillon-/issues/11)).

> **Estimate versus actual:** the risk engine's pre-call estimate is a prediction from the pricing registry. The current HTTP handler supplies fixed estimate units (100 input, 100 output); GenericHTTP's post-call usage is synthetic. Consequently, the current `actual_cost` field is a calculation from placeholder usage, not a provider invoice or independently verified charge.

## Components

| Component | Role |
|---|---|
| Risk engine | Fast allow/deny decisions on every request |
| Reservation manager | In-memory or PostgreSQL-backed reservation state, depending on store mode |
| Circuit breaker | Process-local breaker state; not coordinated across replicas |
| Ledger | In-memory event log; not a durable financial audit store |
| Pricing registry | Per-model cost estimates |
| Provider abstraction | One interface across model providers |
| Control plane (Postgres) | Tenants, API keys, budget accounts |

## Tech

- **Language:** Go
- **Control plane:** PostgreSQL with SQL migrations
- **Configuration:** environment variables loaded by `internal/config`; no `config.yaml` or `--config` support
- **HTTP server:** Standard Go `net/http`, listens on a configurable address
- **CLI:** `cmd/gateway` — the main entry point

## Setup

```bash
# Clone the repository
git clone https://github.com/Neuracoderepo1/Armadillon-.git
cd Armadillon-

# Build
go build ./...

# Lint
go vet ./...
```

### Running the gateway

`cmd/gateway` reads environment variables only. It does **not** read `config.yaml` and does **not** support a `--config` flag.

Important variables:

| Variable | Purpose |
|---|---|
| `VG_ADDR` | HTTP listen address; defaults to `:8080` |
| `VG_STORE_MODE` | `memory` (default) or `postgres` |
| `VG_POSTGRES_DSN` | Required when `VG_STORE_MODE=postgres` |
| `VG_OPERATOR_TOKEN` | Required in PostgreSQL mode; minimum 32 characters |
| `VG_UPSTREAM_URL` | Optional single upstream destination; requires PostgreSQL mode and selects GenericHTTP |
| `VG_RATE_LIMIT_IP_RPS`, `VG_RATE_LIMIT_IP_BURST` | Per-process IP rate limits |
| `VG_RATE_LIMIT_TENANT_RPS`, `VG_RATE_LIMIT_TENANT_BURST` | Per-process tenant rate limits |
| `VG_SHUTDOWN_TIMEOUT` | Graceful shutdown duration, e.g. `15s` |

For a local mock/demo run:

```bash
VG_STORE_MODE=memory go run ./cmd/gateway
```

For PostgreSQL-backed reservation storage, provide `VG_POSTGRES_DSN` and a strong `VG_OPERATOR_TOKEN`. PostgreSQL mode skips demo-tenant creation; tenants, keys, and budgets must currently be provisioned out of band because no admin API exists.

Setting `VG_UPSTREAM_URL` switches from MockProvider to GenericHTTP. **Do not enable real upstream production traffic until P0 issue #11 is closed and independently certified.** The current GenericHTTP usage is synthetic and the HTTP request path is not durably idempotent.

### Running tests

```bash
# Run all unit tests
go test ./...

# Run tests with race detector
go test ./... -race

# Run tests with verbose output
go test -v ./...
```

#### PostgreSQL-backed tests

Some integration tests require a live PostgreSQL database. To run them:

```bash
# Set the Postgres DSN before running tests
export VG_TEST_POSTGRES_DSN="host=localhost port=5432 user=postgres password=postgres dbname=armadillon sslmode=disable"

# Run tests with Postgres
go test -v ./...
```

These tests cover concurrency scenarios, crash/restart recovery, ambiguous failure handling, and the `/readyz` endpoint.

## Known limitations

These limitations are present in the current source and block production certification of the real-upstream path:

1. **Synthetic provider usage (P0)** — `internal/provider/provider.go` returns `Usage{InputUnits: 1, OutputUnits: 0}` for GenericHTTP. It does not parse provider-reported usage or invoice data. The demo pricing registry is not a production provider price book.
2. **No durable HTTP idempotency (P0)** — `X-Request-ID` is used for correlation and forwarded upstream, but the HTTP request path does not use it as a durable idempotency key. The PostgreSQL reservation manager's keyed capability is not wired into this request path.
3. **Upstream response is not transparently forwarded (P0)** — the proxy handler returns gateway decision/accounting JSON and does not forward the upstream response status and body to the caller.
4. **Process-local risk state** — kill-switch flags, velocity tracking, and circuit-breaker state live in the gateway process. Independent replicas do not share these controls.
5. **In-memory event ledger** — the event log, including operator-resolution evidence, is process-local. Durable reservation rows do not make this a durable, tamper-evident audit ledger.
6. **In-process rate limiting** — IP and tenant rate limits are scoped to one gateway process and are not multi-replica safe. The trusted-proxy boundary for forwarded client IPs also needs explicit hardening.
7. **No admin API** — tenant, API-key, and budget provisioning currently requires out-of-band administration.
8. **Legacy unscoped-key compatibility** — the compatibility path remains in the codebase and is being phased out.
9. **Single-period budget behavior** — the schema supports multiple periods, but the current reservation manager uses one flat daily account and does not implement independent hourly/daily/monthly rollover.

Track the P0 closure criteria in [issue #11](https://github.com/Neuracoderepo1/Armadillon-/issues/11). No real upstream production traffic should be enabled while those P0 items remain open.

## Roadmap

The following items remain unimplemented or incomplete:

- Provider-specific usage parsers, versioned pricing, and provider-cost reconciliation
- Durable HTTP request idempotency, including concurrent duplicates and replay after restart
- Transparent upstream status/body forwarding with safe response-## CI / Verification

The GitHub Actions workflow runs on pushes to `main` and pull requests targeting `main`:

| Job | Description |
|---|---|
| **test** | Go 1.22, PostgreSQL 16, migrations, gofmt, vet, build, test compilation, race tests, reservation-ledger race suite (10 repetitions), and fault-injection race suite (10 repetitions) |
| **test-stable** | Current stable Go toolchain, PostgreSQL 16, migrations, vet, and race tests |
| **scan** | Current stable Go toolchain, staticcheck, and govulncheck |

A passing CI run verifies the code and tests on that commit. It does not by itself establish provider-invoice reconciliation, production deployment, multi-replica risk-state safety, or certification of the real-upstream path.

