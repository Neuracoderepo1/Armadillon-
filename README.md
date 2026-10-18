# Armadillon

**A budget-enforcing gateway for LLM API traffic.** Per-tenant spend limits, authenticated access, and a ledger that guarantees AI costs can't run away.

> Status: core engine built and tested; distributed-production hardening in progress. See [Known limitations](#known-limitations).

## The problem

Teams put paid LLM APIs behind their products and discover the bill after the fact. A bug, a retry storm, or a leaked key can burn a month's budget in minutes. Armadillon sits between your applications and your model providers and refuses traffic that would exceed a tenant's budget, before the money is spent.

## What it does

Armadillon is a **Go budget-enforcement / cost-control gateway** for LLM API traffic. It sits between client applications and downstream model providers, enforcing per-tenant spend limits by intercepting every request, reserving estimated cost against a tenant's budget, forwarding the request, and then settling against the actual provider charge.

**Key guarantees:**
- Reserved + settled spend never exceeds the tenant's budget, including under concurrent load.
- A 150-concurrent-request test verifies this invariant.
- In the Postgres-backed mode (`cmd/gateway`), budget holds are durable across gateway restarts and include a reconciliation loop for requests that outlive their time-to-live.

## Request flow

Every request follows a **reserve-then-settle** pattern:

1. **Authenticate** — A Bearer API key is presented. The key is stored only as a SHA-256 hash; tenant identity is derived from the key, never from client-supplied headers or query parameters.
2. **Reserve** — The risk engine estimates the cost using the pricing registry and reserves that amount against the tenant's budget.
3. **Forward** — The request is sent to the downstream provider through a provider abstraction, behind a circuit breaker.
4. **Settle** — The actual cost from the provider is written to the ledger, and the reservation is released or adjusted.

**Handling ambiguous failures:** If a provider call times out or is reset mid-response, the gateway cannot prove the request was *not* dispatched. In those cases the gateway keeps the budget hold (marking the settlement as `UNKNOWN`) rather than prematurely releasing it, preventing budget from being spent twice. A reconciliation loop later resolves these pending entries.

## Persistence modes

Armadillon supports two persistence back-ends for its control plane:

- **In-memory** — Suitable for single-replica development and testing.
- **PostgreSQL** — For production deployments. SQL migrations are in the `migrations/` directory. The gateway runs a startup recovery pass and a background maintenance loop (expiry + recovery of stale reservations and unknown settlements). In this mode the `/readyz` endpoint returns 503 until startup recovery succeeds and the ledger is fully healthy; `/health` remains unchanged.

Tests that require a live database skip gracefully unless the `VG_TEST_POSTGRES_DSN` environment variable is set.

## Supported providers

Providers are configured via the config file (`config.yaml` by default). The gateway's `main.go` (`cmd/gateway/main.go`) wires up the following provider packages from the codebase:

| Provider | Package |
|---|---|
| OpenAI | `internal/provider/openai` |
| Google (Gemini) | `internal/provider/google` |
| Unstructured | `internal/provider/unstructured` |

These are the only provider packages present in the source tree; additional providers can be added by implementing the `provider.Service` interface in `internal/provider/provider.go`.

> **Important distinction:** The cost estimates used by the risk engine are **predictions** derived from the pricing registry. They are **not** the same as the provider's final invoice or actual usage charges. The settle phase captures the real cost, but there can be variance between the estimate and what the provider bills.

## Components

| Component | Role |
|---|---|
| Risk engine | Fast allow/deny decisions on every request |
| Reservation manager | Holds budget for in-flight requests |
| Circuit breaker | Stops traffic to failing providers |
| Ledger | Append-only record of spend |
| Pricing registry | Per-model cost estimates |
| Provider abstraction | One interface across model providers |
| Control plane (Postgres) | Tenants, API keys, budget accounts |

## Tech

- **Language:** Go
- **Control plane:** PostgreSQL with SQL migrations
- **Configuration:** `config.yaml` (path configurable via `--config` flag), env-based config with fail-fast validation
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

```bash
# Use the default config.yaml (placed in the working directory)
./gateway

# Or specify a custom config path
./gateway --config /path/to/config.yaml
```

The gateway requires at least one provider to be enabled in the configuration file. Configuration sections are available for OpenAI, Google, and Unstructured providers, each with `Enabled`, `Name`, and provider-specific fields.

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

These are limitations **currently present in the code**, verified from the source:

1. **In-process (non-distributed) rate limiting** — Rate limiting is scoped to a single gateway process. It is not safe across multiple replicas. *Roadmap item: distributed rate limiting for multi-replica deployments.*
2. **No admin API** — There is currently no admin API for provisioning tenants, keys, and budgets. *Roadmap item: admin API.*
3. **Legacy unscoped-key compatibility** — Legacy unscoped-key compatibility still exists in the codebase and is being phased out.
4. **Estimate-vs-provider-billing variance** — The risk engine uses cost *estimates* from the pricing registry. These estimates may differ from the provider's actual invoice or final usage charges.
5. **In-flight request handling** — When a provider call is ambiguous (timed out or reset mid-response), the gateway conservatively keeps the budget hold rather than releasing it. This protects against overspend but may temporarily tie up budget until reconciliation resolves the entry.

## Roadmap

The following items are on the public roadmap but **not yet implemented**:

- Distributed rate limiting for multi-replica deployments
- Admin API for tenant, key, and budget provisioning
- Webhooks for budget alerts
- CLI tool
- React/TypeScript dashboard
- Live validation against Claude API traffic

## CI / Verification

The GitHub Actions CI (`.github/workflows/ci.yml`) runs on every push to `main` and every pull request against `main`:

| Job | Description |
|---|---|
| **Lint** | `go vet ./...` with Go 1.22 |
| **Test** | `go test -v ./...` with Go 1.22 |
| **Test Stable** | `go test -v ./...` with Go 1.21 |
| **Scan** | `govulncheck ./...` — checks for known Go vulnerabilities |
