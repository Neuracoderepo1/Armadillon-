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

Armadillon supports two persistence backends:

- **In-memory** — Used for development and testing. Nothing survives a process restart.
- **PostgreSQL** — Production mode. Budget holds and settlements are durable across gateway restarts.

## Supported providers

`cmd/gateway/main.go` registers exactly one provider at startup:

- **GenericHTTP** — Registered when `VG_UPSTREAM_URL` is set; routes `/proxy/*` to the configured HTTP upstream.
- **MockProvider** — Registered when `VG_UPSTREAM_URL` is not set; provides a built-in mock for demo/testing.

Provider-specific OpenAI, Google, and Unstructured integrations (referenced in `internal/provider/`) are **not wired into** `cmd/gateway/main.go` and are not active through this entry point.

## Tech

Go, PostgreSQL, `config.yaml`, `net/http`, `cmd/gateway`.

## Setup

Clone, build, and lint:

```bash
go build ./...
go vet ./...
```

## Running the gateway

Use `./gateway` with an optional `--config` flag.

To enable upstream proxying, set `VG_UPSTREAM_URL` to an `http://` or `https://` URL before starting. Without it, the gateway uses the built-in mock provider.

## Running tests

```bash
go test ./...
```

PostgreSQL-backed tests require `VG_TEST_POSTGRES_DSN` to be set.

## Known limitations

- In-process rate limiting only (no distributed rate limiting yet).
- No admin API for tenant/key management.
- Legacy key compatibility mode still present.
- Estimate-vs-billing variance can cause minor budget hold differences.
- Conservative in-flight request handling to prevent budget overruns under ambiguous failures.

## Roadmap

- Distributed rate limiting
- Admin API
- Webhooks for budget alerts
- CLI tool
- React/TypeScript dashboard
- Claude API validation

## CI / Verification

GitHub Actions CI defines three jobs:

- **test** — Go 1.22 on ubuntu-latest with a PostgreSQL 16 service. Steps: apply control-plane migrations, gofmt check, go vet, go build, compile-only tests (`-run '^$'`), race-enabled tests (`-race -count=1`), reservation ledger race tests repeated x10, and fault-injection race tests repeated x10.
- **test-stable** — Stable Go on ubuntu-latest with PostgreSQL 16. Steps: apply migrations, go vet, and race-enabled tests.
- **scan** — Stable Go on ubuntu-latest. Runs `staticcheck` and `govulncheck`.
