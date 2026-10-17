# Armadillon

**A budget-enforcing gateway for LLM API traffic.** Per-tenant spend limits, authenticated access, and a ledger that guarantees AI costs can't run away.

> Status: core engine built and tested; distributed-production hardening in progress. See [Known limitations](#known-limitations).

## The problem

Teams put paid LLM APIs behind their products and discover the bill after the fact. A bug, a retry storm, or a leaked key can burn a month's budget in minutes. Armadillon sits between your applications and your model providers and refuses traffic that would exceed a tenant's budget, before the money is spent.

## How it works

Every request goes through a reserve-then-settle flow:

1. **Authenticate**: Bearer API key, stored only as a SHA-256 hash. Tenant identity comes from the key, never from client-supplied headers or query parameters.
2. **Reserve**: the risk engine estimates cost using the pricing registry and reserves it against the tenant's budget.
3. **Forward**: the request goes to the provider through a provider abstraction, behind a circuit breaker.
4. **Settle**: actual cost is written to the ledger and the reservation is released or adjusted.

Invariant: reserved + settled spend never exceeds the tenant's budget, including under concurrent load. A 150-concurrent-request test verifies this.

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

Go backend, Postgres control plane with SQL migrations, env-based config with fail-fast validation. CLI, webhooks and a React/TypeScript dashboard are on the roadmap.

## Verification

```bash
go build ./...
go vet ./...
go test ./... -race
```

Tests that need a live database skip gracefully unless `VG_TEST_POSTGRES_DSN` is set.

## Known limitations

Stated plainly, because a spend-control tool should be honest about its own limits:

- Rate limiting is currently in-process. It is not yet safe across multiple replicas.
- No admin API yet for provisioning tenants, keys and budgets.
- Legacy unscoped-key compatibility still exists and is being removed.

## Roadmap

- [ ] Distributed rate limiting for multi-replica deployments
- [ ] Admin API for tenant, key and budget provisioning
- [ ] Webhooks for budget alerts
- [ ] CLI
- [ ] Dashboard
- [ ] Live validation against Claude API traffic

## About

Built by [Neuracode Studios](https://github.com/Neuracoderepo1), an AI innovation studio in Accra, Ghana, building secure AI infrastructure for African builders.
