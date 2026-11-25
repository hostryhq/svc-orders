# svc-orders

Orders service: seatings, orders, order lines, KOTs and open checks (ADR-0014). Ordering, KOT routing, the KDS,
present, cancels, moves and merges happen on devices as sync ops; orders **projects** the accepted ops of the
entities it owns into its own PostgreSQL, runs the exception detectors, and serves the cloud read side
(checks, exceptions, cancellation / kitchen-time / KOT / present / open-check reports, a projection digest, and one
internal read for billing). It never writes a synced row. Billing owns numbered bills; counter sales need nothing from orders.

Contract: `api-specs` `openapi/orders/v1`, entities `entities/restaurant`, event `events/orders`. Status per operation:
`docs/operations-status.md`. Provisional choices and spec problems: `docs/notes.md`.

## Layout

```
cmd/svc-orders   wiring: serve | migrate
cmd/gen          generates api/policy_gen.go, service/{catalogue,entities}_gen.go, events/subscriptions_gen.go
api/             controller: generated routes + go-auth Guard, parameter decoding; no business rules
service/         projection (apply engine), detectors, reads; no HTTP, no SQL
repository/      interfaces; memory/ (double, unit tests) and postgres/ (migrations in postgres/migrations)
events/          sync op consumer (inbox handler) and the generated subscription list
double/          in-memory test double: the same service and api over the memory repository, example worlds, failure injection
contract/        shared contract suite (api-specs harness) against the double and, with -tags integration, PostgreSQL
integration/     PostgreSQL tests: RLS, inbox, outbox, memory/PG equality
internal/gen     oapi-codegen output (models + net/http server); never edited
```

## Run

Configuration is environment only; secrets by file path.

| Variable | Meaning |
|---|---|
| `ORDERS_DB_DSN` (required), `ORDERS_DB_ROLE` | PostgreSQL; the role is assumed per transaction so RLS applies |
| `ORDERS_MIGRATE` | `true` applies migrations at start; or run `svc-orders migrate` |
| `ORDERS_TENANT_JWKS` (required), `ORDERS_TENANT_ISSUER`, `ORDERS_TENANT_AUDIENCE` | identity signing keys (URL or file), issuer and audience |
| `ORDERS_SERVICE_KEYS` (required) | JWKS of the service-token keys; `ORDERS_SELF_NAME` (default `orders`) |
| `HOSTRY_NATS_URL`, `HOSTRY_NATS_SEED_FILE` | bus; without them nothing is consumed and events stay in the outbox |
| `ORDERS_LISTEN` (`:8083`), `ORDERS_KEY_REFRESH` | listener, key refresh |
| `ORDERS_KOT_STALE_AFTER` (15m), `ORDERS_GRACE_CANCEL_ALLOWANCE` (10) | detector defaults (notes N5) |

`/healthz`, `/readyz` (PostgreSQL). The request log carries method, path and status only.

## Build and test

```
make gen            # needs `make bundle` in api-specs; records the api-specs commit in GEN_SOURCE
make test           # go test -race ./...   (unit + contract against the double)
make lint           # gofmt + go vet
make integration    # PostgreSQL (infra dev stack):
                    #   HOSTRY_PG_PORT=55462 docker compose -p orders -f ../infra/dev/compose.yaml up -d --wait postgres
                    #   ... then docker compose -p orders -f ../infra/dev/compose.yaml down -v
```

Local development uses an uncommitted `go.work` listing `.`, `../go-common`, `../go-auth` and the api-specs checkout;
private modules need `GOPRIVATE=github.com/hostryhq/*`. Other services' tests import `double`.
