# svc-orders. Local development uses an uncommitted go.work listing ../go-common, ../go-auth and
# ../api-specs-p2 (the api-specs checkout); private modules need GOPRIVATE=github.com/hostryhq/*.
SHELL := /bin/bash
API_SPECS ?= ../api-specs-p2
OAPI_CODEGEN ?= ../api-specs/.tools/bin/oapi-codegen
OAPI_CODEGEN_VERSION := v2.9.0
export GOPRIVATE ?= github.com/hostryhq/*

.PHONY: gen test vet lint integration

# gen regenerates internal/gen (types and net/http server) with the pinned oapi-codegen, plus
# api/policy_gen.go, service/catalogue_gen.go, service/entities_gen.go and events/subscriptions_gen.go,
# and records the api-specs commit in GEN_SOURCE. It needs a bundled spec: run `make bundle` in api-specs first.
gen:
	@test -f $(API_SPECS)/build/orders.yaml || (echo "run 'make bundle' in $(API_SPECS) first" && exit 1)
	$(OAPI_CODEGEN) -config oapi-codegen.yaml $(API_SPECS)/build/orders.yaml
	go run ./cmd/gen -specs $(API_SPECS) -commit "$$(git -C $(API_SPECS) rev-parse HEAD)"
	git -C $(API_SPECS) rev-parse HEAD > GEN_SOURCE
	@echo "generated from api-specs $$(cat GEN_SOURCE) with oapi-codegen $(OAPI_CODEGEN_VERSION)"

vet:
	go vet ./...

test:
	go test -race ./...

# Integration tests need Postgres only (infra dev stack, own compose project and port):
#   HOSTRY_PG_PORT=55462 docker compose -p orders -f ../infra/dev/compose.yaml up -d --wait postgres
#   make integration HOSTRY_PG_DSN='postgres://hostry:hostry-dev-only@127.0.0.1:55462/hostry?sslmode=disable'
#   docker compose -p orders -f ../infra/dev/compose.yaml down -v
integration:
	go test -race -tags integration ./integration/... ./contract/...

lint:
	gofmt -l . | (! grep .)
	go vet ./...
