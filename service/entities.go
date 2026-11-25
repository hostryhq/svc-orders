package service

import (
	"embed"
	"fmt"
	"sync"

	gosync "github.com/hostryhq/go-common/sync"
)

// ServiceName is the owner name orders holds in the entity registry (owner: orders, replicated_to: orders).
const ServiceName = "orders"

// entityFS holds the entity files orders owns or replicates, copied from api-specs by cmd/gen.
//
//go:embed entities/*.yaml
var entityFS embed.FS

var (
	regOnce sync.Once
	reg     *gosync.Registry
	regErr  error
)

// Registry is the go-common/sync registry of the entities orders owns or holds as read-only replicas
// (ADR-0014 R1, A2.4). The entity files are the single source of the field policies the engine applies.
func Registry() (*gosync.Registry, error) {
	regOnce.Do(func() { reg, regErr = gosync.LoadRegistry(entityFS, "entities") })
	if regErr != nil {
		return nil, fmt.Errorf("orders entity registry: %w", regErr)
	}
	return reg, nil
}

func mustRegistry() *gosync.Registry {
	r, err := Registry()
	if err != nil {
		panic(err)
	}
	return r
}

// ownedEntities are the entities orders owns, in name order (the digest covers exactly these; a
// replica is never in a digest, ADR-0014 A2.4 item 3).
func ownedEntities() []string { return mustRegistry().Owned(ServiceName) }

// isReplica reports whether the entity is a declared read-only replica for orders.
func isReplica(entity string) bool {
	return mustRegistry().RoleOf(ServiceName, entity) == gosync.RoleReplica
}
