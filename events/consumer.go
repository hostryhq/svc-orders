// Package events connects orders to the bus (ADR-0011): the consumer of accepted sync ops, and the
// list of intent namespaces it subscribes to (generated). Exceptions leave through the transactional
// outbox written by the service in the state-changing transaction (service/detect.go).
package events

import (
	"context"
	"database/sql"
	"strings"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/inbox"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/repository/postgres"
	"github.com/hostryhq/svc-orders/service"
)

// ConsumerName is the durable consumer and inbox name of orders on the sync stream.
const ConsumerName = "orders"

// SyncOpPrefix is the type prefix of accepted-op events: sync.op.<namespace> (subject evt.sync.op.<namespace>.v1).
const SyncOpPrefix = "sync.op."

// Apply projects the op carried by a sync op event into tx. Events of other types, and ops of
// entities orders does not project, are ignored. A malformed op returns an error: the inbox parks
// it after the last attempt, so it is neither lost nor blocks the consumer.
func Apply(svc *service.Service, tx repository.Tx, env events.Envelope) error {
	if !strings.HasPrefix(env.Type, SyncOpPrefix) {
		return nil
	}
	op, err := service.ParseOp(env.Data)
	if err != nil {
		return err
	}
	return svc.Apply(tx, op, env.OccurredAt)
}

// Handler adapts Apply to inbox.Handler over the PostgreSQL store: the projection and the inbox row
// commit in one transaction.
func Handler(svc *service.Service, st *postgres.Store) inbox.Handler {
	return func(ctx context.Context, sqlTx *sql.Tx, env events.Envelope) error {
		return st.WithSQLTx(ctx, sqlTx, env.TenantID.String(), func(tx repository.Tx) error { return Apply(svc, tx, env) })
	}
}
