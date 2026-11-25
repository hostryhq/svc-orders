// Package events connects orders to the bus (ADR-0011): the consumer of accepted sync ops, and the
// list of intent namespaces it subscribes to (generated). Exceptions leave through the transactional
// outbox written by the service in the state-changing transaction (service/detect.go).
package events

import (
	"context"
	"database/sql"
	"encoding/json"
	"strings"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/inbox"
	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/repository/postgres"
	"github.com/hostryhq/svc-orders/service"
)

// ConsumerName is the durable consumer and inbox name of orders on the sync stream.
const ConsumerName = "orders"

// SyncOpPrefix is the type prefix of accepted-op events: sync.op.<namespace> (subject evt.sync.op.<namespace>.v1).
const SyncOpPrefix = "sync.op."

// PIIErasedType is the type of the gateway event that names an op whose personal data is erased
// (ids only, ADR-0014 A2.1 item 7). The consumer applies it through go-common's erasure path.
const PIIErasedType = "sync.pii.erased"

// erase records the erasure of the named op and empties the values and salts kept for it; it is
// idempotent and holds whether it arrives before or after the op itself.
func erase(tx repository.Tx, env events.Envelope) error {
	var d struct {
		Origin string `json:"origin"`
		Stream string `json:"stream"`
		Seq    int64  `json:"seq"`
	}
	if err := json.Unmarshal(env.Data, &d); err != nil {
		return err
	}
	_, err := tx.SyncStore().AddErasure(tx.Context(), env.TenantID.String(), gosync.OpRef{Origin: d.Origin, Stream: d.Stream, Seq: d.Seq})
	return err
}

// parser is go-common/sync's ParseOp: the one parser of op events (ADR-0014 A2.1). It verifies the body
// hash, the identity fields and the value types, and refuses a malformed op.
var parser gosync.Parser

// Apply projects the op carried by a sync op event into tx. Events of other types, and ops of
// entities orders does not project, are ignored. A malformed op (bad hash, identity or values) returns an error: the inbox parks
// it after the last attempt, so it is neither lost nor blocks the consumer.
func Apply(svc *service.Service, tx repository.Tx, env events.Envelope) error {
	if env.Type == PIIErasedType {
		return erase(tx, env)
	}
	if !strings.HasPrefix(env.Type, SyncOpPrefix) {
		return nil
	}
	op, err := parser.ParseEnvelope(env)
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
