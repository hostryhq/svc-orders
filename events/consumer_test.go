package events_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/inbox"
	"github.com/hostryhq/svc-orders/double"
	oe "github.com/hostryhq/svc-orders/events"
	"github.com/hostryhq/svc-orders/repository"
)

// The consumer reads an op event with go-common/sync's parser: a well-formed one is projected (and a
// redelivery changes nothing), one whose body no longer matches its hash is refused and projects nothing.
func TestConsumerProjectsVerifiedOpsAndRefusesATamperedBody(t *testing.T) {
	d := double.New(double.Options{})
	b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
	op := b.Next("check.create", double.Ins("check", double.Check, map[string]any{"order_type": "dine_in", "business_date": double.Day, "status": "open", "label": "A"}))
	env, err := double.OpEnvelope(ids.New(), op)
	if err != nil {
		t.Fatal(err)
	}
	apply := func(e events.Envelope) error {
		return d.Store.InTx(context.Background(), double.Tenant, func(tx repository.Tx) error { return oe.Apply(d.Svc, tx, e) })
	}
	for i := 0; i < 2; i++ { // the second is a redelivery
		if err := apply(env); err != nil {
			t.Fatal(err)
		}
	}
	var label any
	_ = d.Store.InTx(context.Background(), double.Tenant, func(tx repository.Tx) error {
		r, _, _ := tx.GetRow("check", double.Check)
		label = r.Fields["label"]
		return nil
	})
	if label != "A" {
		t.Fatalf("label = %v, want A", label)
	}
	tampered := env
	tampered.Data = []byte(strings.Replace(string(env.Data), `\"label\":\"A\"`, `\"label\":\"B\"`, 1))
	if string(tampered.Data) == string(env.Data) {
		t.Fatal("test did not change the body")
	}
	if err := apply(tampered); err == nil {
		t.Fatal("a body that does not match its hash was accepted")
	}
}

// Gateway events share the sync.op. prefix but are not ops: they must not block the consumer (they are
// acked and ignored), while a poison op event is a permanent error so the inbox parks it at once.
func TestGatewayEventsDoNotBlockTheConsumerAndPoisonParks(t *testing.T) {
	d := double.New(double.Options{})
	apply := func(e events.Envelope) error {
		return d.Store.InTx(context.Background(), double.Tenant, func(tx repository.Tx) error { return oe.Apply(d.Svc, tx, e) })
	}
	for _, typ := range []string{"sync.op.flagged", "sync.quarantine.recorded"} {
		env := events.Envelope{ID: ids.New(), Type: typ, V: 1, Source: "sync", TenantID: double.Tenant, Data: []byte(`{}`)}
		if err := apply(env); err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
	}
	poison := events.Envelope{ID: ids.New(), Type: "sync.op.order", V: 1, Source: "sync", TenantID: double.Tenant, Data: []byte(`{}`)}
	if err := apply(poison); !errors.Is(err, inbox.ErrPermanent) {
		t.Fatalf("poison op error = %v, want inbox.ErrPermanent", err)
	}
}
