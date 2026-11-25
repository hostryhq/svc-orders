package events_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/svc-orders/double"
	oe "github.com/hostryhq/svc-orders/events"
	"github.com/hostryhq/svc-orders/repository"
)

// The gateway's sync.pii.erased event (ids only) erases the side-table values and salts of the op it
// names, whether it arrives after the op or before it (soak F9).
func TestPIIErasedEventErasesCustomerDetails(t *testing.T) {
	ctx := context.Background()
	for _, eventFirst := range []bool{false, true} {
		d := double.New(double.Options{})
		apply := func(e events.Envelope) {
			t.Helper()
			if err := d.Store.InTx(ctx, double.Tenant, func(tx repository.Tx) error { return oe.Apply(d.Svc, tx, e) }); err != nil {
				t.Fatal(err)
			}
		}
		b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
		op := b.Next("check.create", double.Ins("check", double.Check, map[string]any{"order_type": "takeaway", "business_date": double.Day, "status": "open"}))
		env, err := double.OpEnvelopePII(ids.New(), op, piiOf(guestName))
		if err != nil {
			t.Fatal(err)
		}
		erased := events.Envelope{ID: ids.New(), Type: "sync.pii.erased", V: 1, Source: "sync", TenantID: env.TenantID,
			Data: []byte(fmt.Sprintf(`{"origin":%q,"stream":%q,"seq":%d}`, op.Ref.Origin, op.Ref.Stream, op.Ref.Seq))}
		if eventFirst {
			apply(erased)
			apply(env)
		} else {
			apply(env)
			apply(erased)
		}
		got := d.Memory.PII(double.Tenant, checkKey())
		if !eventFirst && len(got) == 0 {
			t.Fatal("no side-table record to judge")
		}
		for _, w := range got {
			if len(w.Fields) != 0 || len(w.Salts) != 0 {
				t.Fatalf("event first=%v: erasure left values or salts: %+v", eventFirst, w)
			}
		}
	}
}
