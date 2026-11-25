package events_test

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/double"
	oe "github.com/hostryhq/svc-orders/events"
	"github.com/hostryhq/svc-orders/repository"
)

const (
	guestName  = "Test Guest Unique-Name"
	guestPhone = "+91 90000 00000"
)

func piiOf(name any) map[string]map[string]map[string]any {
	return map[string]map[string]map[string]any{"check": {double.Check: {"customer_name": name, "customer_phone": guestPhone}}}
}

func checkKey() gosync.RowKey {
	return gosync.RowKey{Tenant: double.Tenant, Entity: "check", ID: double.Check}
}

// Customer details of a check are verified against their commitment, kept only in the erasable side
// table (never in a projection row) and emptied, with the salts, by a cloud erasure op, whether the
// erasure arrives after the op or before it. The commitments stay.
func TestCustomerDetailsAreVerifiedKeptApartAndErasable(t *testing.T) {
	ctx := context.Background()
	for _, erasureFirst := range []bool{false, true} {
		d := double.New(double.Options{})
		apply := func(e events.Envelope) {
			t.Helper()
			if err := d.Store.InTx(ctx, double.Tenant, func(tx repository.Tx) error { return oe.Apply(d.Svc, tx, e) }); err != nil {
				t.Fatal(err)
			}
		}
		b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
		op := b.Next("check.create", double.Ins("check", double.Check, map[string]any{"order_type": "takeaway", "business_date": double.Day, "status": "open", "label": "A"}))
		env, err := double.OpEnvelopePII(ids.New(), op, piiOf(guestName))
		if err != nil {
			t.Fatal(err)
		}
		cb := double.NewOpBuilder("cloud-1", "outlet:"+double.Outlet, "")
		target := op.Ref.Stream + "|" + op.Ref.Origin + "|1"
		er := cb.Next(gosync.PIIEraseIntent, double.Ins("pii_erase", target, map[string]any{"origin": op.Ref.Origin, "stream": op.Ref.Stream, "seq": int64(1)}))
		eraseEnv, err := double.OpEnvelope(ids.New(), er)
		if err != nil {
			t.Fatal(err)
		}
		if erasureFirst {
			apply(eraseEnv)
			apply(env)
		} else {
			apply(env)
			got := d.Memory.PII(double.Tenant, checkKey())
			if len(got) != 1 || got[0].Fields["customer_name"] != guestName || got[0].Fields["customer_phone"] != guestPhone {
				t.Fatalf("verified details are not in the side table: %+v", got)
			}
			apply(eraseEnv)
		}
		var row string
		_ = d.Store.InTx(ctx, double.Tenant, func(tx repository.Tx) error {
			r, _, _ := tx.GetRow("check", double.Check)
			row = fmt.Sprint(r.Fields)
			return nil
		})
		if strings.Contains(row, guestName) || strings.Contains(row, guestPhone) {
			t.Fatalf("customer details leaked into the projection row: %s", row)
		}
		for _, w := range d.Memory.PII(double.Tenant, checkKey()) {
			if len(w.Fields) != 0 || len(w.Salts) != 0 {
				t.Fatalf("erasure (first=%v) left values or salts: %+v", erasureFirst, w)
			}
			if len(w.Commits) != 2 {
				t.Fatalf("erasure did not keep the commitments: %+v", w)
			}
		}
	}
}

// A value that does not match its commitment is dropped, never kept.
func TestCustomerDetailsWithoutAMatchingCommitmentAreDropped(t *testing.T) {
	d := double.New(double.Options{})
	b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
	op := b.Next("check.create", double.Ins("check", double.Check, map[string]any{"order_type": "takeaway", "business_date": double.Day, "status": "open"}))
	env, err := double.OpEnvelopePII(ids.New(), op, piiOf(guestName))
	if err != nil {
		t.Fatal(err)
	}
	env.Data = []byte(strings.Replace(string(env.Data), guestName, "Someone Else", 1))
	if err := d.Store.InTx(context.Background(), double.Tenant, func(tx repository.Tx) error { return oe.Apply(d.Svc, tx, env) }); err != nil {
		t.Fatal(err)
	}
	for _, w := range d.Memory.PII(double.Tenant, checkKey()) {
		if _, ok := w.Fields["customer_name"]; ok {
			t.Fatalf("a value without a matching commitment was kept: %+v", w)
		}
	}
}
