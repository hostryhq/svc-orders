package service_test

import (
	"context"
	"testing"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-auth/authtest"
	"github.com/hostryhq/go-common/ids"
	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/service"
)

// F25: a set whose every field is personal carries no non-pii field. The commitment still attaches to the existing check,
// whichever op arrives first, and the digest equals that of a node that saw the same ops in the other order.
func TestPIIOnlySetAttachesItsCommitmentWhateverTheArrivalOrder(t *testing.T) {
	ctx := context.Background()
	cat, err := service.NewCatalogue()
	if err != nil {
		t.Fatal(err)
	}
	commit := func(v string) string {
		c, err := gosync.PIICommit([]byte("0123456789abcdef"), v)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	build := func() (*service.Op, *service.Op) {
		b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
		o1 := b.Next("check.create", double.Ins("check", double.Check, map[string]any{"order_type": "dine_in", "business_date": double.Day, "status": "open"}))
		o2 := b.Next("bill.customer_details.edit", double.Set("set", "check", double.Check, nil))
		o2.PIICommits = gosync.PIIC{"check": {double.Check: {"customer_name": commit("A Name"), "customer_gstin": commit("29ABCDE1234F1Z5")}}}
		return o1, o2
	}
	ingest := func(ops ...*service.Op) string {
		d := double.New(double.Options{})
		if err := d.Ingest(ctx, double.Tenant, ops...); err != nil {
			t.Fatal(err)
		}
		out, err := d.Svc.ProjectionDigest(d.Issuer.ContextFor(ctx, cat, authtest.Spec{Type: auth.TypeUser, Tenant: ids.ID(double.Tenant), Grants: authtest.Grants(nil, "report.view.outlet")}), double.Outlet, double.Day)
		if err != nil {
			t.Fatal(err)
		}
		return out.StateHash
	}
	a1, a2 := build()
	inOrder := ingest(a1, a2)
	b1, b2 := build()
	if got := ingest(b2, b1); got != inOrder {
		t.Fatalf("arrival order changed the digest: %s vs %s", got, inOrder)
	}
	c1, _ := build()
	if got := ingest(c1); got == inOrder {
		t.Fatal("the digest ignored a pii-only set")
	}
}
