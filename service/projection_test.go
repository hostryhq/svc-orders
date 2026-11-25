package service_test

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/service"
)

func id(n int) string { return fmt.Sprintf("0190a1b2-c3d4-7e5f-8a9b-%012x", 0x7000+n) }

type fixture struct {
	t   *testing.T
	d   *double.Double
	b   *double.OpBuilder
	ctx context.Context
}

func newFixture(t *testing.T) *fixture {
	d := double.New(double.Options{})
	return &fixture{t: t, d: d, b: double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member), ctx: context.Background()}
}

func (f *fixture) ingest(ops ...*service.Op) {
	f.t.Helper()
	if err := f.d.Ingest(f.ctx, double.Tenant, ops...); err != nil {
		f.t.Fatal(err)
	}
}

func (f *fixture) row(entity, rid string) (repository.Row, bool) {
	f.t.Helper()
	var r repository.Row
	var ok bool
	err := f.d.Store.InTx(f.ctx, double.Tenant, func(tx repository.Tx) (err error) { r, ok, err = tx.GetRow(entity, rid); return })
	if err != nil {
		f.t.Fatal(err)
	}
	return r, ok
}

func (f *fixture) exceptions(status string) []repository.Exception {
	f.t.Helper()
	var xs []repository.Exception
	err := f.d.Store.InTx(f.ctx, double.Tenant, func(tx repository.Tx) (err error) {
		xs, err = tx.Exceptions(repository.ExceptionQuery{Status: status})
		return
	})
	if err != nil {
		f.t.Fatal(err)
	}
	return xs
}

func (f *fixture) kinds(status string) map[string]int {
	m := map[string]int{}
	for _, x := range f.exceptions(status) {
		m[x.Kind]++
	}
	return m
}

func ins(e, rid string, f map[string]any) service.Mut { return double.Ins(e, rid, f) }

// A check with one sent line, used by several tests.
func (f *fixture) basicCheck() {
	f.ingest(
		f.b.Next("seating.open", ins("seating", id(1), map[string]any{"table_id": id(2), "business_date": double.Day, "status": "open"})),
		f.b.Next("check.create", ins("check", id(3), map[string]any{"seating_id": id(1), "order_type": "dine_in", "business_date": double.Day, "status": "open"})),
		f.b.Next("order.line.add", ins("order_line", id(4), map[string]any{"order_id": id(5), "check_id": id(3), "item_id": id(6), "name": "Dosa", "qty_milli": int64(1000),
			"unit_price_paise": int64(5000), "modifier_delta_paise": int64(0), "tax_class": "fnb_standard", "station_code": "KIT", "state": "draft"})),
	)
}

func TestApplyIsIdempotentOnOpIdentity(t *testing.T) {
	f := newFixture(t)
	op := f.b.Next("order.line.add", ins("order_line", id(4), map[string]any{"check_id": id(3), "qty_milli": int64(1000), "state": "draft"}))
	f.ingest(op, op)
	delta := f.b.Next("order.line.edit", service.Mut{E: "order_line", ID: id(4), K: "delta", F: map[string]any{"qty_milli": int64(500)}})
	f.ingest(delta, delta, delta)
	r, ok := f.row("order_line", id(4))
	if !ok || r.Fields["qty_milli"] != int64(1500) {
		t.Fatalf("qty = %v (exists %v), want 1500: a replayed delta must count once", r.Fields["qty_milli"], ok)
	}
}

func TestRowsConvergeWhateverTheArrivalOrder(t *testing.T) {
	build := func() []*service.Op {
		b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
		b2 := double.NewOpBuilder(id(90), "outlet:"+double.Outlet, id(91)).At(double.Start.Add(-25 * time.Minute))
		return []*service.Op{
			b.Next("check.create", ins("check", id(3), map[string]any{"order_type": "dine_in", "business_date": double.Day, "status": "open", "label": "A"})),
			b.Next("bill.customer_details.edit", double.Set("set", "check", id(3), map[string]any{"label": "B"})),
			b2.Next("bill.customer_details.edit", double.Set("set", "check", id(3), map[string]any{"label": "C"})),
			b.Next("order.line.add", ins("order_line", id(4), map[string]any{"check_id": id(3), "qty_milli": int64(1000), "state": "draft"})),
			b2.Next("order.line.edit", service.Mut{E: "order_line", ID: id(4), K: "delta", F: map[string]any{"qty_milli": int64(2000)}}),
			b.Next("kot.send", ins("kot_line", id(7), map[string]any{"order_line_id": id(4), "status": "new"})),
			b2.Next("kds.status", double.Set("rank", "kot_line", id(7), map[string]any{"status": "ready"})),
			b.Next("kds.status", double.Set("rank", "kot_line", id(7), map[string]any{"status": "preparing"})),
			b.Next("bill.finalize", double.Set("rank", "check", id(3), map[string]any{"status": "billed"})),
		}
	}
	digest := func(order []int) string {
		f := newFixture(t)
		ops := build()
		for _, i := range order {
			f.ingest(ops[i])
		}
		out := ""
		for _, k := range []struct{ e, i string }{{"check", id(3)}, {"order_line", id(4)}, {"kot_line", id(7)}} {
			r, _ := f.row(k.e, k.i)
			out += fmt.Sprint(k.e, r.Fields, ";")
		}
		return out
	}
	inOrder := digest([]int{0, 1, 2, 3, 4, 5, 6, 7, 8})
	rng := rand.New(rand.NewSource(7))
	for n := 0; n < 20; n++ {
		perm := rng.Perm(9)
		if got := digest(perm); got != inOrder {
			t.Fatalf("order %v diverged:\n got %s\nwant %s", perm, got, inOrder)
		}
	}
	// the later stamp wins an lww field, rank never moves back, delta sums, status only moves forward
	f := newFixture(t)
	for _, op := range build() {
		f.ingest(op)
	}
	c, _ := f.row("check", id(3))
	k, _ := f.row("kot_line", id(7))
	l, _ := f.row("order_line", id(4))
	if c.Fields["status"] != "billed" || k.Fields["status"] != "ready" || l.Fields["qty_milli"] != int64(3000) {
		t.Fatalf("check %v kot_line %v order_line %v", c.Fields, k.Fields, l.Fields)
	}
}

func TestVoidRemovesTheEffectAndLaterArrivalOfAVoidedOpIsIgnored(t *testing.T) {
	f := newFixture(t)
	f.ingest(f.b.Next("check.create", ins("check", id(3), map[string]any{"order_type": "dine_in", "business_date": double.Day, "status": "open", "label": "A"})))
	edit := f.b.Next("bill.customer_details.edit", double.Set("set", "check", id(3), map[string]any{"label": "refused"}))
	f.ingest(edit)
	cloud := double.NewOpBuilder("cloud-1", "outlet:"+double.Outlet, "").At(double.Start)
	void := func(o *service.Op) *service.Op {
		v := cloud.Next("sync.void")
		v.Voids = []gosync.OpRef{o.Ref} // what ParseOp reads from the sync_void records
		return v
	}
	f.ingest(void(edit))
	if c, _ := f.row("check", id(3)); c.Fields["label"] != "A" {
		t.Fatalf("label = %v after the void, want the earlier value A", c.Fields["label"])
	}
	late := f.b.Next("bill.customer_details.edit", double.Set("set", "check", id(3), map[string]any{"label": "late"}))
	f.ingest(void(late), late) // the void arrives before the op it names
	if c, _ := f.row("check", id(3)); c.Fields["label"] != "A" {
		t.Fatalf("label = %v, a voided op arriving later must not apply", c.Fields["label"])
	}
}

func TestEntitiesOrdersDoesNotProjectAreIgnored(t *testing.T) {
	f := newFixture(t)
	f.ingest(f.b.Next("bill.finalize", ins("bill", id(8), map[string]any{"total_paise": int64(100)}), ins("payment", id(9), map[string]any{"amount_paise": int64(100)})))
	if _, ok := f.row("bill", id(8)); ok {
		t.Fatal("a bill is owned by billing and must not be projected")
	}
}
