//go:build integration

// Package integration runs orders over a real PostgreSQL (the infra dev stack, own compose project
// and port): migrations, row-level security, the projection through the inbox handler, the
// transactional outbox, and equality of the projection with the in-memory one.
//
//	HOSTRY_PG_PORT=55462 docker compose -p orders -f ../infra/dev/compose.yaml up -d --wait postgres
//	go test -tags integration ./integration/...
package integration

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/inbox"
	"github.com/hostryhq/go-common/testkit"
	"github.com/hostryhq/svc-orders/double"
	oe "github.com/hostryhq/svc-orders/events"
	"github.com/hostryhq/svc-orders/internal/pgtest"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/service"
)

const tenantB = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4b60"

func TestMigrationsAreIdempotent(t *testing.T) {
	db := pgtest.New(t)
	if err := migrate(db); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
}

func TestRowLevelSecurityIsolatesTenants(t *testing.T) {
	ctx := context.Background()
	db := pgtest.New(t)
	d := double.New(double.Options{Store: db.Store})
	if err := double.SeedInto(ctx, d, "orphan-line"); err != nil {
		t.Fatal(err)
	}
	// the application role with no tenant set sees nothing
	tx, err := db.Store.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, "SET LOCAL ROLE orders_app"); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"projection_rows", "effects", "exceptions", "applied_ops"} {
		var n int
		if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM "+table).Scan(&n); err != nil || n != 0 {
			t.Errorf("%s with no tenant context: %d rows, err %v; want 0", table, n, err)
		}
	}
	// a tenant context for B sees none of A's rows, and cannot write A's
	if _, err := tx.ExecContext(ctx, "SELECT set_config('app.tenant_id', $1, true)", tenantB); err != nil {
		t.Fatal(err)
	}
	var n int
	if err := tx.QueryRowContext(ctx, "SELECT count(*) FROM projection_rows").Scan(&n); err != nil || n != 0 {
		t.Errorf("tenant B sees %d of A's rows (err %v)", n, err)
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO voided_ops (tenant_id, op_ref) VALUES ($1, 'x')`, double.Tenant); err == nil {
		t.Error("tenant B wrote a row for tenant A")
	}
	// through the store, B reads nothing of A's
	err = db.Store.InTx(ctx, tenantB, func(tx repository.Tx) error {
		r, ok, err := tx.GetRow("check", double.Check)
		if ok || err != nil {
			return fmt.Errorf("B read A's check: %v %v %v", r.ID, ok, err)
		}
		x, ok, _ := tx.GetException(double.Orphan)
		if ok {
			return fmt.Errorf("B read A's exception %s", x.ID)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Both stores must project the same ops to the same state: the in-memory double is only a
// faithful stand-in if its digest equals the PostgreSQL one for a business day (ADR-0014 T9).
func TestProjectionOverPostgresEqualsInMemory(t *testing.T) {
	ctx := context.Background()
	db := pgtest.New(t)
	pg := double.New(double.Options{Store: db.Store})
	mem := double.New(double.Options{})
	for _, d := range []*double.Double{pg, mem} {
		if err := double.SeedInto(ctx, d, "base"); err != nil {
			t.Fatal(err)
		}
	}
	digest := func(d *double.Double) service.ProjectionDigest {
		var out service.ProjectionDigest
		err := d.Store.InTx(ctx, double.Tenant, func(repository.Tx) error { return nil })
		if err != nil {
			t.Fatal(err)
		}
		out, err = d.Svc.ProjectionDigest(d.Issuer.ContextFor(ctx, mustCat(t), spec(double.Tenant)), double.Outlet, double.Day)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	a, b := digest(pg), digest(mem)
	if a.StateHash != b.StateHash {
		t.Fatalf("PostgreSQL digest %s differs from in-memory %s", a.StateHash, b.StateHash)
	}
}

type fakeDelivery struct {
	data  []byte
	acked int
	naks  int
}

func (f *fakeDelivery) Subject() string         { return "evt.sync.op.order.v1" }
func (f *fakeDelivery) Data() []byte            { return f.data }
func (f *fakeDelivery) NumDelivered() uint64    { return 1 }
func (f *fakeDelivery) Ack() error              { f.acked++; return nil }
func (f *fakeDelivery) Nak(time.Duration) error { f.naks++; return errors.New("nak") }

func opEvent(t *testing.T, gen *ids.Generator, op service.Op, body map[string]any) []byte {
	t.Helper()
	b, _ := json.Marshal(body)
	data, _ := json.Marshal(map[string]any{"origin": op.Origin, "stream": op.Stream, "seq": op.Seq, "intent": op.Intent, "status": "accepted", "body": string(b)})
	env := events.Envelope{ID: gen.New(), Type: "sync.op.order", V: 1, Source: "sync", TenantID: ids.ID(double.Tenant), Key: op.Origin + "/" + op.Stream, KeySeq: op.Seq,
		OccurredAt: double.Start, Data: data}
	out, err := env.Marshal()
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestInboxHandlerProjectsOnceAndPublishesTheExceptionThroughTheOutbox(t *testing.T) {
	ctx := context.Background()
	db := pgtest.New(t)
	clk := testkit.NewClock()
	clk.Set(double.Start)
	svc := service.New(db.Store, clk, clk.IDs().New, service.DefaultConfig(), nil)
	sub := &inbox.Subscriber{DB: db.Store.DB(), Consumer: oe.ConsumerName, Handle: oe.Handler(svc, db.Store), Now: clk.Now}
	gen := clk.IDs()

	stream := "outlet:" + double.Outlet
	body := func(seq int64, hlc string, muts ...service.Mut) (service.Op, map[string]any) {
		op := service.Op{Origin: double.Device, Stream: stream, Seq: seq}
		return op, map[string]any{"hlc": hlc, "actor": double.Member, "at": double.Start.Format(time.RFC3339Nano), "muts": muts}
	}
	mk := func(seq int64, hlc string, muts ...service.Mut) *fakeDelivery {
		op, b := body(seq, hlc, muts...)
		return &fakeDelivery{data: opEvent(t, gen, op, b)}
	}
	chk := mk(1, "0000000000000001", double.Ins("check", id(3), map[string]any{"order_type": "dine_in", "business_date": double.Day, "status": "open"}))
	bill := mk(2, "0000000000000002", double.Set("rank", "check", id(3), map[string]any{"status": "billed"}))
	line := mk(3, "0000000000000003", double.Ins("order_line", id(4), map[string]any{"check_id": id(3), "qty_milli": int64(1000), "state": "draft"}))
	for _, d := range []*fakeDelivery{chk, bill, line, line} { // the last one is a redelivery of the same event
		if err := sub.Process(ctx, d); err != nil {
			t.Fatal(err)
		}
		if d.acked < 1 || d.naks != 0 {
			t.Fatalf("delivery acked %d naked %d", d.acked, d.naks)
		}
	}
	var applied, outboxRows int
	_ = db.Admin.QueryRow(`SELECT count(*) FROM applied_ops`).Scan(&applied)
	_ = db.Admin.QueryRow(`SELECT count(*) FROM outbox WHERE type='orders.exception.opened'`).Scan(&outboxRows)
	if applied != 3 {
		t.Errorf("%d ops applied, want 3 (the redelivery is one inbox row)", applied)
	}
	if outboxRows != 1 {
		t.Errorf("%d orders.exception.opened rows in the outbox, want 1 (orphan line, written in the projecting transaction)", outboxRows)
	}
	var payload []byte
	_ = db.Admin.QueryRow(`SELECT payload FROM outbox`).Scan(&payload)
	if env, err := events.Unmarshal(payload); err != nil || env.Source != "orders" || env.TenantID != double.Tenant || env.OutletID != double.Outlet {
		t.Errorf("outbox envelope = %+v (%v)", env, err)
	}
	// a malformed op is refused (the inbox parks it after the last attempt), not half applied
	bad := &fakeDelivery{data: opEvent(t, gen, service.Op{Origin: double.Device, Stream: stream, Seq: 0}, map[string]any{})}
	if err := sub.Process(ctx, bad); bad.naks != 1 {
		t.Errorf("malformed op: err %v naks %d, want a redelivery request", err, bad.naks)
	}
}

func TestFailedProjectionRollsBackTheInboxRow(t *testing.T) {
	ctx := context.Background()
	db := pgtest.New(t)
	clk := testkit.NewClock()
	svc := service.New(db.Store, clk, clk.IDs().New, service.DefaultConfig(), nil)
	boom := errors.New("boom")
	sub := &inbox.Subscriber{DB: db.Store.DB(), Consumer: "t", Now: clk.Now, Handle: func(ctx context.Context, tx *sql.Tx, e events.Envelope) error {
		_ = svc
		return boom
	}}
	d := &fakeDelivery{data: opEvent(t, clk.IDs(), service.Op{Origin: "o", Stream: "outlet:x", Seq: 1}, map[string]any{})}
	_ = sub.Process(ctx, d)
	var n int
	_ = db.Admin.QueryRow(`SELECT count(*) FROM inbox`).Scan(&n)
	if n != 0 || d.naks != 1 {
		t.Fatalf("inbox rows %d naks %d after a failed handler, want 0 and 1", n, d.naks)
	}
}
