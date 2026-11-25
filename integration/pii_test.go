//go:build integration

package integration

import (
	"context"
	"testing"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/double"
	oe "github.com/hostryhq/svc-orders/events"
	"github.com/hostryhq/svc-orders/internal/pgtest"
	"github.com/hostryhq/svc-orders/repository"
)

// Over PostgreSQL the verified customer details sit in sync_pii only, and a cloud erasure op empties
// the values and salts and keeps the commitments (migration 0004).
func TestCustomerDetailsAreErasedInPostgres(t *testing.T) {
	ctx := context.Background()
	db := pgtest.New(t)
	d := double.New(double.Options{Store: db.Store})
	b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
	op := b.Next("check.create", double.Ins("check", double.Check, map[string]any{"order_type": "takeaway", "business_date": double.Day, "status": "open"}))
	env, err := double.OpEnvelopePII(ids.New(), op, map[string]map[string]map[string]any{"check": {double.Check: {"customer_name": "Test Guest Unique-Name"}}})
	if err != nil {
		t.Fatal(err)
	}
	er := double.NewOpBuilder("cloud-1", "outlet:"+double.Outlet, "").Next(gosync.PIIEraseIntent,
		double.Ins("pii_erase", op.Ref.Stream+"|"+op.Ref.Origin+"|1", map[string]any{"origin": op.Ref.Origin, "stream": op.Ref.Stream, "seq": int64(1)}))
	eraseEnv, err := double.OpEnvelope(ids.New(), er)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range []events.Envelope{env, eraseEnv} {
		if err := d.Store.InTx(ctx, double.Tenant, func(tx repository.Tx) error { return oe.Apply(d.Svc, tx, e) }); err != nil {
			t.Fatal(err)
		}
		var fields, salts, commits string
		if err := db.Admin.QueryRowContext(ctx, `SELECT fields::text, salts::text, commits::text FROM sync_pii WHERE tenant_id=$1 AND row_id=$2`, double.Tenant, double.Check).Scan(&fields, &salts, &commits); err != nil {
			t.Fatal(err)
		}
		if e.ID == env.ID && (fields == "{}" || salts == "{}") {
			t.Fatalf("verified details not stored: %s %s", fields, salts)
		}
		if e.ID == eraseEnv.ID && (fields != "{}" || salts != "{}" || commits == "{}") {
			t.Fatalf("erasure: fields %s salts %s commits %s", fields, salts, commits)
		}
	}
}
