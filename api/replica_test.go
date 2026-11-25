package api_test

import (
	"context"
	"testing"

	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/service"
)

// A joined display field comes from a read-only replica that may lag: until the station and table rows
// arrive the field is absent (null), never a placeholder id (ADR-0014 A2.4 item 4); when they arrive it fills in.
func TestJoinedFieldsAreAbsentUntilTheReplicaArrives(t *testing.T) {
	c := newClient(t, "base")
	d := c.d
	d.Memory.Reset()
	b := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, "") // no actor: a row without a member
	b.At(double.Start)
	b.Step = 0
	ops := []*double.Op{
		b.Next("seating.open", double.Ins("seating", double.Seating, map[string]any{"table_id": double.Table, "business_date": double.Day, "status": "open"})),
		b.Next("check.create", double.Ins("check", double.Check, map[string]any{"seating_id": double.Seating, "order_type": "dine_in", "business_date": double.Day, "status": "open"})),
		b.Next("order.line.add", double.Ins("order_line", double.Line, map[string]any{"check_id": double.Check, "qty_milli": int64(1000), "unit_price_paise": int64(100), "name": "Tea", "state": "draft", "station_code": double.StationCode})),
		b.Next("kot.send", double.Ins("kot", double.Kot, map[string]any{"check_id": double.Check, "station_code": double.StationCode, "kind": "original", "kot_no": "P1-1", "business_date": double.Day}),
			double.Ins("kot_line", double.KotLine, map[string]any{"kot_id": double.Kot, "order_line_id": double.Line, "status": "new"})),
		b.Next("order.line.cancel", double.Ins("order_line_cancel", double.Cancel, map[string]any{"order_line_id": double.Line, "kind": "cancel", "qty_milli": int64(1000), "wastage": false})),
	}
	if err := d.Ingest(context.Background(), double.Tenant, ops...); err != nil {
		t.Fatal(err)
	}
	h := c.as(double.Tenant, []string{double.Outlet}, "report.view.outlet")
	first := func(path string) map[string]any {
		t.Helper()
		_, body := c.get(path, h)
		items, _ := decode[map[string]any](t, body)["items"].([]any)
		if len(items) != 1 {
			t.Fatalf("%s: %s", path, body)
		}
		return items[0].(map[string]any)
	}
	for path, absent := range map[string][]string{
		"/v1/reports/open-checks":   {"table_name"},
		"/v1/reports/kot-log":       {"station_id", "member_id"},
		"/v1/reports/cancellations": {"reason_code_id", "member_id"},
	} {
		row := first(path)
		for _, f := range absent {
			if v, ok := row[f]; ok {
				t.Errorf("%s: %s = %v before the replica row exists, want it absent", path, f, v)
			}
		}
	}
	rb := double.NewOpBuilder("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f7f01", "outlet:"+double.Outlet, double.Member).At(double.Start)
	if err := d.Ingest(context.Background(), double.Tenant,
		rb.Next("settings.outlet.edit", double.Ins("station", double.Station, map[string]any{"code": double.StationCode, "name": "Kitchen", "output": "both", "language": "en"})),
		rb.Next("floor.manage", double.Ins("dining_table", double.Table, map[string]any{"name": "T1", "seats": int64(4), "active": true}))); err != nil {
		t.Fatal(err)
	}
	if got := first("/v1/reports/kot-log")["station_id"]; got != double.Station {
		t.Errorf("station_id = %v after the station row arrived, want %s", got, double.Station)
	}
	if got := first("/v1/reports/open-checks")["table_name"]; got != "T1" {
		t.Errorf("table_name = %v after the table row arrived, want T1", got)
	}
	// a replica is read-only reference data: never in the digest of the owned entities
	_, body := c.get("/v1/projection-digests?business_date="+double.Day, h)
	for _, e := range decode[service.ProjectionDigest](t, body).Entities {
		switch e.Entity {
		case "station", "dining_table", "outlet_settings", "business_day", "day_close":
			t.Errorf("replica %s is in the digest", e.Entity)
		}
	}
}
