package api_test

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/service"
)

const otherTenant = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4b60"
const otherOutlet = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4b61"

type client struct {
	t   *testing.T
	d   *double.Double
	srv *httptest.Server
}

func newClient(t *testing.T, world string) *client {
	d := double.New(double.Options{})
	if err := d.SeedWorld(context.Background(), world); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewServer(d.Handler)
	t.Cleanup(srv.Close)
	return &client{t: t, d: d, srv: srv}
}

func (c *client) get(path string, hdr map[string]string) (int, []byte) {
	c.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, c.srv.URL+path, nil)
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		c.t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

func (c *client) as(tenant string, outlets []string, perms ...string) map[string]string {
	return map[string]string{"Authorization": "Bearer " + c.d.Token(tenant, double.Member, outlets, perms...)}
}

func decode[T any](t *testing.T, b []byte) T {
	t.Helper()
	var v T
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("%v in %s", err, b)
	}
	return v
}

func TestAuthenticationAndPermission(t *testing.T) {
	c := newClient(t, "base")
	if s, _ := c.get("/v1/checks", nil); s != 401 {
		t.Fatalf("no token: %d, want 401", s)
	}
	if s, _ := c.get("/v1/checks/"+double.Check, c.as(double.Tenant, nil, "exceptions.view")); s != 403 {
		t.Fatalf("a token without order.edit: %d, want 403", s)
	}
	if s, _ := c.get("/v1/exceptions", c.as(double.Tenant, nil, "order.edit")); s != 403 {
		t.Fatalf("a token without exceptions.view: %d, want 403", s)
	}
	if s, _ := c.get("/v1/reports/kot-log", c.as(double.Tenant, nil, "exceptions.view")); s != 403 {
		t.Fatalf("a token without report.view.outlet: %d, want 403", s)
	}
}

func TestTenantIsolation(t *testing.T) {
	c := newClient(t, "orphan-line")
	other := c.as(otherTenant, nil, "order.edit", "exceptions.view", "report.view.outlet")
	for _, p := range []string{"/v1/checks/" + double.Check, "/v1/exceptions/" + double.Orphan} {
		if s, _ := c.get(p, other); s != 404 {
			t.Errorf("another tenant reading %s: %d, want 404 (never 403)", p, s)
		}
	}
	for p, field := range map[string]string{"/v1/checks": "items", "/v1/exceptions": "items"} {
		s, b := c.get(p, other)
		if s != 200 || len(decode[map[string][]any](t, b)[field]) != 0 {
			t.Errorf("another tenant listing %s: %d %s, want an empty list", p, s, b)
		}
	}
	h := c.as(otherTenant, nil, "report.view.outlet")
	h["X-Outlet-Id"] = double.Outlet
	if s, b := c.get("/v1/reports/cancellations", h); s != 200 || decode[service.CancellationReport](t, b).Totals.Count != 0 {
		t.Errorf("another tenant reading a report of this outlet id: %d %s, want no rows", s, b)
	}
	// the open-check count is for the tenant of the service token only
	svc := map[string]string{"Hostry-Service-Token": c.d.ServiceToken("billing", otherTenant)}
	if s, b := c.get("/internal/v1/outlets/"+double.Outlet+"/open-check-count", svc); s != 200 || decode[service.OpenCheckCount](t, b).OpenChecks != 0 {
		t.Errorf("another tenant's count of this outlet: %d %s, want 0", s, b)
	}
}

func TestOutletScope(t *testing.T) {
	c := newClient(t, "orphan-line")
	elsewhere := c.as(double.Tenant, []string{otherOutlet}, "order.edit", "exceptions.view", "report.view.outlet")
	for _, p := range []string{"/v1/checks/" + double.Check, "/v1/exceptions/" + double.Orphan} {
		if s, _ := c.get(p, elsewhere); s != 404 {
			t.Errorf("a caller of another outlet reading %s: %d, want 404", p, s)
		}
	}
	h := c.as(double.Tenant, []string{otherOutlet}, "order.edit")
	h["X-Outlet-Id"] = double.Outlet
	if s, _ := c.get("/v1/checks", h); s != 404 {
		t.Errorf("X-Outlet-Id outside the grant: %d, want 404", s)
	}
	// without the header a scoped caller sees only its own outlets
	if s, b := c.get("/v1/checks", elsewhere); s != 200 || len(decode[service.CheckPage](t, b).Items) != 0 {
		t.Errorf("a caller of another outlet listing checks: %d %s, want none", s, b)
	}
	own := c.as(double.Tenant, []string{double.Outlet}, "order.edit")
	if s, b := c.get("/v1/checks", own); s != 200 || len(decode[service.CheckPage](t, b).Items) != 1 {
		t.Errorf("a caller of this outlet listing checks: %d %s, want one", s, b)
	}
	// a report names one outlet: an all-outlets caller must say which
	all := c.as(double.Tenant, nil, "report.view.outlet")
	if s, _ := c.get("/v1/reports/kot-log", all); s != 422 {
		t.Errorf("a report without X-Outlet-Id for an all-outlets caller: %d, want 422", s)
	}
}

func TestInternalOpenCheckCount(t *testing.T) {
	c := newClient(t, "base")
	path := "/internal/v1/outlets/" + double.Outlet + "/open-check-count"
	if s, _ := c.get(path, c.as(double.Tenant, nil, "order.edit")); s != 401 {
		t.Errorf("a user token on an internal route: %d, want 401", s)
	}
	if s, _ := c.get(path, map[string]string{"Hostry-Service-Token": c.d.ServiceToken("sync", double.Tenant)}); s != 403 {
		t.Errorf("a service that is not on x-service-callers: %d, want 403", s)
	}
	s, b := c.get(path, map[string]string{"Hostry-Service-Token": c.d.ServiceToken("billing", double.Tenant)})
	if s != 200 || decode[service.OpenCheckCount](t, b).OpenChecks != 1 {
		t.Errorf("billing's count: %d %s, want 1 open check", s, b)
	}
	c2 := newClient(t, "no-open-checks")
	s, b = c2.get(path, map[string]string{"Hostry-Service-Token": c2.d.ServiceToken("billing", double.Tenant)})
	if s != 200 || decode[service.OpenCheckCount](t, b).OpenChecks != 0 {
		t.Errorf("billing's count with every check billed: %d %s, want 0", s, b)
	}
}

func TestReportsOfTheBaseWorld(t *testing.T) {
	c := newClient(t, "base")
	h := c.as(double.Tenant, []string{double.Outlet}, "order.edit", "exceptions.view", "report.view.outlet")

	_, b := c.get("/v1/reports/cancellations", h)
	canc := decode[service.CancellationReport](t, b)
	if canc.Totals.Count != 1 || canc.Totals.ListValue.AmountPaise != 26000 || canc.Totals.WastageValue.AmountPaise != 26000 || canc.Items[0].KitchenStatus != "preparing" {
		t.Errorf("cancellations = %+v", canc)
	}
	_, b = c.get("/v1/reports/cancellations?kind=void", h)
	if decode[service.CancellationReport](t, b).Totals.Count != 0 {
		t.Errorf("kind filter ignored: %s", b)
	}
	_, b = c.get("/v1/reports/kitchen-times", h)
	kt := decode[service.KitchenTimeReport](t, b)
	if len(kt.Items) != 1 || kt.Items[0].StationID != double.Station || kt.Items[0].MedianPrepSeconds != 20 || kt.Items[0].CancelledAfterStart != 1 {
		t.Errorf("kitchen times = %+v, want one station row, 20 s (sent to ready), one cancel after start", kt.Items)
	}
	_, b = c.get("/v1/reports/kot-log", h)
	kl := decode[service.KotLogReport](t, b)
	if len(kl.Items) != 1 || kl.Items[0].KotNo != "P1-0007" || !kl.Items[0].Printed || kl.Items[0].StationID != double.Station {
		t.Errorf("kot log = %+v", kl.Items)
	}
	_, b = c.get("/v1/reports/present-log", h)
	pl := decode[service.PresentLogReport](t, b)
	if pl.Totals.Presents != 1 || pl.Totals.PresentsWithoutBill != 1 || pl.Items[0].Billed {
		t.Errorf("present log = %+v", pl)
	}
	_, b = c.get("/v1/reports/open-checks", h)
	oc := decode[service.OpenChecksReport](t, b)
	if oc.Totals.Count != 1 || oc.Totals.ListTotal.AmountPaise != 52000 || oc.Items[0].TableName != "T1" || !oc.Items[0].Presented {
		t.Errorf("open checks = %+v", oc)
	}
	_, b = c.get("/v1/checks/"+double.Check, h)
	ck := decode[service.CheckDetail](t, b)
	if ck.ListTotal.AmountPaise != 52000 || ck.PresentCount != 1 || ck.Lines[0].QtyMilli != 2000 || ck.Lines[0].KitchenStatus != "ready" || ck.Lines[0].State != "sent" {
		t.Errorf("check = %+v", ck)
	}
	if s, _ := c.get("/v1/reports/cancellations?from=2026-01-01&to=2026-12-31", h); s != 422 {
		t.Errorf("a range over 92 days: %d, want 422", s)
	}
}

func TestProjectionDigestIsStableAndSensitive(t *testing.T) {
	c := newClient(t, "base")
	h := c.as(double.Tenant, []string{double.Outlet}, "report.view.outlet")
	_, b1 := c.get("/v1/projection-digests?business_date=2026-11-24", h)
	_, b2 := c.get("/v1/projection-digests?business_date=2026-11-24", h)
	d1, d2 := decode[service.ProjectionDigest](t, b1), decode[service.ProjectionDigest](t, b2)
	if d1.StateHash != d2.StateHash || d1.Algorithm != "sha256-canon-v1" {
		t.Fatalf("digest not stable: %s vs %s", d1.StateHash, d2.StateHash)
	}
	counts := map[string]int{}
	for _, e := range d1.Entities {
		counts[e.Entity] = e.RowCount
	}
	if counts["check"] != 1 || counts["order_line"] != 2 || counts["kot_line"] != 2 || counts["order_line_cancel"] != 0 { // the cancel row has no kot_id, the only parent reference of its entity
		t.Fatalf("row counts = %v", counts)
	}
	ops := double.NewOpBuilder("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f7f00", "outlet:"+double.Outlet, double.Member).At(double.Start)
	if err := c.d.Ingest(context.Background(), double.Tenant, ops.Next("bill.customer_details.edit", double.Set("set", "check", double.Check, map[string]any{"label": "Guest 3"}))); err != nil {
		t.Fatal(err)
	}
	_, b3 := c.get("/v1/projection-digests?business_date=2026-11-24", h)
	if decode[service.ProjectionDigest](t, b3).StateHash == d1.StateHash {
		t.Fatal("the digest did not change when a row changed")
	}
}

func TestCursorPaging(t *testing.T) {
	c := newClient(t, "base")
	ops := double.NewOpBuilder("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f7f00", "outlet:"+double.Outlet, double.Member).At(double.Start)
	for i := 0; i < 4; i++ {
		rid := "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f70" + string(rune('a'+i)) + "0"
		if err := c.d.Ingest(context.Background(), double.Tenant, ops.Next("check.create", double.Ins("check", rid, map[string]any{"order_type": "counter", "business_date": double.Day, "status": "open"}))); err != nil {
			t.Fatal(err)
		}
	}
	h := c.as(double.Tenant, []string{double.Outlet}, "order.edit")
	seen := map[string]bool{}
	path := "/v1/checks?limit=2"
	for pages := 0; ; pages++ {
		_, b := c.get(path, h)
		p := decode[service.CheckPage](t, b)
		for _, it := range p.Items {
			if seen[it.ID] {
				t.Fatalf("check %s on two pages", it.ID)
			}
			seen[it.ID] = true
		}
		if p.NextCursor == "" {
			break
		}
		if pages > 5 {
			t.Fatal("paging does not end")
		}
		path = "/v1/checks?limit=2&cursor=" + p.NextCursor
	}
	if len(seen) != 5 {
		t.Fatalf("saw %d checks over all pages, want 5", len(seen))
	}
	if s, _ := c.get("/v1/checks?limit=0", h); s != 422 && s != 200 {
		t.Fatalf("limit=0: %d", s)
	}
	if s, _ := c.get("/v1/checks?cursor=!!", h); s != 422 {
		t.Fatalf("a bad cursor: %d, want 422", s)
	}
}

func TestInjectedFailure(t *testing.T) {
	c := newClient(t, "base")
	c.d.Fail("/v1/checks", double.Failure{Status: 503})
	if s, _ := c.get("/v1/checks", c.as(double.Tenant, nil, "order.edit")); s != 503 {
		t.Fatalf("injected failure: %d, want 503", s)
	}
	c.d.Fail("/v1/checks", double.Failure{})
	if s, _ := c.get("/v1/checks", c.as(double.Tenant, nil, "order.edit")); s != 200 {
		t.Fatalf("after clearing: %d, want 200", s)
	}
}
