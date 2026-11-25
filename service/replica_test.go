package service_test

import (
	"testing"
	"time"

	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/service"
)

// The stale-ticket window is read from the outlet_settings replica; when the row arrives later the
// tickets still waiting for a print are judged again (ADR-0014 A2.4 item 4).
func TestStaleTicketWindowComesFromTheOutletSettingsReplica(t *testing.T) {
	f := newFixture(t)
	f.basicCheck()
	kot := f.b.Next("kot.send", ins("kot", id(12), map[string]any{"check_id": id(3), "kind": "original", "station_code": "KIT", "kot_no": "P1-1", "business_date": double.Day}))
	// ten minutes between sending and arrival: inside the default window of 15 minutes
	if err := f.d.Store.InTx(f.ctx, double.Tenant, func(tx repository.Tx) error { return f.d.Svc.Apply(tx, kot, kot.At.Add(10*time.Minute)) }); err != nil {
		t.Fatal(err)
	}
	if got := f.kinds("open"); len(got) != 0 {
		t.Fatalf("open = %v inside the default window, want none", got)
	}
	f.ingest(f.b.Next("settings.outlet.edit", ins("outlet_settings", id(30), map[string]any{"kot_stale_after_seconds": int64(300)})))
	if got := f.kinds("open"); got[service.KindLateTicket] != 1 {
		t.Fatalf("open = %v after the outlet allowed 5 minutes, want late_ticket", got)
	}
}
