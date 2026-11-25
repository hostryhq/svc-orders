package service_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/service"
)

func TestOrphanLineWhenALineReachesACheckAlreadyBilled(t *testing.T) {
	f := newFixture(t)
	f.basicCheck()
	other := double.NewOpBuilder(id(90), "outlet:"+double.Outlet, id(91)).At(double.Start.Add(10 * time.Minute))
	f.ingest(f.b.At(double.Start).Next("bill.finalize", double.Set("rank", "check", id(3), map[string]any{"status": "billed"})))
	if n := len(f.exceptions("open")); n != 0 {
		t.Fatalf("%d exceptions before any line reached the billed check, want 0", n)
	}
	f.ingest(other.Next("order.line.add", ins("order_line", id(10), map[string]any{"check_id": id(3), "qty_milli": int64(1000), "state": "draft"})))
	if got := f.kinds("open"); got[service.KindOrphanLine] != 1 {
		t.Fatalf("open exceptions = %v, want one orphan_line", got)
	}
	// the line is moved to another check: the finding closes itself
	f.ingest(other.Next("order.items.move", double.Set("set", "order_line", id(10), map[string]any{"check_id": id(11)})))
	if got := f.kinds("open"); len(got) != 0 {
		t.Fatalf("open exceptions = %v after the move, want none", got)
	}
	if got := f.kinds("resolved"); got[service.KindOrphanLine] != 1 {
		t.Fatalf("resolved = %v, want the orphan_line", got)
	}
	var published int
	for _, e := range f.d.Memory.Events() {
		if e.Type == "orders.exception.opened" {
			published++
		}
	}
	if published != 1 {
		t.Fatalf("%d orders.exception.opened events, want exactly 1", published)
	}
}

func TestGraceCancelOfAStartedLineAndAllowance(t *testing.T) {
	f := newFixture(t)
	f.basicCheck()
	f.ingest(f.b.Next("kot.send", ins("kot_line", id(7), map[string]any{"kot_id": id(12), "order_line_id": id(4), "status": "new"})))
	f.ingest(f.b.Next("kds.status", double.Set("rank", "kot_line", id(7), map[string]any{"status": "preparing"})))
	cancel := func(n int, kitchen string) service.Op {
		return f.b.Next("order.line.cancel.grace", ins("order_line_cancel", id(20+n), map[string]any{"order_line_id": id(4), "kind": "grace_cancel", "qty_milli": int64(1),
			"kitchen_status_at_cancel": kitchen, "wastage": false}))
	}
	f.ingest(cancel(1, "new"))
	if got := f.kinds("open"); len(got) != 0 {
		t.Fatalf("a grace cancel of a new ticket raised %v", got)
	}
	f.ingest(cancel(2, "preparing"))
	if got := f.kinds("open"); got[service.KindGraceStarted] != 1 {
		t.Fatalf("open = %v, want grace_cancel_started", got)
	}
	for n := 3; n <= 11; n++ { // 11 grace cancels in all: the allowance is 10 per member and business date
		f.ingest(cancel(n, "new"))
	}
	if got := f.kinds("open"); got[service.KindAllowance] != 1 {
		t.Fatalf("open = %v, want cancel_allowance_exceeded", got)
	}
}

func TestTwoSeatingsOnOneTableUntilOneMovesAway(t *testing.T) {
	f := newFixture(t)
	other := double.NewOpBuilder(id(90), "outlet:"+double.Outlet, id(91)).At(double.Start.Add(-20 * time.Minute))
	f.ingest(f.b.Next("seating.open", ins("seating", id(1), map[string]any{"table_id": id(2), "business_date": double.Day, "status": "open"})))
	f.ingest(other.Next("seating.open", ins("seating", id(13), map[string]any{"table_id": id(2), "business_date": double.Day, "status": "open"})))
	if got := f.kinds("open"); got[service.KindTwoSeatings] != 1 {
		t.Fatalf("open = %v, want two_seatings_one_table", got)
	}
	f.ingest(other.Next("order.table.move", ins("seating_move", id(14), map[string]any{"seating_id": id(13), "from_table_id": id(2), "to_table_id": id(15)})))
	if got := f.kinds("open"); len(got) != 0 {
		t.Fatalf("open = %v after the move, want none", got)
	}
}

func TestLateTicketOnArrivalAfterTheStaleWindowUntilPrinted(t *testing.T) {
	f := newFixture(t)
	f.basicCheck()
	kot := f.b.Next("kot.send", ins("kot", id(12), map[string]any{"check_id": id(3), "kind": "original", "station_code": "KIT", "kot_no": "P1-1", "business_date": double.Day}))
	// the ticket was sent long before it reaches orders (a blackout)
	if err := f.d.Store.InTx(f.ctx, double.Tenant, func(tx repository.Tx) error { return f.d.Svc.Apply(tx, kot, double.Start.Add(2*time.Hour)) }); err != nil {
		t.Fatal(err)
	}
	if got := f.kinds("open"); got[service.KindLateTicket] != 1 {
		t.Fatalf("open = %v, want late_ticket", got)
	}
	f.ingest(f.b.Next("kot.print.ack", ins("kot_print", id(16), map[string]any{"kot_id": id(12), "copy_no": int64(1), "fallback": false})))
	if got := f.kinds("open"); len(got) != 0 {
		t.Fatalf("open = %v after the print, want none", got)
	}
}

func TestLateTicketForAHandover(t *testing.T) {
	f := newFixture(t)
	f.basicCheck()
	f.ingest(f.b.Next("kot.send", ins("kot", id(12), map[string]any{"check_id": id(3), "kind": "original", "station_code": "KIT", "kot_no": "P1-1", "business_date": double.Day})))
	f.ingest(f.b.Next("kot.handover", ins("kot_handover", id(17), map[string]any{"kot_id": id(12), "mode": "verbal"})))
	if got := f.kinds("open"); got[service.KindLateTicket] != 1 {
		t.Fatalf("open = %v, want a late_ticket for the handover", got)
	}
}

func TestLinesArrivingAfterDayCloseAreUnresolvedAndPresentedChecksAreFlagged(t *testing.T) {
	f := newFixture(t)
	f.basicCheck()
	f.ingest(f.b.Next("check.present", ins("check_present", id(18), map[string]any{"check_id": id(3), "present_count": int64(1), "total_paise": int64(5000), "mode": "printed", "content_hash": "h"})))
	if got := f.kinds("open"); len(got) != 0 {
		t.Fatalf("open = %v before the day closed", got)
	}
	cloud := double.NewOpBuilder(id(92), "outlet:"+double.Outlet, id(91))
	f.ingest(cloud.Next("day.open", ins("business_day", id(19), map[string]any{"business_date": double.Day, "status": "open"})))
	f.ingest(cloud.Next("day.close", ins("day_close", id(21), map[string]any{"business_day_id": id(19), "z_number": "Z/1"})))
	// everything above arrived before the close, so it was carried over on purpose
	if got := f.kinds("open"); len(got) != 0 {
		t.Fatalf("open = %v for lines that arrived before the close", got)
	}
	late := double.NewOpBuilder(id(90), "outlet:"+double.Outlet, id(91)).At(double.Start.Add(-5 * time.Minute))
	f.ingest(late.Next("order.line.add", ins("order_line", id(22), map[string]any{"check_id": id(3), "qty_milli": int64(1000), "state": "draft"})))
	f.ingest(late.Next("bill.present", ins("check_present", id(23), map[string]any{"check_id": id(3), "present_count": int64(2), "total_paise": int64(9000), "mode": "printed", "content_hash": "h2"})))
	got := f.kinds("open")
	if got[service.KindUnresolvedAtClose] != 1 || got[service.KindPresentedOpen] != 1 {
		t.Fatalf("open = %v, want unresolved_line_at_close and presented_check_unresolved", got)
	}
	f.ingest(late.Next("bill.finalize", double.Set("rank", "check", id(3), map[string]any{"status": "billed"})))
	if got := f.kinds("open"); got[service.KindUnresolvedAtClose] != 0 || got[service.KindPresentedOpen] != 0 {
		t.Fatalf("open = %v after the check was billed, want those two closed", got)
	}
}

func TestExceptionIdsAreDeterministic(t *testing.T) {
	ids := func() string {
		f := newFixture(t)
		f.basicCheck()
		f.ingest(f.b.Next("kot.handover", ins("kot", id(12), map[string]any{"check_id": id(3), "kind": "original", "station_code": "KIT", "business_date": double.Day})))
		f.ingest(f.b.Next("kot.handover", ins("kot_handover", id(17), map[string]any{"kot_id": id(12), "mode": "verbal"})))
		xs := f.exceptions("open")
		if len(xs) != 1 {
			t.Fatalf("got %d exceptions", len(xs))
		}
		return fmt.Sprint(xs[0].ID)
	}
	if a, b := ids(), ids(); a != b {
		t.Fatalf("exception ids differ between two projections of the same ops: %s vs %s", a, b)
	}
}
