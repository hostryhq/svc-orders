package double

import (
	"fmt"
	"time"

	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/service"
)

// Identifiers of the example worlds of openapi/orders/v1 (info.x-example-worlds).
const (
	Tenant   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60"
	Outlet   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a61"
	Member   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a66"
	Device   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5e"
	Item     = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5101"
	Station  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5131"
	Table    = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5152"
	Reason   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5161"
	Check    = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6001"
	Line     = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6011"
	Kot      = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6021"
	KotLine  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6022"
	KotPrint = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6023"
	Cancel   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6024"
	Present  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6025"
	Seating  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6031"
	Orphan   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6041"
	Order    = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6051"
	Day      = "2026-11-24"
)

// Start is the instant of the spec's examples; the double's clock starts here.
var Start = time.Date(2026, 11, 24, 13, 5, 11, 120_000_000, time.UTC)

// Worlds are the example worlds the double and the contract seeding know.
var Worlds = []string{"base", "orphan-line", "no-open-checks"}

// OpBuilder builds ops with increasing sequence numbers and clock values.
type OpBuilder struct {
	Origin string
	Stream string
	Actor  string
	seq    int64
	at     time.Time
}

// NewOpBuilder starts at the example instant.
func NewOpBuilder(origin, stream, actor string) *OpBuilder {
	return &OpBuilder{Origin: origin, Stream: stream, Actor: actor, at: Start.Add(-30 * time.Minute)}
}

// Next builds the next op of the origin, five seconds after the previous one.
func (b *OpBuilder) Next(intent string, muts ...service.Mut) service.Op {
	b.seq++
	b.at = b.at.Add(5 * time.Second)
	return service.Op{Origin: b.Origin, Stream: b.Stream, Seq: b.seq, Intent: intent, Status: "accepted", HLC: fmt.Sprintf("%016x", uint64(b.at.UnixMilli())<<16),
		At: b.at.Format(time.RFC3339Nano), Actor: b.Actor, Muts: muts}
}

// At moves the builder's clock.
func (b *OpBuilder) At(t time.Time) *OpBuilder { b.at = t; return b }

// Ins is an insert mutation.
func Ins(entity, id string, f map[string]any) service.Mut {
	return service.Mut{E: entity, ID: id, K: "insert", F: f}
}

// Set is a set or rank mutation.
func Set(kind, entity, id string, f map[string]any) service.Mut {
	return service.Mut{E: entity, ID: id, K: kind, F: f}
}

// WorldOps returns the ops that build an example world, and the exceptions to place directly
// (the detectors derive their own ids; the spec's example exception has a fixed one).
func WorldOps(world string) ([]service.Op, []repository.Exception, error) {
	switch world {
	case "base", "orphan-line", "no-open-checks":
	default:
		return nil, nil, fmt.Errorf("unknown example world %q", world)
	}
	b := NewOpBuilder(Device, "outlet:"+Outlet, Member)
	status := "open"
	if world == "no-open-checks" {
		status = "billed"
	}
	ops := []service.Op{
		b.Next("settings.outlet.edit", Ins("station", Station, map[string]any{"code": "KIT", "name": "Kitchen", "output": "both", "language": "en"})),
		b.Next("floor.manage", Ins("dining_table", Table, map[string]any{"name": "T4", "seats": int64(4), "active": true})),
		b.Next("seating.open", Ins("seating", Seating, map[string]any{"table_id": Table, "business_date": Day, "status": "open"})),
		b.Next("order.create", Ins("order", Order, map[string]any{"seating_id": Seating, "order_type": "dine_in", "business_date": Day, "source": "pos"})),
		b.Next("check.create", Ins("check", Check, map[string]any{"seating_id": Seating, "order_type": "dine_in", "label": "Guest 2", "business_date": Day, "status": "open"})),
		b.Next("order.line.add", Ins("order_line", Line, map[string]any{"order_id": Order, "check_id": Check, "item_id": Item, "name": "Paneer Tikka",
			"kitchen_name": "Paneer T", "category_name": "Starters", "qty_milli": int64(3000), "unit_price_paise": int64(24000), "modifier_delta_paise": int64(2000),
			"tax_class": "fnb_standard", "station_code": "KIT", "state": "draft"})),
		b.Next("kot.send",
			Ins("kot", Kot, map[string]any{"check_id": Check, "order_id": Order, "station_code": "KIT", "kot_no": "P1-0007", "kind": "original", "order_type": "dine_in", "table_name": "T4", "business_date": Day}),
			Ins("kot_line", KotLine, map[string]any{"kot_id": Kot, "order_line_id": Line, "name": "Paneer Tikka", "qty_milli": int64(3000), "status": "new"}),
			Set("rank", "order_line", Line, map[string]any{"state": "sent"})),
		b.Next("kot.print.ack", Ins("kot_print", KotPrint, map[string]any{"kot_id": Kot, "printer_id": Station, "copy_no": int64(1), "fallback": false})),
		b.Next("kds.status", Set("rank", "kot_line", KotLine, map[string]any{"status": "preparing"})),
		b.Next("order.line.cancel", Ins("order_line_cancel", Cancel, map[string]any{"order_line_id": Line, "kind": "cancel", "qty_milli": int64(1000), "reason_code_id": Reason,
			"kitchen_status_at_cancel": "preparing", "wastage": true})),
		b.Next("kds.status", Set("rank", "kot_line", KotLine, map[string]any{"status": "ready"})),
		b.Next("bill.present", Ins("check_present", Present, map[string]any{"check_id": Check, "present_count": int64(1), "content_hash": "5d41402abc4b2a76b9719d911017c592",
			"total_paise": int64(52000), "mode": "printed"})),
	}
	if status == "billed" {
		ops = append(ops, b.Next("bill.finalize", Set("rank", "check", Check, map[string]any{"status": "billed"})))
	}
	var xs []repository.Exception
	if world == "orphan-line" {
		xs = append(xs, repository.Exception{ID: Orphan, OutletID: Outlet, Kind: service.KindOrphanLine, Severity: "warning", Status: "open", SubjectKind: "order_line",
			SubjectID: Line, BusinessDate: Day, DetectedAt: Start, DetailKey: "exception.orphan_line", Params: map[string]any{"check_id": Check}})
	}
	return ops, xs, nil
}
