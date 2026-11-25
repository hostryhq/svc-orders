package double

import (
	"fmt"
	"strings"
	"time"

	"github.com/hostryhq/go-common/ids"
	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/service"
)

// Op and Mut are the parsed sync op and mutation the builders produce (go-common/sync).
type (
	Op  = service.Op
	Mut = service.Mut
)

// Identifiers of the example worlds of openapi/orders/v1 (info.x-example-worlds), which are the restaurant
// world of api-specs examples/worlds/restaurant.yaml: org 4a60, outlet 4a61, device 4a5e, member Asha 4a64.
const (
	Tenant   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a60"
	Outlet   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a61"
	Member   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a64"
	Device   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5e"
	Item     = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5101"
	Station  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5131"
	Printer  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5141"
	Table    = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5152"
	Reason   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f5161"
	Check    = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6001"
	Line     = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6011"
	LineGone = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6012"
	Kot      = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6021"
	KotLine  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6022"
	KotPrint = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6023"
	Cancel   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6024"
	Present  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6025"
	KotLine2 = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6026"
	Seating  = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6031"
	Orphan   = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6041"
	Order    = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f6051"
	Day      = "2026-11-24"
)

// StationCode is the code of the kitchen station of the example world (catalog's station row, a replica here).
const StationCode = "KITCHEN"

// Start is the instant of the spec's examples; the double's clock starts here.
var Start = time.Date(2026, 11, 24, 13, 5, 11, 120_000_000, time.UTC)

// Worlds are the example worlds the double and the contract seeding know.
var Worlds = []string{"base", "orphan-line", "no-open-checks", "kitchen-times-day", "digest-day"}

// OpBuilder builds ops with increasing sequence numbers and clock values. The HLC is the instant in
// milliseconds followed by a counter that follows the sequence, so ops stamped in one millisecond
// still order.
type OpBuilder struct {
	Origin string
	Stream string
	Actor  string
	// Step is how far each op's clock is after the previous one (default five seconds; zero stamps
	// the ops of a scene all at the same instant, as the spec's examples do).
	Step time.Duration
	seq  int64
	at   time.Time
}

// NewOpBuilder starts at the example instant less half an hour.
func NewOpBuilder(origin, stream, actor string) *OpBuilder {
	return &OpBuilder{Origin: origin, Stream: stream, Actor: actor, Step: 5 * time.Second, at: Start.Add(-30 * time.Minute)}
}

// Next builds the next op of the origin.
func (b *OpBuilder) Next(intent string, muts ...Mut) *Op {
	b.seq++
	b.at = b.at.Add(b.Step)
	ns, _, _ := strings.Cut(intent, ".")
	outlet, _ := strings.CutPrefix(b.Stream, "outlet:")
	return &Op{Ref: gosync.OpRef{Origin: b.Origin, Stream: b.Stream, Seq: b.seq}, Intent: intent, IntentV: 1, Namespace: ns, Status: "accepted",
		TenantID: ids.ID(Tenant), OutletID: ids.ID(outlet), Actor: b.Actor, HLC: fmt.Sprintf("%012x%04x", uint64(b.at.UnixMilli()), uint16(b.seq)),
		At: b.at, Clock: "device", Muts: muts}
}

// At moves the builder's clock.
func (b *OpBuilder) At(t time.Time) *OpBuilder { b.at = t; return b }

// Ins is an insert mutation.
func Ins(entity, id string, f map[string]any) Mut {
	return Mut{E: entity, ID: id, K: gosync.KindInsert, F: f}
}

// Set is a set or rank mutation.
func Set(kind, entity, id string, f map[string]any) Mut {
	return Mut{E: entity, ID: id, K: kind, F: f}
}

// WorldOps returns the ops that build an example world, and the exceptions to place directly
// (the detectors derive their own ids; the spec's example exception has a fixed one).
func WorldOps(world string) ([]*Op, []repository.Exception, error) {
	switch world {
	case "base", "orphan-line", "no-open-checks":
	case "digest-day":
		return digestDayOps(), nil, nil
	case "kitchen-times-day":
		return kitchenTimesOps(), nil, nil
	default:
		return nil, nil, fmt.Errorf("unknown example world %q", world)
	}
	b := NewOpBuilder(Device, "outlet:"+Outlet, Member)
	ops := []*Op{
		// reference rows of catalog (replicas here, ADR-0014 A2.4)
		b.Next("settings.outlet.edit", Ins("station", Station, map[string]any{"code": StationCode, "name": "Kitchen", "output": "both", "language": "en"})),
		b.Next("floor.manage", Ins("dining_table", Table, map[string]any{"name": "T1", "seats": int64(4), "active": true})),
		b.Next("seating.open", Ins("seating", Seating, map[string]any{"table_id": Table, "business_date": Day, "status": "open"})),
		b.Next("order.create", Ins("order", Order, map[string]any{"seating_id": Seating, "order_type": "dine_in", "business_date": Day, "source": "pos"})),
	}
	b.Step = 0 // the scene of the examples happens at one instant, 13:05:11.120
	b.At(Start)
	line := func(id string, qty int64) Mut {
		return Ins("order_line", id, map[string]any{"order_id": Order, "check_id": Check, "item_id": Item, "name": "Paneer Tikka", "variant_name": "Full",
			"kitchen_name": "Paneer T", "category_name": "Starters", "qty_milli": qty, "unit_price_paise": int64(24000), "modifier_delta_paise": int64(2000),
			"tax_class": "fnb_standard", "station_code": StationCode, "state": "draft"})
	}
	ops = append(ops,
		b.Next("check.create", Ins("check", Check, map[string]any{"seating_id": Seating, "order_type": "dine_in", "label": "Guest 2", "business_date": Day, "status": "open"})),
		b.Next("order.line.add", line(Line, 2000)),
		b.Next("order.line.add", line(LineGone, 1000)),
		b.Next("kot.send",
			Ins("kot", Kot, map[string]any{"check_id": Check, "order_id": Order, "station_code": StationCode, "kot_no": "P1-0007", "kind": "original", "order_type": "dine_in", "table_name": "T1", "business_date": Day}),
			Ins("kot_line", KotLine, map[string]any{"kot_id": Kot, "order_line_id": Line, "name": "Paneer Tikka", "qty_milli": int64(2000), "status": "new"}),
			Ins("kot_line", KotLine2, map[string]any{"kot_id": Kot, "order_line_id": LineGone, "name": "Paneer Tikka", "qty_milli": int64(1000), "status": "new"}),
			Set("rank", "order_line", Line, map[string]any{"state": "sent"}),
			Set("rank", "order_line", LineGone, map[string]any{"state": "sent"})),
		b.Next("kot.print.ack", Ins("kot_print", KotPrint, map[string]any{"kot_id": Kot, "printer_id": Printer, "copy_no": int64(1), "fallback": false})),
		b.Next("kds.status", Set("rank", "kot_line", KotLine2, map[string]any{"status": "preparing"})),
		b.Next("order.line.cancel", Ins("order_line_cancel", Cancel, map[string]any{"order_line_id": LineGone, "kind": "cancel", "qty_milli": int64(1000), "reason_code_id": Reason,
			"kitchen_status_at_cancel": "preparing", "wastage": true})),
		b.At(Start.Add(20*time.Second)).Next("kds.status", Set("rank", "kot_line", KotLine, map[string]any{"status": "ready"})), // 20 s of preparation
		b.At(Start).Next("bill.present", Ins("check_present", Present, map[string]any{"check_id": Check, "present_count": int64(1), "content_hash": "5d41402abc4b2a76b9719d911017c592",
			"total_paise": int64(52000), "mode": "printed"})),
	)
	if world == "no-open-checks" {
		ops = append(ops, b.Next("bill.finalize", Set("rank", "check", Check, map[string]any{"status": "billed"})))
	}
	var xs []repository.Exception
	if world == "orphan-line" {
		xs = append(xs, repository.Exception{ID: Orphan, OutletID: Outlet, Kind: service.KindOrphanLine, Severity: "warning", Status: "open", SubjectKind: "order_line",
			SubjectID: Line, BusinessDate: Day, DetectedAt: Start, DetailKey: "exception.orphan_line", Params: map[string]any{"check_id": Check}})
	}
	return ops, xs, nil
}

// digestDayOps holds exactly the rows of api-specs examples/digests/orders-digest-day.json: check 6001,
// order 6051, lines 6011 and 6012 and KOT 6021, and no other row scoped to the day (no seating, no
// ticket lines, no cancel). Fields are the ones of the example, nothing more.
func digestDayOps() []*Op {
	b := NewOpBuilder(Device, "outlet:"+Outlet, Member)
	b.Step = 0
	b.At(Start)
	line := func(id string, qty int64) Mut {
		return Ins("order_line", id, map[string]any{"order_id": Order, "check_id": Check, "item_id": Item, "name": "Paneer Tikka", "kitchen_name": "Paneer Tikka",
			"category_name": "Starters", "qty_milli": qty, "unit_price_paise": int64(24000), "modifier_delta_paise": int64(2000), "tax_class": "fnb_standard",
			"station_code": StationCode, "state": "sent"})
	}
	ops := []*Op{
		b.Next("order.create", Ins("order", Order, map[string]any{"order_type": "dine_in", "business_date": Day, "source": "pos"})),
		b.Next("check.create", Ins("check", Check, map[string]any{"seating_id": Seating, "order_type": "dine_in", "label": "Guest 2", "business_date": Day, "status": "open"})),
		b.Next("order.line.add", line(Line, 2000), line(LineGone, 1000)),
		b.Next("kot.send", Ins("kot", Kot, map[string]any{"check_id": Check, "order_id": Order, "station_code": StationCode, "kot_no": "P1-0007", "kind": "original",
			"order_type": "dine_in", "business_date": Day})),
	}
	// The check's customer name and phone are pii: a node holds their commitments only (examples/digests/orders-digest-day.json,
	// "commitments") and the digest covers those.
	ops[1].PIICommits = gosync.PIIC{"check": {Check: {
		"customer_name":  "fb926b59239c6d646b19799ab4f0c2196c3b9544e1901c9f6873017c570d2766",
		"customer_phone": "2fc77fdc9cd5852397cd7e2dff88dd1387bc4b73a173e5ea5e575e3aa9fb0335"}}}
	return ops
}

// KitchenLines is the number of ticket lines that reached ready in the kitchen-times-day world.
const KitchenLines = 42

// kitchenPrep is the preparation seconds of the ready lines of kitchen-times-day, in ascending order:
// 20 quick lines, line 6011 at the median (420), 18 slower lines, the 95th percentile (980) and two outliers.
func kitchenPrep() []int64 {
	var p []int64
	for i := int64(0); i < 20; i++ {
		p = append(p, 60+10*i)
	}
	p = append(p, 420)
	for i := int64(0); i < 18; i++ {
		p = append(p, 500+10*i)
	}
	return append(p, 980, 1000, 1100)
}

// kitchenTimesOps builds business day 2026-11-24 of station 5131: 42 ticket lines reached ready (line
// 6011 among them) with median 420 s and 95th percentile 980 s, and line 6012 cancelled after the kitchen
// had started it.
func kitchenTimesOps() []*Op {
	b := NewOpBuilder(Device, "outlet:"+Outlet, Member)
	b.Step = 0
	b.At(Start.Add(-time.Hour))
	prep := kitchenPrep()
	// sorted order puts the median line (6011) at index 20; the rest take synthetic ids
	lineID := func(i int) string { return fmt.Sprintf("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f7%03d", i) }
	kotLineID := func(i int) string { return fmt.Sprintf("0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f8%03d", i) }
	idOf := func(i int) string {
		if i == 20 {
			return Line
		}
		return lineID(i)
	}
	ops := []*Op{
		b.Next("settings.outlet.edit", Ins("station", Station, map[string]any{"code": StationCode, "name": "Kitchen", "output": "both", "language": "en"})),
		b.Next("order.create", Ins("order", Order, map[string]any{"order_type": "dine_in", "business_date": Day, "source": "pos"})),
		b.Next("check.create", Ins("check", Check, map[string]any{"order_type": "dine_in", "label": "Guest 2", "business_date": Day, "status": "open"})),
	}
	add := func(id string) Mut {
		return Ins("order_line", id, map[string]any{"order_id": Order, "check_id": Check, "item_id": Item, "name": "Paneer Tikka", "kitchen_name": "Paneer T",
			"category_name": "Starters", "qty_milli": int64(1000), "unit_price_paise": int64(24000), "modifier_delta_paise": int64(2000), "tax_class": "fnb_standard",
			"station_code": StationCode, "state": "draft"})
	}
	muts := []Mut{add(LineGone)}
	for i := range prep {
		muts = append(muts, add(idOf(i)))
	}
	ops = append(ops, b.Next("order.line.add", muts...))
	sent := b.At(Start.Add(-30 * time.Minute))
	kot := []Mut{Ins("kot", Kot, map[string]any{"check_id": Check, "order_id": Order, "station_code": StationCode, "kot_no": "P1-0007", "kind": "original",
		"order_type": "dine_in", "table_name": "T1", "business_date": Day}),
		Ins("kot_line", KotLine2, map[string]any{"kot_id": Kot, "order_line_id": LineGone, "name": "Paneer Tikka", "qty_milli": int64(1000), "status": "new"}),
		Set("rank", "order_line", LineGone, map[string]any{"state": "sent"})}
	for i := range prep {
		kl := kotLineID(i)
		if i == 20 {
			kl = KotLine
		}
		kot = append(kot, Ins("kot_line", kl, map[string]any{"kot_id": Kot, "order_line_id": idOf(i), "name": "Paneer Tikka", "qty_milli": int64(1000), "status": "new"}),
			Set("rank", "order_line", idOf(i), map[string]any{"state": "sent"}))
	}
	ops = append(ops, sent.Next("kot.send", kot...))
	ops = append(ops,
		b.At(Start.Add(-20*time.Minute)).Next("kds.status", Set("rank", "kot_line", KotLine2, map[string]any{"status": "preparing"})),
		b.At(Start.Add(-19*time.Minute)).Next("order.line.cancel", Ins("order_line_cancel", Cancel, map[string]any{"order_line_id": LineGone, "kind": "cancel", "qty_milli": int64(1000),
			"reason_code_id": Reason, "kitchen_status_at_cancel": "preparing", "wastage": true})))
	t0 := Start.Add(-30 * time.Minute)
	for i, secs := range prep {
		kl := kotLineID(i)
		if i == 20 {
			kl = KotLine
		}
		ops = append(ops, b.At(t0.Add(time.Duration(secs)*time.Second)).Next("kds.status", Set("rank", "kot_line", kl, map[string]any{"status": "ready"})))
	}
	return ops
}
