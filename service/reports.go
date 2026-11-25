package service

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"sort"
	"strings"

	"github.com/hostryhq/go-common/money"
	"github.com/hostryhq/go-common/tenancy"
	"github.com/hostryhq/svc-orders/repository"
)

// ReportFilter is the common filter of the report operations.
type ReportFilter struct {
	From, To, Kind, Cursor, OutletHeader string
	Limit                                int
}

func (s *Service) report(ctx context.Context, f ReportFilter, fn func(tx repository.Tx, outlet, from, to string) error) error {
	tc, err := tenancy.Require(ctx)
	if err != nil {
		return err
	}
	outlet, err := oneOutlet(ctx, "report.view.outlet", f.OutletHeader)
	if err != nil {
		return err
	}
	return s.Repo.InTx(ctx, string(tc.TenantID), func(tx repository.Tx) error {
		from, to, err := s.dateRange(tx, outlet, f.From, f.To)
		if err != nil {
			return err
		}
		return fn(tx, outlet, from, to)
	})
}

func stationIDs(tx repository.Tx, outlet string) (map[string]string, error) {
	rows, err := tx.Rows(repository.RowQuery{Entity: "station", Outlet: outlet})
	if err != nil {
		return nil, err
	}
	m := map[string]string{}
	for _, r := range rows {
		m[str(r.Fields["code"])] = r.ID
	}
	return m, nil
}

// cancelRows loads the cancels of checks of a date range with their line and check.
type cancelCtx struct {
	cancel repository.Row
	line   repository.Row
	check  repository.Row
}

func loadCancels(tx repository.Tx, outlet, from, to string) ([]cancelCtx, error) {
	checks, err := tx.Rows(repository.RowQuery{Entity: "check", Outlet: outlet, DateFrom: from, DateTo: to})
	if err != nil || len(checks) == 0 {
		return nil, err
	}
	byCheck := map[string]repository.Row{}
	var cids []string
	for _, c := range checks {
		byCheck[c.ID] = c
		cids = append(cids, c.ID)
	}
	lines, err := tx.Rows(repository.RowQuery{Entity: "order_line", Outlet: outlet, Parents: cids})
	if err != nil || len(lines) == 0 {
		return nil, err
	}
	byLine := map[string]repository.Row{}
	var lids []string
	for _, l := range lines {
		byLine[l.ID] = l
		lids = append(lids, l.ID)
	}
	cs, err := tx.Rows(repository.RowQuery{Entity: "order_line_cancel", Outlet: outlet, Parents: lids})
	if err != nil {
		return nil, err
	}
	out := make([]cancelCtx, 0, len(cs))
	for _, c := range cs {
		l := byLine[c.Parent]
		out = append(out, cancelCtx{c, l, byCheck[l.Parent]})
	}
	return out, nil
}

// CancellationReport lists voids, cancels and grace cancels by business date (report 7, orders part).
func (s *Service) CancellationReport(ctx context.Context, f ReportFilter) (CancellationReport, error) {
	var out CancellationReport
	err := s.report(ctx, f, func(tx repository.Tx, outlet, from, to string) error {
		cs, err := loadCancels(tx, outlet, from, to)
		if err != nil {
			return err
		}
		out = CancellationReport{OutletID: outlet, From: from, To: to, Basis: "business_date", Items: []CancellationRow{}}
		var rows []CancellationRow
		var total, waste int64
		for _, c := range cs {
			kind := str(c.cancel.Fields["kind"])
			if f.Kind != "" && kind != f.Kind {
				continue
			}
			q := intOf(c.cancel.Fields["qty_milli"])
			v := lineValue(c.line, q)
			ks := str(c.cancel.Fields["kitchen_status_at_cancel"])
			if ks == "" {
				ks = "new"
			}
			w, _ := c.cancel.Fields["wastage"].(bool)
			total += v
			if w {
				waste += v
			}
			rows = append(rows, CancellationRow{Kind: kind, OrderLineID: c.line.ID, CheckID: c.check.ID, ItemName: str(c.line.Fields["name"]), QtyMilli: q,
				ListValue: money.New(v), ReasonCodeID: str(c.cancel.Fields["reason_code_id"]), MemberID: c.cancel.Meta.CreatedBy, Wastage: w,
				KitchenStatus: ks, CancelledAt: ts(c.cancel.Meta.CreatedAt), BusinessDate: c.check.BusinessDate})
		}
		pg, next, err := page(rows, func(r CancellationRow) string { return r.CancelledAt + "|" + r.OrderLineID + "|" + r.Kind }, f.Cursor, f.Limit)
		if err != nil {
			return err
		}
		out.Items, out.NextCursor = pg, next
		out.Totals = CancellationTotals{Count: len(rows), ListValue: money.New(total), WastageValue: money.New(waste)}
		return nil
	})
	return out, err
}

// KotLog lists KOTs by business date (report 12).
func (s *Service) KotLog(ctx context.Context, f ReportFilter) (KotLogReport, error) {
	var out KotLogReport
	err := s.report(ctx, f, func(tx repository.Tx, outlet, from, to string) error {
		kots, err := tx.Rows(repository.RowQuery{Entity: "kot", Outlet: outlet, DateFrom: from, DateTo: to})
		if err != nil {
			return err
		}
		out = KotLogReport{OutletID: outlet, From: from, To: to, Basis: "business_date", Items: []KotLogRow{}}
		if len(kots) == 0 {
			return nil
		}
		var kids []string
		for _, k := range kots {
			kids = append(kids, k.ID)
		}
		lines, _ := tx.Rows(repository.RowQuery{Entity: "kot_line", Outlet: outlet, Parents: kids})
		prints, _ := tx.Rows(repository.RowQuery{Entity: "kot_print", Outlet: outlet, Parents: kids})
		hands, _ := tx.Rows(repository.RowQuery{Entity: "kot_handover", Outlet: outlet, Parents: kids})
		stations, err := stationIDs(tx, outlet)
		if err != nil {
			return err
		}
		nl := map[string]int{}
		for _, l := range lines {
			nl[l.Parent]++
		}
		maxCopy := map[string]int64{}
		printed := map[string]bool{}
		for _, p := range prints {
			printed[p.Parent] = true
			if c := intOf(p.Fields["copy_no"]); c > maxCopy[p.Parent] {
				maxCopy[p.Parent] = c
			}
		}
		hand := map[string]string{}
		for _, h := range hands {
			hand[h.Parent] = str(h.Fields["mode"])
		}
		var rows []KotLogRow
		for _, k := range kots {
			lc := nl[k.ID]
			if lc < 1 {
				lc = 1
			}
			rc := 0
			if maxCopy[k.ID] > 1 {
				rc = int(maxCopy[k.ID] - 1)
			}
			rows = append(rows, KotLogRow{KotID: k.ID, KotNo: str(k.Fields["kot_no"]), Kind: str(k.Fields["kind"]), StationID: stations[str(k.Fields["station_code"])],
				CheckID: str(k.Fields["check_id"]), LineCount: lc, SentAt: ts(k.Meta.CreatedAt), MemberID: k.Meta.CreatedBy, DeviceID: k.Meta.Origin,
				Printed: printed[k.ID], Handover: hand[k.ID], ReprintCount: rc, BusinessDate: k.BusinessDate})
		}
		pg, next, err := page(rows, func(r KotLogRow) string { return r.SentAt + "|" + r.KotID }, f.Cursor, f.Limit)
		out.Items, out.NextCursor = pg, next
		return err
	})
	return out, err
}

func nearestRank(sorted []int64, p float64) int64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted))*p+0.999999) - 1
	if idx < 0 {
		idx = 0
	}
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

// KitchenTimes reports median and 95th percentile preparation time per station and business date.
// Preparation is the time from the ticket line being sent to its kitchen status reaching ready.
func (s *Service) KitchenTimes(ctx context.Context, f ReportFilter) (KitchenTimeReport, error) {
	var out KitchenTimeReport
	err := s.report(ctx, f, func(tx repository.Tx, outlet, from, to string) error {
		out = KitchenTimeReport{OutletID: outlet, From: from, To: to, Basis: "business_date", Items: []KitchenTimeRow{}}
		kots, err := tx.Rows(repository.RowQuery{Entity: "kot", Outlet: outlet, DateFrom: from, DateTo: to})
		if err != nil {
			return err
		}
		stations, err := stationIDs(tx, outlet)
		if err != nil {
			return err
		}
		type gk struct{ station, date string }
		type acc struct {
			lines     int
			preps     []int64
			cancelled int
		}
		groups := map[gk]*acc{}
		get := func(k gk) *acc {
			if groups[k] == nil {
				groups[k] = &acc{}
			}
			return groups[k]
		}
		kotByID := map[string]repository.Row{}
		var kids []string
		for _, k := range kots {
			if str(k.Fields["kind"]) == "original" {
				kotByID[k.ID] = k
				kids = append(kids, k.ID)
			}
		}
		if len(kids) > 0 {
			lines, err := tx.Rows(repository.RowQuery{Entity: "kot_line", Outlet: outlet, Parents: kids})
			if err != nil {
				return err
			}
			for _, l := range lines {
				k := kotByID[l.Parent]
				a := get(gk{stations[str(k.Fields["station_code"])], k.BusinessDate})
				a.lines++
				ready := l.Meta.RankAt["status"]["ready"]
				if ready == "" {
					ready = l.Meta.RankAt["status"]["served"]
				}
				if d, ok := secondsBetween(l.Meta.CreatedAt, ready); ok {
					a.preps = append(a.preps, d)
				}
			}
		}
		cs, err := loadCancels(tx, outlet, from, to)
		if err != nil {
			return err
		}
		for _, c := range cs {
			switch str(c.cancel.Fields["kitchen_status_at_cancel"]) {
			case "preparing", "ready", "served":
				get(gk{stations[str(c.line.Fields["station_code"])], c.check.BusinessDate}).cancelled++
			}
		}
		var rows []KitchenTimeRow
		for k, a := range groups {
			sort.Slice(a.preps, func(i, j int) bool { return a.preps[i] < a.preps[j] })
			rows = append(rows, KitchenTimeRow{StationID: k.station, BusinessDate: k.date, LineCount: a.lines, MedianPrepSeconds: nearestRank(a.preps, 0.5),
				P95PrepSeconds: nearestRank(a.preps, 0.95), CancelledAfterStart: a.cancelled})
		}
		pg, next, err := page(rows, func(r KitchenTimeRow) string { return r.BusinessDate + "|" + r.StationID }, f.Cursor, f.Limit)
		out.Items, out.NextCursor = pg, next
		return err
	})
	return out, err
}

// PresentLog lists pre-bill presents with the changes after each (report 14).
func (s *Service) PresentLog(ctx context.Context, f ReportFilter) (PresentLogReport, error) {
	var out PresentLogReport
	err := s.report(ctx, f, func(tx repository.Tx, outlet, from, to string) error {
		out = PresentLogReport{OutletID: outlet, From: from, To: to, Basis: "business_date", Items: []PresentLogRow{}}
		checks, err := tx.Rows(repository.RowQuery{Entity: "check", Outlet: outlet, DateFrom: from, DateTo: to})
		if err != nil || len(checks) == 0 {
			return err
		}
		byCheck := map[string]repository.Row{}
		var cids []string
		for _, c := range checks {
			byCheck[c.ID] = c
			cids = append(cids, c.ID)
		}
		pres, err := tx.Rows(repository.RowQuery{Entity: "check_present", Outlet: outlet, Parents: cids})
		if err != nil {
			return err
		}
		chg, err := tx.Rows(repository.RowQuery{Entity: "check_present_change", Outlet: outlet, Parents: cids})
		if err != nil {
			return err
		}
		type ck struct {
			check string
			n     int64
		}
		after := map[ck]int{}
		for _, c := range chg {
			after[ck{c.Parent, intOf(c.Fields["after_present_count"])}]++
		}
		var rows []PresentLogRow
		without := 0
		for _, p := range pres {
			c := byCheck[p.Parent]
			billed := str(c.Fields["status"]) == "billed"
			if !billed {
				without++
			}
			n := intOf(p.Fields["present_count"])
			rows = append(rows, PresentLogRow{CheckID: c.ID, PresentCount: int(n), ContentHash: str(p.Fields["content_hash"]), Total: money.New(intOf(p.Fields["total_paise"])),
				Mode: str(p.Fields["mode"]), MemberID: p.Meta.CreatedBy, DeviceID: p.Meta.Origin, PresentedAt: ts(p.Meta.CreatedAt),
				ChangesAfter: after[ck{c.ID, n}], Billed: billed, BusinessDate: c.BusinessDate})
		}
		pg, next, err := page(rows, func(r PresentLogRow) string { return r.PresentedAt + "|" + r.CheckID + "|" + itoa(r.PresentCount+1000) }, f.Cursor, f.Limit)
		out.Items, out.NextCursor = pg, next
		out.Totals = PresentLogTotals{Presents: len(rows), PresentsWithoutBill: without}
		return err
	})
	return out, err
}

// OpenChecks lists open checks opened in the range; one opened on a business day that has closed is carried over.
func (s *Service) OpenChecks(ctx context.Context, f ReportFilter) (OpenChecksReport, error) {
	var out OpenChecksReport
	err := s.report(ctx, f, func(tx repository.Tx, outlet, from, to string) error {
		out = OpenChecksReport{OutletID: outlet, From: from, To: to, Basis: "business_date", Items: []OpenCheckRow{}}
		checks, err := tx.Rows(repository.RowQuery{Entity: "check", Outlet: outlet, Tag: "open", DateFrom: from, DateTo: to})
		if err != nil {
			return err
		}
		parts, err := s.loadParts(tx, outlet, checks)
		if err != nil {
			return err
		}
		tables, err := tableNames(tx, outlet)
		if err != nil {
			return err
		}
		var rows []OpenCheckRow
		var total int64
		for _, c := range checks {
			if parts.cancelled[c.ID] {
				continue
			}
			live, t := parts.liveLines(c.ID)
			_, closed, err := closeSeq(tx, outlet, c.BusinessDate)
			if err != nil {
				return err
			}
			total += t
			rows = append(rows, OpenCheckRow{CheckID: c.ID, SeatingID: str(c.Fields["seating_id"]), TableName: tables[str(c.Fields["seating_id"])], Label: str(c.Fields["label"]),
				OpenedBusinessDate: c.BusinessDate, CarriedOver: closed, Presented: parts.presents[c.ID] > 0, LineCount: live, ListTotal: money.New(t)})
		}
		pg, next, err := page(rows, func(r OpenCheckRow) string { return r.OpenedBusinessDate + "|" + r.CheckID }, f.Cursor, f.Limit)
		out.Items, out.NextCursor = pg, next
		out.Totals = OpenChecksTotals{Count: len(rows), ListTotal: money.New(total)}
		return err
	})
	return out, err
}

// tableNames maps seating id to the name of the table it sits on now (reference rows of catalog).
func tableNames(tx repository.Tx, outlet string) (map[string]string, error) {
	by, err := currentTables(tx, outlet)
	if err != nil {
		return nil, err
	}
	dts, err := tx.Rows(repository.RowQuery{Entity: "dining_table", Outlet: outlet})
	if err != nil {
		return nil, err
	}
	names := map[string]string{}
	for _, d := range dts {
		names[d.ID] = str(d.Fields["name"])
	}
	out := map[string]string{}
	for table, ss := range by {
		for _, id := range ss {
			out[id] = names[table]
		}
	}
	return out, nil
}

// ProjectionDigest hashes the owned rows of an outlet business day (ADR-0014 T9). A row belongs to the
// date of itself or, through its parent chain, of its check, kot or seating. Rows whose parent has not
// arrived yet belong to no date and are left out until it does.
func (s *Service) ProjectionDigest(ctx context.Context, header, date string) (ProjectionDigest, error) {
	tc, err := tenancy.Require(ctx)
	if err != nil {
		return ProjectionDigest{}, err
	}
	outlet, err := oneOutlet(ctx, "report.view.outlet", header)
	if err != nil {
		return ProjectionDigest{}, err
	}
	var out ProjectionDigest
	err = s.Repo.InTx(ctx, string(tc.TenantID), func(tx repository.Tx) error {
		d, _, err := s.dateRange(tx, outlet, date, date)
		if err != nil {
			return err
		}
		all := map[string]map[string]repository.Row{}
		for _, e := range ownedEntities() {
			rows, err := tx.Rows(repository.RowQuery{Entity: e, Outlet: outlet, IncludeGone: true})
			if err != nil {
				return err
			}
			all[e] = map[string]repository.Row{}
			for _, r := range rows {
				all[e][r.ID] = r
			}
		}
		parentEntity := map[string]string{"order_line": "check", "kot": "check", "kot_line": "kot", "kot_print": "kot", "kot_handover": "kot", "order_line_cancel": "order_line",
			"order_line_modifier": "order_line", "check_cancel": "check", "check_present": "check", "check_present_change": "check", "bill_request": "seating",
			"seating_move": "seating", "seating_merge": "seating"}
		var dateOf func(e string, r repository.Row, depth int) string
		dateOf = func(e string, r repository.Row, depth int) string {
			if r.BusinessDate != "" {
				return r.BusinessDate
			}
			pe := parentEntity[e]
			if pe == "" || depth > 4 {
				return ""
			}
			p, ok := all[pe][r.Parent]
			if !ok {
				return ""
			}
			return dateOf(pe, p, depth+1)
		}
		out = ProjectionDigest{OutletID: outlet, BusinessDate: d, Algorithm: "sha256-canon-v1", ComputedAt: tsTime(s.Clock.Now()), Entities: []EntityDigest{}}
		state := sha256.New()
		for _, e := range ownedEntities() {
			var lines []string
			for id, r := range all[e] {
				if dateOf(e, r, 0) == d {
					lines = append(lines, e+"|"+id+"|"+canon(r.Fields))
				}
			}
			sort.Strings(lines)
			sum := sha256.Sum256([]byte(strings.Join(lines, "\n")))
			ed := EntityDigest{Entity: e, RowCount: len(lines), Hash: hex.EncodeToString(sum[:])}
			out.Entities = append(out.Entities, ed)
			state.Write([]byte(ed.Entity + "|" + itoa(ed.RowCount) + "|" + ed.Hash + "\n"))
		}
		out.StateHash = hex.EncodeToString(state.Sum(nil))
		return nil
	})
	return out, err
}
