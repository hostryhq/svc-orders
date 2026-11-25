package service

import (
	"sort"
	"time"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/svc-orders/repository"
)

// Exception kinds (openapi/orders/v1 listOrderExceptions).
const (
	KindOrphanLine        = "orphan_line"
	KindUnresolvedAtClose = "unresolved_line_at_close"
	KindGraceStarted      = "grace_cancel_started"
	KindAllowance         = "cancel_allowance_exceeded"
	KindTwoSeatings       = "two_seatings_one_table"
	KindLateTicket        = "late_ticket"
	KindPresentedOpen     = "presented_check_unresolved"
)

type finding struct {
	Kind, Severity, SubjectKind, SubjectID, BusinessDate string
	Params                                               map[string]any
}

type fkey struct{ Kind, SubjectKind, SubjectID, Date string }

func (f finding) key() fkey { return fkey{f.Kind, f.SubjectKind, f.SubjectID, f.BusinessDate} }

// detect runs the detectors that the touched rows can change. Each detector is a derived query
// over the projection (never an op); a finding closes itself when it no longer holds.
func (s *Service) detect(tx repository.Tx, op *Op, touched map[repository.RowKey]bool, arrived time.Time) error {
	outlet := outletOf(op)
	checks := map[string]bool{}
	kots := map[string]bool{}
	cancels := map[string]bool{}
	seatings := false
	for k := range touched {
		row, ok, err := tx.GetRow(k.Entity, k.ID)
		if err != nil {
			return err
		}
		parent := ""
		if ok {
			parent = row.Parent
		}
		switch k.Entity {
		case "check":
			checks[k.ID] = true
		case "order_line", "check_present", "check_present_change", "check_cancel":
			if parent != "" {
				checks[parent] = true
			}
			if k.Entity == "order_line" {
				// a line that moved away must be re-judged on the check it left: its open findings carry that check id
				for _, x := range s.openFindingsOf(tx, outlet, k.ID) {
					checks[x] = true
				}
			}
		case "kot":
			kots[k.ID] = true
		case "kot_print", "kot_handover":
			if parent != "" {
				kots[parent] = true
			}
		case "order_line_cancel":
			cancels[k.ID] = true
		case "seating", "seating_move", "seating_merge":
			seatings = true
		case "outlet_settings":
			// the stale-ticket window changed: re-judge every ticket still waiting for a print (replica row, A2.4 item 4)
			ks, err := tx.Rows(repository.RowQuery{Entity: "kot", Outlet: outlet})
			if err != nil {
				return err
			}
			for _, k := range ks {
				kots[k.ID] = true
			}
		case "day_close", "business_day":
			ids, err := s.checksOfClosedDay(tx, outlet)
			if err != nil {
				return err
			}
			for _, id := range ids {
				checks[id] = true
			}
		}
	}
	for _, id := range sortedKeys(checks) {
		if err := s.evalCheck(tx, outlet, id, op, arrived); err != nil {
			return err
		}
	}
	for _, id := range sortedKeys(kots) {
		if err := s.evalKot(tx, outlet, id, op, arrived); err != nil {
			return err
		}
	}
	for _, id := range sortedKeys(cancels) {
		if err := s.evalCancel(tx, outlet, id, op, arrived); err != nil {
			return err
		}
	}
	if seatings {
		return s.evalSeatings(tx, outlet, op, arrived)
	}
	return nil
}

func sortedKeys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// openFindingsOf returns the check ids named by open line findings about lineID.
func (s *Service) openFindingsOf(tx repository.Tx, outlet, lineID string) []string {
	xs, err := tx.Exceptions(repository.ExceptionQuery{Outlet: outlet, Status: "open", SubjectKind: "order_line", SubjectIDs: []string{lineID}})
	if err != nil {
		return nil
	}
	var out []string
	for _, x := range xs {
		if c, _ := x.Params["check_id"].(string); c != "" {
			out = append(out, c)
		}
	}
	return out
}

// closeSeq is the arrival number of the first day_close of a business date at an outlet.
func closeSeq(tx repository.Tx, outlet, date string) (int64, bool, error) {
	days, err := tx.Rows(repository.RowQuery{Entity: "business_day", Outlet: outlet, DateFrom: date, DateTo: date})
	if err != nil || len(days) == 0 {
		return 0, false, err
	}
	var dayIDs []string
	for _, d := range days {
		dayIDs = append(dayIDs, d.ID)
	}
	closes, err := tx.Rows(repository.RowQuery{Entity: "day_close", Outlet: outlet, Parents: dayIDs})
	if err != nil || len(closes) == 0 {
		return 0, false, err
	}
	min := closes[0].Meta.ApplySeq
	for _, c := range closes[1:] {
		if c.Meta.ApplySeq < min {
			min = c.Meta.ApplySeq
		}
	}
	return min, true, nil
}

func (s *Service) checksOfClosedDay(tx repository.Tx, outlet string) ([]string, error) {
	rows, err := tx.Rows(repository.RowQuery{Entity: "check", Outlet: outlet, Tag: "open"})
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.ID)
	}
	return out, nil
}

func intOf(v any) int64 { n, _ := v.(int64); return n }

// remaining is the quantity of a line not covered by cancels.
func remaining(line repository.Row, cancels []repository.Row) int64 {
	q := intOf(line.Fields["qty_milli"])
	for _, c := range cancels {
		q -= intOf(c.Fields["qty_milli"])
	}
	if q < 0 {
		q = 0
	}
	return q
}

func (s *Service) evalCheck(tx repository.Tx, outlet, checkID string, op *Op, arrived time.Time) error {
	chk, ok, err := tx.GetRow("check", checkID)
	if err != nil {
		return err
	}
	var found []finding
	universe := map[fkey]bool{}
	lineIDs := []string{}
	var lines []repository.Row
	if ok {
		lines, err = tx.Rows(repository.RowQuery{Entity: "order_line", Outlet: outlet, Parents: []string{checkID}})
		if err != nil {
			return err
		}
	}
	for _, l := range lines {
		lineIDs = append(lineIDs, l.ID)
	}
	cancelsByLine := map[string][]repository.Row{}
	if len(lineIDs) > 0 {
		cs, err := tx.Rows(repository.RowQuery{Entity: "order_line_cancel", Outlet: outlet, Parents: lineIDs})
		if err != nil {
			return err
		}
		for _, c := range cs {
			cancelsByLine[c.Parent] = append(cancelsByLine[c.Parent], c)
		}
	}
	// existing open findings about this check, so they can close
	for _, kind := range []string{KindOrphanLine, KindUnresolvedAtClose, KindPresentedOpen} {
		xs, err := tx.Exceptions(repository.ExceptionQuery{Outlet: outlet, Status: "open", Kind: kind})
		if err != nil {
			return err
		}
		for _, x := range xs {
			if c, _ := x.Params["check_id"].(string); c == checkID {
				universe[fkey{x.Kind, x.SubjectKind, x.SubjectID, x.BusinessDate}] = true
			}
		}
	}
	if ok {
		status := str(chk.Fields["status"])
		cancelled := false
		if cc, err := tx.Rows(repository.RowQuery{Entity: "check_cancel", Outlet: outlet, Parents: []string{checkID}}); err == nil && len(cc) > 0 {
			cancelled = true
		}
		date := chk.BusinessDate
		billedAt := ""
		if status == "billed" || status == "closed_internal" {
			billedAt = chk.Meta.RankStamp["status"][status]
		}
		cs, closed, err := closeSeq(tx, outlet, date)
		if err != nil {
			return err
		}
		isOpen := status == "open" && !cancelled
		for _, l := range lines {
			if remaining(l, cancelsByLine[l.ID]) == 0 {
				continue
			}
			p := map[string]any{"check_id": checkID}
			if billedAt != "" && l.Meta.Stamps["check_id"] > billedAt {
				f := finding{KindOrphanLine, "warning", "order_line", l.ID, date, p}
				found = append(found, f)
				universe[f.key()] = true
			}
			if isOpen && closed && l.Meta.ApplySeq > cs {
				f := finding{KindUnresolvedAtClose, "high", "order_line", l.ID, date, p}
				found = append(found, f)
				universe[f.key()] = true
			}
		}
		if isOpen && closed {
			pres, err := tx.Rows(repository.RowQuery{Entity: "check_present", Outlet: outlet, Parents: []string{checkID}})
			if err != nil {
				return err
			}
			late := false
			for _, p := range pres {
				if p.Meta.ApplySeq > cs {
					late = true
				}
			}
			if late {
				f := finding{KindPresentedOpen, "warning", "check", checkID, date, map[string]any{"check_id": checkID, "present_count": int64(len(pres))}}
				found = append(found, f)
				universe[f.key()] = true
			}
		}
	}
	return s.reconcile(tx, outlet, universe, found, op, arrived)
}

func (s *Service) staleAfter(tx repository.Tx, outlet string) time.Duration {
	rows, err := tx.Rows(repository.RowQuery{Entity: "outlet_settings", Outlet: outlet})
	if err == nil {
		for _, r := range rows {
			if n := intOf(r.Fields["kot_stale_after_seconds"]); n > 0 {
				return time.Duration(n) * time.Second
			}
		}
	}
	return s.Cfg.KotStaleAfter
}

func (s *Service) evalKot(tx repository.Tx, outlet, kotID string, op *Op, arrived time.Time) error {
	universe := map[fkey]bool{}
	var found []finding
	xs, err := tx.Exceptions(repository.ExceptionQuery{Outlet: outlet, Kind: KindLateTicket, SubjectKind: "kot", SubjectIDs: []string{kotID}})
	if err != nil {
		return err
	}
	for _, x := range xs {
		universe[fkey{x.Kind, x.SubjectKind, x.SubjectID, x.BusinessDate}] = true
	}
	kot, ok, err := tx.GetRow("kot", kotID)
	if err != nil {
		return err
	}
	if ok && str(kot.Fields["kind"]) == "original" {
		prints, _ := tx.Rows(repository.RowQuery{Entity: "kot_print", Outlet: outlet, Parents: []string{kotID}})
		hand, _ := tx.Rows(repository.RowQuery{Entity: "kot_handover", Outlet: outlet, Parents: []string{kotID}})
		sent, e1 := time.Parse(time.RFC3339Nano, kot.Meta.CreatedAt)
		arr, e2 := time.Parse(time.RFC3339Nano, kot.Meta.Arrived)
		stale := e1 == nil && e2 == nil && arr.Sub(sent) > s.staleAfter(tx, outlet)
		if (stale || len(hand) > 0) && len(prints) == 0 {
			age := int64(0)
			if e1 == nil && e2 == nil {
				age = int64(arr.Sub(sent).Seconds())
			}
			f := finding{KindLateTicket, "info", "kot", kotID, kot.BusinessDate,
				map[string]any{"check_id": str(kot.Fields["check_id"]), "age_seconds": age, "handover": int64(len(hand))}}
			found = append(found, f)
			universe[f.key()] = true
		}
	}
	return s.reconcile(tx, outlet, universe, found, op, arrived)
}

// dateOfLine is the business date of the check a line sits on.
func dateOfLine(tx repository.Tx, outlet, lineID string) (checkID, date string) {
	l, ok, _ := tx.GetRow("order_line", lineID)
	if !ok {
		return "", ""
	}
	c, ok, _ := tx.GetRow("check", l.Parent)
	if !ok {
		return l.Parent, ""
	}
	return c.ID, c.BusinessDate
}

func (s *Service) evalCancel(tx repository.Tx, outlet, cancelID string, op *Op, arrived time.Time) error {
	universe := map[fkey]bool{}
	var found []finding
	xs, err := tx.Exceptions(repository.ExceptionQuery{Outlet: outlet, Status: "open", Kind: KindGraceStarted, SubjectIDs: []string{cancelID}})
	if err != nil {
		return err
	}
	for _, x := range xs {
		universe[fkey{x.Kind, x.SubjectKind, x.SubjectID, x.BusinessDate}] = true
	}
	allowance := map[fkey]bool{}
	c, ok, err := tx.GetRow("order_line_cancel", cancelID)
	if err != nil {
		return err
	}
	if ok && str(c.Fields["kind"]) == "grace_cancel" {
		checkID, date := dateOfLine(tx, outlet, c.Parent)
		if ks := str(c.Fields["kitchen_status_at_cancel"]); ks == "preparing" || ks == "ready" || ks == "served" {
			f := finding{KindGraceStarted, "warning", "order_line_cancel", cancelID, date,
				map[string]any{"order_line_id": c.Parent, "check_id": checkID, "kitchen_status": ks}}
			found = append(found, f)
			universe[f.key()] = true
		}
		member := c.Meta.CreatedBy
		if member != "" && date != "" {
			n, err := s.graceCount(tx, outlet, member, date)
			if err != nil {
				return err
			}
			k := fkey{KindAllowance, "member", member, date}
			allowance[k] = true
			if n > s.Cfg.GraceCancelAllowance {
				found = append(found, finding{KindAllowance, "warning", "member", member, date,
					map[string]any{"count": int64(n), "allowance": int64(s.Cfg.GraceCancelAllowance)}})
				universe[k] = true
			}
		}
	}
	for k := range allowance {
		universe[k] = true
	}
	return s.reconcile(tx, outlet, universe, found, op, arrived)
}

func (s *Service) graceCount(tx repository.Tx, outlet, member, date string) (int, error) {
	cs, err := tx.Rows(repository.RowQuery{Entity: "order_line_cancel", Outlet: outlet})
	if err != nil {
		return 0, err
	}
	n := 0
	for _, c := range cs {
		if str(c.Fields["kind"]) != "grace_cancel" || c.Meta.CreatedBy != member {
			continue
		}
		if _, d := dateOfLine(tx, outlet, c.Parent); d == date {
			n++
		}
	}
	return n, nil
}

// currentTables maps each open seating of an outlet to the table it sits on now: the target of its
// latest move, else the table it was opened on. A merged-away seating is not counted.
func currentTables(tx repository.Tx, outlet string) (map[string][]string, error) {
	seatings, err := tx.Rows(repository.RowQuery{Entity: "seating", Outlet: outlet})
	if err != nil {
		return nil, err
	}
	moves, err := tx.Rows(repository.RowQuery{Entity: "seating_move", Outlet: outlet})
	if err != nil {
		return nil, err
	}
	merges, err := tx.Rows(repository.RowQuery{Entity: "seating_merge", Outlet: outlet})
	if err != nil {
		return nil, err
	}
	absorbed := map[string]bool{}
	for _, m := range merges {
		absorbed[str(m.Fields["absorbed_seating_id"])] = true
	}
	latest := map[string]repository.Row{}
	for _, m := range moves {
		cur, ok := latest[m.Parent]
		if !ok || m.Meta.CreatedAt > cur.Meta.CreatedAt || (m.Meta.CreatedAt == cur.Meta.CreatedAt && m.ID > cur.ID) {
			latest[m.Parent] = m
		}
	}
	by := map[string][]string{}
	for _, st := range seatings {
		if str(st.Fields["status"]) != "open" || absorbed[st.ID] {
			continue
		}
		t := str(st.Fields["table_id"])
		if m, ok := latest[st.ID]; ok {
			t = str(m.Fields["to_table_id"])
		}
		by[t] = append(by[t], st.ID)
	}
	return by, nil
}

func (s *Service) evalSeatings(tx repository.Tx, outlet string, op *Op, arrived time.Time) error {
	by, err := currentTables(tx, outlet)
	if err != nil {
		return err
	}
	universe := map[fkey]bool{}
	var found []finding
	xs, err := tx.Exceptions(repository.ExceptionQuery{Outlet: outlet, Status: "open", Kind: KindTwoSeatings})
	if err != nil {
		return err
	}
	for _, x := range xs {
		universe[fkey{x.Kind, x.SubjectKind, x.SubjectID, x.BusinessDate}] = true
	}
	for table, ss := range by {
		if len(ss) > 1 {
			sort.Strings(ss)
			f := finding{KindTwoSeatings, "warning", "dining_table", table, "", map[string]any{"seatings": int64(len(ss)), "first_seating_id": ss[0]}}
			found = append(found, f)
			universe[f.key()] = true
		}
	}
	return s.reconcile(tx, outlet, universe, found, op, arrived)
}

// reconcile opens, updates and closes the exceptions of the universe so they equal found.
func (s *Service) reconcile(tx repository.Tx, outlet string, universe map[fkey]bool, found []finding, op *Op, arrived time.Time) error {
	byKey := map[fkey]finding{}
	for _, f := range found {
		byKey[f.key()] = f
	}
	keys := make([]fkey, 0, len(universe))
	for k := range universe {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]
		return a.Kind+a.SubjectID+a.Date < b.Kind+b.SubjectID+b.Date
	})
	for _, k := range keys {
		id := detID(tx.Tenant(), outlet, k.Kind, k.SubjectKind, k.SubjectID, k.Date)
		cur, exists, err := tx.GetException(id)
		if err != nil {
			return err
		}
		f, want := byKey[k]
		switch {
		case want && (!exists || cur.Status != "open"):
			x := repository.Exception{ID: id, OutletID: outlet, Kind: f.Kind, Severity: f.Severity, Status: "open", SubjectKind: f.SubjectKind,
				SubjectID: f.SubjectID, BusinessDate: f.BusinessDate, DetectedAt: arrived.UTC(), DetailKey: "exception." + f.Kind, Params: f.Params}
			if err := tx.PutException(x); err != nil {
				return err
			}
			if err := s.publishOpened(tx, x, op, exists); err != nil {
				return err
			}
		case want:
			cur.Params = f.Params
			if err := tx.PutException(cur); err != nil {
				return err
			}
		case exists && cur.Status == "open":
			t := arrived.UTC()
			cur.Status, cur.ResolvedAt, cur.ResolvedBy = "resolved", &t, op.Ref.String()
			if err := tx.PutException(cur); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Service) publishOpened(tx repository.Tx, x repository.Exception, op *Op, reopened bool) error {
	data := map[string]any{"exception_id": x.ID, "kind": x.Kind, "severity": x.Severity, "subject_kind": x.SubjectKind, "subject_id": x.SubjectID}
	if x.BusinessDate != "" {
		data["business_date"] = x.BusinessDate
	}
	b, err := jsonMarshal(data)
	if err != nil {
		return err
	}
	seq := int64(1)
	if reopened {
		seq = s.Clock.Now().UnixMilli()
	}
	return tx.Publish(events.Envelope{ID: s.NewID(), Type: "orders.exception.opened", V: 1, Source: "orders", TenantID: ids.ID(tx.Tenant()),
		OutletID: ids.ID(x.OutletID), Key: x.ID, KeySeq: seq, OccurredAt: x.DetectedAt, Actor: op.Actor, Data: b})
}
