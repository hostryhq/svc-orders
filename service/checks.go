package service

import (
	"context"
	"fmt"
	"math"
	"sort"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-common/clock"
	"github.com/hostryhq/go-common/money"
	"github.com/hostryhq/go-common/tenancy"
	"github.com/hostryhq/svc-orders/repository"
)

// CheckFilter is the filter of ListChecks.
type CheckFilter struct {
	Status, OrderType, SeatingID string
	Cursor                       string
	Limit                        int
	OutletHeader                 string
}

var kitchenOrder = []string{"new", "preparing", "ready", "served"}

type checkParts struct {
	lines     map[string][]repository.Row // check id -> lines
	cancels   map[string][]repository.Row // line id -> cancels
	presents  map[string]int              // check id -> highest present_count
	cancelled map[string]bool             // check id -> cancelled by check_cancel
	kitchen   map[string]string           // line id -> furthest kitchen status
}

func (s *Service) loadParts(tx repository.Tx, outlet string, checks []repository.Row) (*checkParts, error) {
	p := &checkParts{lines: map[string][]repository.Row{}, cancels: map[string][]repository.Row{}, presents: map[string]int{},
		cancelled: map[string]bool{}, kitchen: map[string]string{}}
	if len(checks) == 0 {
		return p, nil
	}
	var checkIDs []string
	for _, c := range checks {
		checkIDs = append(checkIDs, c.ID)
	}
	lines, err := tx.Rows(repository.RowQuery{Entity: "order_line", Outlet: outlet, Parents: checkIDs})
	if err != nil {
		return nil, err
	}
	lineIDs := make([]string, 0, len(lines))
	want := map[string]bool{}
	for _, l := range lines {
		p.lines[l.Parent] = append(p.lines[l.Parent], l)
		lineIDs = append(lineIDs, l.ID)
		want[l.ID] = true
	}
	if len(lineIDs) > 0 {
		cs, err := tx.Rows(repository.RowQuery{Entity: "order_line_cancel", Outlet: outlet, Parents: lineIDs})
		if err != nil {
			return nil, err
		}
		for _, c := range cs {
			p.cancels[c.Parent] = append(p.cancels[c.Parent], c)
		}
		kls, err := tx.Rows(repository.RowQuery{Entity: "kot_line", Outlet: outlet})
		if err != nil {
			return nil, err
		}
		for _, k := range kls {
			lid := str(k.Fields["order_line_id"])
			if !want[lid] {
				continue
			}
			if indexIn(kitchenOrder, str(k.Fields["status"])) > indexIn(kitchenOrder, p.kitchen[lid]) {
				p.kitchen[lid] = str(k.Fields["status"])
			}
		}
	}
	pr, err := tx.Rows(repository.RowQuery{Entity: "check_present", Outlet: outlet, Parents: checkIDs})
	if err != nil {
		return nil, err
	}
	for _, r := range pr {
		if n := int(intOf(r.Fields["present_count"])); n > p.presents[r.Parent] {
			p.presents[r.Parent] = n
		}
	}
	cc, err := tx.Rows(repository.RowQuery{Entity: "check_cancel", Outlet: outlet, Parents: checkIDs})
	if err != nil {
		return nil, err
	}
	for _, r := range cc {
		p.cancelled[r.Parent] = true
	}
	return p, nil
}

func (p *checkParts) liveLines(checkID string) (live int, total int64) {
	for _, l := range p.lines[checkID] {
		q := remaining(l, p.cancels[l.ID])
		if q == 0 {
			continue
		}
		live++
		total += lineValue(l, q)
	}
	return
}

func (p *checkParts) view(c repository.Row) Check {
	live, total := p.liveLines(c.ID)
	return Check{ID: c.ID, OutletID: c.OutletID, SeatingID: str(c.Fields["seating_id"]), Label: str(c.Fields["label"]),
		OrderType: str(c.Fields["order_type"]), Status: str(c.Fields["status"]), Cancelled: p.cancelled[c.ID], PresentCount: p.presents[c.ID],
		BillID: str(c.Fields["bill_id"]), TokenNo: intOf(c.Fields["token_no"]), CreatedAt: ts(c.Meta.CreatedAt), LineCount: live, ListTotal: money.New(total)}
}

// ListChecks lists checks of the caller's outlets: open first, then by creation.
func (s *Service) ListChecks(ctx context.Context, f CheckFilter) (CheckPage, error) {
	tc, err := tenancy.Require(ctx)
	if err != nil {
		return CheckPage{}, err
	}
	outlets, err := scopeOutlets(ctx, "order.edit", f.OutletHeader)
	if err != nil {
		return CheckPage{}, err
	}
	var out CheckPage
	err = s.Repo.InTx(ctx, string(tc.TenantID), func(tx repository.Tx) error {
		var rows []repository.Row
		for _, o := range outlets {
			rs, err := tx.Rows(repository.RowQuery{Entity: "check", Outlet: o, Tag: f.Status})
			if err != nil {
				return err
			}
			rows = append(rows, rs...)
		}
		cancelled := map[string]bool{}
		for _, o := range outlets {
			cc, err := tx.Rows(repository.RowQuery{Entity: "check_cancel", Outlet: o})
			if err != nil {
				return err
			}
			for _, c := range cc {
				cancelled[c.Parent] = true
			}
		}
		var keep []repository.Row
		for _, r := range rows {
			if (f.OrderType != "" && str(r.Fields["order_type"]) != f.OrderType) || (f.SeatingID != "" && str(r.Fields["seating_id"]) != f.SeatingID) {
				continue
			}
			keep = append(keep, r)
		}
		key := func(r repository.Row) string {
			rank := "1"
			if str(r.Fields["status"]) == "open" && !cancelled[r.ID] {
				rank = "0"
			}
			return rank + "|" + r.Meta.CreatedAt + "|" + r.ID
		}
		pg, next, err := page(keep, key, f.Cursor, f.Limit)
		if err != nil {
			return err
		}
		out.NextCursor = next
		out.Items = []Check{}
		for _, o := range outlets {
			var mine []repository.Row
			for _, r := range pg {
				if o == "" || r.OutletID == o {
					mine = append(mine, r)
				}
			}
			parts, err := s.loadParts(tx, o, mine)
			if err != nil {
				return err
			}
			for _, r := range mine {
				out.Items = append(out.Items, parts.view(r))
			}
		}
		sort.SliceStable(out.Items, func(i, j int) bool {
			return key2(out.Items[i]) < key2(out.Items[j])
		})
		return nil
	})
	return out, err
}

func key2(c Check) string {
	rank := "1"
	if c.Status == "open" && !c.Cancelled {
		rank = "0"
	}
	return rank + "|" + c.CreatedAt + "|" + c.ID
}

// GetCheck returns a check with its lines. A check outside the caller's tenant or outlet scope is 404.
func (s *Service) GetCheck(ctx context.Context, id string) (CheckDetail, error) {
	tc, err := tenancy.Require(ctx)
	if err != nil {
		return CheckDetail{}, err
	}
	var out CheckDetail
	err = s.Repo.InTx(ctx, string(tc.TenantID), func(tx repository.Tx) error {
		c, ok, err := tx.GetRow("check", id)
		if err != nil {
			return err
		}
		if !ok {
			return notFound()
		}
		if err := auth.Require(ctx, "order.edit", c.OutletID); err != nil {
			return err
		}
		parts, err := s.loadParts(tx, c.OutletID, []repository.Row{c})
		if err != nil {
			return err
		}
		out.Check = parts.view(c)
		out.Lines = []CheckLine{}
		ls := parts.lines[c.ID]
		sort.Slice(ls, func(i, j int) bool { return ls[i].Meta.CreatedAt+ls[i].ID < ls[j].Meta.CreatedAt+ls[j].ID })
		for _, l := range ls {
			q := remaining(l, parts.cancels[l.ID])
			state := str(l.Fields["state"])
			if q == 0 {
				state, q = "cancelled", intOf(l.Fields["qty_milli"])
			}
			if q < 1 {
				q = 1
			}
			out.Lines = append(out.Lines, CheckLine{OrderLineID: l.ID, ItemID: str(l.Fields["item_id"]), Name: str(l.Fields["name"]), QtyMilli: q,
				UnitPrice: money.New(intOf(l.Fields["unit_price_paise"])), ModifierDelta: money.New(intOf(l.Fields["modifier_delta_paise"])),
				TaxClass: str(l.Fields["tax_class"]), State: state, KitchenStatus: parts.kitchen[l.ID]})
		}
		return nil
	})
	return out, err
}

func excView(x repository.Exception) Exception {
	v := Exception{ID: x.ID, Kind: x.Kind, Severity: x.Severity, Status: x.Status, SubjectKind: x.SubjectKind, SubjectID: x.SubjectID,
		OutletID: x.OutletID, BusinessDate: x.BusinessDate, DetectedAt: tsTime(x.DetectedAt), ResolvedBy: x.ResolvedBy, DetailKey: x.DetailKey, Params: x.Params}
	if x.ResolvedAt != nil {
		v.ResolvedAt = tsTime(*x.ResolvedAt)
	}
	return v
}

// ExceptionFilter is the filter of ListExceptions.
type ExceptionFilter struct {
	Status, Kind, Cursor, OutletHeader string
	Limit                              int
}

// ListExceptions lists detector findings of the caller's outlets, newest detection first.
func (s *Service) ListExceptions(ctx context.Context, f ExceptionFilter) (ExceptionPage, error) {
	tc, err := tenancy.Require(ctx)
	if err != nil {
		return ExceptionPage{}, err
	}
	outlets, err := scopeOutlets(ctx, "exceptions.view", f.OutletHeader)
	if err != nil {
		return ExceptionPage{}, err
	}
	out := ExceptionPage{Items: []Exception{}}
	err = s.Repo.InTx(ctx, string(tc.TenantID), func(tx repository.Tx) error {
		var all []repository.Exception
		for _, o := range outlets {
			xs, err := tx.Exceptions(repository.ExceptionQuery{Outlet: o, Status: f.Status, Kind: f.Kind})
			if err != nil {
				return err
			}
			all = append(all, xs...)
		}
		key := func(x repository.Exception) string {
			return fmt.Sprintf("%020d", int64(math.MaxInt64)-x.DetectedAt.UnixNano()) + "|" + x.ID
		}
		pg, next, err := page(all, key, f.Cursor, f.Limit)
		if err != nil {
			return err
		}
		out.NextCursor = next
		for _, x := range pg {
			out.Items = append(out.Items, excView(x))
		}
		return nil
	})
	return out, err
}

// GetException returns one exception; another tenant's or an out-of-scope outlet's is 404.
func (s *Service) GetException(ctx context.Context, id string) (Exception, error) {
	tc, err := tenancy.Require(ctx)
	if err != nil {
		return Exception{}, err
	}
	var out Exception
	err = s.Repo.InTx(ctx, string(tc.TenantID), func(tx repository.Tx) error {
		x, ok, err := tx.GetException(id)
		if err != nil {
			return err
		}
		if !ok {
			return notFound()
		}
		if err := auth.Require(ctx, "exceptions.view", x.OutletID); err != nil {
			return err
		}
		out = excView(x)
		return nil
	})
	return out, err
}

// OpenCheckCount counts the open checks of an outlet for billing (internal, service token). An
// outlet with no orders data counts 0 (docs/notes.md N6).
func (s *Service) OpenCheckCount(ctx context.Context, outlet string) (OpenCheckCount, error) {
	tc, err := tenancy.Require(ctx)
	if err != nil {
		return OpenCheckCount{}, err
	}
	out := OpenCheckCount{OutletID: outlet, AsOf: tsTime(s.Clock.Now())}
	err = s.Repo.InTx(ctx, string(tc.TenantID), func(tx repository.Tx) error {
		rows, err := tx.Rows(repository.RowQuery{Entity: "check", Outlet: outlet, Tag: "open"})
		if err != nil {
			return err
		}
		cc, err := tx.Rows(repository.RowQuery{Entity: "check_cancel", Outlet: outlet})
		if err != nil {
			return err
		}
		cancelled := map[string]bool{}
		for _, c := range cc {
			cancelled[c.Parent] = true
		}
		for _, r := range rows {
			if !cancelled[r.ID] {
				out.OpenChecks++
			}
		}
		return nil
	})
	return out, err
}

var _ = clock.System
