package service

import (
	"sort"
	"strconv"
	"strings"
	"time"

	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/repository"
)

// Op and Mut are the parsed sync op and its mutation as go-common/sync reads them (gosync.ParseOp is
// the one parser, ADR-0014 A2.1).
type (
	Op  = gosync.Op
	Mut = gosync.Mut
)

// outletOf is the outlet of an op: the envelope's, else the one of an "outlet:<id>" stream.
func outletOf(op *Op) string {
	if op.OutletID != "" {
		return op.OutletID.String()
	}
	if id, ok := strings.CutPrefix(op.Ref.Stream, "outlet:"); ok {
		return id
	}
	return ""
}

// Apply projects one op inside tx with the go-common/sync engine: idempotent on op identity,
// void-aware, order-free. Entities orders neither owns nor replicates are ignored (invariant 8). The
// engine records candidates; the rows written here are their resolution, plus the facts of the op
// (arrival order, device time, member) that the detectors and reports need.
func (s *Service) Apply(tx repository.Tx, op *Op, arrived time.Time) error {
	ref := op.Ref.String()
	if done, err := tx.OpApplied(ref); err != nil || done {
		return err
	}
	switch op.Status {
	case "", "accepted", "flagged":
	default:
		return nil // quarantined, voided, placeholder: nothing to project
	}
	n, err := tx.NextApplySeq()
	if err != nil {
		return err
	}
	outlet := outletOf(op)
	if err := tx.MarkApplied(ref, repository.OpMeta{ApplySeq: n, At: op.At.UTC().Format(time.RFC3339Nano), Actor: op.Actor, OutletID: outlet, Arrived: arrived}); err != nil {
		return err
	}
	eng := &gosync.Engine{Reg: mustRegistry(), Service: ServiceName, Store: tx.SyncStore()}
	res, err := eng.Apply(tx.Context(), op)
	if err != nil {
		return err
	}
	touched := map[repository.RowKey]bool{}
	writes := piiWrites(op)
	for _, row := range res.Rows {
		if err := s.rederive(tx, row, outlet, writes[repository.RowKey{Entity: row.Key.Entity, ID: row.Key.ID}]); err != nil {
			return err
		}
		touched[repository.RowKey{Entity: row.Key.Entity, ID: row.Key.ID}] = true
	}
	// A set whose every field is personal resolves no row (F25): record its commitments anyway, and refresh the row if it
	// exists. If the row arrives later, rederive reads the recorded commitments.
	for k, ws := range writes {
		if touched[k] {
			continue
		}
		if err := tx.AddPIIWrites(k.Entity, k.ID, ws); err != nil {
			return err
		}
		r, ok, err := tx.GetRow(k.Entity, k.ID)
		if err != nil {
			return err
		}
		if !ok {
			continue
		}
		if err := s.derivePII(tx, mustRegistry().Entities[k.Entity], &r); err != nil {
			return err
		}
		if err := tx.PutRow(r); err != nil {
			return err
		}
	}
	return s.detect(tx, op, touched, arrived)
}

// rederive writes the resolved row with the meta derived from its live candidates.
func (s *Service) rederive(tx repository.Tx, row gosync.Row, outlet string, newPII map[string]repository.PIIWrite) error {
	if len(newPII) > 0 { // a commitment may arrive before its row
		if err := tx.AddPIIWrites(row.Key.Entity, row.Key.ID, newPII); err != nil {
			return err
		}
	}
	if !row.Exists {
		return tx.DeleteRow(row.Key.Entity, row.Key.ID)
	}
	cands, err := tx.SyncStore().Candidates(tx.Context(), row.Key)
	if err != nil {
		return err
	}
	refs := make([]gosync.OpRef, 0, len(cands))
	for _, c := range cands {
		refs = append(refs, c.Ref)
	}
	voided, err := tx.SyncStore().Voided(tx.Context(), row.Key.Tenant, refs)
	if err != nil {
		return err
	}
	live := cands[:0:0]
	texts := make([]string, 0, len(cands))
	for _, c := range cands {
		if !voided[c.Ref] {
			live = append(live, c)
			texts = append(texts, c.Ref.String())
		}
	}
	sort.Slice(live, func(i, j int) bool { return live[i].Stamp().Compare(live[j].Stamp()) < 0 })
	metas, err := tx.OpMetas(texts)
	if err != nil {
		return err
	}
	ent := mustRegistry().Entities[row.Key.Entity]
	r := repository.Row{Entity: row.Key.Entity, ID: row.Key.ID, Fields: row.Fields, Gone: row.Deleted}
	r.Meta = deriveMeta(ent, row, live, metas)
	if err := s.derivePII(tx, ent, &r); err != nil {
		return err
	}
	r.OutletID = outlet
	for _, c := range live { // the outlet of the earliest insert
		if c.Kind == gosync.KindInsert {
			if m, ok := metas[c.Ref.String()]; ok && m.OutletID != "" {
				r.OutletID = m.OutletID
			}
			break
		}
	}
	if pf := parentField[row.Key.Entity]; pf != "" {
		r.Parent = str(r.Fields[pf])
	}
	if _, ok := ent.Fields["business_date"]; ok {
		r.BusinessDate = str(r.Fields["business_date"])
	}
	if tf := tagField[row.Key.Entity]; tf != "" {
		r.Tag = str(r.Fields[tf])
	}
	return tx.PutRow(r)
}

var parentField = map[string]string{
	"order_line": "check_id", "kot": "check_id", "kot_line": "kot_id", "kot_print": "kot_id", "kot_handover": "kot_id",
	"order_line_cancel": "order_line_id", "order_line_modifier": "order_line_id", "check_cancel": "check_id",
	"check_present": "check_id", "check_present_change": "check_id", "bill_request": "seating_id",
	"seating_move": "seating_id", "seating_merge": "kept_seating_id", "day_close": "business_day_id",
}

var tagField = map[string]string{"check": "status"}

func str(v any) string { s, _ := v.(string); return s }

// stampText orders candidates as text (hlc, origin, seq, position), so meta stamps compare as strings.
func stampText(c gosync.Candidate) string {
	return c.HLC + "|" + c.Ref.Origin + "|" + pad(c.Ref.Seq, 12) + "|" + pad(int64(c.Idx), 4)
}

func pad(n int64, w int) string {
	s := strconv.FormatInt(n, 10)
	if len(s) >= w {
		return s
	}
	return strings.Repeat("0", w-len(s)) + s
}

// deriveMeta is what the projection knows about a row beyond its resolved fields, from the live
// candidates (sorted by stamp) and the facts of their ops.
func deriveMeta(ent *gosync.Entity, row gosync.Row, live []gosync.Candidate, metas map[string]repository.OpMeta) repository.Meta {
	m := repository.Meta{Stamps: map[string]string{}, RankAt: map[string]map[string]string{}, RankStamp: map[string]map[string]string{}}
	for _, c := range live {
		if c.Kind != gosync.KindInsert {
			continue
		}
		om := metas[c.Ref.String()]
		m.CreatedAt, m.CreatedBy, m.Origin, m.ApplySeq = om.At, om.Actor, c.Ref.Origin, om.ApplySeq
		if !om.Arrived.IsZero() {
			m.Arrived = om.Arrived.UTC().Format(time.RFC3339Nano)
		}
		break
	}
	for _, name := range ent.FieldNames() {
		f := ent.Fields[name]
		if f.Policy == gosync.PolicyDelta {
			continue
		}
		if f.Policy == gosync.PolicyRank {
			for _, c := range live {
				v, ok := c.F[name].(string)
				if !ok || !valueKind(c.Kind) {
					continue
				}
				if m.RankAt[name] == nil {
					m.RankAt[name], m.RankStamp[name] = map[string]string{}, map[string]string{}
				}
				at := metas[c.Ref.String()].At
				if old, ok := m.RankAt[name][v]; !ok || at < old {
					m.RankAt[name][v] = at
				}
				if old, ok := m.RankStamp[name][v]; !ok || stampText(c) < old {
					m.RankStamp[name][v] = stampText(c)
				}
			}
		}
		if _, ok := row.Fields[name]; !ok {
			continue
		}
		// the stamp of the candidate that decided the field's value
		var win *gosync.Candidate
		for i := range live {
			c := &live[i]
			if v, has := c.F[name]; !has || v == nil || !valueKind(c.Kind) {
				continue
			}
			switch f.Policy {
			case gosync.PolicyImmutable:
				if win == nil {
					win = c
				}
			case gosync.PolicyRank:
				if win == nil || indexIn(f.Order, str(c.F[name])) >= indexIn(f.Order, str(win.F[name])) {
					win = c
				}
			default:
				win = c // lww: the highest stamp
			}
		}
		if win != nil {
			m.Stamps[name] = stampText(*win)
		}
	}
	return m
}

func valueKind(k string) bool {
	return k == gosync.KindInsert || k == gosync.KindSet || k == gosync.KindRank
}

func indexIn(order []string, v string) int {
	for i, o := range order {
		if o == v {
			return i
		}
	}
	return -1
}

// piiWrites are the verified commitments the op carries, per owned row and pii field, with the stamp the field's write has
// (the engine keeps no candidate for a pii field, so the winner is decided here). A row's pii is written by its insert or a
// set mutation of the same op.
func piiWrites(op *Op) map[repository.RowKey]map[string]repository.PIIWrite {
	out := map[repository.RowKey]map[string]repository.PIIWrite{}
	reg := mustRegistry()
	for entity, rows := range op.PIICommits {
		ent := reg.Entities[entity]
		if ent == nil || reg.RoleOf(ServiceName, entity) != gosync.RoleOwner {
			continue
		}
		for id, fields := range rows {
			insert, set := false, false
			for _, m := range op.Muts {
				if m.E == entity && m.ID == id {
					insert = insert || m.K == gosync.KindInsert
					set = set || m.K == gosync.KindSet
				}
			}
			for name, c := range fields {
				f := ent.Fields[name]
				if f == nil || !f.PII || c == "" || !(insert || set && f.Policy != gosync.PolicyImmutable) {
					continue
				}
				k := repository.RowKey{Entity: entity, ID: id}
				if out[k] == nil {
					out[k] = map[string]repository.PIIWrite{}
				}
				out[k][name] = repository.PIIWrite{Ref: op.Ref.String(), HLC: op.HLC, Origin: op.Ref.Origin, Seq: op.Ref.Seq, Insert: insert, Commit: c}
			}
		}
	}
	return out
}

// derivePII picks the winner
// of each pii field from every one recorded: the lowest-stamped insert for an immutable field, the highest-stamped write
// otherwise, ignoring voided ops.
func (s *Service) derivePII(tx repository.Tx, ent *gosync.Entity, r *repository.Row) error {
	all, err := tx.PIIWrites(r.Entity, r.ID)
	if err != nil || len(all) == 0 {
		return err
	}
	refOf := func(w repository.PIIWrite) gosync.OpRef {
		return gosync.OpRef{Origin: w.Origin, Stream: strings.SplitN(w.Ref, "|", 2)[0], Seq: w.Seq}
	}
	var refs []gosync.OpRef
	for _, ws := range all {
		for _, w := range ws {
			refs = append(refs, refOf(w))
		}
	}
	voided, err := tx.SyncStore().Voided(tx.Context(), tx.Tenant(), refs)
	if err != nil {
		return err
	}
	r.Meta.PIIC = map[string]string{}
	for f, ws := range all {
		imm := ent.Fields[f] != nil && ent.Fields[f].Policy == gosync.PolicyImmutable
		var win *repository.PIIWrite
		for i := range ws {
			w := &ws[i]
			if voided[refOf(*w)] || (imm && !w.Insert) {
				continue
			}
			if win == nil {
				win = w
				continue
			}
			cmp := (gosync.Stamp{HLC: w.HLC, Origin: w.Origin, Seq: w.Seq}).Compare(gosync.Stamp{HLC: win.HLC, Origin: win.Origin, Seq: win.Seq})
			if (imm && cmp < 0) || (!imm && cmp > 0) {
				win = w
			}
		}
		if win != nil {
			r.Meta.PIIC[f] = win.Commit
		}
	}
	return nil
}
