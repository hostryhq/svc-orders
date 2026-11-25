package service

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/hostryhq/svc-orders/repository"
)

// Mut is one generic mutation of an op (ADR-0002 S4).
type Mut struct {
	E  string         `json:"e"`
	ID string         `json:"id"`
	K  string         `json:"k"`
	F  map[string]any `json:"f"`
}

// Op is an accepted sync op as the bus delivers it (events: evt.sync.op.<namespace>.v1).
type Op struct {
	Origin string
	Stream string
	Seq    int64
	Intent string
	Status string
	HLC    string
	At     string
	Actor  string
	Muts   []Mut
}

// Ref is the identity of the op.
func (o Op) Ref() string { return o.Origin + "/" + o.Stream + "/" + strconv.FormatInt(o.Seq, 10) }

// Outlet is the outlet of an outlet stream ("outlet:<id>"), or "".
func (o Op) Outlet() string {
	if id, ok := strings.CutPrefix(o.Stream, "outlet:"); ok {
		return id
	}
	return ""
}

type opBody struct {
	HLC    string `json:"hlc"`
	Actor  string `json:"actor"`
	At     string `json:"at"`
	Intent string `json:"intent"`
	Muts   []Mut  `json:"muts"`
}

// ParseOp reads the data of a sync op event: origin, stream, seq, intent, status and body, the
// canonical JSON text of the op body (provisional shape, docs/notes.md N1).
func ParseOp(data []byte) (Op, error) {
	var d struct {
		Origin string          `json:"origin"`
		Stream string          `json:"stream"`
		Seq    int64           `json:"seq"`
		Intent string          `json:"intent"`
		Status string          `json:"status"`
		Body   json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(data, &d); err != nil {
		return Op{}, fmt.Errorf("op event: %w", err)
	}
	if d.Origin == "" || d.Stream == "" || d.Seq < 1 {
		return Op{}, fmt.Errorf("op event: origin, stream and seq are required")
	}
	op := Op{Origin: d.Origin, Stream: d.Stream, Seq: d.Seq, Intent: d.Intent, Status: d.Status}
	if len(d.Body) == 0 || string(d.Body) == "null" {
		return op, nil // a slot with no body carries nothing to project
	}
	raw := []byte(d.Body)
	if raw[0] == '"' { // the body travels as a JSON string
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return Op{}, err
		}
		raw = []byte(s)
	}
	var b opBody
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	if err := dec.Decode(&b); err != nil {
		return Op{}, fmt.Errorf("op body: %w", err)
	}
	op.HLC, op.At, op.Actor = b.HLC, b.At, b.Actor
	if op.Intent == "" {
		op.Intent = b.Intent
	}
	for _, m := range b.Muts {
		m.F = normalise(m.F)
		op.Muts = append(op.Muts, m)
	}
	return op, nil
}

// normalise turns json.Number into int64 where integral (money and quantities are integers).
func normalise(f map[string]any) map[string]any {
	for k, v := range f {
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				f[k] = i
			} else {
				delete(f, k) // fractions are not part of the protocol
			}
		}
	}
	return f
}

// Apply projects one op inside tx: idempotent on op identity, void-aware, order-tolerant. Entities
// orders does not project are ignored (one release of forward compatibility, invariant 8).
func (s *Service) Apply(tx repository.Tx, op Op, arrived time.Time) error {
	ref := op.Ref()
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
	if err := tx.MarkApplied(ref, n); err != nil {
		return err
	}
	if voided, err := tx.OpVoided(ref); err != nil || voided {
		return err
	}
	if op.Intent == "sync.void" {
		return s.applyVoids(tx, op, arrived)
	}
	outlet := op.Outlet()
	touched := map[repository.RowKey]bool{}
	for i, m := range op.Muts {
		if _, ok := entityDefs[m.E]; !ok || m.ID == "" {
			continue
		}
		e := repository.Effect{Op: ref, Idx: i, Entity: m.E, ID: m.ID, OutletID: outlet, Kind: m.K, Fields: m.F, HLC: op.HLC,
			Origin: op.Origin, Seq: op.Seq, At: op.At, Actor: op.Actor, ApplySeq: n, Arrived: arrived}
		if err := tx.AddEffect(e); err != nil {
			return err
		}
		touched[repository.RowKey{Entity: m.E, ID: m.ID}] = true
	}
	return s.refresh(tx, op, touched, arrived)
}

func (s *Service) applyVoids(tx repository.Tx, op Op, arrived time.Time) error {
	touched := map[repository.RowKey]bool{}
	for _, m := range op.Muts {
		if m.E != "sync_void" {
			continue
		}
		// id is "<stream>|<origin>|<seq>" (svc-sync contract/README.md)
		p := strings.Split(m.ID, "|")
		if len(p) != 3 {
			continue
		}
		target := p[1] + "/" + p[0] + "/" + p[2]
		if err := tx.MarkVoided(target); err != nil {
			return err
		}
		keys, err := tx.RemoveEffectsOfOp(target)
		if err != nil {
			return err
		}
		for _, k := range keys {
			touched[k] = true
		}
	}
	return s.refresh(tx, op, touched, arrived)
}

// refresh re-derives every touched row and runs the detectors that depend on them.
func (s *Service) refresh(tx repository.Tx, op Op, touched map[repository.RowKey]bool, arrived time.Time) error {
	for k := range touched {
		if err := s.rederive(tx, k, op.Outlet()); err != nil {
			return err
		}
	}
	return s.detect(tx, op, touched, arrived)
}

// rederive recomputes a row from its effects (a pure function of them).
func (s *Service) rederive(tx repository.Tx, k repository.RowKey, outlet string) error {
	effs, err := tx.EffectsOf(k.Entity, k.ID)
	if err != nil {
		return err
	}
	def := entityDefs[k.Entity]
	row, exists := derive(k, def, effs)
	if !exists {
		return tx.DeleteRow(k.Entity, k.ID)
	}
	if row.OutletID == "" {
		row.OutletID = outlet
	}
	return tx.PutRow(row)
}

var parentField = map[string]string{
	"order_line": "check_id", "kot": "check_id", "kot_line": "kot_id", "kot_print": "kot_id", "kot_handover": "kot_id",
	"order_line_cancel": "order_line_id", "order_line_modifier": "order_line_id", "check_cancel": "check_id",
	"check_present": "check_id", "check_present_change": "check_id", "bill_request": "seating_id",
	"seating_move": "seating_id", "seating_merge": "kept_seating_id", "day_close": "business_day_id",
}

var tagField = map[string]string{"check": "status"}

func str(v any) string { s, _ := v.(string); return s }

func rankIndex(order []string, v any) int {
	s := str(v)
	for i, o := range order {
		if o == s {
			return i
		}
	}
	return -1
}

func derive(k repository.RowKey, def EntityDef, effs []repository.Effect) (repository.Row, bool) {
	row := repository.Row{Entity: k.Entity, ID: k.ID, Fields: map[string]any{}}
	var first *repository.Effect
	var tomb *repository.Effect
	for i := range effs {
		e := &effs[i]
		switch e.Kind {
		case "insert":
			if first == nil || e.Stamp() < first.Stamp() {
				first = e
			}
			fallthrough
		case "del", "undel":
			if tomb == nil || e.Stamp() > tomb.Stamp() {
				tomb = e
			}
		}
	}
	if first == nil {
		return row, false
	}
	row.OutletID = first.OutletID
	row.Gone = tomb != nil && tomb.Kind == "del"
	m := repository.Meta{CreatedAt: first.At, CreatedBy: first.Actor, Origin: first.Origin, ApplySeq: first.ApplySeq,
		Arrived: first.Arrived.UTC().Format(time.RFC3339Nano), Stamps: map[string]string{}, RankAt: map[string]map[string]string{}, RankStamp: map[string]map[string]string{}}
	for name, fd := range def.Fields {
		var best *repository.Effect
		var sum int64
		var haveSum bool
		for i := range effs {
			e := &effs[i]
			v, has := e.Fields[name]
			if !has {
				continue
			}
			switch fd.Policy {
			case "delta":
				if e.Kind == "insert" || e.Kind == "delta" {
					n, _ := v.(int64)
					sum += n
					haveSum = true
				}
				continue
			}
			if e.Kind != "insert" && e.Kind != "set" && e.Kind != "rank" {
				continue
			}
			if best == nil {
				best = e
				continue
			}
			switch fd.Policy {
			case "immutable":
				if e.Stamp() < best.Stamp() {
					best = e
				}
			case "rank":
				ri, rb := rankIndex(fd.Order, v), rankIndex(fd.Order, best.Fields[name])
				if ri > rb || (ri == rb && e.Stamp() > best.Stamp()) {
					best = e
				}
			default: // lww, lww_review
				if e.Stamp() > best.Stamp() {
					best = e
				}
			}
		}
		if fd.Policy == "rank" {
			for i := range effs {
				e := &effs[i]
				v, ok := e.Fields[name]
				if !ok || (e.Kind != "insert" && e.Kind != "set" && e.Kind != "rank") {
					continue
				}
				val := str(v)
				if m.RankAt[name] == nil {
					m.RankAt[name], m.RankStamp[name] = map[string]string{}, map[string]string{}
				}
				if old, ok := m.RankAt[name][val]; !ok || e.At < old {
					m.RankAt[name][val] = e.At
				}
				if old, ok := m.RankStamp[name][val]; !ok || e.Stamp() < old {
					m.RankStamp[name][val] = e.Stamp()
				}
			}
		}
		if haveSum {
			row.Fields[name] = sum
		} else if best != nil {
			row.Fields[name] = best.Fields[name]
			m.Stamps[name] = best.Stamp()
		}
	}
	row.Meta = m
	if pf := parentField[k.Entity]; pf != "" {
		row.Parent = str(row.Fields[pf])
	}
	if _, ok := def.Fields["business_date"]; ok {
		row.BusinessDate = str(row.Fields["business_date"])
	}
	if tf := tagField[k.Entity]; tf != "" {
		row.Tag = str(row.Fields[tf])
	}
	return row, true
}
