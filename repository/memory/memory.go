// Package memory is the in-memory repository.Store used by the test double and unit tests.
// A transaction works on a copy of the tenant's state and swaps it in on commit, so an error
// rolls back exactly like the PostgreSQL implementation.
package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/svc-orders/repository"
)

type state struct {
	seq        int64
	applied    map[string]int64
	voided     map[string]bool
	effects    map[repository.RowKey][]repository.Effect
	rows       map[repository.RowKey]repository.Row
	exceptions map[string]repository.Exception
}

func newState() *state {
	return &state{applied: map[string]int64{}, voided: map[string]bool{}, effects: map[repository.RowKey][]repository.Effect{},
		rows: map[repository.RowKey]repository.Row{}, exceptions: map[string]repository.Exception{}}
}

func (s *state) clone() *state {
	c := newState()
	c.seq = s.seq
	for k, v := range s.applied {
		c.applied[k] = v
	}
	for k, v := range s.voided {
		c.voided[k] = v
	}
	for k, v := range s.effects {
		c.effects[k] = append([]repository.Effect(nil), v...)
	}
	for k, v := range s.rows { // rows are replaced, never mutated in place
		c.rows[k] = v
	}
	for k, v := range s.exceptions {
		c.exceptions[k] = v
	}
	return c
}

// Store implements repository.Store.
type Store struct {
	mu      sync.Mutex
	tenants map[string]*state
	events  []events.Envelope
}

// New returns an empty store.
func New() *Store { return &Store{tenants: map[string]*state{}} }

// Ping implements repository.Store.
func (s *Store) Ping(context.Context) error { return nil }

// Events returns the events published (the outbox) in order, for tests.
func (s *Store) Events() []events.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]events.Envelope(nil), s.events...)
}

// Reset drops all state of every tenant and the recorded events.
func (s *Store) Reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tenants = map[string]*state{}
	s.events = nil
}

// InTx implements repository.Store.
func (s *Store) InTx(_ context.Context, tenant string, fn func(repository.Tx) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur := s.tenants[tenant]
	if cur == nil {
		cur = newState()
	}
	t := &tx{tenant: tenant, st: cur.clone()}
	if err := fn(t); err != nil {
		return err
	}
	s.tenants[tenant] = t.st
	s.events = append(s.events, t.out...)
	return nil
}

type tx struct {
	tenant string
	st     *state
	out    []events.Envelope
}

func (t *tx) Tenant() string { return t.tenant }

func (t *tx) NextApplySeq() (int64, error) { t.st.seq++; return t.st.seq, nil }

func (t *tx) OpApplied(ref string) (bool, error) { _, ok := t.st.applied[ref]; return ok, nil }

func (t *tx) MarkApplied(ref string, n int64) error { t.st.applied[ref] = n; return nil }

func (t *tx) OpVoided(ref string) (bool, error) { return t.st.voided[ref], nil }

func (t *tx) MarkVoided(ref string) error { t.st.voided[ref] = true; return nil }

func (t *tx) AddEffect(e repository.Effect) error {
	k := repository.RowKey{Entity: e.Entity, ID: e.ID}
	t.st.effects[k] = append(t.st.effects[k], e)
	return nil
}

func (t *tx) EffectsOf(entity, id string) ([]repository.Effect, error) {
	return append([]repository.Effect(nil), t.st.effects[repository.RowKey{Entity: entity, ID: id}]...), nil
}

func (t *tx) RemoveEffectsOfOp(ref string) ([]repository.RowKey, error) {
	var touched []repository.RowKey
	for k, es := range t.st.effects {
		kept := es[:0:0]
		hit := false
		for _, e := range es {
			if e.Op == ref {
				hit = true
				continue
			}
			kept = append(kept, e)
		}
		if hit {
			touched = append(touched, k)
			t.st.effects[k] = kept
		}
	}
	sort.Slice(touched, func(i, j int) bool {
		if touched[i].Entity != touched[j].Entity {
			return touched[i].Entity < touched[j].Entity
		}
		return touched[i].ID < touched[j].ID
	})
	return touched, nil
}

func (t *tx) PutRow(r repository.Row) error {
	t.st.rows[repository.RowKey{Entity: r.Entity, ID: r.ID}] = r
	return nil
}

func (t *tx) DeleteRow(entity, id string) error {
	delete(t.st.rows, repository.RowKey{Entity: entity, ID: id})
	return nil
}

func (t *tx) GetRow(entity, id string) (repository.Row, bool, error) {
	r, ok := t.st.rows[repository.RowKey{Entity: entity, ID: id}]
	return r, ok, nil
}

func in(list []string, v string) bool {
	for _, x := range list {
		if x == v {
			return true
		}
	}
	return false
}

func (t *tx) Rows(q repository.RowQuery) ([]repository.Row, error) {
	var out []repository.Row
	for k, r := range t.st.rows {
		switch {
		case k.Entity != q.Entity,
			q.Outlet != "" && r.OutletID != q.Outlet,
			len(q.IDs) > 0 && !in(q.IDs, r.ID),
			len(q.Parents) > 0 && !in(q.Parents, r.Parent),
			q.Tag != "" && r.Tag != q.Tag,
			r.Gone && !q.IncludeGone,
			(q.DateFrom != "" || q.DateTo != "") && r.BusinessDate == "",
			q.DateFrom != "" && r.BusinessDate < q.DateFrom,
			q.DateTo != "" && r.BusinessDate > q.DateTo:
			continue
		}
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (t *tx) PutException(x repository.Exception) error { t.st.exceptions[x.ID] = x; return nil }

func (t *tx) GetException(id string) (repository.Exception, bool, error) {
	x, ok := t.st.exceptions[id]
	return x, ok, nil
}

func (t *tx) Exceptions(q repository.ExceptionQuery) ([]repository.Exception, error) {
	var out []repository.Exception
	for _, x := range t.st.exceptions {
		switch {
		case q.Outlet != "" && x.OutletID != q.Outlet,
			q.Status != "" && x.Status != q.Status,
			q.Kind != "" && x.Kind != q.Kind,
			q.SubjectKind != "" && x.SubjectKind != q.SubjectKind,
			len(q.SubjectIDs) > 0 && !in(q.SubjectIDs, x.SubjectID):
			continue
		}
		out = append(out, x)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (t *tx) Publish(env events.Envelope) error {
	if err := env.Validate(); err != nil {
		return err
	}
	t.out = append(t.out, env)
	return nil
}
