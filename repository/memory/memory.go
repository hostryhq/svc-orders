// Package memory is the in-memory repository.Store used by the test double and unit tests.
// A transaction works on a copy of the tenant's state and swaps it in on commit, so an error
// rolls back exactly like the PostgreSQL implementation.
package memory

import (
	"context"
	"sort"
	"sync"

	"github.com/hostryhq/go-common/events"
	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/repository"
)

type state struct {
	seq        int64
	applied    map[string]repository.OpMeta
	cands      map[gosync.RowKey]map[candID]gosync.Candidate
	voids      map[gosync.OpRef]bool
	rows       map[repository.RowKey]repository.Row
	exceptions map[string]repository.Exception
}

type candID struct {
	ref gosync.OpRef
	idx int
}

func newState() *state {
	return &state{applied: map[string]repository.OpMeta{}, cands: map[gosync.RowKey]map[candID]gosync.Candidate{}, voids: map[gosync.OpRef]bool{},
		rows: map[repository.RowKey]repository.Row{}, exceptions: map[string]repository.Exception{}}
}

func (s *state) clone() *state {
	c := newState()
	c.seq = s.seq
	for k, v := range s.applied {
		c.applied[k] = v
	}
	for k, v := range s.voids {
		c.voids[k] = v
	}
	for k, m := range s.cands {
		cp := make(map[candID]gosync.Candidate, len(m))
		for id, c := range m { // candidates are immutable once added
			cp[id] = c
		}
		c.cands[k] = cp
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

func (t *tx) Context() context.Context { return context.Background() }

func (t *tx) NextApplySeq() (int64, error) { t.st.seq++; return t.st.seq, nil }

func (t *tx) OpApplied(ref string) (bool, error) { _, ok := t.st.applied[ref]; return ok, nil }

func (t *tx) MarkApplied(ref string, m repository.OpMeta) error {
	if _, ok := t.st.applied[ref]; !ok {
		t.st.applied[ref] = m
	}
	return nil
}

func (t *tx) OpMetas(refs []string) (map[string]repository.OpMeta, error) {
	out := make(map[string]repository.OpMeta, len(refs))
	for _, r := range refs {
		if m, ok := t.st.applied[r]; ok {
			out[r] = m
		}
	}
	return out, nil
}

// SyncStore implements repository.Tx: the candidates and voids of the transaction's copy of the state.
func (t *tx) SyncStore() gosync.Store { return syncStore{t.st} }

type syncStore struct{ st *state }

func (s syncStore) AddCandidate(_ context.Context, k gosync.RowKey, c gosync.Candidate) error {
	m := s.st.cands[k]
	if m == nil {
		m = map[candID]gosync.Candidate{}
		s.st.cands[k] = m
	}
	if _, ok := m[candID{c.Ref, c.Idx}]; !ok {
		m[candID{c.Ref, c.Idx}] = c
	}
	return nil
}

func (s syncStore) Candidates(_ context.Context, k gosync.RowKey) ([]gosync.Candidate, error) {
	out := make([]gosync.Candidate, 0, len(s.st.cands[k]))
	for _, c := range s.st.cands[k] {
		out = append(out, c)
	}
	return out, nil
}

func (s syncStore) RowsOfOp(_ context.Context, tenant string, r gosync.OpRef) ([]gosync.RowKey, error) {
	var out []gosync.RowKey
	for k, m := range s.st.cands {
		if k.Tenant != tenant {
			continue
		}
		for id := range m {
			if id.ref == r {
				out = append(out, k)
				break
			}
		}
	}
	return out, nil
}

func (s syncStore) AddVoid(_ context.Context, _ string, r gosync.OpRef) error {
	s.st.voids[r] = true
	return nil
}

func (s syncStore) Voided(_ context.Context, _ string, refs []gosync.OpRef) (map[gosync.OpRef]bool, error) {
	out := map[gosync.OpRef]bool{}
	for _, r := range refs {
		if s.st.voids[r] {
			out[r] = true
		}
	}
	return out, nil
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
