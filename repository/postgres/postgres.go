// Package postgres is the PostgreSQL implementation of repository.Store. Tenant rows are isolated
// by row-level security on app.tenant_id, set per transaction, and every statement also carries the
// tenant predicate; events go to the transactional outbox in the same transaction as the state change.
package postgres

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/outbox"
	"github.com/hostryhq/go-common/tenancy"
	_ "github.com/jackc/pgx/v5/stdlib" // registers the "pgx" database/sql driver

	"github.com/hostryhq/svc-orders/repository"
)

//go:embed migrations/*.sql
var migrationFS embed.FS

var roleRE = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

// Store implements repository.Store over database/sql with the pgx driver.
type Store struct {
	db *sql.DB
	// role, when set, is assumed with SET LOCAL ROLE in every transaction so row-level security
	// applies even when the connection is a superuser (development and tests).
	role string
}

// Open connects and returns a Store.
func Open(ctx context.Context, dsn, role string) (*Store, error) {
	if role != "" && !roleRE.MatchString(role) {
		return nil, fmt.Errorf("postgres: invalid role name")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(20)
	db.SetConnMaxLifetime(30 * time.Minute)
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("postgres: ping: %w", err)
	}
	return &Store{db: db, role: role}, nil
}

// DB returns the underlying handle (outbox relay, inbox subscriber, health, tests).
func (s *Store) DB() *sql.DB { return s.db }

// Close closes the pool.
func (s *Store) Close() error { return s.db.Close() }

// Ping implements repository.Store.
func (s *Store) Ping(ctx context.Context) error { return s.db.PingContext(ctx) }

// Migrate applies the embedded forward-only migrations in order, once each, under an advisory lock.
func Migrate(ctx context.Context, db *sql.DB) error {
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock(7102101)`); err != nil {
		return err
	}
	defer conn.ExecContext(context.WithoutCancel(ctx), `SELECT pg_advisory_unlock(7102101)`) //nolint:errcheck
	if _, err := conn.ExecContext(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations (version text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())`); err != nil {
		return err
	}
	entries, err := migrationFS.ReadDir("migrations")
	if err != nil {
		return err
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		names = append(names, e.Name())
	}
	sort.Strings(names)
	for _, n := range names {
		var done bool
		if err := conn.QueryRowContext(ctx, `SELECT EXISTS (SELECT 1 FROM schema_migrations WHERE version = $1)`, n).Scan(&done); err != nil {
			return err
		}
		if done {
			continue
		}
		b, err := migrationFS.ReadFile("migrations/" + n)
		if err != nil {
			return err
		}
		tx, err := conn.BeginTx(ctx, nil)
		if err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, string(b)); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("postgres: migration %s: %w", n, err)
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO schema_migrations (version) VALUES ($1)`, n); err != nil {
			_ = tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) begin(ctx context.Context, tenant string, sqlTx *sql.Tx) error {
	if s.role != "" {
		if _, err := sqlTx.ExecContext(ctx, "SET LOCAL ROLE "+s.role); err != nil {
			return err
		}
	}
	return tenancy.SetDBContext(ctx, sqlTx, ids.ID(tenant))
}

// InTx implements repository.Store.
func (s *Store) InTx(ctx context.Context, tenant string, fn func(repository.Tx) error) error {
	sqlTx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	if err := s.begin(ctx, tenant, sqlTx); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	if err := fn(&tx{ctx: ctx, sql: sqlTx, tenant: tenant}); err != nil {
		_ = sqlTx.Rollback()
		return err
	}
	return sqlTx.Commit()
}

// WithSQLTx runs fn on a transaction the caller already owns (the inbox subscriber's, which commits
// it together with the inbox row), after setting the role and the tenant context on it.
func (s *Store) WithSQLTx(ctx context.Context, sqlTx *sql.Tx, tenant string, fn func(repository.Tx) error) error {
	if err := s.begin(ctx, tenant, sqlTx); err != nil {
		return err
	}
	return fn(&tx{ctx: ctx, sql: sqlTx, tenant: tenant})
}

type tx struct {
	ctx    context.Context
	sql    *sql.Tx
	tenant string
}

func (t *tx) Tenant() string { return t.tenant }

func (t *tx) NextApplySeq() (int64, error) {
	var n int64
	err := t.sql.QueryRowContext(t.ctx, `INSERT INTO apply_counter (tenant_id, n) VALUES ($1, 1)
		ON CONFLICT (tenant_id) DO UPDATE SET n = apply_counter.n + 1 RETURNING n`, t.tenant).Scan(&n)
	return n, err
}

func (t *tx) OpApplied(ref string) (bool, error) {
	var ok bool
	err := t.sql.QueryRowContext(t.ctx, `SELECT EXISTS (SELECT 1 FROM applied_ops WHERE tenant_id=$1 AND op_ref=$2)`, t.tenant, ref).Scan(&ok)
	return ok, err
}

func (t *tx) MarkApplied(ref string, n int64) error {
	_, err := t.sql.ExecContext(t.ctx, `INSERT INTO applied_ops (tenant_id, op_ref, apply_seq) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING`, t.tenant, ref, n)
	return err
}

func (t *tx) OpVoided(ref string) (bool, error) {
	var ok bool
	err := t.sql.QueryRowContext(t.ctx, `SELECT EXISTS (SELECT 1 FROM voided_ops WHERE tenant_id=$1 AND op_ref=$2)`, t.tenant, ref).Scan(&ok)
	return ok, err
}

func (t *tx) MarkVoided(ref string) error {
	_, err := t.sql.ExecContext(t.ctx, `INSERT INTO voided_ops (tenant_id, op_ref) VALUES ($1,$2) ON CONFLICT DO NOTHING`, t.tenant, ref)
	return err
}

func (t *tx) AddEffect(e repository.Effect) error {
	f, err := json.Marshal(e.Fields)
	if err != nil {
		return err
	}
	_, err = t.sql.ExecContext(t.ctx, `INSERT INTO effects (tenant_id, op_ref, idx, entity, id, outlet_id, kind, fields, hlc, origin, seq, at, actor, apply_seq, arrived)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15) ON CONFLICT DO NOTHING`,
		t.tenant, e.Op, e.Idx, e.Entity, e.ID, e.OutletID, e.Kind, f, e.HLC, e.Origin, e.Seq, e.At, e.Actor, e.ApplySeq, e.Arrived)
	return err
}

func (t *tx) EffectsOf(entity, id string) ([]repository.Effect, error) {
	rows, err := t.sql.QueryContext(t.ctx, `SELECT op_ref, idx, outlet_id, kind, fields, hlc, origin, seq, at, actor, apply_seq, arrived
		FROM effects WHERE tenant_id=$1 AND entity=$2 AND id=$3 ORDER BY apply_seq, idx`, t.tenant, entity, id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.Effect
	for rows.Next() {
		e := repository.Effect{Entity: entity, ID: id}
		var f []byte
		if err := rows.Scan(&e.Op, &e.Idx, &e.OutletID, &e.Kind, &f, &e.HLC, &e.Origin, &e.Seq, &e.At, &e.Actor, &e.ApplySeq, &e.Arrived); err != nil {
			return nil, err
		}
		if e.Fields, err = decodeFields(f); err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, rows.Err()
}

// decodeFields reads a JSON object keeping integers as int64 (the projection compares and sums them).
func decodeFields(b []byte) (map[string]any, error) {
	m := map[string]any{}
	dec := json.NewDecoder(strings.NewReader(string(b)))
	dec.UseNumber()
	if err := dec.Decode(&m); err != nil {
		return nil, err
	}
	for k, v := range m {
		if n, ok := v.(json.Number); ok {
			if i, err := n.Int64(); err == nil {
				m[k] = i
			}
		}
	}
	return m, nil
}

func (t *tx) RemoveEffectsOfOp(ref string) ([]repository.RowKey, error) {
	rows, err := t.sql.QueryContext(t.ctx, `DELETE FROM effects WHERE tenant_id=$1 AND op_ref=$2 RETURNING entity, id`, t.tenant, ref)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	seen := map[repository.RowKey]bool{}
	var out []repository.RowKey
	for rows.Next() {
		var k repository.RowKey
		if err := rows.Scan(&k.Entity, &k.ID); err != nil {
			return nil, err
		}
		if !seen[k] {
			seen[k] = true
			out = append(out, k)
		}
	}
	return out, rows.Err()
}

func (t *tx) PutRow(r repository.Row) error {
	f, err := json.Marshal(r.Fields)
	if err != nil {
		return err
	}
	m, err := json.Marshal(r.Meta)
	if err != nil {
		return err
	}
	_, err = t.sql.ExecContext(t.ctx, `INSERT INTO projection_rows (tenant_id, entity, id, outlet_id, parent_id, business_date, tag, fields, meta, gone)
		VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
		ON CONFLICT (tenant_id, entity, id) DO UPDATE SET outlet_id=EXCLUDED.outlet_id, parent_id=EXCLUDED.parent_id, business_date=EXCLUDED.business_date,
		  tag=EXCLUDED.tag, fields=EXCLUDED.fields, meta=EXCLUDED.meta, gone=EXCLUDED.gone`,
		t.tenant, r.Entity, r.ID, r.OutletID, r.Parent, r.BusinessDate, r.Tag, f, m, r.Gone)
	return err
}

func (t *tx) DeleteRow(entity, id string) error {
	_, err := t.sql.ExecContext(t.ctx, `DELETE FROM projection_rows WHERE tenant_id=$1 AND entity=$2 AND id=$3`, t.tenant, entity, id)
	return err
}

const rowCols = `entity, id, outlet_id, parent_id, business_date, tag, fields, meta, gone`

func scanRow(sc interface{ Scan(...any) error }) (repository.Row, error) {
	var r repository.Row
	var f, m []byte
	if err := sc.Scan(&r.Entity, &r.ID, &r.OutletID, &r.Parent, &r.BusinessDate, &r.Tag, &f, &m, &r.Gone); err != nil {
		return r, err
	}
	var err error
	if r.Fields, err = decodeFields(f); err != nil {
		return r, err
	}
	return r, json.Unmarshal(m, &r.Meta)
}

func (t *tx) GetRow(entity, id string) (repository.Row, bool, error) {
	r, err := scanRow(t.sql.QueryRowContext(t.ctx, `SELECT `+rowCols+` FROM projection_rows WHERE tenant_id=$1 AND entity=$2 AND id=$3`, t.tenant, entity, id))
	if errors.Is(err, sql.ErrNoRows) {
		return repository.Row{}, false, nil
	}
	return r, err == nil, err
}

type args struct{ v []any }

func (a *args) add(x any) string { a.v = append(a.v, x); return "$" + strconv.Itoa(len(a.v)) }

func (t *tx) Rows(q repository.RowQuery) ([]repository.Row, error) {
	a := &args{}
	where := []string{"tenant_id=" + a.add(t.tenant), "entity=" + a.add(q.Entity)}
	if q.Outlet != "" {
		where = append(where, "outlet_id="+a.add(q.Outlet))
	}
	if len(q.IDs) > 0 {
		where = append(where, "id = ANY("+a.add(q.IDs)+")")
	}
	if len(q.Parents) > 0 {
		where = append(where, "parent_id = ANY("+a.add(q.Parents)+")")
	}
	if q.Tag != "" {
		where = append(where, "tag="+a.add(q.Tag))
	}
	if !q.IncludeGone {
		where = append(where, "NOT gone")
	}
	if q.DateFrom != "" || q.DateTo != "" {
		where = append(where, "business_date <> ''")
	}
	if q.DateFrom != "" {
		where = append(where, "business_date >= "+a.add(q.DateFrom))
	}
	if q.DateTo != "" {
		where = append(where, "business_date <= "+a.add(q.DateTo))
	}
	rows, err := t.sql.QueryContext(t.ctx, `SELECT `+rowCols+` FROM projection_rows WHERE `+strings.Join(where, " AND ")+` ORDER BY id`, a.v...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.Row
	for rows.Next() {
		r, err := scanRow(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

const excCols = `id, outlet_id, kind, severity, status, subject_kind, subject_id, business_date, detected_at, resolved_at, resolved_by, detail_key, params`

func scanExc(sc interface{ Scan(...any) error }) (repository.Exception, error) {
	var x repository.Exception
	var p []byte
	var ra sql.NullTime
	if err := sc.Scan(&x.ID, &x.OutletID, &x.Kind, &x.Severity, &x.Status, &x.SubjectKind, &x.SubjectID, &x.BusinessDate, &x.DetectedAt, &ra, &x.ResolvedBy, &x.DetailKey, &p); err != nil {
		return x, err
	}
	if ra.Valid {
		t := ra.Time.UTC()
		x.ResolvedAt = &t
	}
	x.DetectedAt = x.DetectedAt.UTC()
	var err error
	x.Params, err = decodeFields(p)
	return x, err
}

func (t *tx) PutException(x repository.Exception) error {
	p, err := json.Marshal(x.Params)
	if err != nil {
		return err
	}
	if x.Params == nil {
		p = []byte("{}")
	}
	_, err = t.sql.ExecContext(t.ctx, `INSERT INTO exceptions (tenant_id, `+excCols+`) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14)
		ON CONFLICT (tenant_id, id) DO UPDATE SET severity=EXCLUDED.severity, status=EXCLUDED.status, detected_at=EXCLUDED.detected_at,
		  resolved_at=EXCLUDED.resolved_at, resolved_by=EXCLUDED.resolved_by, params=EXCLUDED.params`,
		t.tenant, x.ID, x.OutletID, x.Kind, x.Severity, x.Status, x.SubjectKind, x.SubjectID, x.BusinessDate, x.DetectedAt, x.ResolvedAt, x.ResolvedBy, x.DetailKey, p)
	return err
}

func (t *tx) GetException(id string) (repository.Exception, bool, error) {
	x, err := scanExc(t.sql.QueryRowContext(t.ctx, `SELECT `+excCols+` FROM exceptions WHERE tenant_id=$1 AND id=$2`, t.tenant, id))
	if errors.Is(err, sql.ErrNoRows) {
		return repository.Exception{}, false, nil
	}
	return x, err == nil, err
}

func (t *tx) Exceptions(q repository.ExceptionQuery) ([]repository.Exception, error) {
	a := &args{}
	where := []string{"tenant_id=" + a.add(t.tenant)}
	if q.Outlet != "" {
		where = append(where, "outlet_id="+a.add(q.Outlet))
	}
	if q.Status != "" {
		where = append(where, "status="+a.add(q.Status))
	}
	if q.Kind != "" {
		where = append(where, "kind="+a.add(q.Kind))
	}
	if q.SubjectKind != "" {
		where = append(where, "subject_kind="+a.add(q.SubjectKind))
	}
	if len(q.SubjectIDs) > 0 {
		where = append(where, "subject_id = ANY("+a.add(q.SubjectIDs)+")")
	}
	rows, err := t.sql.QueryContext(t.ctx, `SELECT `+excCols+` FROM exceptions WHERE `+strings.Join(where, " AND ")+` ORDER BY id`, a.v...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []repository.Exception
	for rows.Next() {
		x, err := scanExc(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, x)
	}
	return out, rows.Err()
}

func (t *tx) Publish(env events.Envelope) error { return outbox.Publish(t.ctx, t.sql, env) }
