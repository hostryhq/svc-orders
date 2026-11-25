// Package pgtest creates a throwaway PostgreSQL database for integration tests, migrated and opened
// as the application role so row-level security applies. The server is the infra dev stack
// (HOSTRY_PG_DSN, default port 55462); tests skip when none answers.
package pgtest

import (
	"context"
	"database/sql"
	"os"
	"regexp"
	"testing"

	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/svc-orders/repository/postgres"
)

// AdminDSN is the superuser DSN of the dev stack.
func AdminDSN() string {
	if v := os.Getenv("HOSTRY_PG_DSN"); v != "" {
		return v
	}
	return "postgres://hostry:hostry-dev-only@127.0.0.1:55462/hostry?sslmode=disable"
}

// DB is a throwaway database.
type DB struct {
	Store *postgres.Store
	// Admin is a superuser handle on the same database (bypasses row-level security): fixtures and checks only.
	Admin *sql.DB
}

// Truncate empties every projection table (not the migrations).
func (d *DB) Truncate(ctx context.Context) error {
	_, err := d.Admin.ExecContext(ctx, `TRUNCATE apply_counter, applied_ops, voided_ops, effects, projection_rows, exceptions, outbox, inbox, event_parked`)
	return err
}

// New creates, migrates and opens a database, and drops it when the test ends.
func New(t testing.TB) *DB {
	t.Helper()
	ctx := context.Background()
	root, err := sql.Open("pgx", AdminDSN())
	if err != nil {
		t.Fatal(err)
	}
	if err := root.PingContext(ctx); err != nil {
		_ = root.Close()
		t.Skipf("no PostgreSQL at %s: %v", AdminDSN(), err)
	}
	name := "orders_" + regexp.MustCompile(`[^a-z0-9]`).ReplaceAllString(string(ids.New())[19:], "")
	if _, err := root.ExecContext(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	dsn := regexp.MustCompile(`/hostry\?`).ReplaceAllString(AdminDSN(), "/"+name+"?")
	st, err := postgres.Open(ctx, dsn, "orders_app")
	if err != nil {
		t.Fatal(err)
	}
	if err := postgres.Migrate(ctx, st.DB()); err != nil {
		t.Fatal(err)
	}
	admin, err := sql.Open("pgx", dsn)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = admin.Close()
		_ = st.Close()
		_, _ = root.ExecContext(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
		_ = root.Close()
	})
	return &DB{Store: st, Admin: admin}
}
