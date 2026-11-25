//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"

	"github.com/hostryhq/svc-orders/internal/pgtest"
	"github.com/hostryhq/svc-orders/repository/postgres"
)

// The store refuses to start as a role that bypasses row-level security (P2-M6): a superuser connection
// without an assumed role, or an assumed role holding BYPASSRLS; the application role passes.
func TestStoreRefusesRolesThatBypassRLS(t *testing.T) {
	ctx := context.Background()
	db := pgtest.New(t) // opens as orders_app and migrates
	if err := db.Store.CheckRole(ctx); err != nil {
		t.Fatalf("the application role must pass: %v", err)
	}
	var dbname string
	if err := db.Admin.QueryRowContext(ctx, `SELECT current_database()`).Scan(&dbname); err != nil {
		t.Fatal(err)
	}
	dsn := strings.Replace(pgtest.AdminDSN(), "/hostry?", "/"+dbname+"?", 1)
	super, err := postgres.Open(ctx, dsn, "")
	if err != nil {
		t.Fatal(err)
	}
	defer super.Close()
	if err := super.CheckRole(ctx); err == nil {
		t.Fatal("a superuser connection with no assumed role must be refused")
	}
	if _, err := db.Admin.ExecContext(ctx, `CREATE ROLE orders_bypass NOLOGIN BYPASSRLS`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = db.Admin.ExecContext(context.Background(), `DROP ROLE IF EXISTS orders_bypass`) })
	by, err := postgres.Open(ctx, dsn, "orders_bypass")
	if err != nil {
		t.Fatal(err)
	}
	defer by.Close()
	if err := by.CheckRole(ctx); err == nil {
		t.Fatal("an assumed BYPASSRLS role must be refused")
	}
}
