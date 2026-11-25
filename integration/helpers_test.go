//go:build integration

package integration

import (
	"context"
	"fmt"
	"testing"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-auth/authtest"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/svc-orders/internal/pgtest"
	"github.com/hostryhq/svc-orders/repository/postgres"
	"github.com/hostryhq/svc-orders/service"
)

func id(n int) string { return fmt.Sprintf("0190a1b2-c3d4-7e5f-8a9b-%012x", 0x7000+n) }

func migrate(db *pgtest.DB) error { return postgres.Migrate(context.Background(), db.Store.DB()) }

func mustCat(t *testing.T) *auth.Catalogue {
	t.Helper()
	c, err := service.NewCatalogue()
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func spec(tenant string) authtest.Spec {
	return authtest.Spec{Type: auth.TypeUser, Tenant: ids.ID(tenant), Grants: authtest.Grants(nil, "report.view.outlet", "order.edit")}
}
