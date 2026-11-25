//go:build integration

package contract_test

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/hostryhq/svc-orders/contract"
	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/internal/pgtest"
)

// TestContractAgainstPostgres runs the same suite against the real assembly: the service and api
// layers over the PostgreSQL repository with row-level security, seeded through the projection.
func TestContractAgainstPostgres(t *testing.T) {
	db := pgtest.New(t)
	d := double.New(double.Options{Store: db.Store})
	srv := httptest.NewServer(d.Handler)
	defer srv.Close()
	seed := func(ctx context.Context, world string) error {
		if err := db.Truncate(ctx); err != nil {
			return err
		}
		return double.SeedInto(ctx, d, world)
	}
	contract.Run(t, contract.Options(srv.URL, d.ExampleHeader(), seed))
	t.Run("examples", func(t *testing.T) { contract.RunExamples(t, contract.Options(srv.URL, d.ExampleHeader(), seed)) })
}
