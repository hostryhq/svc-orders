package contract_test

import (
	"net/http/httptest"
	"testing"

	"github.com/hostryhq/svc-orders/contract"
	"github.com/hostryhq/svc-orders/double"
)

func TestRouteTableMatchesSpec(t *testing.T) { contract.CheckCoverage(t) }

// TestContractAgainstDouble runs the shared contract suite against the in-memory double.
func TestContractAgainstDouble(t *testing.T) {
	d := double.New(double.Options{})
	srv := httptest.NewServer(d.Handler)
	defer srv.Close()
	contract.Run(t, contract.Options(srv.URL, d.ExampleHeader(), d.SeedWorld))
}
