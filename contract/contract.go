// Package contract runs the shared contract suite of api-specs (harness
// github.com/hostryhq/api-specs/contracttest) against the orders service: once against the
// in-memory double and once against the real assembly over PostgreSQL (integration tag). Each
// operation is called with the examples of the specification as input, so the target is put into
// the operation's example world first (SeedWorld; worlds are declared in the spec's
// info.x-example-worlds).
package contract

import (
	"context"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/getkin/kin-openapi/openapi3"
	"github.com/hostryhq/api-specs/contracttest"
	"github.com/hostryhq/svc-orders/api"
)

// SpecPath is the orders OpenAPI document: HOSTRY_API_SPECS (an api-specs checkout) when set, else
// the sibling checkout ../api-specs-p2, else the api-specs module of go.mod (the pinned commit).
func SpecPath() string {
	const rel = "/openapi/orders/v1/openapi.yaml"
	if v := os.Getenv("HOSTRY_API_SPECS"); v != "" {
		return v + rel
	}
	if _, err := os.Stat("../../api-specs-p2" + rel); err == nil {
		return "../../api-specs-p2" + rel
	}
	out, err := exec.Command("go", "list", "-m", "-f", "{{.Dir}}", "github.com/hostryhq/api-specs").Output()
	if err != nil {
		return "../../api-specs-p2" + rel
	}
	return strings.TrimSpace(string(out)) + rel
}

// SpecOperations lists the operationIds of the specification.
func SpecOperations(t testing.TB) []string {
	t.Helper()
	l := openapi3.NewLoader()
	l.IsExternalRefsAllowed = true
	doc, err := l.LoadFromFile(SpecPath())
	if err != nil {
		t.Fatalf("load spec %s: %v", SpecPath(), err)
	}
	var ops []string
	for _, item := range doc.Paths.Map() {
		for _, op := range item.Operations() {
			ops = append(ops, op.OperationID)
		}
	}
	return ops
}

// CheckCoverage fails when the route table and the specification differ in operations.
func CheckCoverage(t *testing.T) {
	t.Helper()
	spec := map[string]bool{}
	for _, o := range SpecOperations(t) {
		spec[o] = true
	}
	for _, o := range api.Operations() {
		if !spec[o] {
			t.Errorf("route %s is not in the specification", o)
		}
		delete(spec, o)
	}
	for o := range spec {
		t.Errorf("operation %s of the specification has no route", o)
	}
}

// Options builds the harness options for a target.
func Options(base string, header http.Header, seed func(ctx context.Context, world string) error) contracttest.Options {
	return contracttest.Options{
		SpecPath: SpecPath(), BaseURL: base, Header: header,
		SeedWorld: seed,
	}
}

// Run executes the suite and reports each operation as a subtest.
func Run(t *testing.T, o contracttest.Options) { contracttest.Run(t, o) }
