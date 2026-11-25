package contract

import (
	"context"
	"strings"
	"testing"

	"github.com/hostryhq/api-specs/contracttest"
)

// exampleMismatch lists the operations whose spec example no world of this service can reproduce
// (docs/notes.md spec problem 2); every other operation must serve its example exactly.
var exampleMismatch = map[string]string{
	"getCheck":                  "the example lists line 6011 only, but 6012 is a fully cancelled line and the state enum has `cancelled`",
	"getKitchenTimeReport":      "the example says 42 lines and a 420 s median for a world with two lines",
	"getOrdersProjectionDigest": "the example says 120 order_line rows and a hash no world of two lines can produce",
}

// RunExamples runs the suite with ExpectExamples: in the example worlds every response must equal the
// spec's example, except for the operations in exampleMismatch, which must still pass every other check.
func RunExamples(t *testing.T, o contracttest.Options) {
	t.Helper()
	o.ExpectExamples = true
	reps, err := contracttest.Verify(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	for _, r := range reps {
		t.Run(r.OperationID, func(t *testing.T) {
			_, known := exampleMismatch[r.OperationID]
			for _, f := range r.Failures {
				if known && strings.HasPrefix(f, "body does not equal the spec example") {
					t.Logf("known example mismatch: %s", exampleMismatch[r.OperationID])
					continue
				}
				t.Error(f)
			}
		})
	}
}
