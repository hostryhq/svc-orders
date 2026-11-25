package service_test

import (
	"context"
	_ "embed"
	"encoding/base64"
	"encoding/json"
	"testing"
	"time"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-auth/authtest"
	"github.com/hostryhq/go-common/ids"
	gosync "github.com/hostryhq/go-common/sync"
	"github.com/hostryhq/svc-orders/double"
	"github.com/hostryhq/svc-orders/service"
)

//go:embed testdata/digest-row.vectors.v1.json
var digestRowVectors []byte

// The canonical row text covers the non-pii fields and each pii field's commitment, never a value (api-specs sync/conformance
// digest_row vectors, ADR-0014 A3.2): the same text whether the node holds the values or not.
func TestDigestRowTextMatchesTheSharedVectors(t *testing.T) {
	var vs []struct {
		Case      string
		ID        string
		PIIFields []string `json:"pii_fields"`
		Fields    map[string]any
		PIIValues map[string]any    `json:"pii_values"`
		Commits   map[string]string `json:"commitments"`
		RowText   string            `json:"row_text"`
	}
	if err := json.Unmarshal(digestRowVectors, &vs); err != nil || len(vs) < 3 {
		t.Fatalf("vectors: %v (%d)", err, len(vs))
	}
	for _, v := range vs {
		pii := map[string]bool{}
		for _, f := range v.PIIFields {
			pii[f] = true
		}
		withValues := map[string]any{}
		for k, x := range v.Fields {
			withValues[k] = x
		}
		for k, x := range v.PIIValues {
			withValues[k] = x
		}
		for name, fields := range map[string]map[string]any{"values held": withValues, "none held": v.Fields} {
			got, err := service.DigestText(v.ID, fields, pii, v.Commits)
			if err != nil || string(got) != v.RowText {
				t.Errorf("%s (%s): %s (%v), vector %s", v.Case, name, got, err, v.RowText)
			}
		}
	}
}

// A check's customer name is lww pii: the digest covers the commitment of the write that wins (the higher stamp), whatever the
// arrival order, and it does not change when the personal data is erased or when a node never held the values at all.
func TestDigestCoversTheWinningCommitmentAndIsUnchangedByErasure(t *testing.T) {
	ctx := context.Background()
	cat, err := service.NewCatalogue()
	if err != nil {
		t.Fatal(err)
	}
	salt := base64.RawURLEncoding.EncodeToString([]byte("0123456789abcdef"))
	commit := func(v string) string {
		c, err := gosync.PIICommit([]byte("0123456789abcdef"), v)
		if err != nil {
			t.Fatal(err)
		}
		return c
	}
	// op 1 creates the check with a name; op 2, on another device and later, edits it
	build := func(values bool) []*service.Op {
		b1 := double.NewOpBuilder(double.Device, "outlet:"+double.Outlet, double.Member)
		b2 := double.NewOpBuilder(ids.New().String(), "outlet:"+double.Outlet, double.Member).At(double.Start.Add(time.Hour))
		o1 := b1.Next("check.create", double.Ins("check", double.Check, map[string]any{"order_type": "dine_in", "business_date": double.Day, "status": "open"}))
		o2 := b2.Next("bill.customer_details.edit", double.Set("set", "check", double.Check, map[string]any{"label": "B"}))
		for i, name := range []string{"First Name", "Second Name"} {
			op := []*service.Op{o1, o2}[i]
			op.PIICommits = gosync.PIIC{"check": {double.Check: {"customer_name": commit(name)}}}
			if values {
				op.PII = map[string]map[string]map[string]any{"check": {double.Check: {"customer_name": name}}}
				op.PIISalts = map[string]map[string]map[string]string{"check": {double.Check: {"customer_name": salt}}}
			}
		}
		return []*service.Op{o1, o2}
	}
	digest := func(d *double.Double) string {
		out, err := d.Svc.ProjectionDigest(d.Issuer.ContextFor(ctx, cat, authtest.Spec{Type: auth.TypeUser, Tenant: ids.ID(double.Tenant), Grants: authtest.Grants(nil, "report.view.outlet")}), double.Outlet, double.Day)
		if err != nil {
			t.Fatal(err)
		}
		return out.StateHash
	}
	ingest := func(ops ...*service.Op) *double.Double {
		d := double.New(double.Options{})
		if err := d.Ingest(ctx, double.Tenant, ops...); err != nil {
			t.Fatal(err)
		}
		return d
	}

	ops := build(true)
	inOrder := ingest(ops[0], ops[1])
	want := digest(inOrder)
	if got := digest(ingest(build(true)[1], build(true)[0])); got != want { // a fresh pair; the later op arrives first
		t.Fatalf("arrival order changed the digest: %s vs %s", got, want)
	}
	// the winner is the later write: a digest of the first commitment alone differs
	if got := digest(ingest(build(true)[0])); got == want {
		t.Fatal("the digest ignored the winning commitment")
	}
	if got := digest(ingest(build(false)...)); got != want {
		t.Fatalf("commitments only: %s, with values %s", got, want)
	}
	erase := double.NewOpBuilder("cloud-1", "outlet:"+double.Outlet, "").Next(gosync.PIIEraseIntent,
		double.Ins("pii_erase", "x", map[string]any{"origin": ops[0].Ref.Origin, "stream": ops[0].Ref.Stream, "seq": ops[0].Ref.Seq}))
	erase.PIIErase = []gosync.OpRef{ops[0].Ref, ops[1].Ref}
	if err := inOrder.Ingest(ctx, double.Tenant, erase); err != nil {
		t.Fatal(err)
	}
	if got := digest(inOrder); got != want {
		t.Fatalf("erasure changed the digest: %s vs %s", got, want)
	}
}
