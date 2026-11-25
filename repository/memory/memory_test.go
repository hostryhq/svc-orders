package memory_test

import (
	"context"
	"errors"
	"testing"

	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/repository/memory"
)

func TestTransactionsRollBackAndAreTenantScoped(t *testing.T) {
	ctx := context.Background()
	s := memory.New()
	boom := errors.New("boom")
	err := s.InTx(ctx, "a", func(tx repository.Tx) error {
		_ = tx.PutRow(repository.Row{Entity: "check", ID: "c1", OutletID: "o"})
		return boom
	})
	if !errors.Is(err, boom) {
		t.Fatalf("err = %v", err)
	}
	_ = s.InTx(ctx, "a", func(tx repository.Tx) error {
		if _, ok, _ := tx.GetRow("check", "c1"); ok {
			t.Error("a failed transaction left a row behind")
		}
		return tx.PutRow(repository.Row{Entity: "check", ID: "c1", OutletID: "o"})
	})
	_ = s.InTx(ctx, "b", func(tx repository.Tx) error {
		if rows, _ := tx.Rows(repository.RowQuery{Entity: "check"}); len(rows) != 0 {
			t.Error("tenant b sees tenant a's rows")
		}
		return nil
	})
}
