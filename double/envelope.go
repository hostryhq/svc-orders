package double

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/hostryhq/go-common/events"
	"github.com/hostryhq/go-common/ids"
	gosync "github.com/hostryhq/go-common/sync"
)

// OpEnvelope renders an op as the bus message the gateway publishes (sync.op.<namespace>, ADR-0014
// A2.1): the op body with its SHA-256 in h, so gosync.ParseOp accepts it. Tests use it to drive the
// consumer through the real parser. eventID is the envelope id (the inbox key).
func OpEnvelope(eventID ids.ID, op *Op) (events.Envelope, error) {
	muts := op.Muts
	if muts == nil {
		muts = []Mut{}
	}
	body, err := json.Marshal(map[string]any{
		"v": 1, "origin": op.Ref.Origin, "stream": op.Ref.Stream, "seq": op.Ref.Seq, "hlc": op.HLC, "clk": "device",
		"tenant_id": op.TenantID.String(), "actor": op.Actor, "intent": op.Intent, "intent_v": max(op.IntentV, 1),
		"at": op.At.UTC().Format(time.RFC3339Nano), "muts": muts,
	})
	if err != nil {
		return events.Envelope{}, err
	}
	status := op.Status
	if status == "" {
		status = "accepted"
	}
	data, err := json.Marshal(map[string]any{
		"origin": op.Ref.Origin, "stream": op.Ref.Stream, "seq": op.Ref.Seq, "h": gosync.Hash(string(body)), "intent": op.Intent,
		"intent_v": max(op.IntentV, 1), "status": status, "marks": []string{}, "body": string(body),
	})
	if err != nil {
		return events.Envelope{}, err
	}
	ns, _ := namespaceOf(op.Intent)
	return events.Envelope{ID: eventID, Type: "sync.op." + ns, V: 1, Source: "sync", TenantID: op.TenantID, OutletID: op.OutletID,
		Key: op.Ref.Stream + "|" + op.Ref.Origin, KeySeq: op.Ref.Seq, OccurredAt: op.At.UTC(), Data: data}, nil
}

func namespaceOf(intent string) (string, error) {
	for i := 0; i < len(intent); i++ {
		if intent[i] == '.' {
			return intent[:i], nil
		}
	}
	return "", fmt.Errorf("intent %q has no namespace", intent)
}
