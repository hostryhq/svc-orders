package double

import (
	"encoding/base64"
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
	return OpEnvelopePII(eventID, op, nil)
}

// OpEnvelopePII is OpEnvelope for an op that carries personal data beside it: pii is entity -> row id ->
// field -> value. Each field gets a fixed salt and a commitment in body.pii_c, as the gateway does
// (go-common/sync PIICommit), so ParseOp verifies it. The salts are derived from the field name, so
// the output is deterministic.
func OpEnvelopePII(eventID ids.ID, op *Op, pii map[string]map[string]map[string]any) (events.Envelope, error) {
	muts := op.Muts
	if muts == nil {
		muts = []Mut{}
	}
	wire := map[string]map[string]map[string]any{}
	commits := map[string]map[string]map[string]string{}
	for e, rows := range pii {
		wire[e], commits[e] = map[string]map[string]any{}, map[string]map[string]string{}
		for id, fields := range rows {
			wire[e][id], commits[e][id] = map[string]any{}, map[string]string{}
			for n, v := range fields {
				salt := []byte(fmt.Sprintf("%-16.16s", n))
				c, err := gosync.PIICommit(salt, v)
				if err != nil {
					return events.Envelope{}, err
				}
				wire[e][id][n], wire[e][id]["~s:"+n], commits[e][id][n] = v, base64.RawURLEncoding.EncodeToString(salt), c
			}
		}
	}
	bodyMap := map[string]any{
		"v": 1, "origin": op.Ref.Origin, "stream": op.Ref.Stream, "seq": op.Ref.Seq, "hlc": op.HLC, "clk": "device",
		"tenant_id": op.TenantID.String(), "actor": op.Actor, "intent": op.Intent, "intent_v": max(op.IntentV, 1),
		"at": op.At.UTC().Format(time.RFC3339Nano), "muts": muts,
	}
	if len(commits) > 0 {
		bodyMap["pii_c"] = commits
	}
	body, err := json.Marshal(bodyMap)
	if err != nil {
		return events.Envelope{}, err
	}
	status := op.Status
	if status == "" {
		status = "accepted"
	}
	dataMap := map[string]any{
		"origin": op.Ref.Origin, "stream": op.Ref.Stream, "seq": op.Ref.Seq, "h": gosync.Hash(string(body)), "intent": op.Intent,
		"intent_v": max(op.IntentV, 1), "status": status, "marks": []string{}, "body": string(body),
	}
	if len(wire) > 0 {
		dataMap["pii"] = wire
	}
	data, err := json.Marshal(dataMap)
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
