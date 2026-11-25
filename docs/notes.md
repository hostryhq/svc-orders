# Notes: provisional choices and spec problems

Conservative choices made where the specs are silent or inconsistent. Each is a candidate for a ruling; none
is a cross-cutting decision. N1 to N4 are settled by ADR-0014 A2.1 and A2.4 and api-specs 1462fb8; N5 to N11 stay
provisional. Written against api-specs `GEN_SOURCE`.

## Choices

**N1. Op events (settled).** The op event is `evt.sync.op.<namespace>.v1` (api-specs `events/sync/op.v1.yaml`),
read by `gosync.Parser.ParseEnvelope` of go-common `sync` (`events/consumer.go`): it verifies the body hash, the identity
fields and the value types, and refuses a malformed op (the inbox parks it after the last attempt). The outlet comes from
the envelope's `outlet_id` (else an `outlet:<id>` stream). Status `accepted` and `flagged` are projected.

**N2. Apply engine (settled).** `service/apply.go` runs `gosync.Engine` (go-common `sync`, ADR-0014 A2.1) over the
transaction's candidate and void store (`sync_candidates`, `sync_voids`, migration 0003; row-level security as the other
tables). The entity registry is loaded from the entity files `cmd/gen` copies into `service/entities/` (owned or
`replicated_to: orders`), so policies come from the one registry. Orders adds only what the engine does not carry: per
applied op its arrival number, device time, member and outlet (`applied_ops`), from which `deriveMeta` computes the row's
creator, device, arrival order, winning stamps and the first time a rank field reached a value (detectors and reports).
Rows still derive purely from candidates and voids, so replay, reordering and voids converge. The old `effects` and
`voided_ops` tables are no longer read; a later release drops them (expand/contract). There is no data to carry over, since
nothing had been deployed.

**N3. Replicas of other owners' reference entities (ADR-0014 A2.4, accepted in the specs).** `station`, `dining_table`,
`outlet_settings` (catalog) and `business_day`, `day_close` (billing) carry `replicated_to: orders`. The engine applies
them like owned rows (no pii) into the same row table, which only the projection writes: orders never emits them, never
digests them (`ownedEntities` is the registry's owned set), never serves them as the entity. Uses: `station_id` (a ticket
carries `station_code`, the replica maps code to id), `table_name` (seating's table, then `dining_table.name`), the closed
state of a day for the day-close detectors, and `outlet_settings.kot_stale_after_seconds` for late tickets. A join with no
replica row answers null: the field is absent from the JSON until the row arrives. A detector that depends on a replica
re-runs when it arrives: `day_close` and `business_day` re-judge the checks of the closed day, `outlet_settings`
re-judges the tickets still waiting for a print. One row table for owned rows and replicas is a simplification of "tables
only the projection role writes" (orders never writes either outside the engine path); a separate table is a mechanical
change if the architect wants the grant split. `day_close` is `guarded`: the specs declare it a stated exception
(ADR inconsistency already reported to the architect).

**N4. References that rows may lack (settled by the contract).** `reason_code_id`, `member_id`, `device_id`, `station_id`,
`seating_id` and `table_name` are optional in the responses: a reference the row or its replica does not carry is
absent, never the nil UUID (A2.4 item 4). `CancellationRow.approver_id` is still omitted: `order_line_cancel` carries an
approval token id, not the approving member.

**N5. Detectors.** All are derived queries re-run for the rows an op touched, never ops; a finding closes itself
(`resolved`) when it no longer holds. Ids are deterministic (hash of tenant, outlet, kind, subject, date), so a
replay or a restored copy yields the same exception; `orders.exception.opened` is written to the outbox in the
same transaction when one opens (again when a resolved one reopens).
- `orphan_line`: a non-cancelled line whose winning placement stamp is later than the stamp at which its check
  ranked to `billed` or `closed_internal` (HLC order; concurrent counts).
- `unresolved_line_at_close` / `presented_check_unresolved`: the check is open (not cancelled) on a business date
  that has a `day_close`, and the line (or a `check_present`) arrived at orders after the first `day_close`
  (arrival order). Lines and presents that arrived before the close are the carried-over ones (no op marks a carry-over).
- `grace_cancel_started`: a `grace_cancel` whose `kitchen_status_at_cancel` is not `new`; never undone.
- `cancel_allowance_exceeded`: more than `ORDERS_GRACE_CANCEL_ALLOWANCE` (default 10, provisional) grace cancels by
  one member on one business date. The spec names no limit setting for orders.
- `two_seatings_one_table`: two open seatings on one current table (latest `seating_move` target; a merged-away seating does not count).
- `late_ticket`: an `original` KOT with no `kot_print` that either has a `kot_handover` or reached orders more than
  `kot_stale_after` (outlet setting, else 15 minutes) after its `at`. It closes on a `kot_print`. The spec says "print
  or dismiss" but no entity records a dismissal, so a handed-over ticket stays listed.

**N6. `getOpenCheckCount`.** An outlet orders has no rows for counts 0; the spec now says so and removed the 404. The
count excludes cancelled checks.

**N7. Quantities and money.** `order_line.qty_milli` is a delta counter of what was ordered; cancels are separate
facts. Check lines show the quantity not yet cancelled (a fully cancelled line shows its ordered quantity with state
`cancelled`); `list_total` sums remaining quantity x (unit + modifier delta), rounded half up to the paisa per line.
`cancellations.list_value` uses the cancelled quantity the same way; `wastage_value` sums rows flagged `wastage`.
`present_count` is the highest `check_present.present_count`. `Check.cancelled` is "a `check_cancel` exists".

**N8. Reports.** `from`/`to` bound the business date (92 days at most, else 422); with neither, the outlet's latest
check date. `open-checks` bounds the date a check was opened; `carried_over` means that day has a `day_close`.
Reports need one outlet: an all-outlets caller must send `X-Outlet-Id` (422 `outlet.required`). `kitchen-times`:
preparation is from the ticket line's `at` to the first `at` its status reached `ready` (else `served`); percentiles
are nearest-rank; `line_count` is lines sent. `present-log.changes_after` counts `check_present_change` rows whose
`after_present_count` equals that present. The digest hashes `sha256-canon-v1` (sorted-key JSON of a row's fields,
rows sorted by id, entity digests in name order) over owned entities by business date, resolved through the parent
chain; a row whose parent has not arrived yet is left out until it does. The canonical-row rule belongs in
`go-common/sync`; this one is provisional, as the spec says.

**N9. Scale.** Reads load rows by outlet, entity, business date and parent (indexed) and join in Go. That is right for
a pilot (hundreds of checks per outlet-day); `kot_line` is read per outlet when kitchen status is needed. Revisit
with the first load run.

**N10. Not wired yet.** `go-common/restore` (restore guard, `restore_state`, readiness check) exists on go-common
`feat/phase1-go-common` at 95088b5, which is not pushed to the module path orders pins, so the guard is not wired. The
projection is idempotent on op identity and the inbox absorbs a bus replay, so a restore replays cleanly; wire the guard
and the relay generation check when that commit is merged. The go-auth `scopelint` (A1.4 item 5) was not run: orders' SQL
is generic over `(tenant_id, entity)` and every statement carries `tenant_id` with row-level security; list queries take
an outlet, single-row reads are authorised against the stored row's outlet.

**N11. Detector gaps that stay provisional (O3).** The specs still define no entity for a late-ticket dismissal, for the
carry-over of a presented check or a line at close, or for the grace-cancel allowance, so the defaults of N5 stand
(a handed-over ticket stays listed until printed; lines and presents that arrived before the first `day_close` count as
carried over; allowance 10 per member and business date). Nothing was added where the entities do not exist.

## Spec problems found (api-specs 1462fb8, not edited)

Settled by 1462fb8 and removed from this list: the `OutletScope` example outlet, the inconsistent worlds (T1, cancelled
line 6012, outlet 4a61), the required ids the entities do not guarantee, and the 404 of `getOpenCheckCount`. The
double now serves the spec's examples exactly in the example worlds, except:

1. `getCheck` example lists line 6011 only, with `line_count: 1`. World `base` also has line 6012 (fully cancelled), and
   the line `state` enum has `cancelled` ("cancelled when an order_line_cancel covers the full quantity"), so orders lists
   6012 with state `cancelled` and counts only live lines. Either the example needs the cancelled line or the schema
   should say fully cancelled lines are not listed; a ruling is needed. The contract suite lists the three known example
   mismatches (`contract/examples.go`) and still checks their status and schema.
2. `getKitchenTimeReport` example (42 lines, median 420 s) and `getOrdersProjectionDigest` example (120 order_line rows)
   describe a day bigger than any example world, so no double can equal them.
3. No entity records a late-ticket dismissal, a carry-over of a presented check, or a grace-cancel limit (N5, N11).
4. `kitchen-times` rows key on `station_id` while ops carry `station_code`; the join needs the station replica (N3).
