# Notes: provisional choices and spec problems

Conservative choices made where the specs are silent or inconsistent. Each is a candidate for a ruling; none
is a cross-cutting decision except N3 (marked `ADR needed`). Written against api-specs `GEN_SOURCE`.

## Choices

**N1. Shape of the sync op event (provisional).** ADR-0011 B10 names the subject `evt.sync.op.<namespace>.v1`, but
`api-specs` has no `events/sync` schema yet, and the svc-sync PoC event (`sync.op.stored`) carries only
`{origin, stream, seq, h, intent, status}`, no body. A consumer may not call the publisher back (ADR-0011 B5), so
orders reads `data.body`: the canonical JSON text of the op body (or the object), exactly as svc-sync
`contract/README.md` frames it (`hlc`, `actor`, `at`, `muts[{e,id,k,f}]`). The outlet comes from the stream
`outlet:<id>`. Status `accepted` and `flagged` are projected; anything else is skipped. When the schema of record
lands, `service.ParseOp` is the one place to change.

**N2. Apply engine.** go-common has no `sync` package yet, so `service/apply.go` implements the ADR-0002 S6
policies from the generated field table (`entities_gen.go`, from `entities/restaurant`): per field `immutable`
(earliest stamp), `lww` / `lww_review` (latest stamp; the review flag is not acted on, there is no review queue in
orders), `rank` (forward-only), `delta` (sum). A row is a pure function of its effects (kept for good), so replay,
reordering and voids converge. A `sync.void` removes the effects of the named op and re-derives its rows; a void
that arrives before its target marks the target so it is ignored. Entities orders does not project are ignored.
Replace this with `go-common/sync` when it exists.

**N3. Reference rows from other owners. `ADR needed: orders reading catalog and billing reference entities.`**
ADR-0014 R1 says a service applies only entities it owns, but three contract fields cannot be produced without
them: `station_id` (ops carry `station_code`; `station` is catalog's), `table_name` (`dining_table`, catalog's;
`kot.table_name` only exists on tickets), and the closed state of a business day (`day_close` + `business_day`,
billing's) for the day-close detectors; `outlet_settings.kot_stale_after_seconds` (catalog's) for late tickets.
Orders projects these five entities into the same row table, read only, never exposed or digested. If the
architect rules the other way, the alternatives are ids in the ops (`order_line.station_id`) or events from
catalog and billing.

**N4. Required ids that rows may lack.** `reason_code_id` (a void has none), `member_id`, `device_id`,
`station_id` (no `station` row yet) and `seating_id` (takeaway, counter) are required by the response schemas
but optional or derived in the entities. Orders answers the nil UUIDv7 `00000000-0000-7000-8000-000000000000`.
`CancellationRow.approver_id` is omitted: `order_line_cancel` carries an approval token id, not the approving
member.

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

**N6. `getOpenCheckCount`.** Orders has no outlet registry, so an outlet it has no rows for counts 0; it never answers 404.
Billing's guard therefore treats a new outlet as having no open checks. The count excludes cancelled checks.

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
`feat/phase1-go-common` at 95088b5, which is not pushed, so go.mod pins the pushed ad5f1e5 and the guard is not
wired. The projection is idempotent on op identity and the inbox absorbs a bus replay, so a restore replays cleanly;
wire the guard and the relay generation check when that commit is merged. The go-auth `scopelint` (A1.4 item 5) was
not run: orders' SQL is generic over `(tenant_id, entity)` and every statement carries `tenant_id` with row-level
security; list queries take an outlet, single-row reads are authorised against the stored row's outlet.

## Spec problems found (api-specs, not edited)

1. `common/components.yaml` `OutletScope` example is `...4a5c`, but the example worlds live at outlet `...4a61`, so a
   harness request is for an outlet with no data. The contract run rewrites that header value (`double.ExampleClient`).
2. Examples disagree: open-checks row `line_count: 3` vs check `line_count: 1`; KOT-log `line_count: 2` for a ticket
   of one line; cancel `qty_milli: 1000` on a line the check shows with 2000 and total 52000. The base world orders 3000,
   cancels 1000 and shows 2000, which fits the check, the cancel row and the report totals; the others cannot hold at
   once. The double is therefore not run with `ExpectExamples`.
3. `OpenCheckRow.seating_id`, `CancellationRow.reason_code_id`, `KotLogRow.station_id` / `member_id` / `device_id` are
   required `Id`s the entities do not guarantee (N4). `kitchen-times` and `kot-log` use `station_id` while ops carry `station_code` (N3).
4. No entity records a late-ticket dismissal, a carry-over of a presented check, or a grace-cancel limit (N5).
5. `getOpenCheckCount` lists a 404 no implementation without an outlet registry can give (N6).
6. `events/sync` (the schema of `evt.sync.op.<namespace>.v1`) is missing, and the PoC event has no body (N1).
