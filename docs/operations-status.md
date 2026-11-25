# Operation status

Every `operationId` of `api-specs` `openapi/orders/v1` (commit in `GEN_SOURCE`) and whether it is implemented.
"Implemented" means: a handler through the service layer with its go-auth policy, and the shared contract run
(status, media type, response schema, unknown route) against both the in-memory double and the assembly over
PostgreSQL. Business rules have behaviour tests (`service/*_test.go`, `api/api_test.go`). Limits are written beside
the operation; the reasoning is in `notes.md`.

Total: 11 operations, 11 implemented, 0 not yet. Orders writes no synced row through the API; its write path is the
bus consumer (`events.Apply`), which projects accepted sync ops and runs the detectors.

| operationId | Route | x-permission | Status | Note |
|---|---|---|---|---|
| `getCancellationReport` | `GET /v1/reports/cancellations` | `report.view.outlet` | implemented | `approver_id` omitted, nil id for a void's reason (N4) |
| `getCheck` | `GET /v1/checks/{check_id}` | `order.edit` | implemented | Outlet checked against the stored row; foreign tenant or outlet is 404 |
| `getKitchenTimeReport` | `GET /v1/reports/kitchen-times` | `report.view.outlet` | implemented | Needs the `station` reference row for `station_id` (N3) |
| `getKotLog` | `GET /v1/reports/kot-log` | `report.view.outlet` | implemented | |
| `getOpenCheckCount` | `GET /internal/v1/outlets/{outlet_id}/open-check-count` | `orders.check_count.read` | implemented | Service token of `billing` only; unknown outlet counts 0 (N6) |
| `getOpenChecksReport` | `GET /v1/reports/open-checks` | `report.view.outlet` | implemented | `table_name` from the `dining_table` reference row (N3) |
| `getOrderException` | `GET /v1/exceptions/{exception_id}` | `exceptions.view` | implemented | |
| `getOrdersProjectionDigest` | `GET /v1/projection-digests` | `report.view.outlet` | implemented | Provisional canonical-row rule (N8) |
| `getPresentLog` | `GET /v1/reports/present-log` | `report.view.outlet` | implemented | |
| `listChecks` | `GET /v1/checks` | `order.edit` | implemented | Open first; `X-Outlet-Id` optional |
| `listOrderExceptions` | `GET /v1/exceptions` | `exceptions.view` | implemented | Newest detection first |

## Projection and detectors

| Piece | Status | Note |
|---|---|---|
| Consumer of `evt.sync.op.<namespace>.v1` (`events.Apply`, inbox) | implemented | Namespaces generated from `sync/intents`; event body shape is provisional (N1) |
| Orders-owned entities (15) | implemented | seating, order, order_line (+modifier, cancel), check (+cancel, present, present_change), kot (+line, print, handover), bill_request, seating_move, seating_merge |
| `orphan_line`, `unresolved_line_at_close`, `grace_cancel_started`, `cancel_allowance_exceeded`, `two_seatings_one_table`, `late_ticket`, `presented_check_unresolved` | implemented | Rules and gaps in N5 |
| `orders.exception.opened` through the outbox | implemented | Same transaction as the projection |
| Restore guard (`go-common/restore`) | not yet | Commit not pushed (N10) |
