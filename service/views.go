package service

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/clock"
	"github.com/hostryhq/go-common/money"
	"github.com/hostryhq/svc-orders/repository"
)

// The view types are JSON-identical to the schemas of openapi/orders/v1; the api layer writes them as is.

// Optional references: a reference the row or its replica does not carry is absent from the JSON, never a
// placeholder id (ADR-0014 A2.4 item 4, docs/notes.md N4).

type CheckLine struct {
	OrderLineID   string      `json:"order_line_id"`
	ItemID        string      `json:"item_id"`
	Name          string      `json:"name"`
	QtyMilli      int64       `json:"qty_milli"`
	UnitPrice     money.Money `json:"unit_price"`
	ModifierDelta money.Money `json:"modifier_delta"`
	TaxClass      string      `json:"tax_class"`
	State         string      `json:"state"`
	KitchenStatus string      `json:"kitchen_status,omitempty"`
}

type Check struct {
	ID           string      `json:"id"`
	OutletID     string      `json:"outlet_id"`
	SeatingID    string      `json:"seating_id,omitempty"`
	Label        string      `json:"label,omitempty"`
	OrderType    string      `json:"order_type"`
	Status       string      `json:"status"`
	Cancelled    bool        `json:"cancelled"`
	PresentCount int         `json:"present_count"`
	BillID       string      `json:"bill_id,omitempty"`
	TokenNo      int64       `json:"token_no,omitempty"`
	CreatedAt    string      `json:"created_at"`
	LineCount    int         `json:"line_count"`
	ListTotal    money.Money `json:"list_total"`
}

type CheckDetail struct {
	Check
	Lines []CheckLine `json:"lines"`
}

type CheckPage struct {
	Items      []Check `json:"items"`
	NextCursor string  `json:"next_cursor,omitempty"`
}

type Exception struct {
	ID           string         `json:"id"`
	Kind         string         `json:"kind"`
	Severity     string         `json:"severity"`
	Status       string         `json:"status"`
	SubjectKind  string         `json:"subject_kind"`
	SubjectID    string         `json:"subject_id"`
	OutletID     string         `json:"outlet_id"`
	BusinessDate string         `json:"business_date,omitempty"`
	DetectedAt   string         `json:"detected_at"`
	ResolvedAt   string         `json:"resolved_at,omitempty"`
	ResolvedBy   string         `json:"resolved_by_ref,omitempty"`
	DetailKey    string         `json:"detail_key"`
	Params       map[string]any `json:"params,omitempty"`
}

type ExceptionPage struct {
	Items      []Exception `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

type OpenCheckCount struct {
	OutletID   string `json:"outlet_id"`
	OpenChecks int    `json:"open_checks"`
	AsOf       string `json:"as_of"`
}

type CancellationRow struct {
	Kind          string      `json:"kind"`
	OrderLineID   string      `json:"order_line_id"`
	CheckID       string      `json:"check_id"`
	ItemName      string      `json:"item_name"`
	QtyMilli      int64       `json:"qty_milli"`
	ListValue     money.Money `json:"list_value"`
	ReasonCodeID  string      `json:"reason_code_id,omitempty"`
	MemberID      string      `json:"member_id,omitempty"`
	Wastage       bool        `json:"wastage"`
	KitchenStatus string      `json:"kitchen_status"`
	CancelledAt   string      `json:"cancelled_at"`
	BusinessDate  string      `json:"business_date"`
}

type CancellationTotals struct {
	Count        int         `json:"count"`
	ListValue    money.Money `json:"list_value"`
	WastageValue money.Money `json:"wastage_value"`
}

type CancellationReport struct {
	OutletID   string             `json:"outlet_id"`
	From       string             `json:"from"`
	To         string             `json:"to"`
	Basis      string             `json:"basis"`
	Items      []CancellationRow  `json:"items"`
	Totals     CancellationTotals `json:"totals"`
	NextCursor string             `json:"next_cursor,omitempty"`
}

type KotLogRow struct {
	KotID        string `json:"kot_id"`
	KotNo        string `json:"kot_no"`
	Kind         string `json:"kind"`
	StationID    string `json:"station_id,omitempty"`
	CheckID      string `json:"check_id"`
	LineCount    int    `json:"line_count"`
	SentAt       string `json:"sent_at"`
	MemberID     string `json:"member_id,omitempty"`
	DeviceID     string `json:"device_id,omitempty"`
	Printed      bool   `json:"printed"`
	Handover     string `json:"handover,omitempty"`
	ReprintCount int    `json:"reprint_count"`
	BusinessDate string `json:"business_date"`
}

type KotLogReport struct {
	OutletID   string      `json:"outlet_id"`
	From       string      `json:"from"`
	To         string      `json:"to"`
	Basis      string      `json:"basis"`
	Items      []KotLogRow `json:"items"`
	NextCursor string      `json:"next_cursor,omitempty"`
}

type KitchenTimeRow struct {
	StationID           string `json:"station_id,omitempty"`
	BusinessDate        string `json:"business_date"`
	LineCount           int    `json:"line_count"`
	MedianPrepSeconds   int64  `json:"median_prep_seconds"`
	P95PrepSeconds      int64  `json:"p95_prep_seconds"`
	CancelledAfterStart int    `json:"cancelled_after_start"`
}

type KitchenTimeReport struct {
	OutletID   string           `json:"outlet_id"`
	From       string           `json:"from"`
	To         string           `json:"to"`
	Basis      string           `json:"basis"`
	Items      []KitchenTimeRow `json:"items"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type PresentLogRow struct {
	CheckID      string      `json:"check_id"`
	PresentCount int         `json:"present_count"`
	ContentHash  string      `json:"content_hash"`
	Total        money.Money `json:"total"`
	Mode         string      `json:"mode"`
	MemberID     string      `json:"member_id,omitempty"`
	DeviceID     string      `json:"device_id,omitempty"`
	PresentedAt  string      `json:"presented_at"`
	ChangesAfter int         `json:"changes_after"`
	Billed       bool        `json:"billed"`
	BusinessDate string      `json:"business_date"`
}

type PresentLogTotals struct {
	Presents            int `json:"presents"`
	PresentsWithoutBill int `json:"presents_without_bill"`
}

type PresentLogReport struct {
	OutletID   string           `json:"outlet_id"`
	From       string           `json:"from"`
	To         string           `json:"to"`
	Basis      string           `json:"basis"`
	Items      []PresentLogRow  `json:"items"`
	Totals     PresentLogTotals `json:"totals"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type OpenCheckRow struct {
	CheckID            string      `json:"check_id"`
	SeatingID          string      `json:"seating_id,omitempty"`
	TableName          string      `json:"table_name,omitempty"`
	Label              string      `json:"label"`
	OpenedBusinessDate string      `json:"opened_business_date"`
	CarriedOver        bool        `json:"carried_over"`
	Presented          bool        `json:"presented"`
	LineCount          int         `json:"line_count"`
	ListTotal          money.Money `json:"list_total"`
}

type OpenChecksTotals struct {
	Count     int         `json:"count"`
	ListTotal money.Money `json:"list_total"`
}

type OpenChecksReport struct {
	OutletID   string           `json:"outlet_id"`
	From       string           `json:"from"`
	To         string           `json:"to"`
	Basis      string           `json:"basis"`
	Items      []OpenCheckRow   `json:"items"`
	Totals     OpenChecksTotals `json:"totals"`
	NextCursor string           `json:"next_cursor,omitempty"`
}

type EntityDigest struct {
	Entity   string `json:"entity"`
	RowCount int    `json:"row_count"`
	Hash     string `json:"hash"`
}

type ProjectionDigest struct {
	OutletID     string         `json:"outlet_id"`
	BusinessDate string         `json:"business_date"`
	Algorithm    string         `json:"algorithm"`
	StateHash    string         `json:"state_hash"`
	Entities     []EntityDigest `json:"entities"`
	ComputedAt   string         `json:"computed_at"`
}

// ---- helpers ----

// ts formats an instant as RFC 3339 UTC with milliseconds; text that does not parse is returned as is.
func ts(s string) string {
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil {
		return s
	}
	return t.UTC().Format("2006-01-02T15:04:05.000Z")
}

func tsTime(t time.Time) string { return t.UTC().Format("2006-01-02T15:04:05.000Z") }

// lineValue is quantity (thousandths) times the unit price plus modifiers, rounded half up to the paisa.
func lineValue(l repository.Row, qtyMilli int64) int64 {
	return (qtyMilli*(intOf(l.Fields["unit_price_paise"])+intOf(l.Fields["modifier_delta_paise"])) + 500) / 1000
}

// scopeOutlets resolves the outlets a caller may read for a permission, narrowed by X-Outlet-Id.
// "" in the result means every outlet of the tenant. An outlet outside the scope is 404.
func scopeOutlets(ctx context.Context, perm, header string) ([]string, error) {
	scope, err := auth.ScopeFor(ctx, perm)
	if err != nil {
		return nil, err
	}
	if header != "" {
		if !scope.Contains(header) {
			return nil, notFound()
		}
		return []string{header}, nil
	}
	if scope.All() {
		return []string{""}, nil
	}
	return scope.IDs(), nil
}

// oneOutlet resolves exactly one outlet for a report.
func oneOutlet(ctx context.Context, perm, header string) (string, error) {
	o, err := scopeOutlets(ctx, perm, header)
	if err != nil {
		return "", err
	}
	if len(o) != 1 || o[0] == "" {
		return "", apperr.New(apperr.Validation, "outlet.required", "X-Outlet-Id names the outlet of this report")
	}
	return o[0], nil
}

// dateRange validates from/to; when both are absent it is the outlet's latest business date.
func (s *Service) dateRange(tx repository.Tx, outlet, from, to string) (string, string, error) {
	const day = "2006-01-02"
	if from == "" && to == "" {
		latest := ""
		rows, err := tx.Rows(repository.RowQuery{Entity: "check", Outlet: outlet})
		if err != nil {
			return "", "", err
		}
		for _, r := range rows {
			if r.BusinessDate > latest {
				latest = r.BusinessDate
			}
		}
		if latest == "" {
			latest = clock.InIST(s.Clock.Now()).Format(day)
		}
		return latest, latest, nil
	}
	if from == "" {
		from = to
	}
	if to == "" {
		to = from
	}
	f, e1 := time.Parse(day, from)
	t, e2 := time.Parse(day, to)
	if e1 != nil || e2 != nil || t.Before(f) || t.Sub(f) > 92*24*time.Hour {
		return "", "", invalid("range.invalid", "from and to must be dates, to not before from and at most 92 days after")
	}
	return from, to, nil
}

type cursorDoc struct {
	After string `json:"after"`
}

func encodeCursor(after string) string {
	b, _ := json.Marshal(cursorDoc{After: after})
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeCursor(c string) (string, error) {
	if c == "" {
		return "", nil
	}
	b, err := base64.RawURLEncoding.DecodeString(c)
	var d cursorDoc
	if err != nil || json.Unmarshal(b, &d) != nil {
		return "", invalid("cursor.invalid", "the cursor is not valid")
	}
	return d.After, nil
}

func clampLimit(limit int) (int, error) {
	switch {
	case limit == 0:
		return 50, nil
	case limit < 0:
		return 0, invalid("limit.invalid", "limit must be at least 1")
	case limit > 200:
		return 200, nil
	}
	return limit, nil
}

// page returns the items after the cursor, at most limit, in the order of key. next is the cursor of the following page.
func page[T any](items []T, key func(T) string, cursor string, limit int) (out []T, next string, err error) {
	after, err := decodeCursor(cursor)
	if err != nil {
		return nil, "", err
	}
	lim, err := clampLimit(limit)
	if err != nil {
		return nil, "", err
	}
	sort.SliceStable(items, func(i, j int) bool { return key(items[i]) < key(items[j]) })
	start := 0
	if after != "" {
		start = sort.Search(len(items), func(i int) bool { return key(items[i]) > after })
	}
	end := start + lim
	if end >= len(items) {
		return append([]T{}, items[start:]...), "", nil
	}
	return append([]T{}, items[start:end]...), encodeCursor(key(items[end-1])), nil
}

// canon is canonical JSON for digests: sorted keys, no whitespace, integers only.
func canon(v any) string {
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
	return string(bytes.TrimSpace(b.Bytes()))
}

func itoa(n int) string { return strconv.Itoa(n) }

// secondsBetween is the whole seconds from a to b (RFC 3339), false when either is missing or b is before a.
func secondsBetween(a, b string) (int64, bool) {
	ta, e1 := time.Parse(time.RFC3339Nano, a)
	tb, e2 := time.Parse(time.RFC3339Nano, b)
	if e1 != nil || e2 != nil || tb.Before(ta) {
		return 0, false
	}
	return int64(tb.Sub(ta).Seconds()), true
}

func jsonMarshal(v any) (json.RawMessage, error) { return json.Marshal(v) }
