// Package repository holds the persistence contract of svc-orders: the projection of synced
// entities (rows derived from op effects), the applied-op and void bookkeeping, detector
// exceptions and the transactional outbox. The service depends on these interfaces only; the
// in-memory implementation (repository/memory) backs the test double and unit tests, the
// PostgreSQL one (repository/postgres) backs the real service.
//
// Every operation is scoped to the tenant of the transaction (Store.InTx). Rows of outlet-scoped
// entities carry their outlet; list queries name the outlet or none (all outlets of the tenant),
// and single-row reads are authorised by the service against the stored row's outlet, never the
// request's (go-auth Require).
package repository

import (
	"context"
	"time"

	"github.com/hostryhq/go-common/events"
)

// Effect is one mutation of one row, kept for good (the "candidates" of ADR-0002 S6): a row is a
// pure function of its effects, so two nodes holding the same ops and voids hold the same row
// whatever the arrival order.
type Effect struct {
	Op       string // origin/stream/seq
	Idx      int    // position of the mutation inside the op
	Entity   string
	ID       string
	OutletID string
	Kind     string // insert, set, delta, rank, del, undel
	Fields   map[string]any
	HLC      string // 16 hex characters; sorts like time
	Origin   string
	Seq      int64
	At       string // the op's own timestamp (device clock), RFC 3339
	Actor    string // member that recorded the op, when it has one
	ApplySeq int64  // arrival order at this service, per tenant
	Arrived  time.Time
}

// Stamp orders effects: HLC, then origin, then seq, then position in the op.
func (e Effect) Stamp() string { return StampOf(e.HLC, e.Origin, e.Seq, e.Idx) }

// StampOf builds the sortable stamp text.
func StampOf(hlc, origin string, seq int64, idx int) string {
	return hlc + "|" + origin + "|" + pad(seq, 12) + "|" + pad(int64(idx), 4)
}

func pad(n int64, w int) string {
	s := make([]byte, w)
	for i := w - 1; i >= 0; i-- {
		s[i] = byte('0' + n%10)
		n /= 10
	}
	return string(s)
}

// Meta is what the projection derives about a row beyond its fields.
type Meta struct {
	CreatedAt string `json:"created_at,omitempty"` // At of the earliest insert
	CreatedBy string `json:"created_by,omitempty"` // actor of that insert
	Origin    string `json:"origin,omitempty"`     // origin (device) of that insert
	ApplySeq  int64  `json:"apply_seq,omitempty"`  // arrival order of that insert
	Arrived   string `json:"arrived,omitempty"`    // arrival time of that insert, RFC 3339
	// Stamps holds the stamp of the winning effect of each lww field.
	Stamps map[string]string `json:"stamps,omitempty"`
	// RankAt holds, per rank field and value, the earliest At at which an effect set that value
	// (or a later one); RankStamp the earliest stamp.
	RankAt    map[string]map[string]string `json:"rank_at,omitempty"`
	RankStamp map[string]map[string]string `json:"rank_stamp,omitempty"`
}

// Row is the derived current state of one synced row.
type Row struct {
	Entity       string
	ID           string
	OutletID     string
	Parent       string // id of the row it hangs under (see service.parentOf)
	BusinessDate string // YYYY-MM-DD when the row (or its kind) carries one
	Tag          string // secondary key set by the projection (for example a check's status)
	Fields       map[string]any
	Meta         Meta
	Gone         bool // tombstoned
}

// RowKey names a row.
type RowKey struct{ Entity, ID string }

// RowQuery selects rows of one entity. Zero fields do not filter.
type RowQuery struct {
	Entity      string
	Outlet      string // "" means every outlet of the tenant
	IDs         []string
	Parents     []string
	Tag         string
	DateFrom    string // inclusive, on Row.BusinessDate; rows with no date are excluded when a bound is set
	DateTo      string
	IncludeGone bool
}

// Exception is a detector finding. It is derived, never edited: it closes itself when the
// detector no longer fires.
type Exception struct {
	ID           string
	OutletID     string
	Kind         string
	Severity     string
	Status       string // open, resolved, superseded
	SubjectKind  string
	SubjectID    string
	BusinessDate string
	DetectedAt   time.Time
	ResolvedAt   *time.Time
	ResolvedBy   string
	DetailKey    string
	Params       map[string]any
}

// ExceptionQuery selects exceptions.
type ExceptionQuery struct {
	Outlet       string // "" means every outlet of the tenant
	Status       string
	Kind         string
	SubjectKind  string
	SubjectIDs   []string
	KindsAndSubs bool // reserved
}

// Tx is a transaction on one tenant.
type Tx interface {
	Tenant() string

	// NextApplySeq returns the next arrival number of the tenant (1, 2, ...).
	NextApplySeq() (int64, error)
	OpApplied(ref string) (bool, error)
	MarkApplied(ref string, applySeq int64) error
	OpVoided(ref string) (bool, error)
	MarkVoided(ref string) error

	AddEffect(e Effect) error
	EffectsOf(entity, id string) ([]Effect, error)
	// RemoveEffectsOfOp deletes the effects of an op and returns the rows they touched.
	RemoveEffectsOfOp(ref string) ([]RowKey, error)

	PutRow(r Row) error
	DeleteRow(entity, id string) error
	GetRow(entity, id string) (Row, bool, error)
	Rows(q RowQuery) ([]Row, error)

	PutException(x Exception) error
	GetException(id string) (Exception, bool, error)
	Exceptions(q ExceptionQuery) ([]Exception, error)

	// Publish writes the event to the transactional outbox of this transaction (ADR-0011 B4).
	Publish(env events.Envelope) error
}

// Store opens tenant transactions.
type Store interface {
	// InTx runs fn in a transaction on tenant. An error rolls back; nil commits.
	InTx(ctx context.Context, tenant string, fn func(Tx) error) error
	Ping(ctx context.Context) error
}
