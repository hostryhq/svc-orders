// Package service is the business logic of svc-orders: the projection of synced ops into rows
// (generic apply engine, ADR-0014 R1), the exception detectors, and the read side (checks, reports,
// digest). It has no HTTP and no SQL; persistence is behind repository.Store.
package service

import (
	"context"
	"crypto/sha256"
	"fmt"
	"log/slog"
	"time"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/clock"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/svc-orders/repository"
)

type catalogueEntry struct {
	Key, Realm, Service                                                string
	Sensitive, StrongOnly, SupportViewable, SupportActable, OwnerGrant bool
}

// NewCatalogue builds the go-auth permission catalogue from the generated entries.
func NewCatalogue() (*auth.Catalogue, error) {
	ps := make([]auth.Permission, 0, len(catalogueEntries))
	for _, e := range catalogueEntries {
		ps = append(ps, auth.Permission{Name: e.Key, Owner: e.Service, Sensitive: e.Sensitive, StrongOnly: e.StrongOnly,
			SupportViewable: e.SupportViewable, SupportActable: e.SupportActable, OwnerGrant: e.OwnerGrant})
	}
	return auth.NewCatalogue(ps...)
}

// Config holds the provisional rules the specs leave to configuration (docs/notes.md).
type Config struct {
	// KotStaleAfter is the default age at which a KOT that first arrives is a late ticket, used
	// when the outlet has no outlet_settings row (ADR-0014 A1.6: 15 minutes).
	KotStaleAfter time.Duration
	// GraceCancelAllowance is the number of grace cancels one member may make per business date
	// before cancel_allowance_exceeded is raised (provisional, notes N5).
	GraceCancelAllowance int
}

// DefaultConfig returns the documented defaults.
func DefaultConfig() Config { return Config{KotStaleAfter: 15 * time.Minute, GraceCancelAllowance: 10} }

// Service is the orders service.
type Service struct {
	Repo  repository.Store
	Clock clock.Clock
	NewID func() ids.ID
	Cfg   Config
	Log   *slog.Logger
}

// New returns a Service with defaults filled in.
func New(repo repository.Store, clk clock.Clock, newID func() ids.ID, cfg Config, log *slog.Logger) *Service {
	if clk == nil {
		clk = clock.System
	}
	if newID == nil {
		newID = ids.New
	}
	if cfg.KotStaleAfter == 0 {
		cfg.KotStaleAfter = DefaultConfig().KotStaleAfter
	}
	if cfg.GraceCancelAllowance == 0 {
		cfg.GraceCancelAllowance = DefaultConfig().GraceCancelAllowance
	}
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Service{Repo: repo, Clock: clk, NewID: newID, Cfg: cfg, Log: log}
}

// Error helpers: typed, mapped to RFC 9457 by the api layer.
func notFound() error { return apperr.New(apperr.NotFound, "resource.not_found", "resource not found") }

func invalid(code, detail string) error { return apperr.New(apperr.Validation, code, detail) }

// detID builds a deterministic UUIDv7-shaped id from parts (exception ids: the same finding is the
// same row on every replay and on every restored copy).
func detID(parts ...string) string {
	h := sha256.New()
	for _, p := range parts {
		fmt.Fprintf(h, "%d:%s|", len(p), p)
	}
	b := h.Sum(nil)
	b[6] = 0x70 | (b[6] & 0x0f)
	b[8] = 0x80 | (b[8] & 0x3f)
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

var _ = context.Background
