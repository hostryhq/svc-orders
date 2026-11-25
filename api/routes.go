// Package api is the controller layer: generated route registration, authentication and
// authorisation wiring (go-auth), parameter decoding and response writing. It holds no business
// rule; every decision is made in package service.
package api

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"time"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/clock"
	"github.com/hostryhq/go-common/health"
	"github.com/hostryhq/go-common/httpx"
	"github.com/hostryhq/go-common/logx"
	"github.com/hostryhq/svc-orders/internal/gen"
	"github.com/hostryhq/svc-orders/service"
)

type routeSpec struct {
	Pattern, Op, Permission string
	OutletScoped            bool
	Callers                 []string
}

// Deps are the collaborators of the HTTP layer.
type Deps struct {
	Svc      *service.Service
	Cat      *auth.Catalogue
	Tenant   *auth.Verifier // tenant-realm access tokens
	Services *auth.ServiceVerifier
	Clock    clock.Clock
	Log      *slog.Logger
	Health   *health.Handler
}

// Handler implements gen.ServerInterface.
type Handler struct {
	d Deps
	s *service.Service
}

func init() { // once, at start-up: the global is read by every request
	apperr.RequestIDFunc = func(r *http.Request) string { return logx.RequestID(r.Context()) }
}

// New builds the HTTP handler: every route goes through go-auth's Guard with the policy the spec
// declares, and a route missing from the generated table panics at start-up.
func New(d Deps) http.Handler {
	h := &Handler{d: d, s: d.Svc}
	mux := http.NewServeMux()
	g := &auth.Guard{Catalogue: d.Cat, Tenant: d.Tenant, Services: d.Services, Clock: d.Clock, Log: d.Log}
	specs := map[string]routeSpec{}
	for _, r := range routeTable {
		specs[r.Pattern] = r
	}
	gr := &guardRouter{g: g, mux: mux, specs: specs, seen: map[string]bool{}}
	gen.HandlerWithOptions(h, gen.StdHTTPServerOptions{
		BaseRouter: gr,
		ErrorHandlerFunc: func(w http.ResponseWriter, r *http.Request, err error) {
			apperr.WriteProblem(w, r, apperr.New(apperr.Validation, "validation.failed", "the request is invalid"))
		},
	})
	for p := range specs {
		if !gr.seen[p] {
			panic("api: route " + p + " is in the policy table but not in the generated server")
		}
	}
	if d.Health != nil {
		d.Health.Register(mux)
	}
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		apperr.WriteProblem(w, r, apperr.New(apperr.NotFound, "resource.not_found", "resource not found"))
	})
	return httpx.Chain(mux, httpx.RequestID, httpx.Recover, h.accessLog)
}

// RouteCount is the number of operations the table registers.
func RouteCount() int { return len(routeTable) }

// Operations lists every operationId of the table.
func Operations() []string {
	out := make([]string, 0, len(routeTable))
	for _, r := range routeTable {
		out = append(out, r.Op)
	}
	return out
}

// accessLog logs method, path and status only: no bodies, headers or query strings.
func (h *Handler) accessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		sw := &statusWriter{ResponseWriter: w, status: 200}
		next.ServeHTTP(sw, r)
		if h.d.Log != nil {
			h.d.Log.LogAttrs(r.Context(), slog.LevelInfo, "request", slog.String("method", r.Method), slog.String("path", r.URL.Path),
				slog.Int("status", sw.status), slog.Duration("took", time.Since(start)))
		}
	})
}

type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(c int) { w.status = c; w.ResponseWriter.WriteHeader(c) }

func (w *statusWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

type guardRouter struct {
	g     *auth.Guard
	mux   *http.ServeMux
	specs map[string]routeSpec
	seen  map[string]bool
}

func (gr *guardRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) { gr.mux.ServeHTTP(w, r) }

func (gr *guardRouter) HandleFunc(pattern string, fn func(http.ResponseWriter, *http.Request)) {
	spec, ok := gr.specs[pattern]
	if !ok {
		panic("api: route " + pattern + " has no entry in the generated policy table")
	}
	gr.seen[pattern] = true
	inner := http.HandlerFunc(fn)
	if len(spec.Callers) > 0 { // internal: the service token is the only credential (go-auth ServiceOnly)
		gr.g.Handle(gr.mux, pattern, auth.Policy{OperationID: spec.Op, ServiceOnly: true, Internal: true, Callers: spec.Callers}, inner)
		return
	}
	gr.g.Handle(gr.mux, pattern, auth.Policy{OperationID: spec.Op, Permission: spec.Permission}, inner)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	b, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "internal error", http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = w.Write(b)
}

func respond[T any](w http.ResponseWriter, r *http.Request, v T, err error) {
	if err != nil {
		apperr.WriteProblem(w, r, err)
		return
	}
	writeJSON(w, http.StatusOK, v)
}
