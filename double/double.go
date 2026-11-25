// Package double is the in-memory test double of the orders service. It is not a second
// implementation: it composes the same service and api layers as the real service over the
// in-memory repository, a fake clock, deterministic ids and an ephemeral token issuer, so
// behaviour cannot drift from the real service. The shared contract suite (contract/) runs
// against both, and other services' tests import it. Failure injection (a fixed status, delay
// or dropped connection for a route) is available through Fail.
package double

import (
	"context"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-auth/authtest"
	"github.com/hostryhq/go-common/apperr"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/testkit"
	"github.com/hostryhq/svc-orders/api"
	"github.com/hostryhq/svc-orders/repository"
	"github.com/hostryhq/svc-orders/repository/memory"
	"github.com/hostryhq/svc-orders/service"
)

// Options configure a double.
type Options struct {
	// Store replaces the in-memory repository (the integration tests run the same assembly over PostgreSQL).
	Store repository.Store
	// Config overrides the service configuration.
	Config *service.Config
	// Log receives the request log (default: discarded).
	Log *slog.Logger
}

// Failure is an injected fault for requests whose path starts with a prefix.
type Failure struct {
	Status int           // respond with this status and a problem body (0: pass through)
	Delay  time.Duration // wait before answering
	Drop   bool          // close the connection without a response
}

// Double is a running in-memory orders service.
type Double struct {
	Handler  http.Handler
	Svc      *service.Service
	Store    repository.Store
	Memory   *memory.Store // nil when Options.Store replaced the in-memory repository
	Clock    *testkit.FakeClock
	Issuer   *authtest.Issuer
	Services *authtest.ServiceRegistry

	mu   sync.Mutex
	fail map[string]Failure
}

// New builds a double whose clock stands at the instant of the spec's examples.
func New(o Options) *Double {
	clk := testkit.NewClock()
	clk.Set(Start)
	d := &Double{Clock: clk, fail: map[string]Failure{}}
	d.Store = o.Store
	if d.Store == nil {
		d.Memory = memory.New()
		d.Store = d.Memory
	}
	cfg := service.DefaultConfig()
	if o.Config != nil {
		cfg = *o.Config
	}
	gen := clk.IDs()
	d.Svc = service.New(d.Store, clk, gen.New, cfg, o.Log)
	cat, err := service.NewCatalogue()
	if err != nil {
		panic(err)
	}
	d.Issuer = authtest.NewTenantIssuer(clk)
	d.Services = authtest.NewServiceRegistry(clk, "billing", "sync")
	inner := api.New(api.Deps{Svc: d.Svc, Cat: cat, Tenant: d.Issuer.Verifier(), Services: d.Services.Verifier("orders"), Clock: clk, Log: o.Log})
	d.Handler = d.faults(inner)
	return d
}

// Fail injects a fault for every request whose path starts with prefix; a zero Failure clears it.
func (d *Double) Fail(prefix string, f Failure) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if f == (Failure{}) {
		delete(d.fail, prefix)
		return
	}
	d.fail[prefix] = f
}

func (d *Double) faults(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		d.mu.Lock()
		var hit *Failure
		for p, f := range d.fail {
			if strings.HasPrefix(r.URL.Path, p) {
				f := f
				hit = &f
			}
		}
		d.mu.Unlock()
		if hit != nil {
			if hit.Delay > 0 {
				select {
				case <-time.After(hit.Delay):
				case <-r.Context().Done():
					return
				}
			}
			if hit.Drop {
				if hj, ok := w.(http.Hijacker); ok {
					if c, _, err := hj.Hijack(); err == nil {
						_ = c.Close()
					}
				}
				return
			}
			if hit.Status != 0 {
				k := apperr.Internal
				if hit.Status == http.StatusServiceUnavailable {
					k = apperr.Unavailable
				}
				apperr.WriteProblem(w, r, apperr.New(k, "injected.failure", "injected failure"))
				return
			}
		}
		next.ServeHTTP(w, r)
	})
}

// Token mints a tenant-realm user token for a member with the given permissions at the given outlets
// (none: every outlet of the tenant).
func (d *Double) Token(tenant, member string, outlets []string, perms ...string) string {
	return d.Issuer.Mint(authtest.Spec{Type: auth.TypeUser, Tenant: ids.ID(tenant), Subject: member, Grants: authtest.Grants(outlets, perms...)})
}

// ServiceToken mints a service token of a caller (for example "billing") acting for a tenant.
func (d *Double) ServiceToken(caller, tenant string) string {
	tok, err := d.Services.Signer(caller).Sign("orders", tenant)
	if err != nil {
		panic(err)
	}
	return tok
}

// Ingest projects ops for a tenant, the way the bus consumer does. The arrival time of an op is its
// own timestamp, so seeded history carries no late tickets.
func (d *Double) Ingest(ctx context.Context, tenant string, ops ...service.Op) error {
	return d.Store.InTx(ctx, tenant, func(tx repository.Tx) error {
		for _, op := range ops {
			arrived := Start
			if t, err := time.Parse(time.RFC3339Nano, op.At); err == nil {
				arrived = t
			}
			if err := d.Svc.Apply(tx, op, arrived); err != nil {
				return err
			}
		}
		return nil
	})
}

// PutExceptions writes exceptions directly (fixtures with a fixed id).
func (d *Double) PutExceptions(ctx context.Context, tenant string, xs ...repository.Exception) error {
	return d.Store.InTx(ctx, tenant, func(tx repository.Tx) error {
		for _, x := range xs {
			if err := tx.PutException(x); err != nil {
				return err
			}
		}
		return nil
	})
}

// SeedWorld implements contracttest.Options.SeedWorld: it empties the in-memory store (the
// double owns it) and builds the named example world for the example tenant.
func (d *Double) SeedWorld(ctx context.Context, world string) error {
	if d.Memory != nil {
		d.Memory.Reset()
	}
	return SeedInto(ctx, d, world)
}

// SeedInto builds a world into the double's store without emptying it first.
func SeedInto(ctx context.Context, d *Double, world string) error {
	ops, xs, err := WorldOps(world)
	if err != nil {
		return err
	}
	if err := d.Ingest(ctx, Tenant, ops...); err != nil {
		return err
	}
	return d.PutExceptions(ctx, Tenant, xs...)
}

// ExampleHeader is the header set the contract suite sends: a manager of the example outlet and the
// billing service's token, both for the example tenant.
func (d *Double) ExampleHeader() http.Header {
	h := http.Header{}
	h.Set("Authorization", "Bearer "+d.Token(Tenant, Member, []string{Outlet}, "order.edit", "exceptions.view", "report.view.outlet"))
	h.Set("Hostry-Service-Token", d.ServiceToken("billing", Tenant))
	return h
}

// SpecOutletExample is the X-Outlet-Id example of common/components.yaml, which names an outlet other
// than the example world's (docs/notes.md N8).
const SpecOutletExample = "0190a1b2-c3d4-7e5f-8a9b-0c1d2e3f4a5c"

type outletRewrite struct{ next http.RoundTripper }

func (t outletRewrite) RoundTrip(r *http.Request) (*http.Response, error) {
	if r.Header.Get("X-Outlet-Id") == SpecOutletExample {
		r = r.Clone(r.Context())
		r.Header.Set("X-Outlet-Id", Outlet)
	}
	return t.next.RoundTrip(r)
}

// ExampleClient is an HTTP client for the contract suite that sends the example world's outlet where
// the specification's header example names another one.
func ExampleClient() *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: outletRewrite{http.DefaultTransport}}
}
