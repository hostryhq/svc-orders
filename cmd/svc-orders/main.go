// Command svc-orders is the Hostry orders service: it projects accepted sync ops of orders-owned
// entities (seatings, orders, lines, KOTs, checks) into its own database, runs the exception
// detectors and serves the read side. Wiring only.
//
//	svc-orders serve     (default) run the HTTP API, the bus consumer and the outbox relay
//	svc-orders migrate   apply the SQL migrations and exit
//
// Configuration is environment only (12-factor); secrets are referenced by file path. See README.md.
package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	auth "github.com/hostryhq/go-auth"
	"github.com/hostryhq/go-common/bus"
	"github.com/hostryhq/go-common/clock"
	"github.com/hostryhq/go-common/config"
	"github.com/hostryhq/go-common/health"
	"github.com/hostryhq/go-common/ids"
	"github.com/hostryhq/go-common/inbox"
	"github.com/hostryhq/go-common/logx"
	"github.com/hostryhq/go-common/outbox"
	"github.com/hostryhq/go-common/restore"
	"github.com/hostryhq/svc-orders/api"
	"github.com/hostryhq/svc-orders/events"
	"github.com/hostryhq/svc-orders/repository/postgres"
	"github.com/hostryhq/svc-orders/service"
)

type cfg struct {
	Listen, DSN, DBRole, Environment string
	Migrate                          bool
	NATSURL, NATSSeedFile            string
	TenantJWKS, TenantIssuer         string
	PlatformDSN                      string
	TenantAudience, ServiceKeys      string
	SelfName                         string
	KeyRefresh                       time.Duration
	Service                          service.Config
}

func loadConfig(lookup config.Lookup) (cfg, error) {
	l := config.New(lookup)
	c := cfg{
		Listen: l.String("ORDERS_LISTEN", ":8083"), DSN: l.Required("ORDERS_DB_DSN"), DBRole: l.String("ORDERS_DB_ROLE", ""),
		Environment: l.String("HOSTRY_ENVIRONMENT", "dev"), Migrate: l.Bool("ORDERS_MIGRATE", false),
		NATSURL: l.String("HOSTRY_NATS_URL", ""), NATSSeedFile: l.String("HOSTRY_NATS_SEED_FILE", ""),
		TenantJWKS: l.Required("ORDERS_TENANT_JWKS"), TenantIssuer: l.String("ORDERS_TENANT_ISSUER", "hostry-identity"),
		TenantAudience: l.String("ORDERS_TENANT_AUDIENCE", "hostry-tenant"), ServiceKeys: l.Required("ORDERS_SERVICE_KEYS"),
		PlatformDSN: l.Required("ORDERS_PLATFORM_DSN"),
		SelfName:    l.String("ORDERS_SELF_NAME", "orders"), KeyRefresh: l.Duration("ORDERS_KEY_REFRESH", 5*time.Minute),
	}
	c.Service = service.Config{KotStaleAfter: l.Duration("ORDERS_KOT_STALE_AFTER", 15*time.Minute), GraceCancelAllowance: l.Int("ORDERS_GRACE_CANCEL_ALLOWANCE", 10)}
	if c.NATSURL != "" && c.NATSSeedFile == "" {
		return c, errors.New("HOSTRY_NATS_SEED_FILE is required with HOSTRY_NATS_URL")
	}
	return c, l.Err()
}

func main() {
	cmd := "serve"
	if len(os.Args) > 1 {
		cmd = os.Args[1]
	}
	log := logx.New(os.Stderr, slog.LevelInfo, "orders")
	c, err := loadConfig(os.LookupEnv)
	if cmd == "migrate" && c.DSN != "" {
		err = nil // migrating needs only the database
	}
	if err != nil {
		log.Error("invalid configuration", "error", err)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	switch cmd {
	case "migrate":
		err = migrate(ctx, c)
	case "serve":
		err = serve(ctx, c, log)
	default:
		err = fmt.Errorf("unknown command %q", cmd)
	}
	if err != nil {
		log.Error("exit", "error", err)
		os.Exit(1)
	}
}

func migrate(ctx context.Context, c cfg) error {
	st, err := postgres.Open(ctx, c.DSN, "")
	if err != nil {
		return err
	}
	defer st.Close()
	return postgres.Migrate(ctx, st.DB())
}

// keyLoader loads a JWKS document from a URL or a file path.
type keyLoader struct{ src string }

func (k keyLoader) Load(ctx context.Context) (auth.KeyDoc, error) {
	var b []byte
	if strings.HasPrefix(k.src, "http://") || strings.HasPrefix(k.src, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, k.src, nil)
		if err != nil {
			return auth.KeyDoc{}, err
		}
		resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
		if err != nil {
			return auth.KeyDoc{}, err
		}
		defer resp.Body.Close()
		if b, err = io.ReadAll(io.LimitReader(resp.Body, 1<<20)); err != nil {
			return auth.KeyDoc{}, err
		}
	} else {
		var err error
		if b, err = os.ReadFile(k.src); err != nil {
			return auth.KeyDoc{}, err
		}
	}
	keys, err := auth.ParseJWKS(b)
	return auth.KeyDoc{Keys: keys}, err
}

func serve(ctx context.Context, c cfg, log *slog.Logger) error {
	st, err := postgres.Open(ctx, c.DSN, c.DBRole)
	if err != nil {
		return err
	}
	defer st.Close()
	if c.Migrate {
		if err := postgres.Migrate(ctx, st.DB()); err != nil {
			return err
		}
	}
	if err := st.CheckRole(ctx); err != nil {
		return err
	}
	cat, err := service.NewCatalogue()
	if err != nil {
		return err
	}
	keys := auth.NewCachedKeySet(keyLoader{c.TenantJWKS}, log)
	if err := keys.Refresh(ctx); err != nil {
		return fmt.Errorf("load signing keys: %w", err)
	}
	go keys.Run(ctx, c.KeyRefresh)
	svcDoc, err := keyLoader{c.ServiceKeys}.Load(ctx)
	if err != nil {
		return fmt.Errorf("load service keys: %w", err)
	}
	svc := service.New(st, clock.System, ids.New, c.Service, log)
	hc := health.New(2 * time.Second)
	hc.Add("postgres", st.Ping)
	h := api.New(api.Deps{Svc: svc, Cat: cat, Clock: clock.System, Log: log, Health: hc,
		Tenant:   &auth.Verifier{Keys: keys, Issuer: c.TenantIssuer, Audience: c.TenantAudience, Realm: auth.RealmTenant},
		Services: &auth.ServiceVerifier{Registry: auth.NewStaticKeySet(svcDoc), Self: c.SelfName}})

	// ADR-0002 A2.4: the restore guard settles the epoch before the service reports ready and before
	// the outbox relay starts. Orders keeps no state a restore invalidates beyond what op replay
	// rebuilds (apply is idempotent on op identity), so the hook only reports.
	guard := restore.NewGuard(restore.NewReader(c.PlatformDSN), restore.NewSQLState(st.DB(), true), log)
	guard.OnRestore("orders-report-restore", func(_ context.Context, from string, to restore.Epoch) error {
		log.Warn("restore epoch changed: projection converges by op replay", "from", from, "to", to.EpochID)
		return nil
	})
	hc.Add("restore", guard.Check)
	if err := guard.Run(ctx); err != nil {
		log.Error("restore guard not settled; service stays not ready", "error", err)
		go func() {
			for guard.Run(ctx) != nil {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Second):
				}
			}
		}()
	}
	if c.NATSURL != "" {
		if err := startBus(ctx, c, st, svc, log, guard.Done()); err != nil {
			return err
		}
	} else {
		log.Warn("HOSTRY_NATS_URL is not set: no ops are consumed and events stay in the outbox")
	}
	srv := &http.Server{Addr: c.Listen, Handler: h, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	errc := make(chan error, 1)
	go func() { errc <- srv.ListenAndServe() }()
	log.Info("orders listening", "addr", c.Listen, "environment", c.Environment)
	select {
	case err := <-errc:
		return err
	case <-ctx.Done():
	}
	sctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	return srv.Shutdown(sctx)
}

// startBus runs the outbox relay (exceptions out) and the durable consumer of sync ops (ops in).
func startBus(ctx context.Context, c cfg, st *postgres.Store, svc *service.Service, log *slog.Logger, ready <-chan struct{}) error {
	nc, err := bus.Connect(c.NATSURL, "orders", c.NATSSeedFile)
	if err != nil {
		return err
	}
	pub, err := bus.NewPublisher(nc)
	if err != nil {
		return err
	}
	cons, err := bus.BindConsumer(ctx, nc, "sync", events.ConsumerName)
	if err != nil {
		return err
	}
	relay := &outbox.Relay{DB: st.DB(), Sink: pub, LockKey: 7102102, Log: log}
	sub := &inbox.Subscriber{DB: st.DB(), Consumer: events.ConsumerName, Handle: events.Handler(svc, st), Log: log,
		OnPark: func(consumer, eventID, reason string) {
			log.Error("sync op event parked", "consumer", consumer, "event_id", eventID)
		}}
	go func() {
		defer nc.Close()
		select { // no relay and no consumer before the restore guard settled
		case <-ready:
		case <-ctx.Done():
			return
		}
		go func() {
			if err := sub.Run(ctx, cons.Source(2*time.Second)); err != nil && !errors.Is(err, context.Canceled) {
				log.Error("sync op consumer stopped", "error", err)
			}
		}()
		if err := relay.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
			log.Error("outbox relay stopped", "error", err)
		}
	}()
	return nil
}
