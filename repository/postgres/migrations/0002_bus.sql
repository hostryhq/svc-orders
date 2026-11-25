-- Transactional outbox (ADR-0011 B4) and inbox / parked events (B5), copied from go-common
-- outbox.Schema and inbox.Schema. Service-internal tables.
CREATE TABLE IF NOT EXISTS outbox (
  id           uuid        PRIMARY KEY,
  seq          bigint      GENERATED ALWAYS AS IDENTITY,
  type         text        NOT NULL,
  subject      text        NOT NULL,
  tenant_id    text        NOT NULL,
  key          text        NOT NULL DEFAULT '',
  key_seq      bigint      NOT NULL DEFAULT 0,
  payload      bytea       NOT NULL,
  created_at   timestamptz NOT NULL DEFAULT now(),
  published_at timestamptz
);
CREATE INDEX IF NOT EXISTS outbox_unpublished ON outbox (seq) WHERE published_at IS NULL;
CREATE INDEX IF NOT EXISTS outbox_published_at ON outbox (published_at) WHERE published_at IS NOT NULL;

CREATE TABLE IF NOT EXISTS inbox (
  consumer     text        NOT NULL,
  event_id     uuid        NOT NULL,
  processed_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (consumer, event_id)
);
CREATE TABLE IF NOT EXISTS event_parked (
  id          bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  consumer    text        NOT NULL,
  event_id    text        NOT NULL,
  subject     text        NOT NULL,
  payload     bytea       NOT NULL,
  error       text        NOT NULL,
  attempts    bigint      NOT NULL,
  parked_at   timestamptz NOT NULL DEFAULT now(),
  resolved_at timestamptz
);

GRANT SELECT, INSERT, UPDATE, DELETE ON outbox, inbox, event_parked TO orders_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO orders_app;
