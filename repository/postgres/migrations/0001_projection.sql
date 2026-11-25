-- svc-orders schema, forward-only. The projection of synced entities (rows derived from op effects),
-- applied-op and void bookkeeping, and detector exceptions. Every table carries tenant_id and is
-- isolated by row-level security on app.tenant_id (ADR-0003 section 6); outlet_id is the outlet scope.

CREATE TABLE IF NOT EXISTS apply_counter (
  tenant_id text   NOT NULL PRIMARY KEY,
  n         bigint NOT NULL
);

CREATE TABLE IF NOT EXISTS applied_ops (
  tenant_id text   NOT NULL,
  op_ref    text   NOT NULL,
  apply_seq bigint NOT NULL,
  PRIMARY KEY (tenant_id, op_ref)
);

CREATE TABLE IF NOT EXISTS voided_ops (
  tenant_id text NOT NULL,
  op_ref    text NOT NULL,
  PRIMARY KEY (tenant_id, op_ref)
);

CREATE TABLE IF NOT EXISTS effects (
  tenant_id  text        NOT NULL,
  op_ref     text        NOT NULL,
  idx        int         NOT NULL,
  entity     text        NOT NULL,
  id         text        NOT NULL,
  outlet_id  text        NOT NULL,
  kind       text        NOT NULL,
  fields     jsonb       NOT NULL,
  hlc        text        NOT NULL,
  origin     text        NOT NULL,
  seq        bigint      NOT NULL,
  at         text        NOT NULL,
  actor      text        NOT NULL,
  apply_seq  bigint      NOT NULL,
  arrived    timestamptz NOT NULL,
  PRIMARY KEY (tenant_id, op_ref, idx)
);
CREATE INDEX IF NOT EXISTS effects_row ON effects (tenant_id, entity, id);

CREATE TABLE IF NOT EXISTS projection_rows (
  tenant_id     text    NOT NULL,
  entity        text    NOT NULL,
  id            text    NOT NULL,
  outlet_id     text    NOT NULL,
  parent_id     text    NOT NULL DEFAULT '',
  business_date text    NOT NULL DEFAULT '',
  tag           text    NOT NULL DEFAULT '',
  fields        jsonb   NOT NULL,
  meta          jsonb   NOT NULL,
  gone          boolean NOT NULL DEFAULT false,
  PRIMARY KEY (tenant_id, entity, id)
);
CREATE INDEX IF NOT EXISTS projection_rows_day ON projection_rows (tenant_id, entity, outlet_id, business_date);
CREATE INDEX IF NOT EXISTS projection_rows_parent ON projection_rows (tenant_id, entity, parent_id) WHERE parent_id <> '';
CREATE INDEX IF NOT EXISTS projection_rows_tag ON projection_rows (tenant_id, entity, outlet_id, tag) WHERE tag <> '';

CREATE TABLE IF NOT EXISTS exceptions (
  tenant_id     text        NOT NULL,
  id            text        NOT NULL,
  outlet_id     text        NOT NULL,
  kind          text        NOT NULL,
  severity      text        NOT NULL,
  status        text        NOT NULL,
  subject_kind  text        NOT NULL,
  subject_id    text        NOT NULL,
  business_date text        NOT NULL DEFAULT '',
  detected_at   timestamptz NOT NULL,
  resolved_at   timestamptz,
  resolved_by   text        NOT NULL DEFAULT '',
  detail_key    text        NOT NULL,
  params        jsonb       NOT NULL,
  PRIMARY KEY (tenant_id, id)
);
CREATE INDEX IF NOT EXISTS exceptions_outlet ON exceptions (tenant_id, outlet_id, status, kind);

DO $$ BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'orders_app') THEN
    CREATE ROLE orders_app NOLOGIN;
  END IF;
END $$;

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['apply_counter','applied_ops','voided_ops','effects','projection_rows','exceptions'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (tenant_id = current_setting(''app.tenant_id'', true)) WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))', t);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I TO orders_app', t);
  END LOOP;
END $$;
