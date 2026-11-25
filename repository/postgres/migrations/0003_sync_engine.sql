-- Projection engine of go-common/sync (ADR-0014 A2.1, A2.4): candidates and voids replace the
-- effects and voided_ops tables, which stay unread until a later release drops them (expand/contract).
-- sync_candidates and sync_voids are copied from go-common sync.Schema; sync_pii is created for
-- parity with the library but orders owns no pii field. applied_ops gains the facts of the op that
-- the projection keeps beside the candidates (when it was recorded, by whom, when it arrived).

CREATE TABLE IF NOT EXISTS sync_candidates (
  tenant_id text   NOT NULL,
  entity    text   NOT NULL,
  row_id    text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  idx       int    NOT NULL,
  hlc       text   NOT NULL,
  kind      text   NOT NULL,
  fields    jsonb  NOT NULL DEFAULT '{}',
  prev      jsonb  NOT NULL DEFAULT '{}',
  PRIMARY KEY (tenant_id, entity, row_id, origin, stream, seq, idx)
);
CREATE INDEX IF NOT EXISTS sync_candidates_op ON sync_candidates (tenant_id, origin, stream, seq);
CREATE TABLE IF NOT EXISTS sync_voids (
  tenant_id text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  PRIMARY KEY (tenant_id, origin, stream, seq)
);
CREATE TABLE IF NOT EXISTS sync_pii (
  tenant_id text   NOT NULL,
  entity    text   NOT NULL,
  row_id    text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  fields    jsonb  NOT NULL,
  PRIMARY KEY (tenant_id, entity, row_id, origin, stream, seq)
);

ALTER TABLE applied_ops ADD COLUMN IF NOT EXISTS at        text        NOT NULL DEFAULT '';
ALTER TABLE applied_ops ADD COLUMN IF NOT EXISTS actor     text        NOT NULL DEFAULT '';
ALTER TABLE applied_ops ADD COLUMN IF NOT EXISTS outlet_id text        NOT NULL DEFAULT '';
ALTER TABLE applied_ops ADD COLUMN IF NOT EXISTS arrived   timestamptz NOT NULL DEFAULT now();

DO $$
DECLARE t text;
BEGIN
  FOREACH t IN ARRAY ARRAY['sync_candidates','sync_voids','sync_pii'] LOOP
    EXECUTE format('ALTER TABLE %I ENABLE ROW LEVEL SECURITY', t);
    EXECUTE format('ALTER TABLE %I FORCE ROW LEVEL SECURITY', t);
    EXECUTE format('DROP POLICY IF EXISTS tenant_isolation ON %I', t);
    EXECUTE format('CREATE POLICY tenant_isolation ON %I USING (tenant_id = current_setting(''app.tenant_id'', true)) WITH CHECK (tenant_id = current_setting(''app.tenant_id'', true))', t);
    EXECUTE format('GRANT SELECT, INSERT, UPDATE, DELETE ON %I TO orders_app', t);
  END LOOP;
END $$;
