-- Commitment ledger of pii fields (ADR-0014 A3.2): the projection digest covers, per pii field, the commitment of the write
-- that wins it, not the value. go-common keeps no candidate for a pii field, so orders records each verified commitment with
-- its write stamp and picks the winner when it derives the row; a commitment may arrive before its row. Commitments survive
-- erasure; no value is held here. Expand only.
CREATE TABLE IF NOT EXISTS pii_commitments (
  tenant_id text   NOT NULL,
  entity    text   NOT NULL,
  row_id    text   NOT NULL,
  field     text   NOT NULL,
  op_ref    text   NOT NULL,
  hlc       text   NOT NULL,
  origin    text   NOT NULL,
  seq       bigint NOT NULL,
  is_insert boolean NOT NULL,
  commitment text  NOT NULL,
  PRIMARY KEY (tenant_id, entity, row_id, field, op_ref)
);
ALTER TABLE pii_commitments ENABLE ROW LEVEL SECURITY;
ALTER TABLE pii_commitments FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON pii_commitments;
CREATE POLICY tenant_isolation ON pii_commitments USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
GRANT SELECT, INSERT, UPDATE, DELETE ON pii_commitments TO orders_app;
