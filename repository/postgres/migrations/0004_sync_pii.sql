-- go-common sync.Schema additions (pii commitments and cloud-signed erasure, ADR-0014 A2.x): orders
-- owns pii fields on check and seating, so sync_pii holds their erasable values; salts go on
-- erasure and the commitments stay. Expand only.
ALTER TABLE sync_pii ADD COLUMN IF NOT EXISTS salts   jsonb NOT NULL DEFAULT '{}';
ALTER TABLE sync_pii ADD COLUMN IF NOT EXISTS commits jsonb NOT NULL DEFAULT '{}';
CREATE TABLE IF NOT EXISTS sync_pii_erased (
  tenant_id text   NOT NULL,
  origin    text   NOT NULL,
  stream    text   NOT NULL,
  seq       bigint NOT NULL,
  PRIMARY KEY (tenant_id, origin, stream, seq)
);
ALTER TABLE sync_pii_erased ENABLE ROW LEVEL SECURITY;
ALTER TABLE sync_pii_erased FORCE ROW LEVEL SECURITY;
DROP POLICY IF EXISTS tenant_isolation ON sync_pii_erased;
CREATE POLICY tenant_isolation ON sync_pii_erased USING (tenant_id = current_setting('app.tenant_id', true)) WITH CHECK (tenant_id = current_setting('app.tenant_id', true));
GRANT SELECT, INSERT, UPDATE, DELETE ON sync_pii_erased TO orders_app;
