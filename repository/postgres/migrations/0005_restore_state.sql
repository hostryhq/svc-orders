-- go-common restore.StateSchema: the epoch this database last handled (ADR-0002 A2.4). Rolled back
-- with the database, which is how a restore is noticed.
CREATE TABLE IF NOT EXISTS restore_state (
  id         integer     PRIMARY KEY CHECK (id = 1),
  epoch_id   text        NOT NULL,
  handled_at timestamptz NOT NULL
);
GRANT SELECT, INSERT, UPDATE ON restore_state TO orders_app;
