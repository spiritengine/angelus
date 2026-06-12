-- Terminal observation status: 'consumed'.
--
-- Until now a successfully-triaged observation stayed 'ready' forever
-- (mark_triage_success touches only observation_triage), so the `ready` set
-- equalled every observation ever written minus the rare triage_failed ones
-- -- monotonic and unbounded, and every status='ready' reader
-- (ready_observations_for, observations_pending_triage_count) paid for it.
-- 'consumed' is the missing exit: the daemon flips an observation out of
-- 'ready' once every lodged triager matching its source has a terminal
-- triage row (success, or failed with retries exhausted), and a periodic
-- sweep consumes observations whose source has no live triager once a grace
-- period has passed. The flip lives in catalog.consume_observation_if_terminal
-- and catalog.consume_observations_without_triager; this migration only
-- widens the CHECK so the new value is storable.
--
-- SQLite cannot ALTER a CHECK constraint, so the table is rebuilt: create the
-- new shape, copy rows, drop the old table, rename, recreate the indexes
-- (0001's two and 0013's covering index). The DROP runs with
-- PRAGMA foreign_keys=ON, so a database whose findings reference observation
-- rows would fail the implicit DELETE -- acceptable because the production DB
-- starts fresh on deploy (documented in migration 0012) and every test DB is
-- built fresh through this chain. Rename direction matters: renaming the OLD
-- table aside would drag findings' REFERENCES along with it (SQLite >= 3.25
-- rewrites child FKs on RENAME), so the old table is dropped in place and the
-- new one renamed into the vacated name, which findings' existing
-- REFERENCES observations (id) then resolves to.
CREATE TABLE observations_new (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('writing', 'ready', 'failed', 'triage_failed', 'consumed')),
    body_ref TEXT,
    payload_hash TEXT,
    provenance TEXT,
    written_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

INSERT INTO observations_new
    (id, source, status, body_ref, payload_hash, provenance, written_at, created_at, updated_at)
SELECT id, source, status, body_ref, payload_hash, provenance, written_at, created_at, updated_at
FROM observations;

DROP TABLE observations;

ALTER TABLE observations_new RENAME TO observations;

CREATE INDEX idx_observations_source_created_at
    ON observations (source, created_at);

CREATE INDEX idx_observations_status
    ON observations (status);

CREATE INDEX idx_observations_source_status_id
    ON observations (source, status, id);
