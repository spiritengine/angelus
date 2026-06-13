CREATE TABLE source_fires (
    id INTEGER PRIMARY KEY,
    source_name TEXT NOT NULL,
    scheduled_at TEXT,
    fired_at TEXT NOT NULL,
    outcome TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_source_fires_source_fired_at
    ON source_fires (source_name, fired_at);

CREATE TABLE observations (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('writing', 'ready', 'failed', 'triage_failed')),
    body_ref TEXT,
    payload_hash TEXT,
    provenance TEXT,
    written_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_observations_source_created_at
    ON observations (source, created_at);

CREATE INDEX idx_observations_status
    ON observations (status);

CREATE TABLE findings (
    id INTEGER PRIMARY KEY,
    observation_id INTEGER REFERENCES observations (id),
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    entity TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    target_pipes TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('writing', 'ready', 'failed')),
    severity TEXT,
    body_ref TEXT,
    occurred_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_findings_observation_id
    ON findings (observation_id);

CREATE INDEX idx_findings_dedup_key
    ON findings (dedup_key);

CREATE INDEX idx_findings_status
    ON findings (status);

CREATE TABLE incidents (
    id INTEGER PRIMARY KEY,
    source TEXT NOT NULL,
    type TEXT NOT NULL,
    entity TEXT NOT NULL,
    dedup_key TEXT NOT NULL,
    opened_at TEXT NOT NULL,
    closed_at TEXT,
    status TEXT NOT NULL CHECK (status IN ('open', 'closed')),
    latest_finding_id INTEGER REFERENCES findings (id),
    state TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE UNIQUE INDEX idx_incidents_one_open_per_entity
    ON incidents (source, type, entity)
    WHERE status = 'open';

CREATE INDEX idx_incidents_dedup_key_status
    ON incidents (dedup_key, status);

CREATE TABLE triager_state (
    triager_name TEXT NOT NULL,
    source_name TEXT NOT NULL,
    state_blob TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (triager_name, source_name)
);

CREATE TABLE pipe_queues (
    finding_id INTEGER NOT NULL REFERENCES findings (id),
    pipe TEXT NOT NULL,
    status TEXT NOT NULL,
    dispatched_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (finding_id, pipe)
);

CREATE INDEX idx_pipe_queues_pipe_status
    ON pipe_queues (pipe, status);

CREATE TABLE dispatches (
    id INTEGER PRIMARY KEY,
    pipe TEXT NOT NULL,
    channel TEXT NOT NULL,
    finding_ids TEXT NOT NULL,
    status TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    dispatched_at TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE INDEX idx_dispatches_pipe_channel_created_at
    ON dispatches (pipe, channel, created_at);

CREATE TABLE dep_health (
    dep TEXT PRIMARY KEY,
    last_check_at TEXT,
    status TEXT NOT NULL,
    details TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE schedule_registry (
    source_name TEXT PRIMARY KEY,
    schedule_spec TEXT NOT NULL,
    next_fire_at TEXT,
    last_fire_at TEXT,
    status TEXT NOT NULL,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);

CREATE TABLE backoff_store (
    key TEXT PRIMARY KEY,
    attempt INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT,
    last_error TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
