CREATE TABLE observation_triage (
    observation_id INTEGER NOT NULL REFERENCES observations (id),
    triager_name TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('processing', 'success', 'failed', 'skipped')),
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (observation_id, triager_name)
);

CREATE INDEX idx_observation_triage_status
    ON observation_triage (status);
