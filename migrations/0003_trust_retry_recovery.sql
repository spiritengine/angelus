ALTER TABLE observation_triage
    ADD COLUMN attempt INTEGER NOT NULL DEFAULT 1;

ALTER TABLE observation_triage
    ADD COLUMN next_attempt_at TEXT;

CREATE INDEX idx_observation_triage_retry
    ON observation_triage (status, next_attempt_at);

ALTER TABLE pipe_queues
    ADD COLUMN next_attempt_at TEXT;

CREATE INDEX idx_pipe_queues_retry
    ON pipe_queues (pipe, status, next_attempt_at);

CREATE TABLE channel_health (
    channel TEXT PRIMARY KEY,
    status TEXT NOT NULL CHECK (status IN ('healthy', 'unhealthy')),
    last_error TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))
);
