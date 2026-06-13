CREATE TABLE pipe_queues_new (
    finding_id INTEGER NOT NULL REFERENCES findings (id),
    pipe TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'dispatched', 'failed', 'suppressed')),
    dispatched_at TEXT,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    created_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    next_attempt_at TEXT,
    PRIMARY KEY (finding_id, pipe)
);

INSERT INTO pipe_queues_new (
    finding_id, pipe, status, dispatched_at, attempts, last_error,
    created_at, updated_at, next_attempt_at
)
SELECT
    finding_id, pipe, status, dispatched_at, attempts, last_error,
    created_at, updated_at, next_attempt_at
FROM pipe_queues;

DROP TABLE pipe_queues;
ALTER TABLE pipe_queues_new RENAME TO pipe_queues;

CREATE INDEX idx_pipe_queues_pipe_status
    ON pipe_queues (pipe, status);

CREATE INDEX idx_pipe_queues_retry
    ON pipe_queues (pipe, status, next_attempt_at);

ALTER TABLE dispatches
    ADD COLUMN source TEXT;

CREATE INDEX idx_dispatches_channel_status_dispatched_at
    ON dispatches (channel, status, dispatched_at);

CREATE INDEX idx_dispatches_source_status_dispatched_at
    ON dispatches (source, status, dispatched_at);

CREATE TABLE pipe_state (
    pipe_name TEXT PRIMARY KEY,
    last_drain_at TEXT
);
