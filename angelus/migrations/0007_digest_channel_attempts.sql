-- Per-channel digest send attempt counter. The digest path attempts ONE
-- channel per cycle carrying a batch of finding_ids; the immediate path's
-- per-(pipe, finding_id) counter on pipe_queues would inflate the threshold
-- N-per-cycle on the digest path. This table tracks attempts independent
-- of how many findings the cycle carried, so the digest path can consume
-- the same channel_health threshold ladder (MAX_RETRY_ATTEMPTS) the
-- immediate path uses without double-counting. Daemon-restart-scoped to
-- match channel_health (cleared at startup).
CREATE TABLE digest_channel_attempts (
    pipe TEXT NOT NULL,
    channel TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (pipe, channel)
);
