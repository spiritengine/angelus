-- B15 dead-letter-queue. A dispatch that exhausts its per-finding redelivery
-- ladder undelivered is a TERMINAL, operator-actionable state: the content was
-- abandoned and must surface loudly and be replayable, not sit indistinguishable
-- from a transient failure. Today that terminal state reuses status='failed' on
-- pipe_queues -- the same word the dispatches table uses for a single transient
-- per-channel send failure. The two mean opposite things (give-up vs retry-soon),
-- and sharing the name makes "what has angelus permanently given up on" un-
-- queryable. This migration gives the give-up state its own name, 'dead_letter'.
--
-- On pipe_queues, status='failed' is ONLY ever written by
-- catalog.record_pipe_finding_undelivered at exhaustion (verified: it is the sole
-- writer of pipe_queues status='failed' in the codebase -- every other 'failed'
-- write targets observations/observation_triage/dispatches, different tables). So
-- migrating every existing pipe_queues 'failed' row to 'dead_letter' is a clean,
-- lossless rename of that one meaning, not a reinterpretation of mixed data.
--
-- SQLite cannot ALTER a CHECK constraint in place, so we use the table-rebuild
-- pattern (the precedent is migrations/0004_daily_digest.sql): build the new
-- table with the widened+narrowed CHECK, copy rows through (rewriting 'failed' ->
-- 'dead_letter' in flight), drop the old table, rename, and recreate the indexes.
-- 'failed' is deliberately DROPPED from the new CHECK rather than kept alongside
-- 'dead_letter': it now has no writer, so excluding it makes the schema assert the
-- invariant "pipe_queues never carries 'failed' again" -- a later accidental
-- 'failed' write would fail loudly at the constraint instead of silently
-- reintroducing the ambiguous state this migration exists to remove.

CREATE TABLE pipe_queues_new (
    finding_id INTEGER NOT NULL REFERENCES findings (id),
    pipe TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'dispatched', 'dead_letter', 'suppressed')),
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
    finding_id, pipe,
    CASE status WHEN 'failed' THEN 'dead_letter' ELSE status END,
    dispatched_at, attempts, last_error,
    created_at, updated_at, next_attempt_at
FROM pipe_queues;

DROP TABLE pipe_queues;
ALTER TABLE pipe_queues_new RENAME TO pipe_queues;

CREATE INDEX idx_pipe_queues_pipe_status
    ON pipe_queues (pipe, status);

CREATE INDEX idx_pipe_queues_retry
    ON pipe_queues (pipe, status, next_attempt_at);
