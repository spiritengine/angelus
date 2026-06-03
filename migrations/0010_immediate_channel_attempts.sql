-- Per-channel immediate-path send attempt counter. The immediate path's
-- pipe_queues.attempts counter is keyed (finding_id, pipe) -- a PER-FINDING
-- redelivery ladder. After B7 fans internal/* findings to every configured
-- channel, N channels drive that one per-finding row, so it can no longer
-- double as per-CHANNEL health escalation: two failing channels inflate it +N
-- per drain, and the first channel to succeed marks the row terminal
-- ('dispatched') -- so a co-fanned failing channel's failures never reach the
-- threshold and its channel_unhealthy escalation never fires (B7 fell-r1
-- Finding 3, the substrate for B13 transport-failover).
--
-- Channel health is a property of the CHANNEL, not the finding, so this table
-- is keyed (pipe, channel) -- deliberately NOT (pipe, channel, finding_id).
-- The counter accumulates a channel's failures ACROSS findings and resets on a
-- success, exactly like digest_channel_attempts (migration 0007). A per-finding
-- key would reset on every finding and never escalate: the B7 fan retries a
-- finding only until >=1 channel delivers it, so a persistently-down co-fanned
-- channel is rarely re-attempted against the SAME finding -- its failures are
-- spread one apiece across many DIFFERENT findings. N CONSECUTIVE channel
-- failures (across findings) cross the same MAX_RETRY_ATTEMPTS ladder the digest
-- and immediate paths share. Daemon-restart-scoped (cleared at startup) to match
-- channel_health and digest_channel_attempts.
CREATE TABLE immediate_channel_attempts (
    pipe TEXT NOT NULL,
    channel TEXT NOT NULL,
    attempts INTEGER NOT NULL DEFAULT 0,
    last_error TEXT,
    updated_at TEXT NOT NULL DEFAULT (strftime('%Y-%m-%dT%H:%M:%fZ', 'now')),
    PRIMARY KEY (pipe, channel)
);
