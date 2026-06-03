-- Per-pipe delivery SLA (B2). A pipe may declare an expected max interval
-- between successful deliveries (pipes/<name>.yaml `max_interval`); the daemon
-- persists it here at startup so belfry -- the out-of-band, pure-stdlib layer
-- that cannot parse YAML -- can read the contract read-only and assert each
-- pipe is actually delivering on cadence. This is the on-box, all-pipes
-- generalization of the off-box digest dead-man (which only covers the daily
-- pipe).
--
-- tracking_since is the baseline for a pipe that has NEVER delivered: belfry
-- measures overdue against the last successful dispatch, or tracking_since when
-- the pipe has never delivered, so a freshly-deployed pipe gets a full
-- max_interval of grace before it can be flagged, instead of pinging DOWN the
-- instant it is registered. It is set once (the daemon's upsert preserves it,
-- updating only max_interval_seconds) and deliberately NOT moved on daemon
-- restart, so a stall that spans restarts is still caught.
CREATE TABLE pipe_sla (
    pipe_name TEXT PRIMARY KEY,
    max_interval_seconds INTEGER NOT NULL,
    tracking_since TEXT NOT NULL
);
