-- Per-source check SLA (input side). The mirror of pipe_sla (0008), which
-- asserts each PIPE keeps delivering on cadence; this asserts each SOURCE keeps
-- being CHECKED on cadence. belfry's wedge detector reads a GLOBAL
-- max(last_checked_at): it fires only when the daemon stops checking ALL
-- sources, so one healthy source masks any stale subset (a single source whose
-- APScheduler job vanishes on a hot-reload edge or is dropped goes silent while
-- the other ~25 keep max() fresh, and belfry stays green). This table lets
-- belfry assert each source individually -- the founding "can't silently stop
-- watching" promise, generalized from all-sources to any-one-source.
--
-- The daemon persists each scheduled source's cadence here at startup (and on
-- hot reload) so belfry -- the out-of-band, pure-stdlib layer that cannot parse
-- the sources/*.yaml -- can read the contract read-only and ping DOWN
-- (alert-only, never restart) when one source's watch_state.last_checked_at
-- heartbeat lapses past its window.
--
-- max_interval_seconds is the source's cadence PLUS slack. The heartbeat bumps
-- on EVERY fire (observation collapse keeps the row but always advances
-- last_checked_at), so a live source's heartbeat age is at most ~one cadence
-- (just before its next fire) plus tiny execution/scheduler jitter; a stopped
-- source's age grows without bound. The daemon sets the window to
-- cadence + max(cadence, slack_floor): 2x cadence for normal cadences, with a
-- floor so a sub-floor (e.g. 30s) cadence still gets cadence+floor of slack and
-- cannot flap belfry on transient boot-burst scheduler jitter. 2x means "missed
-- an entire additional cycle" -- loose enough never to false-alarm a daily/4h/
-- 12h source that fires a few minutes late, tight enough to catch a stop within
-- one extra period. See AngelusDaemon._sync_source_sla for the exact policy.
--
-- Only interval-cadence sources are tracked here. A crontab-cadence source
-- (cadence string containing whitespace, e.g. '0 7 * * *') has no single
-- interval to convert; the daemon LOGs a warning naming each such source
-- (so the gap is visible, never silent) and leaves it to the global wedge
-- backstop rather than guessing a max-gap from an arbitrary crontab. Deriving a
-- conservative crontab max-gap bound is future work.
--
-- tracking_since is the baseline for a source that has NEVER fired: belfry
-- measures overdue against last_checked_at, or tracking_since when the source
-- has never been checked, so a freshly-registered source gets a full
-- max_interval of grace before it can be flagged instead of pinging DOWN the
-- instant it is registered. It is set once (the daemon's upsert preserves it,
-- updating only max_interval_seconds) and deliberately NOT moved on daemon
-- restart, so a stall that spans restarts is still caught.
CREATE TABLE source_sla (
    source_ref TEXT PRIMARY KEY,
    max_interval_seconds INTEGER NOT NULL,
    tracking_since TEXT NOT NULL
);
