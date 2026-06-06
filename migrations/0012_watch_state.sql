-- Source-side change-detection (observation collapse).
--
-- Until now every source fire (~25 sources, every ~5 min) unconditionally
-- appended a source_fires ledger row AND wrote an observation, even though
-- nearly every tick is byte-identical to the prior one (a web check returning
-- 200 yet again). Both tables grew unboundedly for no signal. The fix writes
-- an observation only when the source's STATE changes, and collapses the
-- per-tick "we checked" bookkeeping into a single overwritten row per source.
--
-- watch_state is that overwritten row: exactly one per source_ref, rewritten
-- in place each tick, so it is fixed-size (~25 rows) and never grows.
--   last_checked_at  -- bumped EVERY fire; the heartbeat belfry's wedge
--                       detection and health's per-source last-fire now read
--                       (replaces source_fires' max(fired_at)).
--   last_state       -- the comparison signature of the most recently WRITTEN
--                       observation. An unchanged tick leaves this alone; a
--                       changed tick advances it. NULL until the first write
--                       (or after a fail-safe write whose signature could not
--                       be computed).
--   last_outcome     -- "ok" | "check_failed" of the most recent fire.
--   last_changed_at  -- when last_state last advanced (the last real
--                       transition), not merely when we last checked.
--   last_observation_id -- the observation written on that last transition.
-- Timestamps are the clock's millisecond ISO-Z strings, matching every other
-- table; updated_at is clock-stamped by the writer (no wall-clock DEFAULT) so
-- a FakeClock sim keeps a single coherent clock across the row.
CREATE TABLE watch_state (
    source_ref TEXT PRIMARY KEY,
    last_checked_at TEXT NOT NULL,
    last_state TEXT,
    last_outcome TEXT,
    last_changed_at TEXT,
    last_observation_id INTEGER,
    updated_at TEXT NOT NULL
);

-- source_fires is gone: nothing reads its history. Every former reader
-- (health's per-source last-fire, belfry's wedge detection, the timeline)
-- now reads watch_state.last_checked_at, and a state transition surfaces as
-- the observation it writes -- the timeline already renders observations.
-- The production DB starts fresh on deploy, so no data migration is needed;
-- the chain stays append-only and correct (0001 creates source_fires, this
-- migration drops it). Drop the index explicitly before the table for intent;
-- DROP TABLE would cascade it anyway.
DROP INDEX idx_source_fires_source_fired_at;
DROP TABLE source_fires;
