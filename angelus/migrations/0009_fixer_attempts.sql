-- Fixer attempt ledger (B11). The in-daemon fixer registry binds a remediation
-- handler to a live condition (an open internal incident, an unhealthy channel)
-- and runs it under guardrails: at most max_attempts within a rolling window,
-- with a minimum backoff between attempts. The dispatcher persists every
-- attempt here so the guardrail survives a daemon restart -- a crash-looping
-- fixer must not get a fresh attempt budget every time the daemon comes back,
-- the same fail-safe reasoning belfry's restart-log guard uses (B12).
--
-- condition_key identifies the condition by its logical IDENTITY, not by
-- episode: for open_internal_incident it is source:type:entity (matching the
-- one-open-per-entity unique index in 0001), so each distinct live condition a
-- fixer binds to gets its own budget, but an instance that clears and later
-- recurs with the same identity SHARES the budget of the prior episode for as
-- long as those attempts remain inside the window. That is deliberate and the
-- safe direction: a flapping dependency cannot earn an unlimited stream of
-- remediations by clearing and re-opening. The cost is that a genuinely new
-- failure episode can go unremediated until the window slides past the old
-- attempts -- acceptable, because the condition itself stays loud via belfry
-- and `angelus health` the whole time; the fixer giving up is not the alarm.
--
-- Append-only; rows are never updated. The dispatcher reads attempts within
-- the window to count against max_attempts and the latest attempt to enforce
-- backoff. No pruning: the table is small (one row per actual remediation
-- attempt, which the guardrails cap), and keeping history makes the daily
-- digest's fixer_actions and any postmortem able to reconstruct what fired.
CREATE TABLE fixer_attempts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    fixer_name TEXT NOT NULL,
    condition_key TEXT NOT NULL,
    attempted_at TEXT NOT NULL,
    outcome TEXT NOT NULL
);

CREATE INDEX idx_fixer_attempts_lookup
    ON fixer_attempts (fixer_name, condition_key, attempted_at);
