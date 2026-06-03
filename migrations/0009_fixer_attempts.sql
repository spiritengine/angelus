-- Fixer attempt ledger (B11). The in-daemon fixer registry binds a remediation
-- handler to a live condition (an open internal incident, an unhealthy channel)
-- and runs it under guardrails: at most max_attempts within a rolling window,
-- with a minimum backoff between attempts. The dispatcher persists every
-- attempt here so the guardrail survives a daemon restart -- a crash-looping
-- fixer must not get a fresh attempt budget every time the daemon comes back,
-- the same fail-safe reasoning belfry's restart-log guard uses (B12).
--
-- condition_key identifies the specific condition instance a fixer is acting
-- on (e.g. open_internal_incident:internal/dep:dependency_unhealthy:<entity>),
-- so attempts accumulate per-condition rather than per-fixer: one fixer bound
-- to a recurring condition class gets an independent budget for each distinct
-- live instance, and an instance that clears and recurs later is measured from
-- its own attempt history within the window.
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
