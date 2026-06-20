#!/usr/bin/env python3
"""Reminder drip source command.

Run by sources/scheduled/reminders.yaml every tick. Drains the OLDEST DUE
unfired reminder (fire_date <= local today) from the store, stamping fired_at,
and prints ONE JSON observation for it; prints a stable idle observation when
nothing is due. See reminder_store.py for the store schema and the drip
rationale.

Before draining it PRUNES fired rows older than RETAIN_DAYS (safe because
reminder ids are random and never recur, so a pruned fired row cannot re-alert).
This keeps the store small so the per-tick full re-parse stays cheap.

Usage:
    reminder_drip.py [STORE_PATH]

STORE_PATH defaults to the REMINDER_STORE env var, else ``state/reminders.jsonl``
relative to cwd (the daemon's working directory is the lodging root, so that
resolves to <lodging>/state/reminders.jsonl).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# reminder_store lives beside this script under <lodging>/bin/; make it
# importable without depending on the lodging root being on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import reminder_store  # noqa: E402  (path injected above)


def main() -> None:
    if len(sys.argv) > 1:
        store = sys.argv[1]
    else:
        store = os.environ.get("REMINDER_STORE", reminder_store.DEFAULT_STORE)

    today = reminder_store.local_today()

    # Prune retired rows first (idempotent, cheap, no observation). Keeps the
    # store from growing without bound across the daemon's lifetime.
    reminder_store.prune_fired(store, today)

    # Drain a DUE reminder, emitting + stamping in ONE locked critical section
    # (drain_one_emitting). Only when none are due does a torn store raise its
    # corruption signal -- a parseable due reminder must never wait behind an
    # unparseable row. The single lock both:
    #   - emits THEN stamps for AT-LEAST-ONCE: a kill after the observation is
    #     captured but before the stamp commits leaves the reminder unfired, so
    #     the next tick re-emits it (the re-emit collapses against the unchanged
    #     `state`, or the catalog emission gate drops it). Stamping first would
    #     mark it fired-yet-unobserved -- a silently missed deadline.
    #   - serializes a concurrent `reminder done <id>`: it cannot delete the
    #     picked row between the emit and the stamp, which would surface a CANCELED
    #     reminder with no fired row.
    #
    # One residual loss window remains and CANNOT be closed lodging-side: a
    # SIGKILL AFTER the stamp's os.replace commits but BEFORE this process exits
    # cleanly. The stamp persisted, but the daemon discards a non-clean exit's
    # stdout, so the printed observation is dropped -- the reminder is
    # fired-but-unseen and never re-emits. The window is sub-millisecond. For a
    # reminder this means a silently missed deadline, so the engine follow-up that
    # closes it (incremental stdout capture / ack the observation before the
    # source process is reaped) matters MORE here than for Pitch; see the tender.
    observation, malformed = reminder_store.drain_one_emitting(store, today, _emit)
    if observation is None:
        if malformed:
            _emit(reminder_store.corruption_observation(malformed))
        else:
            _emit(reminder_store.idle_observation())


def _emit(observation: dict) -> None:
    json.dump(observation, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
