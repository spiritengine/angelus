#!/usr/bin/env python3
"""Pitch seed drip source command.

Run by sources/scheduled/pitch-seeds.yaml every tick. Drains the OLDEST
unemitted seed from the store (stamping emitted_at) and prints ONE JSON
observation for it; prints a stable idle observation when the store is empty or
fully drained. See seed_store.py for the store schema and the drip rationale
(brief-20260619-opi5 option C).

Usage:
    pitch_seed_drip.py [STORE_PATH]

STORE_PATH defaults to the PITCH_SEED_STORE env var, else
``state/pitch-seeds.jsonl`` relative to cwd (the daemon's working directory is
the lodging root, so that resolves to <lodging>/state/pitch-seeds.jsonl).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# seed_store lives beside this script under <lodging>/bin/; make it importable
# without depending on the lodging root being on sys.path.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import seed_store  # noqa: E402  (path injected above)


def main() -> None:
    if len(sys.argv) > 1:
        store = sys.argv[1]
    else:
        store = os.environ.get("PITCH_SEED_STORE", seed_store.DEFAULT_STORE)

    # Drain a GOOD seed first; only when none remain does a torn store raise its
    # corruption signal (a parseable seed must never wait behind an unparseable
    # row). malformed counts the torn lines preserved in the store.
    seed, malformed = seed_store.drip_view(store)
    if seed is None:
        if malformed:
            # No good seed pending but the store has torn rows: raise ONE
            # corruption alert (fixed entity -> catalog gate dedups to a single
            # incident; fixed state -> consecutive ticks collapse). The torn
            # bytes are preserved in the store (write_seeds carries them), so the
            # alert points at a recoverable line. The full close of the torn-line
            # gap (re-parsing/repair) is operator work; the drip's job is to make
            # it loud, which the discarded-stderr path never did.
            _emit(seed_store.corruption_observation(malformed))
        else:
            _emit(seed_store.idle_observation())
        return

    # Emit THEN stamp. Print (and FLUSH) the observation to the daemon's stdout
    # capture BEFORE persisting emitted_at, so the common crash window degrades to
    # at-least-once, not at-most-once: a kill after the observation is captured
    # but before mark_emitted's os.replace commits leaves the seed unstamped, so
    # the next tick re-emits it (the re-emit collapses against the unchanged
    # `state` signature, or the catalog emission gate drops it). Stamping first
    # would instead mark the seed emitted yet unobserved -- skipped forever.
    #
    # One residual loss window remains and CANNOT be closed lodging-side: a
    # SIGKILL AFTER mark_emitted's os.replace commits the stamp but BEFORE this
    # process exits cleanly. The stamp persisted, but the daemon discards a
    # non-clean exit's stdout, so the printed observation is dropped -- the seed
    # is stamped-but-unseen and never re-emits. The window is sub-millisecond
    # (between os.replace returning and process exit). Fully closing it needs
    # daemon cooperation (capture stdout incrementally / ack the observation
    # before the source process is reaped) -- an engine change, out of this
    # lodging-only scope. Recommended engine follow-up; see the tender.
    now = seed_store.utcnow()
    seed["emitted_at"] = now
    _emit(seed_store.seed_observation(seed))
    seed_store.mark_emitted(store, str(seed["id"]), now=now)


def _emit(observation: dict) -> None:
    json.dump(observation, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
