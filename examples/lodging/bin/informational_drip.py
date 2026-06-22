#!/usr/bin/env python3
"""Informational inbox drip source.

Run by sources/scheduled/informational.yaml every tick. Drains EVERY un-emitted
record from the shared inbox into ONE batch observation (the generic
informational triager fans it out into one finding per record), stamping
emitted_at on each. Prints a stable idle observation when nothing is pending, or
a drain-gated corruption observation when the store has torn rows and nothing is
pending. See informational_store.py for the store schema and the batch rationale.

Before draining it prunes the oldest emitted records past the retention cap so
the per-tick full re-parse stays cheap.

Usage:
    informational_drip.py [STORE_PATH]

STORE_PATH defaults to $INFORMATIONAL_STORE, else ``state/informational.jsonl``
relative to cwd (the daemon's working directory is the lodging root, so that
resolves to <lodging>/state/informational.jsonl).
"""

from __future__ import annotations

import json
import os
import sys
from pathlib import Path

# informational_store lives beside this script under <lodging>/bin/.
sys.path.insert(0, str(Path(__file__).resolve().parent))

import informational_store  # noqa: E402 (path injected above)


def main() -> None:
    if len(sys.argv) > 1:
        store = sys.argv[1]
    else:
        store = os.environ.get("INFORMATIONAL_STORE", informational_store.DEFAULT_STORE)

    informational_store.prune_emitted(store)

    # drain_all_emitting emits the batch itself when records are pending. Only
    # when nothing was pending do we raise the idle/corruption signal -- a
    # pending item never waits behind a torn row (drain-gated corruption).
    drained, malformed = informational_store.drain_all_emitting(store, _emit)
    if drained:
        return
    if malformed:
        _emit(informational_store.corruption_observation(malformed))
    else:
        _emit(informational_store.idle_observation())


def _emit(observation: dict) -> None:
    json.dump(observation, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()


if __name__ == "__main__":
    main()
