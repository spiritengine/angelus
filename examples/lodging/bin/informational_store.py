#!/usr/bin/env python3
"""The shared informational inbox: a JSONL store any system drops updates into.

This is the producer front door for the informational lane (migration 0017): a
one-shot content delivery -- a seed, a heads-up, a "CI flaked twice
overnight" -- that reaches Patrick in the daily email under "Updates" and opens
NO incident. A producer adds an item by calling the `inform` CLI (or appending a
record line directly); the `informational_drip` source drains the inbox and a
single generic triager fans each record out into one informational finding.

Record schema (one JSON object per line):
    id          stable, producer-chosen; the dedup key (with `source`). Two
                drops of the same (source, id) deliver once.
    source      the producing system's label (e.g. "build-bot", "pitch").
    title       the headline shown in the email.
    body        optional longer text under the headline.
    severity    optional, DISPLAY ONLY -- it never escalates, pages, or routes.
    created_at  ISO-8601 UTC stamp (the CLI fills this).
    emitted_at  null until the drip drains it; then the stamp. Drained-once.

Mirrors reminder_store.py's robustness (advisory flock across the whole
read-modify-write, atomic temp+fsync+replace rewrite, verbatim preservation of
torn rows, drain-gated corruption signal) -- see that module for the rationale
behind each. The one structural difference: the drip drains ALL pending records
per tick into ONE batch observation (the triager emits one finding per record),
so a burst of N drops does not serialise behind a one-per-tick reader.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import secrets
import tempfile
from pathlib import Path
from typing import Any

DEFAULT_STORE = "state/informational.jsonl"
SOURCE_REF = "scheduled/informational"

# How long a drained (emitted) record is retained before prune drops it. Long
# enough to inspect recent context, short enough that the per-tick full re-parse
# stays cheap. Safe to prune because ids never recur (an emitted+pruned record
# cannot re-deliver: its per-finding dedup is moot once it is gone, and a fresh
# drop would carry a new id).
RETAIN_EMITTED = 500  # records, not days: a count cap on the emitted tail.

# Keys every record must carry to be a usable item. A JSON line that parses but
# lacks any of these is a torn row: preserved verbatim + counted malformed, never
# reaching the drain.
REQUIRED_KEYS = ("id", "source", "title", "emitted_at")

IDLE_STATE = "idle"
CORRUPT_STATE = "store-corrupt"
CORRUPT_TYPE = "store_corrupt"
CORRUPT_ENTITY = "informational-store"


def new_id() -> str:
    """A short random id (the producer may supply its own instead)."""
    return secrets.token_hex(8)


def utcnow() -> str:
    from datetime import UTC, datetime

    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def make_record(
    *,
    source: str,
    title: str,
    body: str | None = None,
    severity: str | None = None,
    record_id: str | None = None,
    created_at: str | None = None,
) -> dict[str, Any]:
    """Build a well-formed record (the CLI's add path)."""
    if not isinstance(source, str) or not source.strip():
        raise ValueError("source must be a non-empty string")
    if not isinstance(title, str) or not title.strip():
        raise ValueError("title must be a non-empty string")
    return {
        "id": str(record_id or new_id()),
        "source": source,
        "title": title,
        "body": body,
        "severity": severity or "low",
        "created_at": created_at or utcnow(),
        "emitted_at": None,
    }


def _is_record(obj: Any) -> bool:
    if not isinstance(obj, dict):
        return False
    for key in REQUIRED_KEYS:
        if key not in obj:
            return False
    if not isinstance(obj.get("id"), str) or not obj["id"].strip():
        return False
    if not isinstance(obj.get("source"), str) or not obj["source"].strip():
        return False
    if not isinstance(obj.get("title"), str) or not obj["title"].strip():
        return False
    emitted = obj.get("emitted_at")
    if emitted is not None and not isinstance(emitted, str):
        return False
    return True


def _row_has_lone_surrogate(obj: Any) -> bool:
    """True if any string in the row carries a lone surrogate (torn UTF-8)."""
    if isinstance(obj, str):
        try:
            obj.encode("utf-8")
        except UnicodeEncodeError:
            return True
        return False
    if isinstance(obj, dict):
        return any(_row_has_lone_surrogate(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_row_has_lone_surrogate(v) for v in obj)
    return False


@contextlib.contextmanager
def store_lock(path: str | Path):
    """Advisory exclusive flock across a whole read-modify-write (see
    reminder_store.store_lock for the lost-update rationale)."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    lock_path = p.with_name(p.name + ".lock")
    fd = os.open(str(lock_path), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX)
        yield
    finally:
        fcntl.flock(fd, fcntl.LOCK_UN)
        os.close(fd)


def _parse_store(path: str | Path) -> tuple[list[dict[str, Any]], list[str]]:
    """Read the store into (valid records, malformed lines kept VERBATIM)."""
    p = Path(path)
    if not p.exists():
        return [], []
    records: list[dict[str, Any]] = []
    malformed: list[str] = []
    with open(p, encoding="utf-8", errors="surrogateescape") as handle:
        for raw in handle:
            line = raw.rstrip("\n")
            if not line.strip():
                continue
            try:
                obj = json.loads(line)
                if _is_record(obj) and not _row_has_lone_surrogate(obj):
                    records.append(obj)
                else:
                    malformed.append(line)
            except Exception:
                malformed.append(line)
    return records, malformed


def load_records(path: str | Path) -> list[dict[str, Any]]:
    return _parse_store(path)[0]


def write_store(path: str | Path, records: list[dict[str, Any]]) -> None:
    """Rewrite the whole store atomically (temp+fsync+replace), preserving torn
    rows verbatim. Caller holds store_lock. Mirrors reminder_store.write_store."""
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    _, malformed = _parse_store(p)
    body = "".join(json.dumps(r, sort_keys=True) + "\n" for r in records)
    body += "".join(line + "\n" for line in malformed)
    fd, tmp_name = tempfile.mkstemp(dir=str(p.parent), prefix=p.name + ".", suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8", errors="surrogateescape") as handle:
            handle.write(body)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp_name, p)
        _fsync_dir(p.parent)
    except BaseException:
        with contextlib.suppress(OSError):
            os.unlink(tmp_name)
        raise


def _fsync_dir(directory: Path) -> None:
    with contextlib.suppress(OSError):
        dir_fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


def add_record(path: str | Path, record: dict[str, Any]) -> None:
    """Append a record under lock (load->append->atomic rewrite)."""
    with store_lock(path):
        records = load_records(path)
        records.append(record)
        write_store(path, records)


def drain_all_emitting(
    path: str | Path, emit, now: str | None = None
) -> tuple[int, int]:
    """Drain EVERY un-emitted record in ONE locked critical section: emit a
    single batch observation carrying them all, then stamp emitted_at on each.
    Returns (drained_count, malformed_count) -- the caller emits the
    idle/corruption signal only when drained_count is 0.

    AT-LEAST-ONCE within the drip, mirroring reminder_store.drain_one_emitting:
    emit BEFORE the stamp persists, so a crash INSIDE the drip (between the
    stdout print and write_store) exits non-zero, the daemon discards the
    stdout, the store is unchanged, and the next tick re-drains -- the re-emit
    collapses against the unchanged batch `state`, and the per-finding
    informational dedup (source:id) drops anything already delivered.

    RESIDUAL daemon-side window (inherited from the drip pattern, NOT closed
    here): the drip can exit cleanly with the stamp persisted, yet the daemon be
    hard-killed before it commits the observation -- the records read emitted,
    so they never re-emit. Because this drains ALL pending records at once, that
    one ill-timed crash loses the whole tick's batch, not a single item. This is
    a best-effort, no-incident lane, so that is accepted (the same window the
    reminder/seed drips carry); a producer that needs stronger delivery should
    re-drop with the same stable id (the dedup makes it idempotent). Batching N
    records under one lock means a burst does not serialise behind a
    one-per-tick reader.
    """
    with store_lock(path):
        records, malformed = _parse_store(path)
        pending = [r for r in records if r.get("emitted_at") is None]
        if not pending:
            return 0, len(malformed)
        stamp = now or utcnow()
        emit(batch_observation(pending))
        for record in pending:
            record["emitted_at"] = stamp
        write_store(path, records)
        return len(pending), len(malformed)


def prune_emitted(path: str | Path, retain: int = RETAIN_EMITTED) -> int:
    """Drop all but the most-recent `retain` emitted records. Returns removed
    count. Un-emitted records are never pruned. Runs under store_lock."""
    with store_lock(path):
        records = load_records(path)
        emitted = [r for r in records if r.get("emitted_at") is not None]
        if len(emitted) <= retain:
            return 0
        # Keep the newest `retain` by emitted_at; drop the oldest.
        emitted_sorted = sorted(emitted, key=lambda r: str(r.get("emitted_at")))
        drop = set(id(r) for r in emitted_sorted[: len(emitted) - retain])
        kept = [r for r in records if id(r) not in drop]
        removed = len(records) - len(kept)
        if removed:
            write_store(path, kept)
        return removed


def batch_observation(records: list[dict[str, Any]]) -> dict[str, Any]:
    """ONE observation carrying every drained record. `state` is a stable
    signature of the pending set, so an unchanged pending set collapses (no
    duplicate observation) and a changed set is a real change.

    The signature is SOURCE-NAMESPACED (`source:id`), EXACTLY as discriminating
    as the finding dedup_key the handler builds. A bare-id signature would let
    two batches whose id-SETS match but whose sources differ collapse on the
    same `state` -- the daemon would suppress the second observation, the
    triager would never run, and the source-namespaced finding dedup that
    protects this case would never be reached (it sits below the collapse gate).
    Generic ids shared across producers (`status`, `nightly`, `summary`) are
    exactly that collision, so the collapse token must carry the source too."""
    keys = sorted(f"{r.get('source')}:{r['id']}" for r in records)
    return {
        "source_ref": SOURCE_REF,
        "entity": "informational-batch",
        "type": "batch",
        "records": records,
        "state": "batch:" + ",".join(keys),
    }


def idle_observation() -> dict[str, Any]:
    return {
        "source_ref": SOURCE_REF,
        "entity": "informational",
        "type": IDLE_STATE,
        "state": IDLE_STATE,
    }


def corruption_observation(malformed_count: int) -> dict[str, Any]:
    """Drain-gated torn-store signal -- a real CONDITION (store_corrupt is NOT an
    informational type), so it opens an incident and is the operator's to fix."""
    return {
        "source_ref": SOURCE_REF,
        "entity": CORRUPT_ENTITY,
        "type": CORRUPT_TYPE,
        "severity": "high",
        "malformed_lines": int(malformed_count),
        "state": CORRUPT_STATE,
    }
