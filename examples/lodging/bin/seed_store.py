#!/usr/bin/env python3
"""Pitch seed store -- the drip adapter's shared data layer.

Pitch (the daily ``pitch`` mill chain) is a long LLM scan that finds SEEDS:
events that changed the world in one of Patrick's areas of interest -- what
happened, where, and when. It reports the change, never a move to make on it.
Pitch produces several seeds per run. Angelus is the scheduling spine that gives
them a home, but a native source fire writes AT MOST ONE observation (collapsed
on a single ``state`` change-signature), so N seeds per run do not drop into the
one-observation-per-fire model.

The drip adapter (brief-20260619-opi5 option C) resolves that without touching
the engine: Pitch writes seeds to this store on its OWN schedule; a fast lodging
source (sources/scheduled/pitch-seeds.yaml) drains the OLDEST unemitted seed per
tick, stamping ``emitted_at``, and prints ONE observation whose ``state`` is the
seed id. Distinct seeds are distinct states -> one observation each; a fully
drained store prints a stable idle state that collapses to no observation. N
seeds drain over N ticks. This module is the read/append/drain layer the source
script (pitch_seed_drip.py) and the tests share.

Store format
------------
A JSONL file (one seed object per line) under the lodging root, conventionally
``state/pitch-seeds.jsonl``. The daemon's working directory IS the lodging root,
so the source command resolves ``state/pitch-seeds.jsonl`` relative to cwd.

Seed schema (every row)
-----------------------
- ``id``            stable hash of ``event``; the dedup key and the observation
                    ``state`` token. Identical event -> identical id -> never
                    alerts twice (see append_seed and the catalog emission gate).
- ``discovered_at`` ISO-8601 UTC millisecond timestamp (when Pitch found it).
                    Drip order is OLDEST discovered_at first.
- ``event``         what changed in the world (one line).
- ``source``        the primary URL for the change (may be empty).
- ``date``          when the change happened, as Pitch reported it (may be empty).
- ``emitted_at``    ``null`` until the drip source drains it, then the ISO-8601
                    UTC stamp of the tick that emitted it. A seed is drained
                    exactly once.

Out of scope here (deployed from the PRIVATE lodging repo, not this examples
tree): the live ``pitch`` schedule that WRITES to the store, the real interest
list, and the live state/ file. This repo ships the contract and the fixtures
only -- never real interests or seed data.
"""

from __future__ import annotations

import contextlib
import fcntl
import hashlib
import json
import os
import sys
import tempfile
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

# The drip source's ref (matches sources/scheduled/pitch-seeds.yaml ->
# scheduled/<stem>). The handler and tests key off this single definition.
SOURCE_REF = "scheduled/pitch-seeds"

# The collapse token an idle (fully-drained) tick prints. Distinct from any seed
# id (seed ids are 16-hex-char digests), so a seed-state -> idle transition
# always writes one observation and idle -> idle collapses.
IDLE_STATE = "idle"

# The corruption signal an unparseable store raises. ``state`` is FIXED (not the
# malformed count) so consecutive corrupt ticks collapse to no observation -- it
# does not loop. ``entity`` is fixed too, so the catalog emission gate (keyed on
# source+type+entity) opens ONE incident for a torn store: the operator gets
# exactly one alert even if the corruption observation is re-written later (after
# a good seed drains in between, the state is no longer consecutive). The torn
# bytes are PRESERVED across rewrites (see write_seeds), so the alert points at a
# recoverable line, not a deleted one. type/severity carried for the triager.
CORRUPT_STATE = "store-corrupt"
CORRUPT_ENTITY = "pitch-seed-store"
CORRUPT_TYPE = "store_corrupt"

# Observation collapse/entity tokens the drip reserves for its NON-seed signals.
# A seed id is BOTH the observation ``state`` and ``entity`` token (see
# seed_observation), while the idle signal collapses on ``state == IDLE_STATE``
# and the corruption signal collapses on ``state == CORRUPT_STATE`` /
# ``entity == CORRUPT_ENTITY``. A seed whose id equals one of these would emit an
# observation that collides with the idle/corruption collapse signature and
# mis-collapse a real seed against an idle or corruption tick (a dropped signal).
# Real make_seed ids are 16-hex-char digests, so this never arises in practice,
# but _is_seed_row rejects such a row (routing it to malformed) as a cheap
# correctness guard rather than trusting the digest invariant.
RESERVED_OBSERVATION_TOKENS = frozenset({IDLE_STATE, CORRUPT_STATE, CORRUPT_ENTITY})

# Default store location, relative to the daemon's cwd (the lodging root).
DEFAULT_STORE = "state/pitch-seeds.jsonl"

# Keys every seed row must carry to be a seed: the dedup id, the drain-order
# timestamp, the emitted-stamp slot, and the one content field a seed exists to
# carry (the event). A JSON line that parses but lacks any of these is NOT a seed
# -- it is treated as a torn row (preserved + counted malformed), so a non-seed
# object can never reach next_unemitted's seed.get(...) and crash the drip while
# starving the good seed behind it. make_seed always populates all four
# (emitted_at starts as null, but the KEY is present).
#
# Requiring discovered_at, emitted_at, AND event (not just id) is INTENTIONAL: a
# row missing any is quarantined + alerted (preserved verbatim, counted
# malformed), never silently dropped. So a future live writer that drifts from
# make_seed (a new schema, a dropped field) trips a loud corruption alert by
# design rather than mis-draining a contentless seed (an empty daily card) --
# strict classification is the fail-loud contract, not an accident.
REQUIRED_SEED_KEYS = ("id", "discovered_at", "emitted_at", "event")


def _row_has_lone_surrogate(obj: Any) -> bool:
    """True if any string anywhere in the row carries a lone surrogate codepoint
    (U+D800..U+DFFF).

    The whole-file read uses errors="surrogateescape", so an invalid UTF-8 byte
    INSIDE an otherwise-complete JSON string value (a writer killed mid-multibyte,
    but the line still closes its braces and quotes) decodes to a lone surrogate
    (U+DC80..U+DCFF) and parses cleanly through json.loads. Left unchecked it
    would become a "seed": on rewrite json.dumps re-encodes the surrogate as a
    \\udcXX escape (the value is silently MUTATED, not preserved, and not counted
    as corruption), and a lone surrogate can later raise UnicodeEncodeError on a
    strict-UTF-8 channel. Treating any such row as malformed keeps the
    surrogateescape contract whole: invalid bytes are preserved verbatim and
    alerted, never silently normalized. Scans keys and values, walking into
    nested dicts/lists so a surrogate in any string field is caught.

    The walk is ITERATIVE (an explicit worklist, not Python recursion): a torn
    row can carry an arbitrarily deep nested value, and a recursive scan would
    raise RecursionError on it -- a second unbounded recursion on top of the one
    json.loads already risks. _parse_store catches any per-line exception, so a
    RecursionError here would only route the line to malformed rather than wedge
    the load, but keeping the scan iterative removes the failure mode entirely
    (no stack-depth ceiling on how deep a quarantined surrogate can hide).
    """
    stack: list[Any] = [obj]
    while stack:
        item = stack.pop()
        if isinstance(item, str):
            if any(0xD800 <= ord(ch) <= 0xDFFF for ch in item):
                return True
        elif isinstance(item, dict):
            for key, value in item.items():
                stack.append(key)
                stack.append(value)
        elif isinstance(item, list):
            stack.extend(item)
    return False


def _is_seed_row(obj: Any) -> bool:
    """True only for a dict carrying every required seed key with usable types.

    Guards _parse_store against JSON that is valid but cannot safely drain. A row
    is rejected (routed to the malformed/torn-row path, not the seed list) when:
      - it is not a dict (a bare list ``[]``, a scalar, a string), OR
      - it is missing any required seed key, OR
      - its ``id`` is not a NON-EMPTY str. The id is the dedup key AND the
        observation ``state``/``entity`` token, so it must be a scalar with stable
        equality. A non-str id (e.g. ``[]``) would pass a presence-only check,
        get SELECTED by next_unemitted, be emitted as ``state == str(seed["id"])``
        (``"[]"``), then NEVER stamped -- mark_emitted compares the raw value
        ``seed.get("id") == seed_id`` i.e. ``[] == "[]"`` which is False -- so the
        row re-selects every tick forever, livelocking the drain and starving
        every good seed behind it (the never-miss-a-seed failure, silently). An
        empty-string id collides with no real digest but is still a degenerate
        token, so it is rejected too.
      - its ``id`` equals a reserved observation token
        (RESERVED_OBSERVATION_TOKENS -- the idle state token, or the
        store-corrupt state/entity token). A seed id IS the observation
        ``state``/``entity`` (see seed_observation), so a seed whose id collided
        with the idle or corruption collapse signature would mis-collapse a real
        signal against an idle/corruption tick. Real 16-hex digests never
        collide, so this only quarantines a hand-crafted or drifted row, but it
        is a cheap correctness guard.
      - its ``discovered_at`` is not a str. next_unemitted sorts pending rows
        lexicographically by ``str(discovered_at)``; a non-str value can mis-order
        the drain (or worse, vary across rows). Any row that could wedge or
        mis-order the drain is malformed, not a seed.
    """
    if not isinstance(obj, dict):
        return False
    if not all(key in obj for key in REQUIRED_SEED_KEYS):
        return False
    if not isinstance(obj.get("id"), str) or not obj["id"]:
        return False
    if obj["id"] in RESERVED_OBSERVATION_TOKENS:
        return False
    if not isinstance(obj.get("discovered_at"), str):
        return False
    if not isinstance(obj.get("event"), str) or not obj["event"]:
        # ``event`` is the one content field a seed exists to carry, and the drip
        # emits it verbatim into the daily card. A row missing it (or with an
        # empty/non-str event) would drain into a contentless card that falls
        # back to the opaque seed id, with NO corruption alert -- a silent,
        # low-information delivery. Rejecting it routes the row to the malformed
        # path (preserved + counted + store_corrupt alert) instead.
        return False
    if not (obj["emitted_at"] is None or isinstance(obj["emitted_at"], str)):
        # emitted_at is the drained-marker: null until drained, then an ISO
        # timestamp str. next_unemitted treats ANY non-None value as "already
        # emitted" (``emitted_at is not None``), so a wrong-typed value (e.g.
        # ``false``) would SILENTLY skip the row -- a dropped seed that never
        # alerts. Only null-or-str can drain safely; anything else is malformed.
        return False
    return True


def seed_id(event: str) -> str:
    """Stable 16-hex-char id for an event.

    The same event is the same seed, so its id is stable across Pitch runs --
    that identity is what dedups a recurring event (append_seed skips a duplicate
    id; the catalog emission gate drops a repeat finding for the same entity).
    """
    digest = hashlib.sha256(event.encode()).hexdigest()
    return digest[:16]


def make_seed(
    event: str,
    *,
    source: str = "",
    date: str = "",
    discovered_at: str | None = None,
    sid: str | None = None,
) -> dict[str, Any]:
    """Build a seed row with emitted_at unset (null until drained)."""
    return {
        "id": sid or seed_id(event),
        "discovered_at": discovered_at or utcnow(),
        "event": event,
        "source": source,
        "date": date,
        "emitted_at": None,
    }


@contextlib.contextmanager
def store_lock(path: str | Path):
    """Advisory exclusive flock held across a WHOLE load->modify->write.

    The live ``pitch`` chain calls append_seed in its own process while the drip
    source's drain runs in the daemon's process. Both do an unlocked
    read-modify-write of the whole JSONL file; without a lock one writer's stale
    snapshot clobbers a seed the other just wrote (a lost update -- a brand-new
    seed silently dropped, the exact failure this source exists to prevent). The
    lock lives on a sibling ``.lock`` file so it is independent of the store's
    own atomic os.replace, which swaps the inode out from under any open fd. The
    "idempotent append on id" property must hold ACROSS processes, not just
    within one, so every writer (append_seed, mark_emitted, drain_one) takes
    this lock around its entire critical section.
    """
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


def _parse_store(
    path: str | Path, *, warn: bool = False
) -> tuple[list[dict[str, Any]], list[str]]:
    """Read the store into (valid seed rows, malformed lines kept VERBATIM).

    A malformed line is not fatal: valid seeds before AND after it still parse,
    so a torn row never swallows every seed behind it. A line is malformed if it
    is one of:
      - not parseable as JSON (a truncated/torn append), OR
      - JSON-valid but not a usable seed -- a bare list (``[]``), a scalar, a
        dict missing a required seed key, a non-str/empty ``id``, or a non-str
        ``discovered_at`` (see _is_seed_row). Without this check a non-seed (or
        ill-typed) object would land in ``seeds`` and either crash
        next_unemitted's ``seed.get(...)`` or livelock the drain (a non-scalar id
        is selected, emitted, then never stamped because ``[] == "[]"`` is False,
        so it re-selects every tick and starves the good seeds behind it), OR
      - a seed-shaped row whose string fields carry a lone surrogate codepoint --
        an invalid UTF-8 byte decoded via surrogateescape inside an otherwise
        complete JSON string (see _row_has_lone_surrogate). Quarantining it keeps
        the bytes preserved + counted instead of being silently re-encoded as a
        ``\\udcXX`` escape on rewrite, OR
      - a line that raises ANY exception while being parsed or classified. The
        per-line guard catches every exception, not just json.JSONDecodeError: a
        pathologically nested value makes json.loads raise RecursionError (a
        RuntimeError subclass), which would otherwise propagate out of the load,
        fail every drip tick, and starve all good seeds. Such a line is routed to
        the malformed path like any other torn row -- no single line can wedge the
        whole drain.
    Malformed lines are returned verbatim (the raw line, sans newline) so
    write_seeds can carry them through a rewrite -- a torn row is RECOVERABLE,
    never deleted. The whole-file read uses errors="surrogateescape" so an
    invalid-UTF-8 byte (a writer killed mid-multibyte append) does NOT raise on
    the read and wedge every seed in the store; the bad bytes round-trip into a
    preserved malformed line (write_seeds writes with the same codec) and the
    good seeds drain first. Blank lines are ignored (formatting, not corruption).
    ``warn`` echoes each torn line to stderr for the CLI/manual path; the live
    daemon discards a successful command's stderr, so the operator-visible signal
    is the drip's corruption observation (see drip_view / corruption_observation),
    not this.
    """
    p = Path(path)
    if not p.exists():
        return [], []
    seeds: list[dict[str, Any]] = []
    malformed: list[str] = []
    text = p.read_text(encoding="utf-8", errors="surrogateescape")
    for lineno, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        reason: str | None = None
        try:
            parsed = json.loads(raw)
            if not _is_seed_row(parsed):
                reason = "JSON is not a seed object (missing required keys or wrong type)"
            elif _row_has_lone_surrogate(parsed):
                reason = "seed string carries a lone surrogate (invalid UTF-8 byte)"
        except Exception as exc:
            # ANY exception while parsing OR classifying a SINGLE line routes that
            # line to the malformed path -- it never propagates out of the load.
            # json.loads raises json.JSONDecodeError on torn JSON, but it also
            # raises RecursionError (a RuntimeError, NOT a JSONDecodeError) on a
            # pathologically nested value -- e.g. a seed-shaped row with a ~thousand
            # -deep nested extra field -- and the classification helpers run inside
            # this guard too. No single toxic line may crash _parse_store and
            # starve every good seed behind it (the module's "no malformed line
            # wedges the drain" contract); it becomes a preserved malformed line +
            # a store_corrupt signal, exactly like an unparseable line.
            reason = f"{type(exc).__name__}: {exc}"
        if reason is not None:
            if warn:
                sys.stderr.write(
                    f"seed_store: preserving malformed line {lineno} in {p}: {reason}\n"
                )
            malformed.append(raw)
            continue
        seeds.append(parsed)
    return seeds, malformed


def load_seeds(path: str | Path) -> list[dict[str, Any]]:
    """Read every valid seed row from the JSONL store (missing file -> []).

    A malformed line is skipped (and reported to stderr) but NOT deleted -- it is
    preserved across the next write_seeds rewrite so a torn seed stays
    recoverable, and the drip raises a corruption observation so the condition is
    visible on the live daemon path. See _parse_store and write_seeds.
    """
    return _parse_store(path, warn=True)[0]


def write_seeds(path: str | Path, seeds: list[dict[str, Any]]) -> None:
    """Rewrite the whole store atomically (write unique temp, fsync, replace).

    The temp file uses a PER-PROCESS-UNIQUE name (tempfile.mkstemp in the store
    directory) so two concurrent writers never share a temp path -- a fixed temp
    name let both truncate the same file and produce a torn store. os.replace is
    atomic, so a crash leaves either the old store or the new one, never a
    half-written one. After the replace the PARENT DIRECTORY is fsync'd so the
    rename itself survives a power loss (the temp's data fsync alone does not
    durably record the new directory entry on every filesystem). Callers hold
    store_lock() across load->modify->write, so the rewrite never clobbers a
    concurrent writer's seed.

    Any malformed lines already in the store are carried through VERBATIM (read
    from the current file under the caller's lock and re-appended), so a rewrite
    triggered by an unrelated append/stamp never deletes a torn -- and possibly
    recoverable -- seed row. The write handle uses errors="surrogateescape" to
    match the read in _parse_store: a torn invalid-UTF-8 line decoded into
    surrogates re-encodes to its ORIGINAL bytes, so preservation is byte-faithful
    (valid seeds are pure ASCII via json.dumps and re-encode unchanged).
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    # Preserve torn rows across this rewrite (warn=False: load_seeds already
    # warned for this load->modify->write cycle; no double echo).
    _, malformed = _parse_store(p)
    body = "".join(json.dumps(s, sort_keys=True) + "\n" for s in seeds)
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
    """fsync a directory so a rename into it is durable across a power loss.

    Best-effort: some filesystems reject O_RDONLY fsync on a directory, and the
    data is already swapped in by os.replace, so a failure here costs durability
    of the rename ordering only -- never correctness. Suppressed accordingly.
    """
    with contextlib.suppress(OSError):
        dir_fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


def append_seed(path: str | Path, seed: dict[str, Any]) -> bool:
    """Append a seed unless its id is already present. Returns True if added.

    Idempotent ingest: the live ``pitch`` chain can re-surface the same seed
    across runs without double-queuing it. This is the first of two dedup
    layers (the second is the catalog emission gate, keyed on the seed id as the
    finding entity). The whole read-check-write runs under store_lock so the
    idempotency holds against a concurrent drain in the daemon process.
    """
    with store_lock(path):
        seeds = load_seeds(path)
        if any(s.get("id") == seed.get("id") for s in seeds):
            return False
        seeds.append(seed)
        write_seeds(path, seeds)
        return True


def next_unemitted(seeds: list[dict[str, Any]]) -> dict[str, Any] | None:
    """The oldest seed with emitted_at unset, or None.

    Order is by discovered_at, with file order as a stable tiebreak so two
    seeds discovered in the same millisecond drain in append order.
    """
    pending = [
        (index, seed)
        for index, seed in enumerate(seeds)
        if seed.get("emitted_at") is None
    ]
    if not pending:
        return None
    pending.sort(key=lambda item: (str(item[1].get("discovered_at") or ""), item[0]))
    return pending[0][1]


def peek_unemitted(path: str | Path) -> dict[str, Any] | None:
    """The oldest unemitted seed WITHOUT stamping it (the emit side of the drain).

    This is half of the emit-then-stamp drain the drip uses: the source prints
    the observation for this seed and only THEN calls mark_emitted. Picking
    without stamping is what makes the drain at-least-once: if the process dies
    after the daemon has captured the observation but before the stamp persists,
    the seed is still unemitted, so the next tick re-emits it (the catalog gate /
    state-collapse drops the duplicate) instead of marking it emitted-but-unseen
    and skipping it forever. Returns a fresh dict (mutating it does not touch the
    store); read under store_lock for a snapshot consistent with concurrent
    writers.
    """
    with store_lock(path):
        return next_unemitted(load_seeds(path))


def drip_view(path: str | Path) -> tuple[dict[str, Any] | None, int]:
    """One locked snapshot for the drip: (oldest unemitted seed or None, count of
    malformed lines).

    The drip drains GOOD seeds first and only raises the corruption signal once
    none remain -- a torn row must never block the seeds that DO parse (the
    "miss a seed" failure this source exists to prevent). Picking the seed and
    counting the torn rows in one critical section keeps the two reads consistent
    with concurrent writers. Does not stamp -- the drip still emits THEN stamps
    (peek/mark_emitted) for at-least-once.
    """
    with store_lock(path):
        seeds, malformed = _parse_store(path)
        return next_unemitted(seeds), len(malformed)


def mark_emitted(
    path: str | Path, seed_id: str, now: str | None = None
) -> bool:
    """Stamp emitted_at on the seed with this id (the stamp side of the drain).

    Re-reads the store UNDER LOCK and rewrites, so a seed the live ``pitch``
    chain appended between peek_unemitted and here is NOT clobbered -- the stamp
    never writes back a stale snapshot. Returns True if a still-unemitted seed
    with that id was stamped, False if none was found (already stamped, or the
    row is gone).
    """
    with store_lock(path):
        seeds = load_seeds(path)
        for seed in seeds:
            if seed.get("id") == seed_id and seed.get("emitted_at") is None:
                seed["emitted_at"] = now or utcnow()
                write_seeds(path, seeds)
                return True
        return False


def drain_one(path: str | Path, now: str | None = None) -> dict[str, Any] | None:
    """Stamp and return the oldest unemitted seed, or None if all are emitted.

    Pick-and-stamp in a SINGLE locked critical section. The drip source does NOT
    use this -- it splits the drain into peek_unemitted + mark_emitted so it can
    emit the observation before persisting the stamp (at-least-once). drain_one
    is the convenience path for callers that have nothing to emit between the
    pick and the stamp. One call drains one seed.
    """
    with store_lock(path):
        seeds = load_seeds(path)
        target = next_unemitted(seeds)
        if target is None:
            return None
        target["emitted_at"] = now or utcnow()
        write_seeds(path, seeds)
        return target


def seed_observation(seed: dict[str, Any]) -> dict[str, Any]:
    """The single JSON observation the drip prints for one drained seed.

    ``state`` is the seed id (the observation-collapse token), so each distinct
    seed is a change -> one observation. ``entity`` is the seed id too, so each
    seed gets its own incident in the catalog and a repeat never re-alerts.
    """
    return {
        "source_ref": SOURCE_REF,
        "entity": str(seed["id"]),
        "type": "seed",
        "seed_id": str(seed["id"]),
        "event": seed.get("event"),
        "source": seed.get("source"),
        "date": seed.get("date"),
        "discovered_at": seed.get("discovered_at"),
        "emitted_at": seed.get("emitted_at"),
        "state": str(seed["id"]),
    }


def idle_observation() -> dict[str, Any]:
    """The stable JSON the drip prints when there is nothing to drain.

    ``state`` == IDLE_STATE, so consecutive idle ticks collapse to no
    observation. The triager emits no finding for an idle observation.
    """
    return {
        "source_ref": SOURCE_REF,
        "entity": "pitch-seed",
        "type": IDLE_STATE,
        "state": IDLE_STATE,
    }


def corruption_observation(malformed_count: int) -> dict[str, Any]:
    """The single JSON observation the drip prints when the store has torn rows.

    ``state`` == CORRUPT_STATE (fixed), so consecutive corrupt ticks collapse to
    no observation -- the signal does not loop. ``entity`` == CORRUPT_ENTITY
    (fixed), so the catalog emission gate opens ONE incident for the torn store
    and drops repeats: the operator gets exactly one alert. severity=="high" so
    the triager jumps it to the urgent pipe (a possibly-lost seed is urgent),
    with the daily seeds pipe as the never-drop floor. ``malformed_lines`` is
    carried for the finding body ("seed store has N malformed lines") but is
    deliberately NOT in ``state``/``entity`` -- the count is informational, the
    alert is one-per-torn-store.

    DRAIN-GATED: the drip raises this only once every GOOD seed has drained (see
    drip_view) -- a parseable seed must never wait behind a torn row. The
    trade-off is latency: under a sustained backlog of good seeds the corruption
    alert is deferred until the backlog clears. Accepted by design (losing a
    parseable seed is the worse failure); the alert still fires, just not ahead of
    the seeds that DO parse.
    """
    return {
        "source_ref": SOURCE_REF,
        "entity": CORRUPT_ENTITY,
        "type": CORRUPT_TYPE,
        "severity": "high",
        "malformed_lines": int(malformed_count),
        "state": CORRUPT_STATE,
    }


def utcnow() -> str:
    """ISO-8601 UTC millisecond timestamp with a trailing Z (matches the rest of
    angelus's timestamp shape)."""
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")
