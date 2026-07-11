#!/usr/bin/env python3
"""Reminder store -- the reminder drip's shared data layer.

A reminder is a PRE-KNOWN future date plus a message: an agent (or Patrick) files
one and it surfaces ONCE in a daily digest on (or near) its date, then retires.
Unlike a watch it polls no external state -- the source reads this local store.
It is the unifying primitive behind CFP deadlines, renewals, token rotations,
"nudge me to check X in three weeks".

Why this mirrors the Pitch seed store
-------------------------------------
A native angelus source fire writes AT MOST ONE observation (the engine
``json.loads``-es a shell source's whole stdout into a single object, and
observations collapse on one ``state`` change-signature). Several reminders can
fall due on the same day, so -- exactly like Pitch's N-seeds-per-run problem --
they cannot all drop through the one-observation-per-fire model in a single fire.
The resolution is the same DRIP: a fast source (sources/scheduled/reminders.yaml)
drains the OLDEST DUE unfired reminder per tick, stamping ``fired_at`` and
printing ONE observation whose ``state`` is the reminder id. Distinct reminders
are distinct states -> one observation each; a tick with nothing due prints a
stable idle state that collapses to no observation. N due reminders drain over N
ticks, all well before the morning digest renders.

This module is the read/append/drain/prune layer the drip source script
(reminder_drip.py) and the CLI (the ``reminder`` command) share. It is a near
sibling of seed_store.py and copies its robustness wholesale -- atomic rewrite,
an advisory flock across load->modify->write, and torn-row preservation -- which
is needed regardless of the drip design because the CLI process and the daemon's
drip process are TWO processes writing one file.

How reminders DIFFER from seeds (the load-bearing deltas)
---------------------------------------------------------
1. ID IS RANDOM, NOT A CONTENT HASH. A seed's id hashes its event BECAUSE
   the same event legitimately recurs across Pitch runs and must dedup forever. A
   reminder does not recur -- each ``reminder add`` is a distinct intent, even if
   two carry the same date and message ("call mom" filed twice). A content hash
   would silently drop the second add (append dedups on id), collapse two
   genuinely-distinct reminders against one observation ``state``, AND let the
   catalog emission gate drop the second incident. So make_reminder mints a fresh
   random id per call and persists it. Persisted-random keeps the at-least-once
   re-emit correct (the id is read back from disk, same ``state`` collapses), it
   just removes the unwanted content-dedup. It is also prefix-addressable for the
   CLI's ``reminder done <id>``.
2. FIRE_DATE IS A REAL DATE THAT IS COMPARED, NOT JUST SORTED. A seed's
   discovered_at is only string-sorted, so seed_store validates it is a str but
   never that it parses. A reminder's fire_date is date-COMPARED against today, so
   a non-date ("2026-13-99", "someday") is a NEW failure mode: string-compared it
   sorts as forever-future and silently never fires; parsed-at-compare-time it
   raises and wedges the drain. _is_reminder_row therefore requires fire_date to
   ``date.fromisoformat`` cleanly -- a bad date is quarantined as corruption
   (preserved + alerted), never silently stuck. due comparison is on parsed
   ``date`` objects.
3. "DUE" IS A WINDOW, NOT "ASAP". next_unemitted (seeds) drains everything
   pending immediately; next_due(reminders, today) drains only reminders with
   fire_date <= today. A future-dated reminder is skipped (not drained, not idle)
   until its day arrives.
4. FIRED ROWS ARE PRUNED. A seed is kept after emission (pruning would let it
   re-alert). A reminder's id is random and never recurs, so a fired row is safe
   to prune -- prune_fired drops rows fired more than RETAIN_DAYS ago so the store
   does not grow without bound (every tick re-parses the whole file).

Store format
------------
A JSONL file (one reminder object per line) under the lodging root,
conventionally ``state/reminders.jsonl``. The daemon's working directory IS the
lodging root, so the source command resolves it relative to cwd.

Reminder schema (every row)
---------------------------
- ``id``         random 16-hex-char token; the observation ``state``/``entity``
                 token and the CLI address. Distinct per add (NO content dedup).
- ``fire_date``  ``YYYY-MM-DD`` calendar date to surface on; compared against
                 LOCAL today (see local_today). Must parse via date.fromisoformat.
- ``message``    the reminder text shown in the digest.
- ``deadline``   ``YYYY-MM-DD`` or null. The real underlying date when the fire
                 date was derived via ``--lead`` (fire_date = deadline - lead).
                 Carried as DATA so the digest template can render "in N days"
                 at render time, not baked into prose at file time.
- ``severity``   ``"low"`` by default; passes through to the finding.
- ``created_by`` who filed it (CLI ``--by`` or ``$USER``); informational.
- ``created_at`` ISO-8601 UTC ms timestamp when filed.
- ``fired_at``   ``null`` until the drip surfaces it, then the ISO-8601 UTC stamp
                 of the tick that emitted it. A reminder fires exactly once.

Out of scope here (the live state/ file lives in the PRIVATE lodging repo): this
examples tree ships the contract + fixtures only, never Patrick's real reminders.
"""

from __future__ import annotations

import contextlib
import fcntl
import json
import os
import secrets
import sys
import tempfile
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

# The drip source's ref (matches sources/scheduled/reminders.yaml ->
# scheduled/<stem>). The handler and tests key off this single definition.
SOURCE_REF = "scheduled/reminders"

# The collapse token an idle (nothing due) tick prints. Distinct from any
# reminder id (ids are 16-hex-char tokens), so a reminder-state -> idle
# transition always writes one observation and idle -> idle collapses.
IDLE_STATE = "idle"

# The corruption signal an unparseable store raises. ``state``/``entity`` are
# FIXED (not the malformed count) so consecutive corrupt ticks collapse to no
# observation and the catalog emission gate opens ONE incident for a torn store.
# Mirrors seed_store's store-corrupt signal exactly.
CORRUPT_STATE = "store-corrupt"
CORRUPT_ENTITY = "reminder-store"
CORRUPT_TYPE = "store_corrupt"

# Tokens the idle/corruption signals reserve. A reminder id colliding with one
# would mis-collapse a real reminder against an idle/corruption tick. Real ids
# are 16-hex random tokens so this never arises, but _is_reminder_row rejects
# such a row (routing it to malformed) as a cheap correctness guard.
RESERVED_OBSERVATION_TOKENS = frozenset({IDLE_STATE, CORRUPT_STATE, CORRUPT_ENTITY})

# Default store location, relative to the daemon's cwd (the lodging root).
DEFAULT_STORE = "state/reminders.jsonl"

# Override for "local today" (see local_today). Defaults to the system local
# zone, which on the live box is America/New_York -- the same zone the daemon's
# CronTrigger digest schedules fire in, so a reminder's due-day and the digest's
# render-day agree. Set REMINDER_TZ when the daemon process's ambient TZ cannot
# be trusted (e.g. a systemd unit that inherits TZ=UTC).
TZ_ENV = "REMINDER_TZ"

# How long a fired reminder is retained before prune_fired drops it. Long enough
# for ``reminder ls`` to show recently-fired context, short enough that the store
# stays small (every drip tick re-parses the whole file).
RETAIN_DAYS = 30

# Keys every reminder row must carry. A JSON line that parses but lacks any of
# these is NOT a reminder -- it is a torn row (preserved + counted malformed), so
# a non-reminder object can never reach the drain and crash or starve it.
REQUIRED_REMINDER_KEYS = ("id", "fire_date", "message", "fired_at")


def local_today(tz_name: str | None = None) -> date:
    """Today's calendar date in the chosen local zone.

    ``tz_name`` defaults to ``$REMINDER_TZ``; if that is unset too, the SYSTEM
    local zone (``datetime.now().astimezone()``) is used. The drip and the CLI
    both call this so a reminder filed "today" and the tick that fires it agree
    on what "today" is -- and so ``--lead`` arithmetic at file time matches the
    due comparison at fire time.

    Why not naive ``date.today()``: it reads the process's ambient TZ, which under
    systemd is frequently UTC. Near midnight that makes "today" wrong by a full
    day, firing "surface on the 1st" on the 2nd (or the 31st). DST does not affect
    a calendar-date comparison; the ambient-TZ problem is the real one. Resolving
    through an explicit zone (or the deliberately-chosen system local zone) closes
    it. The digest renders in the system local calendar day (clock.now_local), so
    the default keeps the reminder due-day aligned with the digest day.
    """
    name = tz_name or os.environ.get(TZ_ENV)
    if name:
        return datetime.now(ZoneInfo(name)).date()
    return datetime.now().astimezone().date()


def _is_reminder_row(obj: Any) -> bool:
    """True only for a dict carrying every required key with usable types.

    Guards _parse_store against JSON that is valid but cannot safely drain. A row
    is rejected (routed to the malformed/torn-row path) when:
      - it is not a dict, OR
      - it is missing any required key, OR
      - its ``id`` is not a NON-EMPTY str, or equals a reserved observation token.
        The id is the dedup-free identity AND the observation ``state``/``entity``
        token, so it must be a stable scalar that never collides with the
        idle/corruption collapse signatures.
      - its ``fire_date`` is not a str, or does not ``date.fromisoformat`` cleanly.
        Unlike seed_store (which only string-sorts its timestamp), the reminder
        drip date-COMPARES fire_date against today. A non-date string-compared
        sorts as forever-future and silently never fires; parsed-at-compare-time
        it raises and wedges the drain. Either way it is corruption, not a
        reminder -- quarantine it loud.
      - its ``message`` is not a non-empty str (an empty digest line is useless
        and signals a malformed write).
      - its ``fired_at`` is neither null nor a str. next_due treats ANY non-None
        value as "already fired"; a wrong-typed value (e.g. ``false``) would
        SILENTLY skip the row -- a missed reminder that never alerts.
    """
    if not isinstance(obj, dict):
        return False
    if not all(key in obj for key in REQUIRED_REMINDER_KEYS):
        return False
    if not isinstance(obj.get("id"), str) or not obj["id"]:
        return False
    if obj["id"] in RESERVED_OBSERVATION_TOKENS:
        return False
    if not isinstance(obj.get("fire_date"), str) or not _is_isodate(obj["fire_date"]):
        return False
    if not isinstance(obj.get("message"), str) or not obj["message"]:
        return False
    if not (obj["fired_at"] is None or _is_iso_timestamp(obj["fired_at"])):
        # fired_at is the fired-marker: null until fired, then an ISO timestamp.
        # next_due treats ANY non-None value as "already fired", so an empty or
        # garbage str (e.g. ``""`` or ``"yes"``) would SILENTLY skip the row -- a
        # permanently-stuck reminder that also dodges prune_fired (its date won't
        # parse). Only null-or-parseable-timestamp can drain/prune safely;
        # anything else is corruption, not a reminder. (seed_store leaves this
        # looser, but a reminder is deadline-critical, so it is tightened here.)
        return False
    return True


def _is_isodate(value: str) -> bool:
    """True only if ``value`` is a clean canonical ``YYYY-MM-DD`` calendar date.

    ``date.fromisoformat`` ALSO accepts non-canonical ISO forms on Python 3.11+
    (``"20200101"``, ``"2020-W01-1"``). Those must be rejected: the store sorts
    the drain by the raw ``fire_date`` STRING (next_due), so a mixed-format value
    would mis-order the drain and render a non-contract date in the digest card.
    Re-serializing the parsed date and requiring it equal the input pins the
    format to canonical YYYY-MM-DD without a regex -- it rejects "2026-13-99",
    "someday", a compact/week-date form, and a datetime-with-time alike.
    """
    try:
        return date.fromisoformat(value).isoformat() == value
    except (ValueError, TypeError):
        return False


def _is_iso_timestamp(value: Any) -> bool:
    """True if ``value`` is a parseable ISO-8601 timestamp str (``...Z`` ok).

    The fired_at marker is written by mark_fired/utcnow as ``...Z``; accept that
    and any offset form. A non-str or unparseable value is rejected so a corrupt
    fired_at is quarantined rather than silently skipping the reminder forever.
    """
    if not isinstance(value, str):
        return False
    try:
        datetime.fromisoformat(value.replace("Z", "+00:00"))
        return True
    except ValueError:
        return False


def new_id() -> str:
    """A fresh random 16-hex-char reminder id.

    Random (not a content hash) so every ``reminder add`` is a distinct reminder
    even when date+message repeat -- see the module docstring's delta #1. 64 bits
    of entropy makes accidental collision negligible and a 4-6 char prefix
    normally unique for CLI addressing.
    """
    return secrets.token_hex(8)


def make_reminder(
    fire_date: str,
    message: str,
    *,
    deadline: str | None = None,
    severity: str = "low",
    created_by: str | None = None,
    created_at: str | None = None,
    rid: str | None = None,
) -> dict[str, Any]:
    """Build a reminder row with fired_at unset (null until surfaced).

    Raises ValueError if fire_date (or a given deadline) is not a clean
    ``YYYY-MM-DD`` -- the CLI catches and reports it, so a bad date never reaches
    the store and becomes a quarantined corruption row.
    """
    if not _is_isodate(fire_date):
        raise ValueError(f"fire_date is not YYYY-MM-DD: {fire_date!r}")
    if deadline is not None and not _is_isodate(deadline):
        raise ValueError(f"deadline is not YYYY-MM-DD: {deadline!r}")
    if not message:
        raise ValueError("message must be non-empty")
    return {
        "id": rid or new_id(),
        "fire_date": fire_date,
        "message": message,
        "deadline": deadline,
        "severity": severity,
        "created_by": created_by,
        "created_at": created_at or utcnow(),
        "fired_at": None,
    }


@contextlib.contextmanager
def store_lock(path: str | Path):
    """Advisory exclusive flock held across a WHOLE load->modify->write.

    The CLI (``reminder add`` / ``done``) runs in its own process while the drip
    source's drain runs in the daemon's process. Both do an unlocked
    read-modify-write of the whole JSONL file; without a lock one writer's stale
    snapshot clobbers a row the other just wrote (a lost update -- a brand-new
    reminder silently dropped, or a fired stamp lost so it re-fires). The lock
    lives on a sibling ``.lock`` file, independent of the store's own atomic
    os.replace which swaps the inode out from under any open fd. Every writer
    takes this lock around its entire critical section.
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
    """Read the store into (valid reminder rows, malformed lines kept VERBATIM).

    A malformed line is not fatal: valid reminders before AND after it still
    parse, so a torn row never swallows the reminders behind it. A line is
    malformed if it is one of:
      - not parseable as JSON (a truncated/torn append), OR
      - JSON-valid but not a usable reminder -- missing a required key, a
        non-str/empty/reserved ``id``, a ``fire_date`` that is not a clean
        calendar date, an empty/non-str ``message``, or a wrong-typed ``fired_at``
        (see _is_reminder_row). Without this a non-reminder (or ill-typed) object
        would land in the drain and crash next_due's date parse or silently skip a
        reminder, OR
      - a reminder-shaped row whose strings carry a lone surrogate codepoint -- an
        invalid UTF-8 byte decoded via surrogateescape inside an otherwise
        complete JSON string. Quarantining it keeps the bytes preserved + counted
        instead of being silently re-encoded as a ``\\udcXX`` escape on rewrite,
        OR
      - a line that raises ANY exception while being parsed or classified. The
        per-line guard catches every exception (not just JSONDecodeError) -- a
        pathologically nested value makes json.loads raise RecursionError -- so no
        single toxic line can wedge the whole drain and starve the good reminders.
    Malformed lines are returned verbatim so write_store can carry them through a
    rewrite -- a torn row is RECOVERABLE, never deleted. The whole-file read uses
    errors="surrogateescape" so an invalid-UTF-8 byte does not raise on the read;
    the bad bytes round-trip into a preserved malformed line. Blank lines are
    ignored. ``warn`` echoes each torn line to stderr for the CLI/manual path; the
    live daemon discards a successful command's stderr, so the operator-visible
    signal is the drip's corruption observation, not this.
    """
    p = Path(path)
    if not p.exists():
        return [], []
    reminders: list[dict[str, Any]] = []
    malformed: list[str] = []
    text = p.read_text(encoding="utf-8", errors="surrogateescape")
    for lineno, raw in enumerate(text.splitlines(), 1):
        if not raw.strip():
            continue
        reason: str | None = None
        try:
            parsed = json.loads(raw)
            if not _is_reminder_row(parsed):
                reason = "JSON is not a reminder object (missing keys, bad date, or wrong type)"
            elif _row_has_lone_surrogate(parsed):
                reason = "reminder string carries a lone surrogate (invalid UTF-8 byte)"
        except Exception as exc:
            reason = f"{type(exc).__name__}: {exc}"
        if reason is not None:
            if warn:
                sys.stderr.write(
                    f"reminder_store: preserving malformed line {lineno} in {p}: {reason}\n"
                )
            malformed.append(raw)
            continue
        reminders.append(parsed)
    return reminders, malformed


def _row_has_lone_surrogate(obj: Any) -> bool:
    """True if any string anywhere in the row carries a lone surrogate codepoint.

    The whole-file read uses errors="surrogateescape", so an invalid UTF-8 byte
    inside an otherwise-complete JSON string decodes to a lone surrogate
    (U+DC80..U+DCFF) and parses through json.loads. Left unchecked it would become
    a "reminder": on rewrite json.dumps re-encodes the surrogate as a \\udcXX
    escape (the value silently MUTATED, not preserved), and it can later raise
    UnicodeEncodeError on a strict-UTF-8 channel. Treating any such row as
    malformed keeps the surrogateescape contract whole. The walk is ITERATIVE (an
    explicit worklist, not recursion) so an arbitrarily deep nested value cannot
    raise RecursionError here. Mirrors seed_store._row_has_lone_surrogate.
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


def load_reminders(path: str | Path) -> list[dict[str, Any]]:
    """Read every valid reminder row from the JSONL store (missing file -> [])."""
    return _parse_store(path, warn=True)[0]


def write_store(path: str | Path, reminders: list[dict[str, Any]]) -> None:
    """Rewrite the whole store atomically (write unique temp, fsync, replace).

    The temp file uses a PER-PROCESS-UNIQUE name (tempfile.mkstemp) so two
    concurrent writers never share a temp path. os.replace is atomic, so a crash
    leaves either the old store or the new one, never a half-written one. After
    the replace the PARENT DIRECTORY is fsync'd so the rename survives a power
    loss. Callers hold store_lock() across load->modify->write, so the rewrite
    never clobbers a concurrent writer's row.

    Malformed lines already in the store are carried through VERBATIM (read under
    the caller's lock and re-appended), so a rewrite triggered by an unrelated
    add/stamp/prune never deletes a torn -- and possibly recoverable -- row. The
    write handle uses errors="surrogateescape" to match the read: a torn
    invalid-UTF-8 line re-encodes to its ORIGINAL bytes. Mirrors
    seed_store.write_seeds.
    """
    p = Path(path)
    p.parent.mkdir(parents=True, exist_ok=True)
    _, malformed = _parse_store(p)
    body = "".join(json.dumps(r, sort_keys=True) + "\n" for r in reminders)
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
    of the rename ordering only -- never correctness.
    """
    with contextlib.suppress(OSError):
        dir_fd = os.open(str(directory), os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)


def add_reminder(path: str | Path, reminder: dict[str, Any]) -> bool:
    """Append a reminder. Returns True (always added).

    Unlike seed_store.append_seed there is NO id-dedup: a reminder id is random
    and unique per add (make_reminder), so every add is a distinct reminder even
    when date+message repeat -- filing "call mom" twice keeps both. The
    read-modify-write runs under store_lock so a concurrent drain's stamp is not
    clobbered. (A duplicate id can only arise from a hand-crafted ``rid``; the
    guard rejects it loudly rather than silently shadowing an existing row.)
    """
    with store_lock(path):
        reminders = load_reminders(path)
        if any(r.get("id") == reminder.get("id") for r in reminders):
            raise ValueError(f"reminder id already present: {reminder.get('id')}")
        reminders.append(reminder)
        write_store(path, reminders)
        return True


def next_due(reminders: list[dict[str, Any]], today: date) -> dict[str, Any] | None:
    """The oldest unfired reminder whose fire_date <= today, or None.

    "Oldest" is by fire_date, with file order as a stable tiebreak so two
    reminders due the same day drain in add order. A future-dated reminder
    (fire_date > today) is skipped -- not yet due. fire_date is parsed to a
    ``date`` for comparison (every surviving row passed _is_reminder_row, so the
    parse cannot raise here). A past-due reminder (fire_date < today, e.g. filed
    with a past date, or the daemon was down on the due day) is still due and
    fires on the next tick -- "the date passed, tell me now".
    """
    pending = [
        (index, reminder)
        for index, reminder in enumerate(reminders)
        if reminder.get("fired_at") is None
        and date.fromisoformat(reminder["fire_date"]) <= today
    ]
    if not pending:
        return None
    pending.sort(key=lambda item: (item[1]["fire_date"], item[0]))
    return pending[0][1]


def peek_due(path: str | Path, today: date) -> dict[str, Any] | None:
    """The oldest due unfired reminder WITHOUT stamping it (the emit side).

    Half of the emit-then-stamp drain: the source prints the observation for this
    reminder and only THEN calls mark_fired. Picking without stamping is what
    makes the drain at-least-once -- if the process dies after the daemon captured
    the observation but before the stamp persists, the reminder is still unfired,
    so the next tick re-emits it (the catalog gate / state-collapse drops the
    duplicate) instead of marking it fired-but-unseen and skipping it forever.
    Read under store_lock for a snapshot consistent with concurrent writers.
    """
    with store_lock(path):
        return next_due(load_reminders(path), today)


def drain_one_emitting(
    path: str | Path,
    today: date,
    emit,
    now: str | None = None,
) -> tuple[dict[str, Any] | None, int]:
    """Drain the oldest DUE reminder, calling ``emit(observation)`` and stamping
    fired_at in ONE locked critical section. Returns (observation emitted or None,
    malformed-line count).

    Folding select+emit+stamp under a single lock closes a race the split
    peek/mark_fired path leaves open: a concurrent ``reminder done <id>`` could
    delete the picked row between the emit and the stamp, surfacing a CANCELED
    reminder (the observation was already printed) with no fired row left behind.
    Holding the lock across the emit serializes done-vs-drip -- ``done`` either
    runs fully before the pick (the row is gone, nothing drains) or fully after
    the stamp (it clears a fired row), never in between.

    AT-LEAST-ONCE is preserved: ``emit`` is called BEFORE write_store persists the
    stamp, so a crash (or a raising emit, e.g. a dead daemon pipe) between them
    leaves the row unfired and the next tick re-emits it -- the re-emit collapses
    against the unchanged ``state`` / the catalog emission gate. The emit writes
    one small JSON line to a pipe the daemon drains after the process exits (far
    under the pipe buffer), so holding the flock across it does not block.

    DRAIN-GATED corruption: when nothing is due, ``emit`` is NOT called and
    (None, malformed_count) is returned, so the caller raises the idle/corruption
    signal itself -- a parseable due reminder never waits behind a torn row.
    """
    with store_lock(path):
        reminders, malformed = _parse_store(path)
        target = next_due(reminders, today)
        if target is None:
            return None, len(malformed)
        stamp = now or utcnow()
        # Emit a stamped COPY first (observation carries fired_at), then persist
        # the stamp on the real row. ``target`` is the live list element, so the
        # mutation + write_store below records the fire.
        observation = reminder_observation({**target, "fired_at": stamp})
        emit(observation)
        target["fired_at"] = stamp
        write_store(path, reminders)
        return observation, len(malformed)


def mark_fired(path: str | Path, rid: str, now: str | None = None) -> bool:
    """Stamp fired_at on the reminder with this id (the stamp side of the drain).

    Re-reads the store UNDER LOCK and rewrites, so a reminder the CLI added
    between peek_due and here is NOT clobbered. Returns True if a still-unfired
    reminder with that id was stamped, False if none was found (already stamped,
    or the row is gone).
    """
    with store_lock(path):
        reminders = load_reminders(path)
        for reminder in reminders:
            if reminder.get("id") == rid and reminder.get("fired_at") is None:
                reminder["fired_at"] = now or utcnow()
                write_store(path, reminders)
                return True
        return False


def remove_reminder(path: str | Path, rid_or_prefix: str) -> dict[str, Any] | None:
    """Delete the reminder addressed by an exact id or a unique id prefix.

    The CLI's ``reminder done``: cancel a pending reminder before it fires, or
    clear a fired row early. Returns the removed row, or None if no row matched.
    Raises ValueError if the prefix is empty/whitespace (an empty prefix matches
    EVERY row -- a footgun that would delete the sole reminder, or be ambiguous),
    or if the prefix is ambiguous (matches more than one), so the caller never
    deletes the wrong reminder silently. Runs under store_lock.
    """
    query = rid_or_prefix.strip()
    if not query:
        raise ValueError("id (or id prefix) must be non-empty")
    rid_or_prefix = query
    with store_lock(path):
        reminders = load_reminders(path)
        matches = [r for r in reminders if _id_matches(r.get("id"), rid_or_prefix)]
        if not matches:
            return None
        if len(matches) > 1:
            ids = ", ".join(str(r.get("id")) for r in matches)
            raise ValueError(f"ambiguous id prefix {rid_or_prefix!r} matches: {ids}")
        target = matches[0]
        write_store(path, [r for r in reminders if r is not target])
        return target


def _id_matches(rid: Any, query: str) -> bool:
    """True if ``query`` is the full id or a prefix of it (git-style addressing)."""
    return isinstance(rid, str) and (rid == query or rid.startswith(query))


def prune_fired(
    path: str | Path, today: date, retain_days: int = RETAIN_DAYS
) -> int:
    """Drop fired rows older than ``retain_days``. Returns the count removed.

    Safe BECAUSE ids are random and never recur: dropping a fired reminder cannot
    let it re-alert (contrast a seed, whose stable id would re-fire if its emitted
    row were pruned). Keeps the store small so every drip tick's full re-parse
    stays cheap. A fired_at that does not parse is conservatively KEPT (never
    pruned on a parse error). Runs under store_lock.
    """
    with store_lock(path):
        reminders = load_reminders(path)
        kept = [r for r in reminders if not _is_prunable(r, today, retain_days)]
        removed = len(reminders) - len(kept)
        if removed:
            write_store(path, kept)
        return removed


def _is_prunable(reminder: dict[str, Any], today: date, retain_days: int) -> bool:
    """True if this reminder is fired and its fired_at is older than retain_days."""
    fired_at = reminder.get("fired_at")
    if not isinstance(fired_at, str):
        return False
    try:
        fired_date = datetime.fromisoformat(fired_at.replace("Z", "+00:00")).date()
    except ValueError:
        return False
    return (today - fired_date).days > retain_days


def reminder_observation(reminder: dict[str, Any]) -> dict[str, Any]:
    """The single JSON observation the drip prints for one drained reminder.

    ``state`` is the reminder id (the collapse token), so each distinct reminder
    is a change -> one observation. ``entity`` is the id too, so each reminder
    gets its own catalog incident and a re-emit (at-least-once) collapses.
    deadline is carried as DATA so the digest template computes "in N days" at
    render time.
    """
    return {
        "source_ref": SOURCE_REF,
        "entity": str(reminder["id"]),
        "type": "reminder",
        "reminder_id": str(reminder["id"]),
        "message": reminder.get("message"),
        "fire_date": reminder.get("fire_date"),
        "deadline": reminder.get("deadline"),
        "severity": reminder.get("severity") or "low",
        "created_by": reminder.get("created_by"),
        "created_at": reminder.get("created_at"),
        "fired_at": reminder.get("fired_at"),
        "state": str(reminder["id"]),
    }


def idle_observation() -> dict[str, Any]:
    """The stable JSON the drip prints when nothing is due.

    ``state`` == IDLE_STATE, so consecutive idle ticks collapse to no
    observation. The triager emits no finding for an idle observation.
    """
    return {
        "source_ref": SOURCE_REF,
        "entity": "reminder",
        "type": IDLE_STATE,
        "state": IDLE_STATE,
    }


def corruption_observation(malformed_count: int) -> dict[str, Any]:
    """The single JSON observation the drip prints when the store has torn rows.

    ``state`` == CORRUPT_STATE / ``entity`` == CORRUPT_ENTITY (both fixed), so
    consecutive corrupt ticks collapse and the catalog emission gate opens ONE
    incident for the torn store. severity=="high" so the triager jumps it to the
    urgent pipe with the daily reminders pipe as the never-drop floor -- a
    possibly-lost reminder (a missed deadline) is urgent. DRAIN-GATED: raised only
    once nothing is due, so a due reminder never waits behind a torn row. Mirrors
    seed_store.corruption_observation.
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
