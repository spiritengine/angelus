"""Agent-authored reminders (relay brief-20260620-p56d).

A reminder is a pre-known future date + a message: an agent (or Patrick) files
one via the `reminder` CLI and it surfaces ONCE in the daily `reminders` digest
on (or near) its date, then retires. Like Pitch it hits the
one-observation-per-fire wall (several reminders can fall due the same day), so it
uses the same DRIP: a fast source drains the OLDEST DUE unfired reminder per tick,
stamping fired_at and printing one observation whose `state` is the reminder id.

Coverage:
  - reminder_store: due order (oldest fire_date), future held back, past fires,
    all-fired -> None, random id (NO content dedup -- two identical adds both
    kept), bad fire_date quarantined.
  - the drip script: one reminder per fire (stamped), the NEXT on the second fire,
    idle when nothing due, a future reminder is not drained.
  - _fire_source: distinct reminders each write an observation; a re-fired idle
    collapses to NO observation.
  - the triager: a reminder observation triages into a finding routed to
    `reminders`; a store_corrupt observation jumps to `now` with `reminders` as a
    floor; idle yields no finding; a typo'd urgent_pipe still delivers via the
    floor.
  - catalog: two DISTINCT reminders both enqueue (the inverse of Pitch dedup),
    while an at-least-once re-emit of the SAME id collapses to one.
  - the shipped example lodging wires the source/triager/pipe and cross-refs
    clean, and the reminder-cards preamble renders.
  - "never silently dropped": torn line preserved + reported, carried across a
    rewrite, emit-then-stamp re-emits on a crash before the stamp.
  - prune: fired rows age out (safe with random ids), pending never pruned.
  - the CLI: add / add --lead / bad date / ls / done (prefix, ambiguous, empty).
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
import sys
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon
from angelus.lodging import load_lodging
from angelus.storage import Catalog, init_db
from angelus.triage import run_python_triager


REPO_ROOT = Path(__file__).resolve().parents[1]
LODGING = REPO_ROOT / "examples" / "lodging"
BIN = LODGING / "bin"
DRIP = BIN / "reminder_drip.py"
CLI = BIN / "reminder"
HANDLER = LODGING / "triagers" / "handlers" / "reminder.py"


def _load_reminder_store():
    """Import the example reminder_store module by path (it lives under the
    lodging bin/, not on the package path)."""
    spec = importlib.util.spec_from_file_location("reminder_store", BIN / "reminder_store.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


rs = _load_reminder_store()

# Always-due / never-due fixed dates so the drip subprocess's REAL-wall-clock
# local_today() stays deterministic regardless of the day the suite runs.
PAST_A = "2020-01-01"
PAST_B = "2020-01-02"
FUTURE = "2099-09-01"
TODAY_2020 = date(2020, 6, 1)  # for direct next_due() calls (between the pasts)


def _rem(fire_date, message, **kw):
    return rs.make_reminder(fire_date, message, **kw)


def _drip(store: Path) -> dict:
    result = subprocess.run(
        [sys.executable, str(DRIP), str(store)],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


def _cli(*args, store: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        [sys.executable, str(CLI), "--store", str(store), *args],
        capture_output=True,
        text=True,
    )


# --------------------------------------------------------------------------
# reminder_store: due order, future held back, random id (no dedup)
# --------------------------------------------------------------------------


def test_next_due_takes_oldest_due_skips_future(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_B, "newer-due"))
    rs.add_reminder(store, _rem(PAST_A, "older-due"))
    rs.add_reminder(store, _rem(FUTURE, "not-yet"))

    reminders = rs.load_reminders(store)
    first = rs.next_due(reminders, TODAY_2020)
    assert first["message"] == "older-due", "oldest fire_date <= today drains first"

    # The future reminder is never selected while it is not due.
    only_future = [r for r in reminders if r["message"] == "not-yet"]
    assert rs.next_due(only_future, TODAY_2020) is None


def test_next_due_none_when_all_fired(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "x"))
    assert rs.mark_fired(store, rs.load_reminders(store)[0]["id"])
    assert rs.next_due(rs.load_reminders(store), TODAY_2020) is None


def test_next_due_missing_store_is_none(tmp_path: Path) -> None:
    assert rs.next_due(rs.load_reminders(tmp_path / "absent.jsonl"), TODAY_2020) is None


def test_add_does_not_dedup_identical_reminders(tmp_path: Path) -> None:
    """The inverse of Pitch: a reminder id is RANDOM, so two identical adds are two
    distinct reminders (filing 'call mom' twice keeps both)."""
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "call mom"))
    rs.add_reminder(store, _rem(PAST_A, "call mom"))
    rows = rs.load_reminders(store)
    assert len(rows) == 2, "identical reminders are NOT deduped"
    assert rows[0]["id"] != rows[1]["id"], "each add mints a distinct random id"


def test_new_id_is_random_and_not_reserved() -> None:
    ids = {rs.new_id() for _ in range(100)}
    assert len(ids) == 100, "random ids do not collide in practice"
    assert not (ids & rs.RESERVED_OBSERVATION_TOKENS)


def test_make_reminder_rejects_bad_dates() -> None:
    import pytest

    with pytest.raises(ValueError):
        rs.make_reminder("2026-13-99", "bad fire date")
    with pytest.raises(ValueError):
        rs.make_reminder("someday", "not a date")
    with pytest.raises(ValueError):
        rs.make_reminder(PAST_A, "bad deadline", deadline="2026-13-01")
    with pytest.raises(ValueError):
        rs.make_reminder(PAST_A, "")


# --------------------------------------------------------------------------
# the drip script: one per fire, then next, then idle; future held back
# --------------------------------------------------------------------------


def test_drip_emits_one_per_fire_then_next_then_idle(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "first"))
    rs.add_reminder(store, _rem(PAST_B, "second"))
    rs.add_reminder(store, _rem(FUTURE, "later"))

    first = _drip(store)
    assert first["type"] == "reminder"
    assert first["message"] == "first"
    assert first["state"] == first["reminder_id"]
    fired = [r for r in rs.load_reminders(store) if r["fired_at"] is not None]
    assert [r["message"] for r in fired] == ["first"]

    second = _drip(store)
    assert second["message"] == "second", "the second fire drains the NEXT due reminder"

    idle = _drip(store)
    assert idle["type"] == "idle", "the future reminder is not due -> idle"
    assert idle["state"] == rs.IDLE_STATE
    # The future reminder is still pending (not drained).
    pending = [r for r in rs.load_reminders(store) if r["fired_at"] is None]
    assert [r["message"] for r in pending] == ["later"]


def test_drip_carries_deadline_as_data(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "CFP closes", deadline="2020-01-15"))
    obs = _drip(store)
    assert obs["deadline"] == "2020-01-15", "deadline rides as data for render-time"
    assert obs["fire_date"] == PAST_A


# --------------------------------------------------------------------------
# _fire_source: distinct reminders each write; idle re-fire collapses
# --------------------------------------------------------------------------


PINNED = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
SOURCE = "scheduled/reminders"


def _drip_lodging(root: Path, store: Path) -> None:
    """A minimal lodging whose one source IS the real drip pointed at `store`,
    plus a token pipe/channel so load_lodging is satisfied."""
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    (scheduled / "reminders.yaml").write_text(
        "cadence: 10m\n"
        "check:\n"
        "  kind: shell\n"
        f"  command: {sys.executable} {DRIP} {store}\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )


def _observation_count(daemon: AngelusDaemon) -> int:
    return int(
        daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM observations"
        ).fetchone()["n"]
    )


def _fire(daemon: AngelusDaemon) -> int | None:
    result = asyncio.run(daemon._fire_source(SOURCE))
    assert result is not None
    obs_id, _outcome = result
    return obs_id


def test_distinct_reminders_each_write_and_idle_collapses(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "first"))
    rs.add_reminder(store, _rem(PAST_B, "second"))

    lodging_root = tmp_path / "lodging"
    _drip_lodging(lodging_root, store)
    daemon = AngelusDaemon(lodging_root, clock=FakeClock(PINNED))
    try:
        assert _fire(daemon) is not None, "first reminder must write an observation"
        assert _fire(daemon) is not None, "second (distinct) reminder must write"
        assert _fire(daemon) is not None, "reminder -> idle is a transition, writes"
        assert _fire(daemon) is None, "idle -> idle must collapse (no observation)"
        assert _observation_count(daemon) == 3
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# the triager through the real loaded Triager
# --------------------------------------------------------------------------


def _triage(observation: dict):
    lodging = load_lodging(LODGING)
    triager = lodging.triagers["reminders-watch"]
    return asyncio.run(run_python_triager(triager, observation, {}))


def _run_handler(observation: dict, metadata: dict) -> list[dict]:
    payload = json.dumps(
        {
            "observation": observation,
            "prior_state": {},
            "triager": {
                "name": "reminders-watch",
                "source_ref": SOURCE,
                "metadata": metadata,
            },
        }
    )
    result = subprocess.run(
        [sys.executable, str(HANDLER)],
        input=payload,
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)["findings"]


def test_reminder_observation_triages_to_reminders_pipe() -> None:
    reminder = _rem(PAST_A, "rotate the token", deadline="2020-01-15")
    obs = rs.reminder_observation(reminder)
    obs["fired_at"] = "2026-06-19T12:00:00.000Z"
    findings, _state = _triage(obs)
    assert len(findings) == 1
    finding = findings[0]
    assert finding["type"] == "reminder"
    assert finding["entity"] == reminder["id"]
    assert finding["severity"] == "low"
    assert finding["target_pipes"] == ["reminders"]
    assert finding["body"]["message"] == "rotate the token"
    assert finding["body"]["deadline"] == "2020-01-15"
    assert finding["body"]["fire_date"] == PAST_A


def test_store_corrupt_jumps_to_now_with_reminders_floor() -> None:
    findings, _state = _triage(rs.corruption_observation(3))
    assert findings[0]["type"] == "store_corrupt"
    assert findings[0]["severity"] == "high"
    assert findings[0]["target_pipes"] == ["now", "reminders"], (
        "a possibly-lost reminder jumps to now but reminders stays a floor"
    )
    assert findings[0]["body"]["malformed_lines"] == 3


def test_idle_observation_yields_no_finding() -> None:
    findings, _state = _triage(rs.idle_observation())
    assert findings == []


def test_typod_urgent_pipe_still_delivers_via_floor() -> None:
    """urgent_pipe is NOT cross-ref validated at load, so a typo must not be able
    to drop the alert -- the daily reminders pipe stays a never-drop floor."""
    findings = _run_handler(
        rs.corruption_observation(1),
        {"target_pipe": "reminders", "urgent_pipe": "nwo"},  # typo
    )
    assert "reminders" in findings[0]["target_pipes"], (
        "the floor pipe is present even when urgent_pipe is unknown"
    )


# --------------------------------------------------------------------------
# catalog: distinct reminders both enqueue; same id (re-emit) collapses
# --------------------------------------------------------------------------


def test_distinct_reminders_both_enqueue(tmp_path: Path) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    try:
        for message in ("call mom", "call mom"):  # identical content, distinct ids
            reminder = _rem(PAST_A, message)
            obs = rs.reminder_observation(reminder)
            obs["fired_at"] = "2026-06-19T12:00:00.000Z"
            findings, _ = _triage(obs)
            catalog.write_finding(None, findings[0], known_pipes={"reminders"})
        queued = catalog.pending_pipe_items("reminders", limit=None)
        assert len(queued) == 2, "two distinct reminders both enqueue (no content dedup)"
    finally:
        connection.close()


def test_same_id_reemit_collapses_through_catalog(tmp_path: Path) -> None:
    """At-least-once: a re-emitted SAME id (crash-before-stamp re-fire) must not
    alert twice -- the catalog emission gate drops the repeat."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    reminder = _rem(PAST_A, "single reminder")
    obs = rs.reminder_observation(reminder)
    obs["fired_at"] = "2026-06-19T12:00:00.000Z"
    try:
        findings, _ = _triage(obs)
        finding = findings[0]
        catalog.write_finding(None, finding, known_pipes={"reminders"})
        catalog.write_finding(None, finding, known_pipes={"reminders"})
        assert len(catalog.pending_pipe_items("reminders", limit=None)) == 1, (
            "the same reminder id must not alert twice"
        )
    finally:
        connection.close()


# --------------------------------------------------------------------------
# the shipped example lodging wiring + the reminder-cards preamble
# --------------------------------------------------------------------------


def test_example_lodging_wires_reminder_source_triager_pipe() -> None:
    lodging = load_lodging(LODGING)
    assert "scheduled/reminders" in lodging.sources
    assert "reminders" in lodging.pipes
    triager = lodging.triagers["reminders-watch"]
    assert triager.source_ref == "scheduled/reminders"
    assert triager.metadata["target_pipe"] == "reminders"
    assert triager.metadata["urgent_pipe"] == "now"


def test_reminder_cards_template_renders() -> None:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    environment = Environment(
        loader=FileSystemLoader(LODGING / "render-templates"),
        autoescape=select_autoescape(enabled_extensions=()),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = environment.get_template("reminder-cards.j2")
    findings = [
        {"severity": "low", "entity": "abc", "body": {"message": "CFP closes", "deadline": "2026-09-01"}},
        {"severity": "low", "entity": "def", "body": {"message": "no deadline one", "deadline": None}},
    ]
    rendered = template.render(findings_since_last_drain=findings)
    assert "Reminders (2)" in rendered
    assert "CFP closes" in rendered
    assert "deadline: 2026-09-01" in rendered
    assert "no deadline one" in rendered
    # The deadline line only appears when a deadline is present.
    assert rendered.count("deadline:") == 1

    empty = template.render(findings_since_last_drain=[])
    assert "No reminders today" in empty


# --------------------------------------------------------------------------
# "never silently dropped" -- torn rows, at-least-once
# --------------------------------------------------------------------------


def test_load_skips_malformed_keeps_valid(tmp_path: Path, capsys) -> None:
    store = tmp_path / "reminders.jsonl"
    good_a = _rem(PAST_A, "a")
    good_b = _rem(PAST_B, "b")
    store.write_text(
        json.dumps(good_a, sort_keys=True) + "\n"
        + "{not valid json at all\n"
        + json.dumps(good_b, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    reminders = rs.load_reminders(store)
    assert {r["message"] for r in reminders} == {"a", "b"}, "valid rows survive a torn line"
    assert "preserving malformed line 2" in capsys.readouterr().err


def test_bad_fire_date_is_quarantined_as_corruption(tmp_path: Path) -> None:
    """A row whose fire_date does not parse is a NEW failure mode Pitch's store
    doesn't cover: string-compared it sorts as forever-future (silently never
    fires); parsed-at-compare it would raise. It must be malformed, not a row."""
    store = tmp_path / "reminders.jsonl"
    good = _rem(PAST_A, "good")
    bad = json.dumps(
        {"id": "1234abcd1234abcd", "fire_date": "2026-13-99", "message": "bad", "fired_at": None}
    )
    store.write_text(json.dumps(good, sort_keys=True) + "\n" + bad + "\n", encoding="utf-8")
    reminders, malformed = rs._parse_store(store)
    assert {r["message"] for r in reminders} == {"good"}
    assert malformed == [bad], "the unparseable-date row is quarantined, not drained"
    # The drip raises a corruption observation once the good row has drained.
    assert _drip(store)["type"] == "reminder"
    assert _drip(store)["type"] == "store_corrupt", "torn row alerts after good drains"


def test_write_preserves_torn_row_across_rewrite(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    good = _rem(PAST_A, "a")
    torn = '{"id": "deadbeefdeadbeef", "message": "broken'  # truncated
    store.write_text(json.dumps(good, sort_keys=True) + "\n" + torn + "\n", encoding="utf-8")
    rs.add_reminder(store, _rem(PAST_B, "b"))  # rewrites the store
    raw = store.read_text(encoding="utf-8")
    assert torn in raw, "the torn bytes survive the rewrite"
    reminders, malformed = rs._parse_store(store)
    assert {r["message"] for r in reminders} == {"a", "b"}
    assert malformed == [torn]


def test_emit_then_stamp_reemits_if_crash_before_stamp(tmp_path: Path) -> None:
    """Pick (emit) without stamping, simulate a crash before mark_fired persists,
    and assert the reminder is NOT lost -- it re-emits next tick."""
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "only"))

    picked = rs.peek_due(store, TODAY_2020)
    assert picked is not None and picked["message"] == "only"
    # <crash here: no mark_fired> -- the reminder is still unfired.
    again = rs.peek_due(store, TODAY_2020)
    assert again is not None and again["message"] == "only", "re-emits, not lost"
    # Only after mark_fired lands is it drained.
    assert rs.mark_fired(store, picked["id"])
    assert rs.peek_due(store, TODAY_2020) is None


# --------------------------------------------------------------------------
# prune: fired rows age out (safe with random ids); pending never pruned
# --------------------------------------------------------------------------


def test_prune_drops_old_fired_keeps_recent_and_pending(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "pending"))  # unfired -> never pruned
    rs.add_reminder(store, _rem(PAST_B, "old-fired"))
    rs.add_reminder(store, _rem(PAST_B, "recent-fired"))
    rows = {r["message"]: r for r in rs.load_reminders(store)}
    rs.mark_fired(store, rows["old-fired"]["id"], now="2026-05-01T00:00:00.000Z")
    rs.mark_fired(store, rows["recent-fired"]["id"], now="2026-06-19T00:00:00.000Z")

    today = date(2026, 6, 20)  # old-fired is 50d old; recent-fired 1d
    removed = rs.prune_fired(store, today, retain_days=30)
    assert removed == 1
    left = {r["message"] for r in rs.load_reminders(store)}
    assert left == {"pending", "recent-fired"}, "old fired pruned; pending+recent kept"


# --------------------------------------------------------------------------
# the CLI
# --------------------------------------------------------------------------


def test_cli_add_sets_fire_date(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    result = _cli("add", "2030-01-01", "check the thing", store=store)
    assert result.returncode == 0, result.stderr
    rows = rs.load_reminders(store)
    assert len(rows) == 1
    assert rows[0]["fire_date"] == "2030-01-01"
    assert rows[0]["message"] == "check the thing"
    assert rows[0]["deadline"] is None


def test_cli_add_lead_stores_deadline_and_computes_fire(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    result = _cli("add", "2030-01-10", "--lead", "3d", "CFP closes", store=store)
    assert result.returncode == 0, result.stderr
    row = rs.load_reminders(store)[0]
    assert row["deadline"] == "2030-01-10", "the positional date becomes the deadline"
    assert row["fire_date"] == "2030-01-07", "fire = deadline - lead"


def test_cli_add_relative_date(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    today = rs.local_today()
    result = _cli("add", "+2w", "ping me", store=store)
    assert result.returncode == 0, result.stderr
    expected = (today + timedelta(days=14)).isoformat()
    assert rs.load_reminders(store)[0]["fire_date"] == expected


def test_cli_add_bad_date_exits_nonzero(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    result = _cli("add", "2026-13-99", "bad", store=store)
    assert result.returncode == 2
    assert not store.exists() or rs.load_reminders(store) == []


def test_cli_done_by_prefix_and_ambiguity_and_empty(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "keep me", rid="aaaa111122223333"))
    rs.add_reminder(store, _rem(PAST_B, "twin one", rid="bbbb111122223333"))
    rs.add_reminder(store, _rem(PAST_B, "twin two", rid="bbbb444455556666"))

    # Unique prefix removes exactly one.
    assert _cli("done", "aaaa", store=store).returncode == 0
    assert {r["message"] for r in rs.load_reminders(store)} == {"twin one", "twin two"}

    # Ambiguous prefix is rejected (both bbbb...), nothing removed.
    amb = _cli("done", "bbbb", store=store)
    assert amb.returncode == 2 and "ambiguous" in amb.stderr
    assert len(rs.load_reminders(store)) == 2, "an ambiguous prefix removes nothing"

    # Empty prefix is rejected (would match everything).
    empty = _cli("done", "", store=store)
    assert empty.returncode == 2
    assert len(rs.load_reminders(store)) == 2

    # No match is a clean miss.
    miss = _cli("done", "zzzz", store=store)
    assert miss.returncode == 1


def test_cli_ls_lists_pending_and_fired(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(FUTURE, "pending one"))
    rs.add_reminder(store, _rem(PAST_A, "fired one"))
    rs.mark_fired(store, [r for r in rs.load_reminders(store) if r["message"] == "fired one"][0]["id"])
    out = _cli("ls", store=store).stdout
    assert "pending" in out and "pending one" in out
    assert "fired" in out and "fired one" in out


def test_cli_ls_empty(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    assert "no reminders" in _cli("ls", store=store).stdout


# --------------------------------------------------------------------------
# local_today honours REMINDER_TZ
# --------------------------------------------------------------------------


def test_local_today_honours_tz_override() -> None:
    # A fixed zone resolves a date; the function does not raise and returns a date.
    assert isinstance(rs.local_today("America/New_York"), date)
    assert isinstance(rs.local_today("UTC"), date)


# --------------------------------------------------------------------------
# fell round 1 fixes: atomic drain (done race + emit-before-stamp), strict
# date/fired_at validation, no-urgent-for-normal-reminders, CLI overflow.
# --------------------------------------------------------------------------


def test_drain_one_emitting_holds_lock_across_emit(tmp_path: Path) -> None:
    """The done-vs-drip race fix: select+emit+stamp run under ONE lock, so a
    concurrent `done` cannot delete the row between the emit and the stamp. Prove
    it by trying to grab the store lock non-blocking from inside the emit."""
    import fcntl
    import os

    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "x"))
    held = {}

    def emit(_obs):
        fd = os.open(str(store) + ".lock", os.O_RDWR | os.O_CREAT, 0o644)
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            held["held"] = False  # acquired -> NOT held by the drain (bug)
            fcntl.flock(fd, fcntl.LOCK_UN)
        except BlockingIOError:
            held["held"] = True  # could not acquire -> the drain holds it
        finally:
            os.close(fd)

    obs, _ = rs.drain_one_emitting(store, TODAY_2020, emit)
    assert held["held"] is True, "the store lock is held across the emit"
    assert obs["message"] == "x"
    assert rs.load_reminders(store)[0]["fired_at"] is not None


def test_drain_one_emitting_emit_raises_leaves_unfired(tmp_path: Path) -> None:
    """at-least-once: emit is called BEFORE the stamp persists, so a raising emit
    (a dead daemon pipe) leaves the reminder unfired -> it re-emits next tick."""
    import pytest

    store = tmp_path / "reminders.jsonl"
    rs.add_reminder(store, _rem(PAST_A, "x"))

    def boom(_obs):
        raise BrokenPipeError("daemon gone")

    with pytest.raises(BrokenPipeError):
        rs.drain_one_emitting(store, TODAY_2020, boom)
    assert rs.load_reminders(store)[0]["fired_at"] is None, "unfired -> will re-emit"

    obs, _ = rs.drain_one_emitting(store, TODAY_2020, lambda _o: None)
    assert obs["message"] == "x"
    assert rs.load_reminders(store)[0]["fired_at"] is not None


def test_high_severity_reminder_does_not_jump_to_now() -> None:
    """A reminder is never an emergency: whatever its severity it routes only to
    the daily `reminders` pipe. Only a store_corrupt alert jumps to `now`."""
    reminder = _rem(PAST_A, "still just a reminder", severity="high")
    obs = rs.reminder_observation(reminder)
    obs["fired_at"] = "2026-06-19T12:00:00.000Z"
    findings, _ = _triage(obs)
    assert findings[0]["target_pipes"] == ["reminders"], (
        "a reminder never pages now, whatever its severity"
    )


def test_non_canonical_date_is_rejected() -> None:
    """date.fromisoformat accepts compact/week forms on 3.11+; the store must
    reject them so the string-sorted drain order and the rendered date stay
    canonical YYYY-MM-DD."""
    import pytest

    assert rs._is_isodate("2020-01-01")
    assert not rs._is_isodate("20200101"), "compact form rejected"
    assert not rs._is_isodate("2020-W01-1"), "week-date form rejected"
    with pytest.raises(ValueError):
        rs.make_reminder("20200101", "compact")


def test_garbage_fired_at_is_quarantined(tmp_path: Path) -> None:
    """An empty/garbage fired_at would be treated as 'already fired' (silently
    stuck, never pruned). It must be quarantined as corruption instead."""
    store = tmp_path / "reminders.jsonl"
    good = _rem(PAST_A, "good")
    bad = json.dumps(
        {"id": "1111aaaa2222bbbb", "fire_date": "2020-01-01", "message": "stuck", "fired_at": ""}
    )
    store.write_text(json.dumps(good, sort_keys=True) + "\n" + bad + "\n", encoding="utf-8")
    reminders, malformed = rs._parse_store(store)
    assert {r["message"] for r in reminders} == {"good"}
    assert malformed == [bad], "empty-string fired_at is quarantined, not stuck"


def test_lone_surrogate_row_is_quarantined(tmp_path: Path) -> None:
    """An invalid-UTF-8 byte inside a reminder string decodes (surrogateescape) to
    a lone surrogate; the row is preserved + counted, never silently re-encoded."""
    store = tmp_path / "reminders.jsonl"
    good = _rem(PAST_A, "good")
    bad = (
        b'{"id": "aaaabbbbccccdddd", "fire_date": "2020-01-01", '
        b'"message": "bad\xff", "fired_at": null}'
    )
    with open(store, "wb") as fh:
        fh.write((json.dumps(good, sort_keys=True) + "\n").encode())
        fh.write(bad + b"\n")
    reminders, malformed = rs._parse_store(store)
    assert {r["message"] for r in reminders} == {"good"}
    assert len(malformed) == 1, "the invalid-UTF-8 row is quarantined"


def test_reminder_cards_renders_store_corrupt() -> None:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    environment = Environment(
        loader=FileSystemLoader(LODGING / "render-templates"),
        autoescape=select_autoescape(enabled_extensions=()),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = environment.get_template("reminder-cards.j2")
    findings = [{"type": "store_corrupt", "severity": "high", "body": {"malformed_lines": 2}}]
    rendered = template.render(findings_since_last_drain=findings)
    assert "reminder store corruption" in rendered
    assert "2 malformed line(s)" in rendered


def test_cli_overflow_relative_date_clean_error(tmp_path: Path) -> None:
    store = tmp_path / "reminders.jsonl"
    r1 = _cli("add", "+999999999999d", "x", store=store)
    assert r1.returncode == 2, r1.stderr
    assert "too far out" in r1.stderr
    r2 = _cli("add", "2030-01-01", "--lead", "999999999w", "x", store=store)
    assert r2.returncode == 2, r2.stderr
    assert "out of range" in r2.stderr
