"""Pitch seed drip (brief-20260619-opi5 option C).

Pitch produces N SEEDS per run, but a native source fire writes at
most ONE observation. The drip adapter resolves that lodging-side: Pitch writes
seeds to a store; a fast source drains the OLDEST unemitted seed per tick,
stamping emitted_at and printing one observation whose `state` is the seed id,
so N seeds drain over N ticks with no engine change.

Coverage:
  - seed_store: drain order, one-per-tick stamping, all-emitted -> None, id dedup.
  - the drip script: one seed per fire (stamped), the NEXT seed on the second
    fire, idle JSON when fully drained.
  - _fire_source: distinct seeds each write an observation; a re-fired idle
    state collapses to NO observation (the engine's change-signature, no patch).
  - the triager: a seed observation triages into a finding routed digest-only to
    the daily pipe (informational -- never jumps to `now`); idle yields no finding.
  - dedup end to end: the same seed never alerts twice through the catalog
    emission gate.
  - the shipped example lodging wires the source/triager/pipe and cross-refs
    clean, and the seed-cards preamble renders.
"""

from __future__ import annotations

import asyncio
import importlib.util
import json
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon
from angelus.lodging import load_lodging
from angelus.storage import Catalog, init_db
from angelus.triage import run_python_triager


REPO_ROOT = Path(__file__).resolve().parents[1]
LODGING = REPO_ROOT / "examples" / "lodging"
BIN = LODGING / "bin"
DRIP = BIN / "pitch_seed_drip.py"
HANDLER = LODGING / "triagers" / "handlers" / "pitch_seed.py"


def _load_seed_store():
    """Import the example seed_store module by path (it lives under the lodging
    bin/, not on the package path)."""
    spec = importlib.util.spec_from_file_location("seed_store", BIN / "seed_store.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


ss = _load_seed_store()


def _seed(cannon, event, build_move, *, severity="low", discovered_at, source="", date=""):
    """Build a seed for the drain/corruption tests below.

    The seed schema is now event-only: the id hashes the event, with source/date
    as optional context; cannon/build_move/severity are gone. This helper keeps
    the legacy positional shape so the many drain/order/corruption call sites
    below stay put -- only ``event`` (and optional source/date) feed the new
    make_seed; the cannon/build_move/severity args are ignored.
    """
    return ss.make_seed(event, source=source, date=date, discovered_at=discovered_at)


def _drip(store: Path) -> dict:
    result = subprocess.run(
        [sys.executable, str(DRIP), str(store)],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


# --------------------------------------------------------------------------
# seed_store: drain order + one-per-tick stamping + dedup
# --------------------------------------------------------------------------


def test_drain_one_takes_oldest_unemitted_and_stamps(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ss.append_seed(store, _seed("cB", "newer", "b", discovered_at="2026-06-19T11:00:00.000Z"))
    ss.append_seed(store, _seed("cA", "older", "a", discovered_at="2026-06-19T10:00:00.000Z"))

    first = ss.drain_one(store, now="2026-06-19T12:00:00.000Z")
    assert first["event"] == "older", "oldest discovered_at drains first, not file order"
    assert first["emitted_at"] == "2026-06-19T12:00:00.000Z"

    # The stamp persisted to the store, so the next drain skips it.
    second = ss.drain_one(store, now="2026-06-19T12:10:00.000Z")
    assert second["event"] == "newer"
    assert second["emitted_at"] == "2026-06-19T12:10:00.000Z"


def test_drain_one_returns_none_when_all_emitted(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ss.append_seed(store, _seed("cA", "ev", "a", discovered_at="2026-06-19T10:00:00.000Z"))
    assert ss.drain_one(store) is not None
    assert ss.drain_one(store) is None, "a fully-drained store drains to None"


def test_drain_one_missing_store_is_none(tmp_path: Path) -> None:
    assert ss.drain_one(tmp_path / "absent.jsonl") is None


def test_append_seed_dedups_on_id(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    assert ss.append_seed(store, _seed("cA", "same", "a", discovered_at="2026-06-19T10:00:00.000Z"))
    # Same event -> same id -> not added again (idempotent ingest).
    assert not ss.append_seed(store, _seed("cA", "same", "a", discovered_at="2026-06-19T11:00:00.000Z"))
    assert len(ss.load_seeds(store)) == 1


def test_seed_id_stable_and_event_keyed() -> None:
    assert ss.seed_id("the world changed") == ss.seed_id("the world changed")
    # Distinct events -> distinct ids (the id is the dedup key).
    assert ss.seed_id("event a") != ss.seed_id("event b")


# --------------------------------------------------------------------------
# the drip script: one seed per fire, then the next, then idle
# --------------------------------------------------------------------------


def test_drip_emits_one_seed_per_fire_then_next_then_idle(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ss.append_seed(store, _seed("cA", "first", "a", discovered_at="2026-06-19T10:00:00.000Z"))
    ss.append_seed(store, _seed("cB", "second", "b", discovered_at="2026-06-19T11:00:00.000Z"))

    first = _drip(store)
    assert first["type"] == "seed"
    assert first["event"] == "first"
    assert first["state"] == first["seed_id"]
    # The store now records the first seed as emitted.
    emitted = [s for s in ss.load_seeds(store) if s["emitted_at"] is not None]
    assert [s["event"] for s in emitted] == ["first"]

    second = _drip(store)
    assert second["event"] == "second", "the second fire drains the NEXT seed"

    idle = _drip(store)
    assert idle["type"] == "idle"
    assert idle["state"] == ss.IDLE_STATE


# --------------------------------------------------------------------------
# _fire_source: distinct seeds each write; idle re-fire collapses
# --------------------------------------------------------------------------


PINNED = datetime(2026, 6, 19, 12, 0, 0, tzinfo=UTC)
SOURCE = "scheduled/pitch-seeds"


def _drip_lodging(root: Path, store: Path) -> None:
    """A minimal lodging whose one source IS the real drip script pointed at
    `store`, plus a token pipe/channel so load_lodging is satisfied. Mirrors
    test_change_detection._lodge."""
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    (scheduled / "pitch-seeds.yaml").write_text(
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


def test_distinct_seeds_each_write_and_idle_collapses(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ss.append_seed(store, _seed("cA", "first", "a", discovered_at="2026-06-19T10:00:00.000Z"))
    ss.append_seed(store, _seed("cB", "second", "b", discovered_at="2026-06-19T11:00:00.000Z"))

    lodging_root = tmp_path / "lodging"
    _drip_lodging(lodging_root, store)
    daemon = AngelusDaemon(lodging_root, clock=FakeClock(PINNED))
    try:
        # Two distinct seeds -> two observations (distinct `state` tokens).
        assert _fire(daemon) is not None, "first seed must write an observation"
        assert _fire(daemon) is not None, "second (distinct) seed must write an observation"
        # Store drained -> idle. First idle is a state change (seed -> idle), so
        # it writes; the second idle collapses to no observation.
        assert _fire(daemon) is not None, "seed -> idle is a transition, writes"
        assert _fire(daemon) is None, "idle -> idle must collapse (no observation)"
        assert _observation_count(daemon) == 3
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# the triager through the real loaded Triager
# --------------------------------------------------------------------------


def _triage(observation: dict):
    lodging = load_lodging(LODGING)
    triager = lodging.triagers["pitch-seeds-watch"]
    return asyncio.run(run_python_triager(triager, observation, {}))


def _run_handler(observation: dict, metadata: dict) -> list[dict]:
    """Invoke the real handler script with arbitrary triager metadata.

    Mirrors run_python_triager's request shape, but lets a test override
    metadata (e.g. a typo'd urgent_pipe) without editing the shipped example.
    """
    payload = json.dumps(
        {
            "observation": observation,
            "prior_state": {},
            "triager": {
                "name": "pitch-seeds-watch",
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


def test_seed_observation_triages_to_seeds_pipe() -> None:
    seed = _seed(
        "cannon",
        "the world changed",
        "ignored",
        discovered_at="2026-06-19T10:00:00.000Z",
        source="https://example.com/change",
        date="2026-06-19",
    )
    obs = ss.seed_observation(seed)
    obs["emitted_at"] = "2026-06-19T12:00:00.000Z"
    findings, _state = _triage(obs)
    assert len(findings) == 1
    finding = findings[0]
    assert finding["type"] == "seed"
    assert finding["entity"] == seed["id"]
    assert finding["severity"] == "low"
    # Seeds are informational -- digest-only, never `now` (target_pipe only).
    assert finding["target_pipes"] == ["seeds"]
    # The change content rides through the body for the digest card.
    assert finding["body"]["event"] == "the world changed"
    assert finding["body"]["source"] == "https://example.com/change"
    assert finding["body"]["date"] == "2026-06-19"
    # body.title = event so the compact push leg headlines the event, not the
    # opaque seed id (_informational_headline prefers body.title).
    assert finding["body"]["title"] == "the world changed"


def test_seed_never_jumps_to_now() -> None:
    """Seeds are informational: every seed routes digest-only, regardless of what
    urgent_pipe is configured. Only the store_corrupt CONDITION uses urgent_pipe
    (see test_corruption_observation_triages_to_now_and_seeds_floor)."""
    seed = _seed("cannon", "a notable change", "ignored", discovered_at="2026-06-19T10:00:00.000Z")
    obs = ss.seed_observation(seed)
    obs["emitted_at"] = "2026-06-19T12:00:00.000Z"
    findings = _run_handler(obs, {"target_pipe": "seeds", "urgent_pipe": "now"})
    assert len(findings) == 1
    assert findings[0]["target_pipes"] == ["seeds"], "a seed never jumps to now"


def test_idle_observation_yields_no_finding() -> None:
    findings, _state = _triage(ss.idle_observation())
    assert findings == []


# --------------------------------------------------------------------------
# dedup end to end: same seed never alerts twice through the catalog gate
# --------------------------------------------------------------------------


def test_same_seed_alerts_once_through_catalog(tmp_path: Path) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    seed = _seed("cannon", "recurring event", "build it", discovered_at="2026-06-19T10:00:00.000Z")
    obs = ss.seed_observation(seed)
    obs["emitted_at"] = "2026-06-19T12:00:00.000Z"
    try:
        findings, _ = _triage(obs)
        finding = findings[0]
        # First sighting opens an incident and enqueues to the seeds pipe.
        catalog.write_finding(None, finding, known_pipes={"seeds"})
        queued = catalog.pending_pipe_items("seeds", limit=None)
        assert len(queued) == 1, "the seed enqueues to the seeds pipe once"

        # The SAME seed (same id -> same entity) re-triaged: the emission gate
        # drops it -- no second enqueue, no re-alert.
        catalog.write_finding(None, finding, known_pipes={"seeds"})
        assert len(catalog.pending_pipe_items("seeds", limit=None)) == 1, (
            "a repeat of the same seed must not alert twice"
        )
    finally:
        connection.close()


# --------------------------------------------------------------------------
# the shipped example lodging wiring + the seed-cards preamble
# --------------------------------------------------------------------------


def test_example_lodging_wires_pitch_source_triager_pipe() -> None:
    lodging = load_lodging(LODGING)
    assert "scheduled/pitch-seeds" in lodging.sources
    assert "seeds" in lodging.pipes
    triager = lodging.triagers["pitch-seeds-watch"]
    assert triager.source_ref == "scheduled/pitch-seeds"
    assert triager.metadata["target_pipe"] == "seeds"
    assert triager.metadata["urgent_pipe"] == "now"


def test_seed_cards_template_renders_cards() -> None:
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    environment = Environment(
        loader=FileSystemLoader(LODGING / "render-templates"),
        autoescape=select_autoescape(enabled_extensions=()),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = environment.get_template("seed-cards.j2")
    findings = [
        {
            "severity": "low",
            "entity": "abc123",
            "body": {
                "event": "New model shipped",
                "source": "https://example.com/model",
                "date": "2026-06-20",
            },
        }
    ]
    rendered = template.render(findings_since_last_drain=findings)
    assert "Pitch seeds since last digest (1)" in rendered
    assert "New model shipped" in rendered
    assert "https://example.com/model" in rendered
    assert "(2026-06-20)" in rendered
    assert "cannon:" not in rendered and "build:" not in rendered

    empty = template.render(findings_since_last_drain=[])
    assert "No new Pitch seeds" in empty


# --------------------------------------------------------------------------
# "never silently dropped" -- the invariant the blockers threatened
# --------------------------------------------------------------------------


def test_load_seeds_skips_malformed_line_keeps_valid(tmp_path: Path, capsys) -> None:
    """A torn JSONL line is skipped (loud on stderr), not fatal -- valid seeds
    behind it still load and drain."""
    store = tmp_path / "seeds.jsonl"
    good_a = _seed("cA", "a", "x", discovered_at="2026-06-19T10:00:00.000Z")
    good_b = _seed("cB", "b", "y", discovered_at="2026-06-19T11:00:00.000Z")
    store.write_text(
        json.dumps(good_a, sort_keys=True) + "\n"
        + "{not valid json at all\n"
        + json.dumps(good_b, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    seeds = ss.load_seeds(store)
    assert [s["event"] for s in seeds] == ["a", "b"], "valid seeds survive a torn line"
    # Fail-LOUD: the torn line is reported so a corrupt store stays visible on the
    # CLI/manual path (the live daemon path uses the corruption observation).
    assert "preserving malformed line 2" in capsys.readouterr().err


def test_write_seeds_preserves_torn_row_across_rewrite(tmp_path: Path) -> None:
    """FINDING 1(a): a rewrite triggered by an unrelated append/stamp must NOT
    delete the torn row -- the malformed bytes are carried through verbatim, so a
    torn seed stays recoverable instead of being silently and permanently lost."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "a", "x", discovered_at="2026-06-19T10:00:00.000Z")
    torn = '{"id": "deadbeefdeadbeef", "cannon": "broken'  # truncated, unparseable
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n" + torn + "\n",
        encoding="utf-8",
    )
    # An append rewrites the store. The torn row must survive the rewrite.
    fresh = _seed("cB", "b", "y", discovered_at="2026-06-19T11:00:00.000Z")
    assert ss.append_seed(store, fresh)

    raw = store.read_text(encoding="utf-8")
    assert torn in raw, "the torn bytes must be preserved across the rewrite"
    # Valid rows still parse; the torn row is still counted as malformed.
    seeds, malformed = ss._parse_store(store)
    assert {s["event"] for s in seeds} == {"a", "b"}
    assert malformed == [torn], "the torn row is carried, not deleted"

    # And it survives a SECOND rewrite (a stamp), still not deleted.
    assert ss.mark_emitted(store, good["id"], now="2026-06-19T12:00:00.000Z")
    assert torn in store.read_text(encoding="utf-8"), "still preserved after a stamp"


def test_emit_then_stamp_reemits_if_crash_before_stamp(tmp_path: Path) -> None:
    """BLOCKING 1: pick (emit) without stamping, simulate a crash before the
    stamp persists, and assert the seed is NOT lost -- it re-emits next tick.
    Only after mark_emitted lands is it drained."""
    store = tmp_path / "seeds.jsonl"
    ss.append_seed(store, _seed("cA", "only", "a", discovered_at="2026-06-19T10:00:00.000Z"))

    # Tick 1: the drip peeks + emits the observation, then the process dies
    # BEFORE mark_emitted -- so emitted_at never persists.
    picked = ss.peek_unemitted(store)
    assert picked is not None and picked["event"] == "only"
    # <crash here: no mark_emitted>

    # Tick 2: the seed is still unemitted, so it re-emits (at-least-once) rather
    # than being marked emitted-but-unseen and skipped forever (at-most-once).
    again = ss.peek_unemitted(store)
    assert again is not None and again["id"] == picked["id"], "seed not lost on crash"

    # Once the stamp lands the seed is drained and never re-emits.
    assert ss.mark_emitted(store, again["id"], now="2026-06-19T12:00:00.000Z")
    assert ss.peek_unemitted(store) is None
    persisted = ss.load_seeds(store)
    assert persisted[0]["emitted_at"] == "2026-06-19T12:00:00.000Z"


def _const_obs_lodging(root: Path, observation: dict) -> None:
    """A lodging whose drip source emits a FIXED observation every fire -- models
    a crash re-emit (same seed dripped twice because the stamp never landed),
    so the collapse/gate behaviour can be exercised through the real daemon."""
    obs_file = root / "fixed-observation.json"
    root.mkdir(parents=True, exist_ok=True)
    obs_file.write_text(json.dumps(observation) + "\n", encoding="utf-8")
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    (scheduled / "pitch-seeds.yaml").write_text(
        "cadence: 10m\ncheck:\n  kind: shell\n  command: cat " + str(obs_file) + "\n",
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


def test_reemitted_seed_collapses_to_no_double_alert(tmp_path: Path) -> None:
    """BLOCKING 1 (second half): a re-emitted seed (same id -> same `state`)
    collapses against the unchanged change-signature, so the at-least-once
    re-emit never produces a second observation -> no double-alert."""
    seed = _seed("cR", "re-emit", "b", discovered_at="2026-06-19T10:00:00.000Z")
    observation = ss.seed_observation(seed)

    lodging_root = tmp_path / "lodging"
    _const_obs_lodging(lodging_root, observation)
    daemon = AngelusDaemon(lodging_root, clock=FakeClock(PINNED))
    try:
        first_obs, _ = asyncio.run(daemon._fire_source(SOURCE))
        assert first_obs is not None, "first emission writes an observation"
        # Identical observation again (the crash re-emit) -- same `state` token,
        # so the engine collapses it: no new observation, no second alert.
        second_obs, _ = asyncio.run(daemon._fire_source(SOURCE))
        assert second_obs is None, "a re-emitted seed collapses (no double-alert)"
        assert _observation_count(daemon) == 1
    finally:
        daemon.connection.close()


def test_mark_emitted_does_not_clobber_concurrent_append(tmp_path: Path) -> None:
    """BLOCKING 2 (lost update): an append that lands between peek and stamp must
    survive the stamp's rewrite. mark_emitted re-reads under lock, so it stamps
    the picked seed WITHOUT writing back a stale snapshot that drops the new one."""
    store = tmp_path / "seeds.jsonl"
    first = _seed("cA", "first", "a", discovered_at="2026-06-19T10:00:00.000Z")
    ss.append_seed(store, first)

    # Drip picks `first` (snapshot taken)...
    picked = ss.peek_unemitted(store)
    assert picked["id"] == first["id"]

    # ...meanwhile the live pitch chain appends a brand-new seed...
    fresh = _seed("cB", "fresh", "b", discovered_at="2026-06-19T11:00:00.000Z")
    assert ss.append_seed(store, fresh)

    # ...then the drip stamps `first`. The fresh seed must NOT be clobbered.
    assert ss.mark_emitted(store, picked["id"], now="2026-06-19T12:00:00.000Z")
    seeds = {s["id"]: s for s in ss.load_seeds(store)}
    assert fresh["id"] in seeds, "a concurrent append must not be lost by the stamp"
    assert seeds[first["id"]]["emitted_at"] == "2026-06-19T12:00:00.000Z"
    assert seeds[fresh["id"]]["emitted_at"] is None, "the fresh seed is still pending"


def test_concurrent_appends_and_drains_no_loss_no_corruption(tmp_path: Path) -> None:
    """BLOCKING 2 (stress): many appenders and drainers hammering the store at
    once must lose no seed and leave a parseable (untorn) file -- the flock plus
    the per-process-unique temp name hold under contention."""
    import threading

    store = tmp_path / "seeds.jsonl"
    count = 30
    expected_ids = {ss.seed_id(f"e{i}") for i in range(count)}

    barrier = threading.Barrier(count + 6)

    def appender(i: int) -> None:
        barrier.wait()
        ss.append_seed(
            store,
            _seed(f"c{i}", f"e{i}", "b", discovered_at=f"2026-06-19T10:00:{i % 60:02d}.000Z"),
        )

    def drainer() -> None:
        barrier.wait()
        ss.drain_one(store)  # races the appends; must never tear the file

    threads = [threading.Thread(target=appender, args=(i,)) for i in range(count)]
    threads += [threading.Thread(target=drainer) for _ in range(6)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    seeds = ss.load_seeds(store)  # raises / drops rows if the file is torn
    ids = {s["id"] for s in seeds}
    assert ids == expected_ids, "every appended seed present, none lost or duplicated"
    assert len(seeds) == count, "no torn or duplicated rows"


def test_unknown_urgent_pipe_falls_back_to_seeds_floor() -> None:
    """WORTH-FIXING 3: a typo'd urgent_pipe (not a real pipe) must NOT drop the
    store_corrupt alert -- the daily `seeds` floor still carries it. urgent_pipe is
    validated at load (finding-20260619-dle0), so a typo fails the config; this
    exercises the handler floor that stays as defense-in-depth beneath that
    load-time check. (Seeds themselves never use urgent_pipe -- only this
    torn-store CONDITION does.)"""
    obs = ss.corruption_observation(1)
    findings = _run_handler(obs, {"target_pipe": "seeds", "urgent_pipe": "nwo"})
    assert len(findings) == 1
    # The typo'd "nwo" is carried (the engine skips it as unknown at enqueue),
    # but `seeds` is in the list as the never-drop floor -> the alert is delivered.
    assert "seeds" in findings[0]["target_pipes"], (
        "an unknown urgent_pipe must not drop the alert -- seeds is the floor"
    )


def test_drip_signals_corruption_only_after_good_seeds_drain(tmp_path: Path) -> None:
    """FINDING 1: a torn row never blocks a parseable seed -- the good seed drains
    FIRST. Only once no good seed remains does the drip raise the corruption
    signal (high severity, fixed collapse state), and the torn bytes stay in the
    store, recoverable."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    torn = '{"id": "deadbeefdeadbeef", "cannon": "broken'  # truncated, unparseable
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n" + torn + "\n",
        encoding="utf-8",
    )

    # The parseable seed drains first.
    first = _drip(store)
    assert first["type"] == "seed" and first["event"] == "good"

    # No good seed left + torn row preserved -> the corruption signal.
    corrupt = _drip(store)
    assert corrupt["type"] == ss.CORRUPT_TYPE
    assert corrupt["state"] == ss.CORRUPT_STATE
    assert corrupt["entity"] == ss.CORRUPT_ENTITY
    assert corrupt["severity"] == "high"
    assert corrupt["malformed_lines"] == 1

    # The signal keeps the SAME state on repeat ticks (so the engine collapses
    # it -- verified at daemon level below), and the torn bytes are still there.
    again = _drip(store)
    assert again["state"] == ss.CORRUPT_STATE
    assert torn in store.read_text(encoding="utf-8"), "torn bytes not deleted"


def test_corruption_observation_triages_to_now_and_seeds_floor() -> None:
    """FINDING 1: the corruption observation triages into a high-severity finding
    that jumps to `now` with `seeds` as the never-drop floor, fixed entity for the
    one-incident dedup, and the malformed count carried for the body."""
    findings, _state = _triage(ss.corruption_observation(2))
    assert len(findings) == 1
    finding = findings[0]
    assert finding["type"] == "store_corrupt"
    assert finding["severity"] == "high"
    assert finding["entity"] == ss.CORRUPT_ENTITY
    assert finding["target_pipes"] == ["now", "seeds"]
    assert finding["body"]["malformed_lines"] == 2
    assert "2 malformed" in finding["body"]["text"]


def test_consecutive_corruption_observations_collapse(tmp_path: Path) -> None:
    """FINDING 1: a torn store does not loop -- consecutive corruption ticks carry
    the same fixed `state`, so the engine collapses the repeat to no observation."""
    lodging_root = tmp_path / "lodging"
    _const_obs_lodging(lodging_root, ss.corruption_observation(1))
    daemon = AngelusDaemon(lodging_root, clock=FakeClock(PINNED))
    try:
        first, _ = asyncio.run(daemon._fire_source(SOURCE))
        assert first is not None, "first corrupt tick writes an observation"
        second, _ = asyncio.run(daemon._fire_source(SOURCE))
        assert second is None, "a torn store does not loop -- the repeat collapses"
        assert _observation_count(daemon) == 1
    finally:
        daemon.connection.close()


def test_torn_store_corruption_alerts_once_through_catalog(tmp_path: Path) -> None:
    """FINDING 1: the torn store raises EXACTLY ONE alert. The fixed corruption
    entity means the catalog emission gate opens one incident and drops the
    repeat, even when a good seed dripping in between re-writes the (now
    non-consecutive) corruption observation."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    findings, _ = _triage(ss.corruption_observation(1))
    finding = findings[0]
    try:
        catalog.write_finding(None, finding, known_pipes={"now", "seeds"})
        assert len(catalog.pending_pipe_items("seeds", limit=None)) == 1, (
            "the torn store enqueues once"
        )
        # The same corruption finding again (same fixed entity): gated, no re-alert.
        catalog.write_finding(None, finding, known_pipes={"now", "seeds"})
        assert len(catalog.pending_pipe_items("seeds", limit=None)) == 1, (
            "a torn store alerts exactly once"
        )
    finally:
        connection.close()


def test_invalid_utf8_line_drains_good_seed_preserves_bytes_alerts_once(
    tmp_path: Path,
) -> None:
    """BLOCKING 1: an invalid-UTF-8 line (a writer killed mid-multibyte append)
    must NOT wedge the store. A strict whole-file read would raise
    UnicodeDecodeError BEFORE any line parses -- so no good seed drains, no append
    lands, and the corruption alert can never fire. With errors="surrogateescape"
    the bad bytes round-trip into a preserved malformed line: the good seed drains
    first, the invalid bytes survive byte-for-byte, and corruption alerts once."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    # A lone 0xFF byte: invalid UTF-8 AND truncated JSON (the realistic torn
    # mid-multibyte append). The whole-file read must not choke on it.
    torn_bytes = b'{"id": "deadbeefdeadbeef", "cannon": "br\xff'
    store.write_bytes(
        (json.dumps(good, sort_keys=True) + "\n").encode("utf-8")
        + torn_bytes
        + b"\n"
    )

    # The good seed drains despite the invalid-UTF-8 line (the read no longer
    # wedges the whole store).
    first = _drip(store)
    assert first["type"] == "seed" and first["event"] == "good"

    # The torn bytes survive the rewrite the drain triggered, byte-for-byte.
    assert torn_bytes in store.read_bytes(), "invalid-UTF-8 line preserved verbatim"

    # No good seed left + a preserved torn row -> the corruption signal fires.
    corrupt = _drip(store)
    assert corrupt["type"] == ss.CORRUPT_TYPE
    assert corrupt["entity"] == ss.CORRUPT_ENTITY
    assert corrupt["severity"] == "high"
    assert corrupt["malformed_lines"] == 1

    # ...and it alerts EXACTLY ONCE through the catalog emission gate.
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    try:
        findings, _ = _triage(ss.corruption_observation(corrupt["malformed_lines"]))
        catalog.write_finding(None, findings[0], known_pipes={"now", "seeds"})
        catalog.write_finding(None, findings[0], known_pipes={"now", "seeds"})
        assert len(catalog.pending_pipe_items("seeds", limit=None)) == 1, (
            "an invalid-UTF-8 torn store alerts exactly once"
        )
    finally:
        connection.close()


def test_non_seed_json_lines_counted_malformed_not_seeds(tmp_path: Path) -> None:
    """BLOCKING 2: a line that parses as JSON but is not a seed -- a bare list
    (``[]``) or a dict missing required keys -- must be counted+preserved as
    MALFORMED, not appended to `seeds`. Otherwise next_unemitted/seed_observation
    crash on it (list has no .get / dict has no id), the drip exits nonzero, no
    corruption signal fires, and the good seed behind it is starved."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    not_a_dict = "[]"  # valid JSON, not a seed object
    missing_keys = json.dumps({"cannon": "c", "event": "e"})  # dict, no id/...
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n"
        + not_a_dict + "\n"
        + missing_keys + "\n",
        encoding="utf-8",
    )

    # Both non-seed lines are malformed, not seeds -- only the good seed parses.
    seeds, malformed = ss._parse_store(store)
    assert [s["event"] for s in seeds] == ["good"]
    assert set(malformed) == {not_a_dict, missing_keys}, (
        "non-seed JSON is preserved as malformed, not added to seeds"
    )

    # The good seed drains without crashing on the non-seed rows.
    first = _drip(store)
    assert first["type"] == "seed" and first["event"] == "good"

    # No good seed left -> corruption signal, counting BOTH bad lines, which stay
    # in the store (preserved, not deleted, not treated as seeds).
    corrupt = _drip(store)
    assert corrupt["type"] == ss.CORRUPT_TYPE
    assert corrupt["malformed_lines"] == 2
    raw = store.read_text(encoding="utf-8")
    assert not_a_dict in raw and missing_keys in raw, "bad lines preserved verbatim"

    # Exactly one alert through the catalog emission gate.
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    try:
        findings, _ = _triage(ss.corruption_observation(corrupt["malformed_lines"]))
        catalog.write_finding(None, findings[0], known_pipes={"now", "seeds"})
        catalog.write_finding(None, findings[0], known_pipes={"now", "seeds"})
        assert len(catalog.pending_pipe_items("seeds", limit=None)) == 1, (
            "a non-seed-JSON torn store alerts exactly once"
        )
    finally:
        connection.close()


def test_non_scalar_id_is_malformed_no_livelock(tmp_path: Path) -> None:
    """ROUND-5 BLOCKING: a row whose ``id`` is not a scalar str ({"id": []}) must
    be classified MALFORMED, not a seed. With a presence-only check it passed
    _is_seed_row, got SELECTED by next_unemitted, emitted as state=="[]", then
    NEVER stamped (mark_emitted compares ``[] == "[]"`` which is False), so it
    re-emitted every tick forever -- a livelock that starves the good seed behind
    it. Assert: the bad row is malformed (preserved + counted), the good seed
    drains, and the bad row is never selected as a seed or re-emitted."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    # A row with every OTHER required key present (incl. a valid event) but a
    # NON-SCALAR id -- the livelock. The valid event keeps the id-scalar check the
    # single defect under test, not masked by the missing-event rejection.
    bad_id = json.dumps(
        {"id": [], "discovered_at": "2026-06-19T09:00:00.000Z", "emitted_at": None,
         "event": "e"}
    )
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n" + bad_id + "\n",
        encoding="utf-8",
    )

    # The bad row is malformed, not a seed -- only the good seed parses.
    seeds, malformed = ss._parse_store(store)
    assert [s["event"] for s in seeds] == ["good"], "non-scalar id is not a seed"
    assert malformed == [bad_id], "the bad row is preserved + counted malformed"

    # The good seed is what next_unemitted picks (the bad row never wedges ahead
    # of it despite its older discovered_at).
    picked = ss.next_unemitted(seeds)
    assert picked is not None and picked["event"] == "good"

    # The good seed drains through the real drip...
    first = _drip(store)
    assert first["type"] == "seed" and first["event"] == "good"

    # ...and on the NEXT tick the store is corrupt (the bad row is the only thing
    # left), NOT a re-emit of the bad row -- no livelock, no second seed.
    nxt = _drip(store)
    assert nxt["type"] == ss.CORRUPT_TYPE, "the bad row never re-emits as a seed"
    assert nxt["malformed_lines"] == 1
    assert bad_id in store.read_text(encoding="utf-8"), "bad row preserved verbatim"

    # A third tick stays corrupt with the SAME state -- no per-tick re-selection.
    third = _drip(store)
    assert third["type"] == ss.CORRUPT_TYPE and third["state"] == ss.CORRUPT_STATE


def test_missing_or_empty_event_is_malformed(tmp_path: Path) -> None:
    """`event` is the one content field a seed exists to carry, and the drip emits
    it verbatim into the daily card. A row missing it (or with an empty/non-str
    event) would drain into a contentless card that falls back to the opaque seed
    id, with NO corruption alert. So such a row is malformed (preserved + counted),
    not a seed -- the fail-loud contract, not a silent low-information delivery."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    missing = json.dumps(
        {"id": "deadbeefdeadbeef", "discovered_at": "2026-06-19T09:00:00.000Z", "emitted_at": None}
    )
    empty = json.dumps(
        {"id": "feedfacefeedface", "discovered_at": "2026-06-19T09:30:00.000Z",
         "emitted_at": None, "event": ""}
    )
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n" + missing + "\n" + empty + "\n",
        encoding="utf-8",
    )
    assert not ss._is_seed_row(json.loads(missing)), "a row with no event is not a seed"
    assert not ss._is_seed_row(json.loads(empty)), "a row with an empty event is not a seed"
    seeds, malformed = ss._parse_store(store)
    assert [s["event"] for s in seeds] == ["good"], "only the row with an event is a seed"
    assert set(malformed) == {missing, empty}, "contentless rows preserved + counted malformed"


def test_non_str_discovered_at_is_malformed(tmp_path: Path) -> None:
    """ROUND-5 BLOCKING (companion): discovered_at drives the lexicographic drain
    order; a non-str value could mis-order the drain, so a row with a non-str
    discovered_at is malformed, not a seed."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    # Valid event so the non-str discovered_at is the single defect under test
    # (not masked by the missing-event rejection).
    bad_ts = json.dumps(
        {"id": "deadbeefdeadbeef", "discovered_at": 12345, "emitted_at": None,
         "event": "e"}
    )
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n" + bad_ts + "\n",
        encoding="utf-8",
    )
    seeds, malformed = ss._parse_store(store)
    assert [s["event"] for s in seeds] == ["good"]
    assert malformed == [bad_ts], "a non-str discovered_at is malformed, not a seed"


def test_wrong_typed_emitted_at_is_malformed_not_silently_skipped(tmp_path: Path) -> None:
    """ROUND-6 (companion): emitted_at is the drained-marker (null until drained,
    then an ISO str). next_unemitted treats ANY non-None value as already-emitted,
    so a wrong-typed value like ``false`` would SILENTLY skip the row -- a dropped
    seed that never alerts. Only null-or-str can drain, so a bool/number emitted_at
    is malformed (preserved + counted), not a seed."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    # Valid event so the wrong-typed emitted_at is the single defect under test
    # (not masked by the missing-event rejection).
    bad = json.dumps(
        {
            "id": "deadbeefdeadbeef",
            "discovered_at": "2026-06-19T09:00:00.000Z",
            "emitted_at": False,
            "event": "e",
        }
    )
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n" + bad + "\n",
        encoding="utf-8",
    )
    assert not ss._is_seed_row(json.loads(bad)), "a non-null/str emitted_at is rejected"
    seeds, malformed = ss._parse_store(store)
    assert [s["event"] for s in seeds] == ["good"]
    assert malformed == [bad], "a wrong-typed emitted_at is malformed, not a silent skip"


def test_deeply_nested_toxic_row_is_malformed_drain_not_wedged(tmp_path: Path) -> None:
    """ROUND-6 BLOCKING: a seed-shaped row with a pathologically deep nested value
    makes json.loads raise RecursionError -- a RuntimeError, NOT a JSONDecodeError --
    and the surrogate scan would add a second recursion over the same tree. Either,
    uncaught, raises out of _parse_store: every drip tick exits nonzero and all good
    seeds are starved. Assert the toxic row is classified malformed (preserved +
    counted), the good seed drains, store_corrupt fires, and NO crash -- the drain
    is not wedged."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    # A seed-SHAPED row (id/discovered_at/emitted_at all present) whose `extra`
    # field nests far deeper than the interpreter's recursion limit, so json.loads
    # raises RecursionError on it. Depth is derived from the live limit (which the
    # drip subprocess shares) so the row is toxic regardless of how it is set.
    depth = sys.getrecursionlimit() * 5
    toxic = (
        '{"id": "deadbeefdeadbeef", "discovered_at": "2026-06-19T09:00:00.000Z", '
        '"emitted_at": null, "extra": '
        + "[" * depth
        + "]" * depth
        + "}"
    )
    store.write_text(
        json.dumps(good, sort_keys=True) + "\n" + toxic + "\n",
        encoding="utf-8",
    )

    # The toxic row is malformed (the RecursionError is caught per-line), the good
    # seed still parses -- no exception escapes the load.
    seeds, malformed = ss._parse_store(store)
    assert [s["event"] for s in seeds] == ["good"], "the good seed survives the toxic row"
    assert malformed == [toxic], "the toxic row is preserved + counted malformed"

    # The good seed drains through the real drip (the tick does not crash)...
    first = _drip(store)
    assert first["type"] == "seed" and first["event"] == "good"

    # ...and the next tick raises the corruption signal for the quarantined row, not
    # a nonzero exit. The toxic bytes stay in the store, recoverable.
    corrupt = _drip(store)
    assert corrupt["type"] == ss.CORRUPT_TYPE
    assert corrupt["malformed_lines"] == 1
    assert toxic in store.read_text(encoding="utf-8"), "toxic row preserved verbatim"


def test_reserved_token_id_is_malformed_not_a_seed(tmp_path: Path) -> None:
    """ROUND-6: a row whose id equals a reserved observation token (the idle state
    token or the store-corrupt state/entity token) passes a digest-blind
    presence/type check, but a seed id IS the observation `state`/`entity` -- so if
    emitted it would collide with the idle/corruption collapse signature and
    mis-collapse a real signal. Assert each reserved-token id is classified
    malformed (never emitted as a seed) and the good seed still drains."""
    for token in (ss.IDLE_STATE, ss.CORRUPT_STATE, ss.CORRUPT_ENTITY):
        store = tmp_path / f"seeds-{token}.jsonl"
        good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
        # A row carrying every required key but an id equal to a reserved token.
        reserved = json.dumps(
            {"id": token, "discovered_at": "2026-06-19T09:00:00.000Z", "emitted_at": None}
        )
        store.write_text(
            json.dumps(good, sort_keys=True) + "\n" + reserved + "\n",
            encoding="utf-8",
        )

        # The reserved-token row is malformed, not a seed.
        assert not ss._is_seed_row(json.loads(reserved)), f"{token!r} id is rejected"
        seeds, malformed = ss._parse_store(store)
        assert [s["event"] for s in seeds] == ["good"], f"{token!r} id is not a seed"
        assert malformed == [reserved], f"{token!r}-id row preserved + counted malformed"

        # The good seed drains; the reserved-token row never emits as a seed -- the
        # next tick is the corruption signal, not a seed observation for the token.
        first = _drip(store)
        assert first["type"] == "seed" and first["event"] == "good"
        nxt = _drip(store)
        assert nxt["type"] == ss.CORRUPT_TYPE, f"{token!r}-id row never emits as a seed"


def test_lone_surrogate_in_seed_string_is_malformed_not_normalized(
    tmp_path: Path,
) -> None:
    """ROUND-5 WORTH-FIXING: an invalid UTF-8 byte INSIDE a structurally-complete
    JSON string value decodes (via surrogateescape) to a lone surrogate and parses
    cleanly through json.loads + the key/type checks. Left unchecked it becomes a
    "seed"; on rewrite json.dumps re-encodes the surrogate as a \\udcXX escape --
    the value is silently MUTATED (not preserved, not counted corrupt) and a lone
    surrogate can later raise on a strict-UTF-8 channel. Assert it is classified
    MALFORMED (preserved verbatim + store_corrupt fires), not accepted+normalized."""
    store = tmp_path / "seeds.jsonl"
    good = _seed("cA", "good", "x", discovered_at="2026-06-19T10:00:00.000Z")
    # A COMPLETE JSON seed (closes its quotes and braces) whose event carries a raw
    # 0xFF byte INSIDE the string -- valid JSON structure, invalid UTF-8. json.dumps
    # is pure ASCII, so splice the 0xFF into the event value's bytes directly (still
    # a closed, parseable line once surrogateescape-decoded).
    bad_seed = _seed("cB", "EVENTTOK", "y", discovered_at="2026-06-19T11:00:00.000Z")
    bad_line_bytes = (
        json.dumps(bad_seed, sort_keys=True).encode("utf-8").replace(b"EVENTTOK", b"EVEN\xffTOK")
    )
    store.write_bytes(
        (json.dumps(good, sort_keys=True) + "\n").encode("utf-8")
        + bad_line_bytes
        + b"\n"
    )

    # The line parses as JSON and carries every seed key, but the lone surrogate
    # routes it to malformed -- NOT accepted as a seed and NOT normalized.
    seeds, malformed = ss._parse_store(store)
    assert [s["event"] for s in seeds] == ["good"], "surrogate row is not a seed"
    assert len(malformed) == 1, "the surrogate-bearing row is counted malformed"

    # The good seed drains; the bad bytes survive byte-for-byte (not re-encoded
    # as a \\udcXX escape).
    first = _drip(store)
    assert first["type"] == "seed" and first["event"] == "good"
    assert b"\xff" in store.read_bytes(), "invalid byte preserved verbatim, not normalized"

    # No good seed left -> the corruption signal fires for the quarantined row.
    corrupt = _drip(store)
    assert corrupt["type"] == ss.CORRUPT_TYPE
    assert corrupt["malformed_lines"] == 1


def test_seed_cards_template_renders_corruption_row() -> None:
    """NIT: a store_corrupt finding also lands in the daily `seeds` pipe (the
    never-drop floor), so the seed-cards template must render it legibly -- a
    corruption row, not an empty seed card with blank cannon/build fields."""
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    environment = Environment(
        loader=FileSystemLoader(LODGING / "render-templates"),
        autoescape=select_autoescape(enabled_extensions=()),
        trim_blocks=True,
        lstrip_blocks=True,
    )
    template = environment.get_template("seed-cards.j2")
    findings = [
        {
            "type": "store_corrupt",
            "severity": "high",
            "entity": "pitch-seed-store",
            "body": {"malformed_lines": 3, "text": "..."},
        }
    ]
    rendered = template.render(findings_since_last_drain=findings)
    assert "seed store corruption -- 3 malformed line(s)" in rendered
    assert "cannon:" not in rendered, "a corruption row is not a (blank) seed card"


def _load_drip_module():
    """Import the real drip script as a module so its main() can be driven
    in-process (the subprocess _drip helper cannot shim mark_emitted)."""
    spec = importlib.util.spec_from_file_location("pitch_seed_drip", DRIP)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def test_drip_main_emits_observation_before_stamping(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """FINDING 2: drive the REAL drip main() with mark_emitted shimmed to raise.
    The script emits THEN stamps, so the observation must already be printed when
    the stamp step blows up, and the seed must stay UNEMITTED (re-emittable next
    tick). Reordering main() to stamp-first makes this fail: the stamp would raise
    before the print, so no observation is emitted and the seed would be lost."""
    drip = _load_drip_module()
    store = tmp_path / "seeds.jsonl"
    ss.append_seed(store, _seed("cA", "only", "a", discovered_at="2026-06-19T10:00:00.000Z"))

    def boom(*args, **kwargs):
        raise RuntimeError("crash at the stamp step (after emit)")

    monkeypatch.setattr(drip.seed_store, "mark_emitted", boom)
    monkeypatch.setattr(sys, "argv", ["pitch_seed_drip.py", str(store)])

    with pytest.raises(RuntimeError):
        drip.main()

    # The observation was printed BEFORE the stamp step blew up.
    printed = json.loads(capsys.readouterr().out)
    assert printed["type"] == "seed" and printed["event"] == "only"

    # The seed is still UNEMITTED -- the next tick re-emits it (at-least-once)
    # rather than marking it emitted-but-unseen and skipping it forever.
    seeds = ss.load_seeds(store)
    assert seeds[0]["emitted_at"] is None, "a crash before the stamp leaves the seed re-emittable"
    assert ss.peek_unemitted(store) is not None


def test_example_urgent_pipe_names_a_real_pipe() -> None:
    """The shipped example's urgent_pipe must name a real pipe. config.py now
    cross-ref validates urgent_pipe at load (finding-20260619-dle0), so a bad value
    would make load_lodging raise; this asserts the example is clean explicitly."""
    lodging = load_lodging(LODGING)
    triager = lodging.triagers["pitch-seeds-watch"]
    urgent_pipe = triager.metadata["urgent_pipe"]
    assert urgent_pipe in lodging.pipes, (
        f"urgent_pipe {urgent_pipe!r} is not a real pipe"
    )
