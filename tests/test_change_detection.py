"""Source-side change-detection (observation collapse).

angelus exists to catch STATE TRANSITIONS -- the activating example is a
website going up->down within the check cadence. So the overriding invariant
here is NEVER MISS A STATE TRANSITION: a fire writes an observation on every
real change (and on the first sighting), and collapses only fires that are
provably identical to the last one already recorded. Over-writing is harmless;
skipping a transition is the one unacceptable failure.

These tests drive _fire_source -- the exact body APScheduler and the
`fire_source` op run -- through a controllable source (its check `cat`s a JSON
fixture the test rewrites between fires, or deletes to force a check_failed).
Each asserts at the OBSERVATION layer (the collapse decision), independent of
any triager. The matching mutation that breaks each transition test is noted in
the shard report (always-write / never-write / drop-the-outcome-fold).
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import angelus.daemon as daemon_mod
from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon, _change_signature

PINNED = datetime(2026, 6, 6, 12, 0, 0, tzinfo=UTC)
SOURCE = "scheduled/s"


def _lodge(root: Path) -> Path:
    """Minimal lodging with one source whose check `cat`s a JSON fixture, plus
    a token pipe/channel so load_lodging is happy. Returns the fixture path the
    test rewrites to drive transitions."""
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    fixture = root / "payload.json"
    (scheduled / "s.yaml").write_text(
        f"cadence: 1h\ncheck:\n  kind: shell\n  command: 'cat {fixture}'\n",
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
    return fixture


def _set(fixture: Path, payload: dict) -> None:
    fixture.write_text(json.dumps(payload), encoding="utf-8")


def _observation_count(daemon: AngelusDaemon) -> int:
    return int(
        daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM observations"
        ).fetchone()["n"]
    )


def _watch_row(daemon: AngelusDaemon) -> dict:
    return dict(
        daemon.connection.execute(
            "SELECT * FROM watch_state WHERE source_ref = ?", (SOURCE,)
        ).fetchone()
    )


def _fire(daemon: AngelusDaemon) -> tuple[int | None, str]:
    result = asyncio.run(daemon._fire_source(SOURCE))
    assert result is not None
    return result


# --------------------------------------------------------------------------
# _change_signature unit: the simple-state token, the hash fallback, and the
# outcome fold -- the three rules every collapse decision rides on.
# --------------------------------------------------------------------------


def test_signature_prefers_simple_state_field() -> None:
    """When the payload carries `state`, that IS the token -- so a CI run with a
    new sha but the same conclusion collapses. Discrimination: two payloads that
    differ everywhere EXCEPT state hash to the same signature."""
    a = {"conclusion": "success", "sha": "aaa", "run_started": "t1", "state": "success"}
    b = {"conclusion": "success", "sha": "bbb", "run_started": "t2", "state": "success"}
    assert _change_signature(a, "ok") == _change_signature(b, "ok") == "success"
    c = {"state": "failure"}
    assert _change_signature(c, "ok") != _change_signature(a, "ok")


def test_signature_falls_back_to_full_payload_hash() -> None:
    """No `state` field -> canonical full-payload hash, so an unconfigured check
    still collapses on identity and never silently loses data. Key order does
    not matter (sort_keys); a value change does."""
    assert _change_signature({"a": 1, "b": 2}, "ok") == _change_signature(
        {"b": 2, "a": 1}, "ok"
    )
    assert _change_signature({"a": 1}, "ok") != _change_signature({"a": 2}, "ok")


def test_signature_folds_outcome_so_failure_is_always_a_change() -> None:
    """A check that starts failing or recovers MUST read as a change even if the
    state token is unchanged: check_failed is prefixed so it can never collide
    with an ok signature. This is the outcome-fold guard."""
    ok = _change_signature({"state": "200"}, "ok")
    failed = _change_signature({"state": "200"}, "check_failed")
    assert ok == "200"
    assert failed != ok
    assert failed.startswith("check_failed:")


# --------------------------------------------------------------------------
# Transition invariants through _fire_source (the never-miss guarantees).
# --------------------------------------------------------------------------


def test_first_ever_fire_writes_an_observation(tmp_path: Path) -> None:
    """No prior watch_state row is a first sighting -> always an observation,
    and watch_state is seeded with its signature."""
    fixture = _lodge(tmp_path)
    _set(fixture, {"entity": "site", "status_code": 200, "state": "200"})
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        obs_id, outcome = _fire(daemon)
        assert outcome == "ok"
        assert obs_id is not None, "first sighting must write an observation"
        assert _observation_count(daemon) == 1
        assert _watch_row(daemon)["last_state"] == "200"
    finally:
        daemon.connection.close()


def test_unchanged_state_collapses_to_no_observation(tmp_path: Path) -> None:
    """200 then 200 again -> exactly ONE observation (the first); the second
    tick writes none but still bumps last_checked_at (the heartbeat)."""
    fixture = _lodge(tmp_path)
    _set(fixture, {"entity": "site", "status_code": 200, "state": "200"})
    clock = FakeClock(PINNED)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        first_id, _ = _fire(daemon)
        assert first_id is not None
        first_checked = _watch_row(daemon)["last_checked_at"]

        clock.advance(timedelta(minutes=5))
        second_id, _ = _fire(daemon)
        assert second_id is None, "unchanged 200 must collapse (no observation)"
        assert _observation_count(daemon) == 1

        row = _watch_row(daemon)
        # Heartbeat advanced even though no observation was written...
        assert row["last_checked_at"] != first_checked
        # ...but last_changed_at / last_observation_id still point at the
        # one real transition (the first sighting).
        assert row["last_observation_id"] == first_id
        assert row["last_changed_at"] == first_checked
    finally:
        daemon.connection.close()


def test_flap_catches_every_transition(tmp_path: Path) -> None:
    """200 -> 503 -> 200 writes THREE observations: every transition is caught.
    This is the never-miss-a-transition guard -- a down that recovers within the
    cadence must not be collapsed away."""
    fixture = _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        for code in (200, 503, 200):
            _set(fixture, {"entity": "site", "status_code": code, "state": str(code)})
            obs_id, outcome = _fire(daemon)
            assert outcome == "ok"
            assert obs_id is not None, f"transition to {code} must write"
        assert _observation_count(daemon) == 3
    finally:
        daemon.connection.close()


def test_repeated_down_between_transitions_collapses(tmp_path: Path) -> None:
    """200 -> 503 -> 503 -> 200: the middle repeat 503 collapses, so only THREE
    observations (the two transitions plus the first sighting), not four. A
    persistent outage does not churn observations every tick."""
    fixture = _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        wrote = []
        for code in (200, 503, 503, 200):
            _set(fixture, {"entity": "site", "status_code": code, "state": str(code)})
            obs_id, _ = _fire(daemon)
            wrote.append(obs_id is not None)
        assert wrote == [True, True, False, True], wrote
        assert _observation_count(daemon) == 3
    finally:
        daemon.connection.close()


def test_ok_to_check_failed_to_ok_writes_on_both_edges(tmp_path: Path) -> None:
    """ok -> check_failed -> ok writes an observation on BOTH transitions. A
    check that goes blind (the binary vanished, a timeout) and then recovers
    must surface both edges -- the outcome fold makes the check_failed signature
    distinct from any ok one."""
    fixture = _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        _set(fixture, {"entity": "site", "status_code": 200, "state": "200"})
        id1, out1 = _fire(daemon)
        assert (out1, id1 is not None) == ("ok", True)

        # check_failed: delete the fixture so `cat` exits non-zero.
        fixture.unlink()
        id2, out2 = _fire(daemon)
        assert out2 == "check_failed"
        assert id2 is not None, "ok -> check_failed must write"

        # ok again: recreate the same payload.
        _set(fixture, {"entity": "site", "status_code": 200, "state": "200"})
        id3, out3 = _fire(daemon)
        assert (out3, id3 is not None) == ("ok", True), "check_failed -> ok must write"

        assert _observation_count(daemon) == 3
        assert _watch_row(daemon)["last_state"] == "200"
    finally:
        daemon.connection.close()


def test_persistent_check_failed_collapses(tmp_path: Path) -> None:
    """ok -> check_failed -> check_failed: the second failure collapses (the
    error payload is identical), so the blind period does not churn. The first
    failure (the transition) still wrote."""
    fixture = _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        _set(fixture, {"entity": "site", "status_code": 200, "state": "200"})
        _fire(daemon)
        fixture.unlink()
        id2, _ = _fire(daemon)
        id3, _ = _fire(daemon)
        assert id2 is not None and id3 is None, (id2, id3)
        assert _observation_count(daemon) == 2
    finally:
        daemon.connection.close()


def test_ci_new_green_run_does_not_write_but_failure_does(tmp_path: Path) -> None:
    """ci-failing semantics: success with sha A then success with sha B and a
    different run_started writes NO new observation (state=conclusion collapses
    the new-green-run churn); success -> failure writes one. This is the case a
    whole-payload diff gets wrong (the sha/run_started change every push)."""
    fixture = _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        _set(fixture, {"conclusion": "success", "sha": "A",
                       "run_started": "t1", "state": "success"})
        id1, _ = _fire(daemon)
        assert id1 is not None  # first sighting

        # New green run: different sha + run_started, same conclusion.
        _set(fixture, {"conclusion": "success", "sha": "B",
                       "run_started": "t2", "state": "success"})
        id2, _ = _fire(daemon)
        assert id2 is None, "a new green run (same conclusion) must collapse"

        # Now it breaks.
        _set(fixture, {"conclusion": "failure", "sha": "C",
                       "run_started": "t3", "state": "failure"})
        id3, _ = _fire(daemon)
        assert id3 is not None, "success -> failure must write"

        assert _observation_count(daemon) == 2
    finally:
        daemon.connection.close()


def test_signature_error_is_fail_safe_writes_observation(
    tmp_path: Path, monkeypatch
) -> None:
    """If signature computation raises for any reason, the fire is treated as a
    CHANGE and the observation is written. Missing a transition is the one
    unacceptable failure, so an error must never silently skip the write -- even
    on an otherwise-unchanged state."""
    fixture = _lodge(tmp_path)
    _set(fixture, {"entity": "site", "status_code": 200, "state": "200"})
    daemon = AngelusDaemon(tmp_path, clock=FakeClock(PINNED))
    try:
        # Establish a prior row so a working signature WOULD collapse.
        _fire(daemon)
        assert _observation_count(daemon) == 1

        def boom(_payload, _outcome):
            raise RuntimeError("signature blew up")

        monkeypatch.setattr(daemon_mod, "_change_signature", boom)
        obs_id, _ = _fire(daemon)
        assert obs_id is not None, (
            "fail-safe: a signature error must still write the observation"
        )
        assert _observation_count(daemon) == 2
        # The fail-safe write stores a NULL baseline (signature unknown).
        assert _watch_row(daemon)["last_state"] is None
    finally:
        daemon.connection.close()


# --------------------------------------------------------------------------
# Bookkeeping / growth: watch_state stays one row per source, the heartbeat
# advances every tick, and the row tracks the last real transition.
# --------------------------------------------------------------------------


def test_watch_state_is_one_row_and_heartbeat_advances(tmp_path: Path) -> None:
    """Across many ticks (changed and unchanged) watch_state holds exactly ONE
    row for the source -- it never grows -- and last_checked_at advances on every
    tick, including the collapsed ones."""
    fixture = _lodge(tmp_path)
    clock = FakeClock(PINNED)
    daemon = AngelusDaemon(tmp_path, clock=clock)
    try:
        checked_stamps = []
        # 10 ticks, mostly unchanged 200s with one 503 blip in the middle.
        for i in range(10):
            code = 503 if i == 5 else 200
            _set(fixture, {"entity": "site", "status_code": code, "state": str(code)})
            _fire(daemon)
            checked_stamps.append(_watch_row(daemon)["last_checked_at"])
            clock.advance(timedelta(minutes=5))

        row_count = daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM watch_state"
        ).fetchone()["n"]
        assert row_count == 1, "watch_state must hold one row per source"
        # last_checked_at advanced on every single tick (strictly increasing).
        assert checked_stamps == sorted(checked_stamps)
        assert len(set(checked_stamps)) == 10, "heartbeat must advance each tick"
        # Only the transitions wrote observations: 200(first), 503, 200.
        assert _observation_count(daemon) == 3
    finally:
        daemon.connection.close()
