"""Per-source persistent-check-failure alarm (the fire-time failure tally).

A persistently broken check (expired auth, a CLI missing, an output-shape
change) fails EVERY fire of a source while the daemon itself stays healthy:
the heartbeat advances each fire so belfry's wedge/SLA checks stay green, the
repo-watch handlers skip check_failed observations by design, and observation
collapse means repeat failures write no observation at all -- so before this
alarm, a blind watch produced zero signal anywhere, indefinitely.

These tests drive _fire_source -- the exact body APScheduler and the
`fire_source` op run -- through controllable sources (each check `cat`s a
JSON fixture the test deletes to force check_failed and rewrites to recover),
and assert at the incident/finding layer: N consecutive failed fires open ONE
internal/source incident, short blips stay silent, recovery clears and
re-arms the B30 gate, and -- the discriminating case -- collapsed failures
that write NO observation still advance the tally, proving the counter lives
at fire time and not in triage.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

from angelus.daemon import (
    DEFAULT_SOURCE_FAIL_ALARM_AFTER,
    AngelusDaemon,
    _source_fail_alarm_after,
)
from angelus.lodging import load_lodging


def _lodge(root: Path, sources: tuple[str, ...] = ("s",)) -> dict[str, Path]:
    """Minimal lodging with one fixture-backed source per name, plus a token
    pipe/channel so load_lodging is happy. Returns fixture paths by source
    name; delete one to force check_failed on that source only."""
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    fixtures: dict[str, Path] = {}
    for name in sources:
        fixture = root / f"payload-{name}.json"
        (scheduled / f"{name}.yaml").write_text(
            f"cadence: 1h\ncheck:\n  kind: shell\n  command: 'cat {fixture}'\n",
            encoding="utf-8",
        )
        fixtures[name] = fixture
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
    return fixtures


def _ok(fixture: Path) -> None:
    fixture.write_text(
        json.dumps({"entity": "site", "status_code": 200, "state": "200"}),
        encoding="utf-8",
    )


def _fire(daemon: AngelusDaemon, name: str = "s") -> tuple[int | None, str]:
    result = asyncio.run(daemon._fire_source(f"scheduled/{name}"))
    assert result is not None
    return result


def _open_alarm_incidents(daemon: AngelusDaemon) -> list[dict]:
    return [
        i
        for i in daemon.catalog.open_incidents()
        if i["source"] == "internal/source"
    ]


def _alarm_findings(daemon: AngelusDaemon) -> list[dict]:
    rows = daemon.connection.execute(
        "SELECT * FROM findings WHERE source = 'internal/source' "
        "AND type = 'source_check_failing' ORDER BY id"
    )
    return [dict(row) for row in rows]


def test_persistent_failure_opens_incident_other_sources_unaffected(
    tmp_path: Path,
) -> None:
    """N consecutive check_failed fires on ONE source open exactly one
    internal/source incident for that source; a healthy sibling source is
    untouched. The finding rides the internal/* machinery: severity high,
    routed to `now` (so it fans to every channel, B7), and it bumps the
    open-internal tally belfry's open-incident check reads."""
    fixtures = _lodge(tmp_path, sources=("bad", "good"))
    for fixture in fixtures.values():
        _ok(fixture)
    daemon = AngelusDaemon(tmp_path)
    try:
        _fire(daemon, "bad")
        _fire(daemon, "good")
        fixtures["bad"].unlink()
        for i in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER):
            assert _open_alarm_incidents(daemon) == [], f"opened early at {i}"
            _, outcome = _fire(daemon, "bad")
            assert outcome == "check_failed"
            _fire(daemon, "good")

        incidents = _open_alarm_incidents(daemon)
        assert len(incidents) == 1
        assert incidents[0]["entity"] == "scheduled/bad"
        assert daemon.catalog.open_internal_incident_count() == 1

        findings = _alarm_findings(daemon)
        assert len(findings) == 1
        assert findings[0]["entity"] == "scheduled/bad"
        assert findings[0]["severity"] == "high"
        queued = daemon.connection.execute(
            "SELECT pipe FROM pipe_queues WHERE finding_id = ?",
            (findings[0]["id"],),
        ).fetchall()
        assert [row["pipe"] for row in queued] == ["now"]
    finally:
        daemon.connection.close()


def test_blip_below_threshold_stays_silent(tmp_path: Path) -> None:
    """N-1 failures then success never opens an incident (the handlers'
    no-churn intent), and the success RESETS the tally: another N-1 failures
    after the recovery still stay silent -- two blips don't add up to one
    alarm. The recovery's clearance is also a gate-dropped no-op (no row)."""
    fixtures = _lodge(tmp_path)
    _ok(fixtures["s"])
    daemon = AngelusDaemon(tmp_path)
    try:
        _fire(daemon)
        for _ in range(2):
            fixtures["s"].unlink()
            for _ in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER - 1):
                _fire(daemon)
            _ok(fixtures["s"])
            _, outcome = _fire(daemon)
            assert outcome == "ok"

        assert _open_alarm_incidents(daemon) == []
        assert _alarm_findings(daemon) == []
        all_internal = daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM findings WHERE source = 'internal/source'"
        ).fetchone()["n"]
        assert all_internal == 0, "no clearance row when nothing was open"
    finally:
        daemon.connection.close()


def test_recovery_clears_and_gate_rearms(tmp_path: Path) -> None:
    """Past the threshold the alarm fires once: continued failures are dropped
    by the B30 gate (one incident, one finding row). The first successful fire
    closes the incident via the paired clearance, re-arming the gate so a
    later genuine re-failure opens a NEW incident with a fresh finding."""
    fixtures = _lodge(tmp_path)
    _ok(fixtures["s"])
    daemon = AngelusDaemon(tmp_path)
    try:
        _fire(daemon)
        fixtures["s"].unlink()
        # Two fires past the threshold: the repeats must not duplicate.
        for _ in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER + 2):
            _fire(daemon)
        assert len(_open_alarm_incidents(daemon)) == 1
        assert len(_alarm_findings(daemon)) == 1

        _ok(fixtures["s"])
        _, outcome = _fire(daemon)
        assert outcome == "ok"
        assert _open_alarm_incidents(daemon) == []
        closures = [
            c
            for c in daemon.catalog.clearance_findings_since(None)
            if c["entity"] == "scheduled/s"
        ]
        assert len(closures) == 1, "recovery must record the clearance"

        # Gate re-armed: a genuine re-failure alarms again.
        fixtures["s"].unlink()
        for _ in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER):
            _fire(daemon)
        assert len(_open_alarm_incidents(daemon)) == 1
        assert len(_alarm_findings(daemon)) == 2
    finally:
        daemon.connection.close()


def test_collapsed_failures_still_advance_the_tally(tmp_path: Path) -> None:
    """THE discriminating test: after the first check_failed observation (the
    ok->check_failed transition), every further failed fire collapses --
    identical outcome and folded state token, NO new observation -- yet the
    alarm still opens on schedule. This proves the counter lives at fire time
    in the daemon, not in triage: a triager literally never sees the repeats
    it would need to count."""
    fixtures = _lodge(tmp_path)
    _ok(fixtures["s"])
    daemon = AngelusDaemon(tmp_path)
    try:
        _fire(daemon)  # ok: first sighting (observation 1)
        fixtures["s"].unlink()
        wrote = []
        for _ in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER):
            obs_id, outcome = _fire(daemon)
            assert outcome == "check_failed"
            wrote.append(obs_id is not None)
        # Only the transition wrote; the repeats collapsed to nothing.
        assert wrote == [True] + [False] * (DEFAULT_SOURCE_FAIL_ALARM_AFTER - 1)
        obs_count = daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM observations"
        ).fetchone()["n"]
        assert obs_count == 2

        incidents = _open_alarm_incidents(daemon)
        assert len(incidents) == 1, (
            "collapsed (observation-less) failed fires must still advance "
            "the tally and open the alarm"
        )
    finally:
        daemon.connection.close()


def test_threshold_env_override_and_fallback(tmp_path: Path, monkeypatch) -> None:
    """ANGELUS_SOURCE_FAIL_ALARM_AFTER tunes the threshold (read at daemon
    construction); invalid and non-positive values fall back to the default
    so a misconfigured env can never disable the alarm."""
    monkeypatch.setenv("ANGELUS_SOURCE_FAIL_ALARM_AFTER", "2")
    fixtures = _lodge(tmp_path)
    _ok(fixtures["s"])
    daemon = AngelusDaemon(tmp_path)
    try:
        assert daemon._source_fail_alarm_after == 2
        _fire(daemon)
        fixtures["s"].unlink()
        _fire(daemon)
        assert _open_alarm_incidents(daemon) == []
        _fire(daemon)
        assert len(_open_alarm_incidents(daemon)) == 1
    finally:
        daemon.connection.close()

    monkeypatch.setenv("ANGELUS_SOURCE_FAIL_ALARM_AFTER", "bananas")
    assert _source_fail_alarm_after() == DEFAULT_SOURCE_FAIL_ALARM_AFTER
    monkeypatch.setenv("ANGELUS_SOURCE_FAIL_ALARM_AFTER", "0")
    assert _source_fail_alarm_after() == DEFAULT_SOURCE_FAIL_ALARM_AFTER
    monkeypatch.setenv("ANGELUS_SOURCE_FAIL_ALARM_AFTER", "-3")
    assert _source_fail_alarm_after() == DEFAULT_SOURCE_FAIL_ALARM_AFTER


def test_incident_open_across_restart_clears_on_first_healthy_fire(
    tmp_path: Path,
) -> None:
    """The tally is process state, so a restart resets it -- but an incident
    left open across the restart must still close on the new process's first
    successful fire. This pins the ok-path clearance being UNCONDITIONAL (not
    gated on a nonzero in-memory count)."""
    fixtures = _lodge(tmp_path)
    _ok(fixtures["s"])
    daemon = AngelusDaemon(tmp_path)
    try:
        _fire(daemon)
        fixtures["s"].unlink()
        for _ in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER):
            _fire(daemon)
        assert len(_open_alarm_incidents(daemon)) == 1
    finally:
        daemon.connection.close()

    # Restart: fresh daemon, zero tally, source healthy again.
    _ok(fixtures["s"])
    daemon = AngelusDaemon(tmp_path)
    try:
        assert len(_open_alarm_incidents(daemon)) == 1, "incident survived"
        _, outcome = _fire(daemon)
        assert outcome == "ok"
        assert _open_alarm_incidents(daemon) == []
    finally:
        daemon.connection.close()


def test_hot_removed_source_clears_its_open_incident(tmp_path: Path) -> None:
    """A hot-removed source never fires again -- the incident's only recovery
    edge is gone -- so apply_lodging clears it (and drops the tally so a
    re-added source starts fresh). Without this the removed watch would keep
    belfry red forever."""
    fixtures = _lodge(tmp_path, sources=("s", "keep"))
    for fixture in fixtures.values():
        _ok(fixture)
    daemon = AngelusDaemon(tmp_path)
    try:
        _fire(daemon)
        fixtures["s"].unlink()
        for _ in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER):
            _fire(daemon)
        assert len(_open_alarm_incidents(daemon)) == 1
        assert daemon._source_fail_counts.get("scheduled/s")

        (tmp_path / "sources" / "scheduled" / "s.yaml").unlink()
        asyncio.run(daemon.apply_lodging(load_lodging(tmp_path)))

        assert _open_alarm_incidents(daemon) == []
        assert "scheduled/s" not in daemon._source_fail_counts
    finally:
        daemon.connection.close()


def test_startup_reconcile_clears_only_unlodged_source_incidents(
    tmp_path: Path,
) -> None:
    """A source removed while the daemon was DOWN orphans its incident (no
    fire, no hot-reload event -- no recovery edge at all), so the startup
    reconcile sweep clears it. A still-lodged source's incident is NOT
    blind-cleared: it recovers off its own next successful fire, and clearing
    it at boot would go false-green while the check is still failing."""
    fixtures = _lodge(tmp_path, sources=("gone", "lodged"))
    for fixture in fixtures.values():
        _ok(fixture)
    daemon = AngelusDaemon(tmp_path)
    try:
        for name in ("gone", "lodged"):
            _fire(daemon, name)
            fixtures[name].unlink()
            for _ in range(DEFAULT_SOURCE_FAIL_ALARM_AFTER):
                _fire(daemon, name)
        assert len(_open_alarm_incidents(daemon)) == 2
    finally:
        daemon.connection.close()

    # Remove one source while "down", then boot a fresh daemon and run the
    # exact startup sweep.
    (tmp_path / "sources" / "scheduled" / "gone.yaml").unlink()
    daemon = AngelusDaemon(tmp_path)
    try:
        daemon._reconcile_orphaned_internal_incidents()
        incidents = _open_alarm_incidents(daemon)
        assert [i["entity"] for i in incidents] == ["scheduled/lodged"]
    finally:
        daemon.connection.close()
