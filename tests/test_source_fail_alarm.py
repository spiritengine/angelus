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


def test_outcome_dropped_when_source_hot_removed_mid_fire(
    tmp_path: Path, monkeypatch
) -> None:
    """The mid-flight hot-remove race (ztc2 fell finding): _fire_source
    captures the source and then AWAITS its shell check, so apply_lodging can
    remove the source while the check runs. apply_lodging pops the tally and
    clears any incident at removal -- a late outcome landing after that must
    be dropped, not recorded: a recorded check_failed would open an
    internal/source incident with NO remaining recovery edge (the source
    never fires again), keeping belfry red until restart reconciliation, and
    the tally re-increment would leave residue that makes a re-added source
    alarm early.

    Reproduces the reviewer's interleaving exactly: threshold 1, a slow
    failing check held open by a flag file, hot-remove + apply_lodging while
    it runs, then release the check and let the fire finish. Asserts no
    incident, no finding/clearance rows, no tally residue -- and that a
    still-lodged sibling alarms normally, so the guard only drops unlodged
    outcomes."""
    monkeypatch.setenv("ANGELUS_SOURCE_FAIL_ALARM_AFTER", "1")
    fixtures = _lodge(tmp_path, sources=("s", "keep"))
    for fixture in fixtures.values():
        _ok(fixture)
    started = tmp_path / "fire-started"
    release = tmp_path / "fire-release"
    # Replace s's check with one that signals it is running, then blocks
    # until released, then fails -- a deterministic slow failing command.
    (tmp_path / "sources" / "scheduled" / "s.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n"
        f"  command: 'touch {started}; until [ -f {release} ]; "
        "do sleep 0.05; done; exit 1'\n",
        encoding="utf-8",
    )
    daemon = AngelusDaemon(tmp_path)
    try:

        async def scenario() -> tuple[int | None, str]:
            fire = asyncio.create_task(daemon._fire_source("scheduled/s"))
            # The marker proves the fire captured the source and is awaiting
            # the check subprocess before we pull the source out from under it.
            while not started.exists():
                await asyncio.sleep(0.01)
            (tmp_path / "sources" / "scheduled" / "s.yaml").unlink()
            await daemon.apply_lodging(load_lodging(tmp_path))
            release.touch()
            result = await fire
            assert result is not None
            return result

        _, outcome = asyncio.run(scenario())
        assert outcome == "check_failed"

        assert _open_alarm_incidents(daemon) == []
        assert _alarm_findings(daemon) == []
        assert "scheduled/s" not in daemon._source_fail_counts
        all_internal = daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM findings WHERE source = 'internal/source'"
        ).fetchone()["n"]
        assert all_internal == 0, "dropped outcome must leave no rows at all"

        # The guard drops ONLY unlodged outcomes: the surviving sibling still
        # alarms on its own failure (threshold 1).
        fixtures["keep"].unlink()
        _, outcome = _fire(daemon, "keep")
        assert outcome == "check_failed"
        incidents = _open_alarm_incidents(daemon)
        assert [i["entity"] for i in incidents] == ["scheduled/keep"]
    finally:
        daemon.connection.close()


def test_outcome_dropped_when_source_removed_and_readded_mid_fire(
    tmp_path: Path, monkeypatch
) -> None:
    """The remove-then-RE-ADD interleaving (fell-r1 finding): a membership
    guard alone cannot catch a fire launched against a source that was
    hot-removed and then re-added under the same ref while the check ran --
    the ref IS lodged again when the stale outcome lands, so membership
    passes, the stale check_failed opens an incident and seeds the re-added
    source's tally with residue from a generation that no longer exists. The
    outcome must be tied to the lodging GENERATION the fire was launched
    against and dropped when removal has bumped it since.

    Reproduces the reviewer's interleaving exactly: threshold 1, a slow
    failing check held open by a flag file, hot-remove + apply_lodging, then
    re-add the same ref + apply_lodging, then release the check. Asserts no
    incident, no rows, and NO tally residue on the re-added source -- then
    proves the re-added source's own (current-generation) fires still record
    normally by failing it once and seeing the alarm open on schedule."""
    monkeypatch.setenv("ANGELUS_SOURCE_FAIL_ALARM_AFTER", "1")
    fixtures = _lodge(tmp_path)
    _ok(fixtures["s"])
    started = tmp_path / "fire-started"
    release = tmp_path / "fire-release"
    source_yaml = tmp_path / "sources" / "scheduled" / "s.yaml"
    readded_yaml = source_yaml.read_text(encoding="utf-8")
    source_yaml.write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n"
        f"  command: 'touch {started}; until [ -f {release} ]; "
        "do sleep 0.05; done; exit 1'\n",
        encoding="utf-8",
    )
    daemon = AngelusDaemon(tmp_path)
    try:

        async def scenario() -> tuple[int | None, str]:
            fire = asyncio.create_task(daemon._fire_source("scheduled/s"))
            while not started.exists():
                await asyncio.sleep(0.01)
            # Remove, reload, RE-ADD the same ref, reload again -- all while
            # the old generation's check is still running.
            source_yaml.unlink()
            await daemon.apply_lodging(load_lodging(tmp_path))
            source_yaml.write_text(readded_yaml, encoding="utf-8")
            await daemon.apply_lodging(load_lodging(tmp_path))
            release.touch()
            result = await fire
            assert result is not None
            return result

        _, outcome = asyncio.run(scenario())
        assert outcome == "check_failed"

        # The stale outcome belongs to the removed generation: no incident,
        # no rows, and -- the contamination the membership guard missed -- no
        # tally residue on the freshly re-added source.
        assert _open_alarm_incidents(daemon) == []
        assert _alarm_findings(daemon) == []
        assert "scheduled/s" not in daemon._source_fail_counts
        all_internal = daemon.connection.execute(
            "SELECT COUNT(*) AS n FROM findings WHERE source = 'internal/source'"
        ).fetchone()["n"]
        assert all_internal == 0, "dropped outcome must leave no rows at all"

        # Control: the re-added source's OWN fires are current-generation and
        # record normally -- one genuine failure (threshold 1) alarms.
        fixtures["s"].unlink()
        _, outcome = _fire(daemon)
        assert outcome == "check_failed"
        incidents = _open_alarm_incidents(daemon)
        assert [i["entity"] for i in incidents] == ["scheduled/s"]
        assert daemon._source_fail_counts.get("scheduled/s") == 1
    finally:
        daemon.connection.close()


def test_outcome_recorded_when_reload_keeps_source_lodged_mid_fire(
    tmp_path: Path, monkeypatch
) -> None:
    """Control for the generation guard's other edge: a hot-reload that KEEPS
    the source lodged (here: an unrelated source added) swaps in a fresh
    Source object mid-fire, but the in-flight outcome is still from the
    current lodging generation and must record normally. This pins the guard
    to removal -- an object-identity check would wrongly drop this outcome
    and silently eat real consecutive failures across every reload."""
    monkeypatch.setenv("ANGELUS_SOURCE_FAIL_ALARM_AFTER", "1")
    fixtures = _lodge(tmp_path)
    _ok(fixtures["s"])
    started = tmp_path / "fire-started"
    release = tmp_path / "fire-release"
    (tmp_path / "sources" / "scheduled" / "s.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n"
        f"  command: 'touch {started}; until [ -f {release} ]; "
        "do sleep 0.05; done; exit 1'\n",
        encoding="utf-8",
    )
    daemon = AngelusDaemon(tmp_path)
    try:

        async def scenario() -> tuple[int | None, str]:
            fire = asyncio.create_task(daemon._fire_source("scheduled/s"))
            while not started.exists():
                await asyncio.sleep(0.01)
            # Reload with s still lodged: a new Lodging snapshot (and a new
            # Source object for s), same generation.
            (tmp_path / "sources" / "scheduled" / "other.yaml").write_text(
                "cadence: 1h\ncheck:\n  kind: shell\n  command: 'true'\n",
                encoding="utf-8",
            )
            await daemon.apply_lodging(load_lodging(tmp_path))
            release.touch()
            result = await fire
            assert result is not None
            return result

        _, outcome = asyncio.run(scenario())
        assert outcome == "check_failed"

        # Still lodged, same generation: the failure counts and (threshold 1)
        # alarms exactly as it would without the reload.
        assert daemon._source_fail_counts.get("scheduled/s") == 1
        incidents = _open_alarm_incidents(daemon)
        assert [i["entity"] for i in incidents] == ["scheduled/s"]
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


# --- Orphaned PRODUCT incident reconcile -------------------------------------
#
# A product incident (source = a real scheduled source; type = thesis-trigger /
# web-important / ...; keyed (source, type, entity)) closes ONLY via a
# type="clearance" finding for (source, entity) from the source firing again.
# A source REMOVED from lodging never fires again, so that close path is gone
# and the incident orphans forever (re-surfacing in every daily digest). The
# reconcile sweep clears any open incident whose non-internal source is no
# longer in self.lodging.sources, mirroring the internal/source coverage.


def _seed_product_incident(
    daemon: AngelusDaemon,
    source: str,
    ftype: str = "thesis-trigger",
    entity: str = "mos-sbec",
) -> int:
    """Open a product incident keyed (source, type, entity) by writing one
    non-clearance finding -- the same edge a triaged source fire takes."""
    return daemon.catalog.write_finding(
        None,
        {
            "source": source,
            "type": ftype,
            "entity": entity,
            "severity": "high",
            "target_pipes": ["now"],
            "body": f"{ftype} fired for {entity}",
        },
        set(daemon.lodging.pipes),
    )


def _open_product_incidents(daemon: AngelusDaemon) -> list[dict]:
    return [
        i
        for i in daemon.catalog.open_incidents()
        if not i["source"].startswith("internal/")
    ]


def _clearance_bodies(daemon: AngelusDaemon, source: str) -> list[str]:
    rows = daemon.connection.execute(
        "SELECT body_ref FROM findings WHERE source = ? AND type = 'clearance' "
        "ORDER BY id",
        (source,),
    )
    return [daemon.catalog.read_body(row["body_ref"]).get("text", "") for row in rows]


def test_product_reconcile_clears_unlodged_source_incident(tmp_path: Path) -> None:
    """A product incident whose source is not in lodging is closed by the
    startup reconcile, with a clearance recording 'no longer lodged'."""
    _lodge(tmp_path)  # lodges scheduled/s only
    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_product_incident(
            daemon, "scheduled/gone", "thesis-trigger", "mos-sbec"
        )
        assert [i["source"] for i in _open_product_incidents(daemon)] == [
            "scheduled/gone"
        ]

        daemon._reconcile_orphaned_product_incidents()

        assert _open_product_incidents(daemon) == []
        bodies = _clearance_bodies(daemon, "scheduled/gone")
        assert len(bodies) == 1
        assert "no longer lodged" in bodies[0]
    finally:
        daemon.connection.close()


def test_product_reconcile_leaves_lodged_source_incident(tmp_path: Path) -> None:
    """A product incident whose source is STILL lodged is untouched -- it
    recovers off its source's own next clearing fire. Mutation: inverting the
    predicate (close lodged sources) fails this."""
    _lodge(tmp_path)  # lodges scheduled/s
    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_product_incident(daemon, "scheduled/s", "thesis-trigger", "e")
        daemon._reconcile_orphaned_product_incidents()

        assert [i["source"] for i in _open_product_incidents(daemon)] == [
            "scheduled/s"
        ]
        assert _clearance_bodies(daemon, "scheduled/s") == []
    finally:
        daemon.connection.close()


def test_product_reconcile_leaves_internal_incidents(tmp_path: Path) -> None:
    """The product pass must not touch internal/* incidents (their own
    reconcile owns them). Seed an internal/render incident -- whose source is
    not in lodging.sources -- and confirm the product pass leaves it open.
    Mutation: dropping the _is_internal guard closes it and fails this."""
    _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        daemon.catalog.write_internal_finding(
            "internal/render",
            "render_failed",
            "digest",
            "render blew up",
            set(daemon.lodging.pipes),
        )
        assert daemon.catalog.open_internal_incident_count() == 1

        daemon._reconcile_orphaned_product_incidents()

        assert daemon.catalog.open_internal_incident_count() == 1
        assert _clearance_bodies(daemon, "internal/render") == []
    finally:
        daemon.connection.close()


def test_product_reconcile_via_apply_lodging_on_source_removal(
    tmp_path: Path,
) -> None:
    """Hot reload: removing the incident's source via apply_lodging closes it;
    removing an UNRELATED source leaves it open (its source is still lodged)."""
    _lodge(tmp_path, sources=("gone", "other"))
    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_product_incident(
            daemon, "scheduled/gone", "thesis-trigger", "mos-sbec"
        )

        # Remove the unrelated source: gone is still lodged -> incident stays.
        (tmp_path / "sources" / "scheduled" / "other.yaml").unlink()
        asyncio.run(daemon.apply_lodging(load_lodging(tmp_path)))
        assert [i["source"] for i in _open_product_incidents(daemon)] == [
            "scheduled/gone"
        ]

        # Remove the incident's own source: now it has no recovery edge -> close.
        (tmp_path / "sources" / "scheduled" / "gone.yaml").unlink()
        asyncio.run(daemon.apply_lodging(load_lodging(tmp_path)))
        assert _open_product_incidents(daemon) == []
        assert any(
            "no longer lodged" in b
            for b in _clearance_bodies(daemon, "scheduled/gone")
        )
    finally:
        daemon.connection.close()


def test_product_reconcile_keeps_transiently_absent_source_incident(
    tmp_path: Path,
) -> None:
    """A source whose file transiently fails to parse leaves apply_lodging
    UNCALLED (the reloader swaps self.lodging only on a validated load), so the
    source is still in self.lodging.sources and its incident must stay open.
    The reconcile keys off self.lodging.sources -- the live validated set --
    not the on-disk files."""
    _lodge(tmp_path)  # lodges scheduled/s
    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_product_incident(daemon, "scheduled/s", "thesis-trigger", "e")

        # Corrupt the source file on disk, but do NOT call apply_lodging --
        # exactly what the reloader does when load_lodging raises: it keeps the
        # prior snapshot. self.lodging.sources still holds scheduled/s.
        (tmp_path / "sources" / "scheduled" / "s.yaml").write_text(
            "cadence: 1h\ncheck: [not, valid\n", encoding="utf-8"
        )
        assert "scheduled/s" in daemon.lodging.sources

        daemon._reconcile_orphaned_product_incidents()
        assert [i["source"] for i in _open_product_incidents(daemon)] == [
            "scheduled/s"
        ]
    finally:
        daemon.connection.close()


def test_product_reconcile_clears_all_incidents_on_one_removed_source(
    tmp_path: Path,
) -> None:
    """Multiple open incidents on one removed source all close in a single
    pass (the sweep iterates every open incident, not just the first)."""
    _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_product_incident(daemon, "scheduled/gone", "thesis-trigger", "e1")
        _seed_product_incident(
            daemon, "scheduled/gone", "watch-check-failed", "e2"
        )
        assert len(_open_product_incidents(daemon)) == 2

        daemon._reconcile_orphaned_product_incidents()

        assert _open_product_incidents(daemon) == []
        assert len(_clearance_bodies(daemon, "scheduled/gone")) == 2
    finally:
        daemon.connection.close()


def test_product_reconcile_is_idempotent(tmp_path: Path) -> None:
    """A second reconcile pass writes nothing -- write_clearance is a gate-
    dropped no-op once the incident is already closed."""
    _lodge(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_product_incident(daemon, "scheduled/gone", "thesis-trigger", "e")
        daemon._reconcile_orphaned_product_incidents()
        assert len(_clearance_bodies(daemon, "scheduled/gone")) == 1

        daemon._reconcile_orphaned_product_incidents()
        assert len(_clearance_bodies(daemon, "scheduled/gone")) == 1
    finally:
        daemon.connection.close()
