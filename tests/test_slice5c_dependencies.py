"""Slice 5c: dependency registry.

dep_health gets a real writer (the dep_record control op, run inside the
daemon -- the single sqlite writer) and a real reader (the health op).
The dep-check probe runs a dependency's check command with the shared
kill-on-timeout subprocess helper and reports the result over the control
socket; it never opens sqlite. These tests drive dep_record over a real
control-socket round trip (the 5b-2 pattern) and assert resulting state.

The contract this slice must hold:

  * dep_record is idempotent on natural state (the dep_health PK +
    ON CONFLICT upsert is the whole mechanism -- no request-id cache);
  * an unhealthy dep_record emits a fresh internal/dep finding to `now`
    EVERY time (repeats are not deduped -- slice-3 digest-failure
    precedent); a recovery emits none;
  * dep_health is surfaced by the health op (its mandatory reader -- a
    written-but-unread table would be dead config);
  * dependencies/ is a flat, absent-dir-safe, .disabled-honoring,
    hot-reloadable lodging dir;
  * a hung check is killed on timeout and reported unhealthy;
  * the lodged notify-pat / patbot-email checks do NOT send anything.

The daemon-required (no second writer) and cancel-safety contracts for
dep_record are exercised in test_slice5b2_write_ops.py alongside the
other write ops, where those contract tests live.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import shutil
import sqlite3
import time
from pathlib import Path

from angelus.daemon import AngelusDaemon
from angelus.lodging import Dependency
from angelus.lodging.config import ScheduledSource, _load_dependencies, parse_dependency
from angelus.lodging.reloader import LodgingReloader, _identify
from angelus.sources import run_dep_check, run_shell_source
from angelus.storage import init_db
from angelus.storage.migrations import DEFAULT_MIGRATIONS_DIR, migrate

REPO_ROOT = Path(__file__).resolve().parents[1]


# --- control-socket harness (mirrors test_slice5b2_write_ops) ------------


def _write_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )


async def _ask(sock_path: Path, payload: dict) -> dict:
    reader, writer = await asyncio.open_unix_connection(str(sock_path))
    try:
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
        await asyncio.gather(writer.wait_closed(), return_exceptions=True)
    return json.loads(line.decode("utf-8"))


def _serve(tmp_path: Path):
    """Build a daemon, start only its control server, hand it to the test."""

    def decorator(body):
        async def driver():
            _write_lodging(tmp_path)
            daemon = AngelusDaemon(tmp_path)
            await daemon.control.start()
            try:
                return await body(daemon)
            finally:
                await daemon.control.stop()
                daemon.connection.close()

        return asyncio.run(driver())

    return decorator


def _now_findings(connection, name: str) -> list[sqlite3.Row]:
    """internal/dep dependency_unhealthy findings for `name` queued to now."""
    return list(
        connection.execute(
            """
            SELECT f.id
            FROM findings f
            JOIN pipe_queues pq ON pq.finding_id = f.id AND pq.pipe = 'now'
            WHERE f.source = 'internal/dep'
              AND f.type = 'dependency_unhealthy'
              AND f.entity = ?
            ORDER BY f.id
            """,
            (name,),
        )
    )


# --- dep_record: idempotency on natural state ----------------------------


def test_dep_record_healthy_is_idempotent(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        first = await _ask(
            daemon.socket_path,
            {
                "op": "dep_record",
                "args": {"name": "skein", "status": "healthy",
                         "detail": "ok"},
            },
        )
        assert first["ok"] is True
        assert first["result"] == {"name": "skein", "status": "healthy"}

        # At-least-once retry: same op again. The PK + ON CONFLICT upsert
        # is the whole idempotency mechanism -> one row, same end state.
        second = await _ask(
            daemon.socket_path,
            {
                "op": "dep_record",
                "args": {"name": "skein", "status": "healthy",
                         "detail": "ok"},
            },
        )
        assert second["ok"] is True

        rows = list(
            daemon.connection.execute(
                "SELECT status, detail FROM dep_health "
                "WHERE dependency_name = 'skein'"
            )
        )
        assert len(rows) == 1
        assert rows[0]["status"] == "healthy"
        assert rows[0]["detail"] == "ok"


# --- dep_record unhealthy: edge-triggered, ONE finding while open --------


def test_dep_record_unhealthy_emits_one_finding_while_incident_open(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        for _i in range(2):
            resp = await _ask(
                daemon.socket_path,
                {
                    "op": "dep_record",
                    "args": {
                        "name": "mill-wheel",
                        "status": "unhealthy",
                        "detail": "exit 1: connection refused",
                    },
                },
            )
            assert resp["ok"] is True

        # One dep_health row (upsert), unhealthy.
        dep_rows = list(
            daemon.connection.execute(
                "SELECT status FROM dep_health "
                "WHERE dependency_name = 'mill-wheel'"
            )
        )
        assert [r["status"] for r in dep_rows] == ["unhealthy"]

        # ONE finding under the B30 emission gate: the first unhealthy record
        # opens the internal/dep incident and emits; the second, while that
        # incident is still open, is dropped entirely (no row, no now-enqueue).
        # This is the amplifier the gate exists to kill -- a stuck-down dep
        # polled repeatedly must not flood. Duration lives on incidents.
        findings = _now_findings(daemon.connection, "mill-wheel")
        assert len(findings) == 1

        # The single open incident carries the condition; the repeat refreshed
        # nothing visible to the operator beyond it.
        opens = [
            i for i in daemon.catalog.open_incidents()
            if i["entity"] == "mill-wheel" and i["type"] == "dependency_unhealthy"
        ]
        assert len(opens) == 1


def test_dep_record_recovery_emits_no_finding(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        await _ask(
            daemon.socket_path,
            {
                "op": "dep_record",
                "args": {"name": "spindle", "status": "unhealthy",
                         "detail": "down"},
            },
        )
        assert len(_now_findings(daemon.connection, "spindle")) == 1

        # Recovery: status flips to healthy, NO new finding.
        await _ask(
            daemon.socket_path,
            {
                "op": "dep_record",
                "args": {"name": "spindle", "status": "healthy",
                         "detail": "ok"},
            },
        )
        row = daemon.connection.execute(
            "SELECT status FROM dep_health WHERE dependency_name = 'spindle'"
        ).fetchone()
        assert row["status"] == "healthy"
        assert len(_now_findings(daemon.connection, "spindle")) == 1


# --- dep_health's mandatory reader: the health op ------------------------


def test_health_op_surfaces_dep_health(tmp_path) -> None:
    @_serve(tmp_path)
    async def _(daemon):
        daemon.catalog.record_dep_health(
            "skein", "healthy", "2026-05-19T00:00:00.000Z", "ok"
        )
        daemon.catalog.record_dep_health(
            "patbot-email", "unhealthy",
            "2026-05-19T00:05:00.000Z", "exit 2: auth failed"
        )

        resp = await _ask(daemon.socket_path, {"op": "health"})
        assert resp["ok"] is True
        deps = {d["dependency_name"]: d for d in resp["result"]["deps"]}
        # Fails if the health op does not surface dep_health (the
        # written-but-unread / dead-config lesson, encoded).
        assert deps["skein"]["status"] == "healthy"
        assert deps["skein"]["last_check_at"] == "2026-05-19T00:00:00.000Z"
        assert deps["patbot-email"]["status"] == "unhealthy"
        assert deps["patbot-email"]["detail"] == "exit 2: auth failed"


# --- lodging: _load_dependencies + hot-reload ----------------------------


def test_load_dependencies_missing_dir_is_empty(tmp_path) -> None:
    # No dependencies/ dir at all -> empty, not an error (absent-dir-safe).
    assert _load_dependencies(tmp_path) == {}


def test_load_dependencies_loads_and_honors_disabled(tmp_path) -> None:
    deps_dir = tmp_path / "dependencies"
    deps_dir.mkdir()
    (deps_dir / "skein.yaml").write_text(
        "name: skein\ncheck: skein --help\n", encoding="utf-8"
    )
    (deps_dir / "spindle.yaml").write_text(
        "name: spindle\ncheck: spindle --help\n", encoding="utf-8"
    )
    (deps_dir / "spindle.yaml.disabled").write_text("", encoding="utf-8")

    loaded = _load_dependencies(tmp_path)
    # Enabled one loads; the .disabled-twinned one is treated as removed.
    assert set(loaded) == {"skein"}
    assert loaded["skein"].check == "skein --help"


def test_dependency_name_must_match_filename_stem(tmp_path) -> None:
    deps_dir = tmp_path / "dependencies"
    deps_dir.mkdir()
    path = deps_dir / "skein.yaml"
    path.write_text("name: not-skein\ncheck: skein --help\n", encoding="utf-8")
    try:
        parse_dependency(path)
        raise AssertionError("expected ValueError on name/stem mismatch")
    except ValueError as exc:
        assert "must match filename stem" in str(exc)


def _identified_kind(root: Path, path: Path) -> str | None:
    ident = _identify(root, path)
    return None if ident is None else ident.kind


def test_dependencies_is_flat_depth_two_like_channels(tmp_path) -> None:
    # dependencies/<name>.yaml is identified (depth 2, like channels).
    ident = _identify(tmp_path, tmp_path / "dependencies" / "skein.yaml")
    assert ident is not None
    assert ident.kind == "dependency"
    assert ident.key == "skein"
    # A nested file (depth 3) is NOT a dependency (flat, unlike
    # sources/scheduled which is depth 3).
    assert (
        _identified_kind(tmp_path, tmp_path / "dependencies" / "sub" / "x.yaml")
        is None
    )


def test_dependency_hot_reload_add_change_remove(tmp_path) -> None:
    async def driver():
        _write_lodging(tmp_path)
        deps_dir = tmp_path / "dependencies"
        deps_dir.mkdir()
        daemon = AngelusDaemon(tmp_path)
        reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)
        try:
            assert daemon.lodging.dependencies == {}

            # Add.
            path = deps_dir / "skein.yaml"
            path.write_text(
                "name: skein\ncheck: skein --help\n", encoding="utf-8"
            )
            reloader.event_queue.put(str(path))
            await reloader.process_pending_events()
            assert "skein" in daemon.lodging.dependencies
            assert daemon.lodging.dependencies["skein"].check == "skein --help"

            # Change.
            path.write_text(
                "name: skein\ncheck: skein --version\n", encoding="utf-8"
            )
            reloader.event_queue.put(str(path))
            await reloader.process_pending_events()
            assert (
                daemon.lodging.dependencies["skein"].check == "skein --version"
            )

            # Remove via .disabled twin.
            (deps_dir / "skein.yaml.disabled").write_text(
                "", encoding="utf-8"
            )
            reloader.event_queue.put(str(deps_dir / "skein.yaml.disabled"))
            await reloader.process_pending_events()
            assert "skein" not in daemon.lodging.dependencies
        finally:
            daemon.connection.close()

    asyncio.run(driver())


def test_reloader_start_makes_dependencies_dir(tmp_path) -> None:
    # Absent-at-startup: start() ensures the dir exists so the observer
    # can watch it (and a later-created file is seen), then stops cleanly.
    async def driver():
        _write_lodging(tmp_path)
        daemon = AngelusDaemon(tmp_path)
        reloader = LodgingReloader(daemon, tmp_path)
        try:
            assert not (tmp_path / "dependencies").exists()
            reloader.start()
            assert (tmp_path / "dependencies").is_dir()
        finally:
            await reloader.stop()
            daemon.connection.close()

    asyncio.run(driver())


# --- dep-check probe: kill-on-timeout ------------------------------------


def test_run_dep_check_healthy_and_unhealthy() -> None:
    healthy = asyncio.run(
        run_dep_check(Dependency(name="x", check="exit 0"))
    )
    assert healthy[0] == "healthy"
    unhealthy = asyncio.run(
        run_dep_check(
            Dependency(name="x", check="echo boom >&2; exit 3")
        )
    )
    assert unhealthy[0] == "unhealthy"
    assert "exit 3" in unhealthy[1]
    assert "boom" in unhealthy[1]


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _forking_hang_command(marker: Path) -> str:
    """A FORKING command: dash stays resident and the real work runs as a
    backgrounded grandchild whose pid is written to `marker`.

    A forking command is REQUIRED, not incidental. With a non-forking
    simple command (`sleep 30`) dash execs it directly, so the subprocess
    pid IS the sleep -- a plain process.kill() on the shell reaps it just
    as fast and the test would pass with OR without the process-group
    hardening (non-discriminating). With this forking command an
    un-hardened kill of the shell alone orphans the grandchild (it
    survives, reparented to init); only the start_new_session +
    process-group SIGKILL reaps it. Do not "simplify" this back to a
    bare `sleep 30`.
    """
    return f"sleep 30 & echo $! > {marker}; wait"


def _read_grandchild_pid(marker: Path) -> int:
    for _ in range(200):
        if marker.exists():
            text = marker.read_text().strip()
            if text:
                return int(text)
        time.sleep(0.01)
    raise AssertionError("grandchild never recorded its pid")


def _assert_grandchild_reaped(pid: int) -> None:
    for _ in range(200):
        if not _pid_alive(pid):
            return
        time.sleep(0.01)
    # Best-effort cleanup so a failing (un-hardened) run does not leak a
    # 30s sleep for the rest of the suite.
    with contextlib.suppress(ProcessLookupError):
        os.kill(pid, 9)
    raise AssertionError(
        f"grandchild pid {pid} still alive after timeout kill -- the "
        "process-group hardening did not reap the orphaned child"
    )


def test_run_dep_check_kills_hung_command_on_timeout(tmp_path) -> None:
    marker = tmp_path / "grandchild.pid"
    started = time.monotonic()
    status, detail = asyncio.run(
        run_dep_check(
            Dependency(
                name="x",
                check=_forking_hang_command(marker),
                timeout_seconds=0.3,
            )
        )
    )
    elapsed = time.monotonic() - started
    assert status == "unhealthy"
    assert "timed out" in detail
    # Returns ~immediately after the 0.3s timeout, nowhere near 30s.
    assert elapsed < 5
    # The discriminating assertion: the forking command's grandchild was
    # actually reaped by the process-group kill, not left orphaned.
    _assert_grandchild_reaped(_read_grandchild_pid(marker))


def test_run_shell_source_kills_hung_command_on_timeout(tmp_path) -> None:
    # run_shell_source is the LIVE daemon scheduled-fire path; it gets the
    # same process-group hardening as run_dep_check. Same discriminating
    # shape: a forking command whose grandchild must be reaped on timeout.
    marker = tmp_path / "grandchild.pid"
    started = time.monotonic()
    ok, payload = asyncio.run(
        run_shell_source(
            ScheduledSource(
                name="watch",
                source_ref="scheduled/watch",
                cadence="1h",
                command=_forking_hang_command(marker),
                timeout_seconds=0.3,
            )
        )
    )
    elapsed = time.monotonic() - started
    assert ok is False
    assert "timed out" in payload["error"]
    assert elapsed < 5
    _assert_grandchild_reaped(_read_grandchild_pid(marker))


# --- the lodged checks must NOT send -------------------------------------


def test_lodged_notify_and_email_checks_are_non_sending() -> None:
    deps = REPO_ROOT / "examples" / "lodging" / "dependencies"
    notify = parse_dependency(deps / "notify-pat.yaml")
    email = parse_dependency(deps / "patbot-email.yaml")

    # A help/version probe, never an actual send. Guards the phone-spam
    # trap: a bare `notify-pat "..."` would notify Patrick every tick.
    assert notify.check.strip().endswith("--help")
    assert "--help" in email.check
    # patbot-email's send path is the explicit `send` subcommand; the
    # probe must not invoke it.
    assert "send" not in email.check.split()


def test_all_six_dependencies_are_lodged() -> None:
    loaded = _load_dependencies(REPO_ROOT / "examples" / "lodging")
    assert set(loaded) == {
        "mill-wheel",
        "spindle",
        "skein",
        "notify-pat",
        "patbot-email",
        "healthchecks.io",
    }


# --- 0006 migration: fresh + upgrade -------------------------------------

_NEW_DEP_COLS = {
    "dependency_name",
    "status",
    "last_check_at",
    "detail",
    "updated_at",
}


def _dep_health_columns(connection) -> set[str]:
    return {
        row[1]
        for row in connection.execute("PRAGMA table_info(dep_health)")
    }


def test_0006_applies_on_fresh_db(tmp_path) -> None:
    connection = init_db(tmp_path / "fresh.sqlite3")
    try:
        assert _dep_health_columns(connection) == _NEW_DEP_COLS
        applied = {
            r["version"]
            for r in connection.execute(
                "SELECT version FROM schema_migrations"
            )
        }
        assert "0006_dependency_registry.sql" in applied
        # The new CHECK is live: a bad status is rejected.
        try:
            connection.execute(
                "INSERT INTO dep_health (dependency_name, status, "
                "last_check_at, detail, updated_at) "
                "VALUES ('x', 'bogus', 't', NULL, 't')"
            )
            raise AssertionError("status CHECK did not fire")
        except sqlite3.IntegrityError:
            pass
    finally:
        connection.close()


def test_0006_applies_as_upgrade_from_existing_db(tmp_path) -> None:
    # Stage migrations 0001..0005 only -> the OLD dep_health from 0001.
    staged = tmp_path / "migrations"
    staged.mkdir()
    originals = sorted(Path(DEFAULT_MIGRATIONS_DIR).glob("0*.sql"))
    pre_0006 = [p for p in originals if not p.name.startswith("0006")]
    for path in pre_0006:
        shutil.copy(path, staged / path.name)

    db_path = tmp_path / "upgrade.sqlite3"
    connection = init_db(db_path, staged)
    try:
        # Old schema: a 'dep' column, no CHECK. Seed a row that the new
        # schema's NOT NULL/CHECK would reject -- the upgrade must drop it.
        assert "dep" in _dep_health_columns(connection)
        connection.execute(
            "INSERT INTO dep_health (dep, status) VALUES ('legacy', 'weird')"
        )
        connection.commit()
    finally:
        connection.close()

    # Now stage 0006 and re-migrate the SAME db (the upgrade path).
    shutil.copy(
        Path(DEFAULT_MIGRATIONS_DIR) / "0006_dependency_registry.sql",
        staged / "0006_dependency_registry.sql",
    )
    connection = init_db(db_path, staged)
    try:
        assert _dep_health_columns(connection) == _NEW_DEP_COLS
        # Old dead row was dropped with the old table; clean slate.
        n = connection.execute(
            "SELECT COUNT(*) AS n FROM dep_health"
        ).fetchone()["n"]
        assert n == 0
        # Re-running migrate again is a no-op (idempotent bookkeeping).
        migrate(connection, staged)
        assert _dep_health_columns(connection) == _NEW_DEP_COLS
    finally:
        connection.close()
