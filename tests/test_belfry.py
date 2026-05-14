from __future__ import annotations

import importlib.util
import os
import sqlite3
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path


BELFRY_PATH = Path(__file__).resolve().parents[1] / "belfry" / "belfry.py"


class _Response:
    status = 200

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return None


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_under_test", BELFRY_PATH)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_source_fire(root: Path, fired_at: datetime) -> None:
    state = root / "state"
    state.mkdir(exist_ok=True)
    connection = sqlite3.connect(state / "angelus.sqlite3")
    try:
        connection.execute(
            """
            CREATE TABLE source_fires (
                id INTEGER PRIMARY KEY,
                source_name TEXT NOT NULL,
                scheduled_at TEXT,
                fired_at TEXT NOT NULL,
                outcome TEXT NOT NULL
            )
            """
        )
        connection.execute(
            """
            INSERT INTO source_fires (source_name, scheduled_at, fired_at, outcome)
            VALUES ('scheduled/test', NULL, ?, 'ok')
            """,
            (fired_at.isoformat(timespec="milliseconds").replace("+00:00", "Z"),),
        )
        connection.commit()
    finally:
        connection.close()


def _set_urls(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_BELFRY_SUCCESS_URL", "https://hc.example/success")
    monkeypatch.setenv("ANGELUS_BELFRY_DOWN_URL", "https://hc.example/down")


def test_belfry_imports_no_angelus_modules() -> None:
    for name in list(sys.modules):
        if name == "angelus" or name.startswith("angelus."):
            sys.modules.pop(name)

    _load_belfry()

    assert "angelus" not in sys.modules


def test_pid_dead_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("999999", encoding="utf-8")
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    assert "dead: PID 999999 is not running" in calls[0][1]


def test_pid_missing_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    assert "missing PID file" in calls[0][1]


def test_pid_permission_error_is_treated_as_alive(tmp_path, monkeypatch, capsys) -> None:
    belfry = _load_belfry()
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text("12345", encoding="utf-8")

    def deny_kill(_pid: int, _signal: int) -> None:
        raise PermissionError

    monkeypatch.setattr(belfry.os, "kill", deny_kill)

    assert belfry.pid_failure(tmp_path / "state" / "angelus.pid") is None
    assert "permission denied" in capsys.readouterr().err


def test_wedge_path_hits_down_and_escalates(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")
    _write_source_fire(tmp_path, datetime.now(UTC) - timedelta(minutes=30))
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 1
    assert pings == ["https://hc.example/down"]
    assert calls
    assert "wedged: last source fire" in calls[0][1]


def test_happy_path_hits_success_without_escalation(tmp_path, monkeypatch) -> None:
    belfry = _load_belfry()
    _set_urls(monkeypatch)
    (tmp_path / "state").mkdir()
    (tmp_path / "state" / "angelus.pid").write_text(str(os.getpid()), encoding="utf-8")
    _write_source_fire(tmp_path, datetime.now(UTC))
    pings: list[str] = []
    calls: list[list[str]] = []

    monkeypatch.setattr(
        belfry.urllib.request,
        "urlopen",
        lambda url, timeout: pings.append(url) or _Response(),
    )
    monkeypatch.setattr(
        belfry.subprocess,
        "run",
        lambda args, check: calls.append(args)
        or subprocess.CompletedProcess(args, 0),
    )

    assert belfry.main([str(tmp_path)]) == 0
    assert pings == ["https://hc.example/success"]
    assert calls == []
