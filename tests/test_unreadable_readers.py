"""Absent reader inputs stay quiet; unreadable inputs log their errno."""

from __future__ import annotations

import errno
import importlib.util
import logging
import sys
from datetime import UTC, datetime
from pathlib import Path
from types import SimpleNamespace

import pytest

from angelus import cli, provenance
from angelus.pipes import runner as pipe_runner


REPO_ROOT = Path(__file__).resolve().parents[1]


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def deploy_module():
    return _load_module("deploy_unreadable_test", REPO_ROOT / "deploy" / "deploy.py")


@pytest.fixture
def belfry_module():
    return _load_module("belfry_unreadable_test", REPO_ROOT / "belfry" / "belfry.py")


def _engine_repo(root: Path) -> Path:
    repo = root / "engine"
    (repo / ".git").mkdir(parents=True)
    (repo / "angelus").mkdir()
    (repo / "angelus" / "__init__.py").write_text("", encoding="utf-8")
    (repo / "Makefile").write_text("", encoding="utf-8")
    return repo


def _deny_read(monkeypatch: pytest.MonkeyPatch, target: Path) -> None:
    original = Path.read_text

    def denied(path: Path, *args, **kwargs):
        if path == target:
            raise PermissionError(errno.EACCES, "Permission denied", str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", denied)


def test_cli_installed_version_logs_only_unreadable_errno(
    tmp_path, monkeypatch, caplog
) -> None:
    caplog.set_level(logging.WARNING, logger=cli.__name__)
    assert cli._installed_version(tmp_path) == "unknown"
    assert not caplog.records

    stamp = tmp_path / "state" / "installed-version"
    stamp.parent.mkdir()
    stamp.write_text("abc\n", encoding="utf-8")
    _deny_read(monkeypatch, stamp)
    assert cli._installed_version(tmp_path) == "unknown"
    assert "errno=13" in caplog.text


def test_is_engine_repo_logs_only_unreadable_errno(
    tmp_path, monkeypatch, caplog
) -> None:
    caplog.set_level(logging.WARNING, logger=provenance.__name__)
    assert provenance._is_engine_repo(tmp_path / "absent") is False
    assert not caplog.records

    repo = _engine_repo(tmp_path)
    original = Path.stat

    def denied(path: Path, *args, **kwargs):
        if path == repo:
            raise PermissionError(errno.EACCES, "Permission denied", str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", denied)
    assert provenance._is_engine_repo(repo) is False
    assert "errno=13" in caplog.text


def test_deploy_staleness_logs_probe_oserror_errno(
    tmp_path, monkeypatch, caplog
) -> None:
    repo = _engine_repo(tmp_path)
    caplog.set_level(logging.WARNING, logger=provenance.__name__)

    def missing_git(*args, **kwargs):
        raise FileNotFoundError(errno.ENOENT, "No such file", "git")

    monkeypatch.setattr(provenance.subprocess, "run", missing_git)
    assert provenance.deploy_staleness(0, repo=repo, installed_sha="abc") is None
    assert "errno=2" in caplog.text

    caplog.clear()

    def unreadable(*args, **kwargs):
        raise PermissionError(errno.EACCES, "Permission denied", "git")

    monkeypatch.setattr(provenance.subprocess, "run", unreadable)
    assert provenance.deploy_staleness(0, repo=repo, installed_sha="abc") is None
    assert "errno=13" in caplog.text


def test_deploy_read_stamp_logs_only_unreadable_errno(
    deploy_module, tmp_path, monkeypatch, capsys
) -> None:
    stamp = tmp_path / "installed-version"
    cfg = SimpleNamespace(stamp_file=stamp)
    assert deploy_module.read_stamp(cfg) == "unknown"
    assert capsys.readouterr().err == ""

    stamp.write_text("abc\n", encoding="utf-8")
    _deny_read(monkeypatch, stamp)
    assert deploy_module.read_stamp(cfg) == "unknown"
    assert "errno=13" in capsys.readouterr().err


def test_pipe_deploy_staleness_logs_escaped_probe_oserror_errno(
    monkeypatch, caplog
) -> None:
    caplog.set_level(logging.WARNING, logger=pipe_runner.__name__)
    clock = SimpleNamespace(now=lambda: datetime(2026, 7, 26, tzinfo=UTC))
    drain = SimpleNamespace(_clock=clock)

    def escaped_missing_probe(*args, **kwargs):
        raise FileNotFoundError(errno.ENOENT, "No such file", "git")

    monkeypatch.setattr(pipe_runner, "deploy_staleness", escaped_missing_probe)
    assert pipe_runner.PipeDrain._deploy_staleness(drain) is None
    assert "errno=2" in caplog.text

    caplog.clear()

    def unreadable(*args, **kwargs):
        raise PermissionError(errno.EACCES, "Permission denied", "git")

    monkeypatch.setattr(pipe_runner, "deploy_staleness", unreadable)
    assert pipe_runner.PipeDrain._deploy_staleness(drain) is None
    assert "errno=13" in caplog.text


def test_prune_digest_staging_logs_only_unreadable_errno(
    tmp_path, monkeypatch, caplog
) -> None:
    caplog.set_level(logging.WARNING, logger=pipe_runner.__name__)
    absent = tmp_path / "absent"
    assert pipe_runner._prune_digest_staging(absent) is None
    assert not caplog.records

    staging = tmp_path / "staging"
    staging.mkdir()
    original = Path.iterdir

    def denied(path: Path, *args, **kwargs):
        if path == staging:
            raise PermissionError(errno.EACCES, "Permission denied", str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "iterdir", denied)
    assert pipe_runner._prune_digest_staging(staging) is None
    assert "errno=13" in caplog.text


def test_gather_fixer_actions_logs_only_unreadable_errno(
    tmp_path, monkeypatch, caplog
) -> None:
    caplog.set_level(logging.WARNING, logger=pipe_runner.__name__)
    log_path = tmp_path / "fixers.log"
    assert pipe_runner._gather_fixer_actions(log_path, None) == []
    assert not caplog.records

    log_path.write_text("ignored\n", encoding="utf-8")
    _deny_read(monkeypatch, log_path)
    assert pipe_runner._gather_fixer_actions(log_path, None) == []
    assert "errno=13" in caplog.text


def test_excerpt_sre_report_logs_only_unreadable_errno(
    tmp_path, monkeypatch, caplog
) -> None:
    caplog.set_level(logging.WARNING, logger=pipe_runner.__name__)
    report = tmp_path / "report.md"
    assert pipe_runner._excerpt_sre_report(report) == {}
    assert not caplog.records

    report.write_text("outcome: fixed\n", encoding="utf-8")
    _deny_read(monkeypatch, report)
    assert pipe_runner._excerpt_sre_report(report) == {}
    assert "errno=13" in caplog.text


def test_belfry_hold_status_logs_only_unreadable_errno(
    belfry_module, tmp_path, monkeypatch, capsys
) -> None:
    state = tmp_path / "state"
    assert belfry_module.hold_status(state) == ("absent", None)
    assert capsys.readouterr().err == ""

    state.mkdir()
    hold = state / belfry_module.DEFAULT_HOLD_FILENAME
    hold.write_text("hold\n", encoding="utf-8")
    original = Path.stat

    def denied(path: Path, *args, **kwargs):
        if path == hold:
            raise PermissionError(errno.EACCES, "Permission denied", str(path))
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "stat", denied)
    assert belfry_module.hold_status(state) == ("absent", None)
    assert "errno=13" in capsys.readouterr().err
