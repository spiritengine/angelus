"""The daemon snapshots its own installed sha to state/running-version once at
boot (deploy-identity / belfry drift rework, brief-20260613-3spy piece 2).

This file is the BOOT SNAPSHOT belfry and `angelus health` compare against the
installed pin to detect code drift: it must record the commit_id the live
process actually loaded (or the literal 'editable' marker on a dev tree with no
pin), be written atomically, and never abort boot if provenance can't be read.
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import angelus.daemon
from angelus.daemon import AngelusDaemon, _write_running_version


def _minimal_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        'cadence: 1s\ncheck:\n  kind: shell\n  command: "echo {}"\n',
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
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )


def _running_version(root: Path) -> str | None:
    path = root / "state" / "running-version"
    if not path.exists():
        return None
    return path.read_text(encoding="utf-8").strip()


def _boot_and_capture(tmp_path: Path, monkeypatch, commit_id) -> str | None:
    """Boot a real daemon far enough to write running-version, then stop it
    cleanly and return the file's contents."""
    _minimal_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")
    # Control the install provenance the daemon reads at boot (the name is
    # imported into the daemon module's namespace).
    monkeypatch.setattr(angelus.daemon, "installed_commit_id", lambda: commit_id)

    captured: dict = {}

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        run_task = asyncio.create_task(daemon.run())
        try:
            for _ in range(400):
                if _running_version(tmp_path) is not None:
                    break
                await asyncio.sleep(0.02)
            captured["value"] = _running_version(tmp_path)
        finally:
            daemon.request_stop()
            try:
                await asyncio.wait_for(run_task, timeout=10)
            except asyncio.TimeoutError:
                run_task.cancel()

    asyncio.run(driver())
    return captured.get("value")


def test_boot_writes_commit_id_in_pinned_case(tmp_path, monkeypatch) -> None:
    """A pinned (non-editable) install -> running-version is the commit_id the
    live process loaded."""
    value = _boot_and_capture(tmp_path, monkeypatch, "feedface1234")
    assert value == "feedface1234"


def test_boot_writes_editable_marker_when_no_pin(tmp_path, monkeypatch) -> None:
    """A dev/editable tree with no PEP 610 pin (installed_commit_id -> None) ->
    the literal 'editable' marker, which belfry/health read as 'no pin to drift
    against'."""
    value = _boot_and_capture(tmp_path, monkeypatch, None)
    assert value == "editable"


def test_write_running_version_atomic_and_overwrites(tmp_path) -> None:
    """_write_running_version writes the value (trailing newline, like the
    other stamps) and replaces any prior content; no leftover .tmp sibling."""
    path = tmp_path / "state" / "running-version"
    _write_running_version(path, "sha-one")
    assert path.read_text(encoding="utf-8") == "sha-one\n"
    _write_running_version(path, "sha-two")
    assert path.read_text(encoding="utf-8") == "sha-two\n"
    assert not (tmp_path / "state" / "running-version.tmp").exists()


def test_write_running_version_fail_soft(tmp_path, caplog) -> None:
    """A write failure (parent path is a FILE, so mkdir/replace raise OSError)
    is logged and swallowed -- a boot must never abort on the stamp."""
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file", encoding="utf-8")
    # blocker/state/running-version -> mkdir under a file raises OSError.
    _write_running_version(blocker / "state" / "running-version", "sha")
    # No exception escaped; that is the contract.
