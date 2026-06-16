"""The health `code:` line reconciles the RUNNING sha against the installed
pin (deploy-identity / belfry drift rework, brief-20260613-3spy piece 2).

The primary value is what the live process loaded (state/running-version); the
pin (state/installed-version) is surfaced only on divergence -- the
restart-pending case. The five renderings below are the contract _render_health
and the daemon-down fallback both emit via _code_line.
"""

from __future__ import annotations

from pathlib import Path

from angelus.cli import _code_line


def _write(root: Path, *, running: str | None = None, installed: str | None = None) -> None:
    (root / "state").mkdir(parents=True, exist_ok=True)
    if running is not None:
        (root / "state" / "running-version").write_text(running, encoding="utf-8")
    if installed is not None:
        (root / "state" / "installed-version").write_text(installed, encoding="utf-8")


def test_code_line_running_equals_installed(tmp_path) -> None:
    """Deployed-and-restarted: running == installed -> one bare sha line."""
    _write(tmp_path, running="abc123\n", installed="abc123\n")
    assert _code_line(tmp_path) == "code: abc123"


def test_code_line_running_differs_from_installed(tmp_path) -> None:
    """A deploy landed a new pin the live process never restarted into ->
    surface BOTH with the restart-pending note."""
    _write(tmp_path, running="oldsha\n", installed="newsha\n")
    assert (
        _code_line(tmp_path)
        == "code: oldsha running, PINNED newsha — restart pending"
    )


def test_code_line_editable(tmp_path) -> None:
    """A dev/editable tree snapshots the literal 'editable' marker -> code:
    editable, regardless of any installed-version present."""
    _write(tmp_path, running="editable\n", installed="newsha\n")
    assert _code_line(tmp_path) == "code: editable"


def test_code_line_running_absent_installed_present(tmp_path) -> None:
    """A daemon predating this feature (no running-version) before its first
    redeploy -> graceful 'pinned; running unknown', not a scary false unknown."""
    _write(tmp_path, installed="abc123\n")
    assert _code_line(tmp_path) == "code: abc123 (pinned; running unknown)"


def test_code_line_both_absent(tmp_path) -> None:
    """Neither file present -> code: unknown."""
    (tmp_path / "state").mkdir(parents=True, exist_ok=True)
    assert _code_line(tmp_path) == "code: unknown"


def test_code_line_blank_files_read_as_absent(tmp_path) -> None:
    """A present-but-empty running-version and a blank installed-version both
    read as absent -> code: unknown, never a blank or half-rendered line."""
    _write(tmp_path, running="\n", installed="\n")
    assert _code_line(tmp_path) == "code: unknown"
