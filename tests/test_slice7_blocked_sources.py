from __future__ import annotations

import asyncio

from angelus.cli import _render_health
from angelus.daemon import AngelusDaemon


def _write_lodging(root) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "chain-check.yaml").write_text(
        "cadence: 1h\n"
        "depends_on: [mill-wheel]\n"
        "check:\n"
        "  kind: shell\n"
        "  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "sources" / "scheduled" / "healthy-check.yaml").write_text(
        "cadence: 1h\n"
        "depends_on: [skein]\n"
        "check:\n"
        "  kind: shell\n"
        "  command: 'echo {}'\n",
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
    (root / "dependencies").mkdir()
    (root / "dependencies" / "mill-wheel.yaml").write_text(
        "name: mill-wheel\ncheck: 'true'\n", encoding="utf-8"
    )
    (root / "dependencies" / "skein.yaml").write_text(
        "name: skein\ncheck: 'true'\n", encoding="utf-8"
    )


def _health_with_dep_statuses(tmp_path):
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)

    async def driver():
        daemon.catalog.record_dep_health(
            "mill-wheel", "unhealthy", "2026-05-20T00:00:00.000Z", "down"
        )
        daemon.catalog.record_dep_health(
            "skein", "healthy", "2026-05-20T00:01:00.000Z", "ok"
        )
        return await daemon._op_health({})

    try:
        return asyncio.run(driver())
    finally:
        daemon.connection.close()


def test_op_health_surfaces_sources_blocked_by_unhealthy_lodged_deps(
    tmp_path,
) -> None:
    health = _health_with_dep_statuses(tmp_path)
    sources = {row["name"]: row for row in health["sources"]}
    assert (
        sources["scheduled/chain-check"]["blocked_by_unhealthy_deps"]
        == ["mill-wheel"]
    )
    assert (
        sources["scheduled/healthy-check"]["blocked_by_unhealthy_deps"] == []
    )


def test_health_render_surfaces_blocked_source_line(tmp_path, capsys) -> None:
    health = _health_with_dep_statuses(tmp_path)
    _render_health(health)
    rendered = capsys.readouterr().out
    assert "scheduled/chain-check" in rendered
    assert "blocked by: mill-wheel" in rendered
    assert "scheduled/healthy-check" in rendered
