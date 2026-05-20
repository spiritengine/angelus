"""M2 slice 9: mute-aware deps, digest ladder, channel-health rail.

The health surface grows in three additive ways:

* unhealthy deps carry the effective active mute, if any
* digest retry ladder state is surfaced before channel_health threshold
* channel_health stays visible in health even when the matching
  internal/dispatch finding is muted on the now pipe
"""

from __future__ import annotations

import asyncio
from pathlib import Path

from click.testing import CliRunner

from angelus.cli import main
from angelus.daemon import AngelusDaemon
from angelus.storage import Catalog, init_db
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS


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
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [email]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n      - open_incidents\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )
    (root / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: person@example.com\n",
        encoding="utf-8",
    )
    (root / "render-templates").mkdir()
    (root / "render-templates" / "rate-limit-callout.j2").write_text(
        "Suppressed:\n", encoding="utf-8"
    )


def test_health_surfaces_active_mute_on_unhealthy_dep(tmp_path) -> None:
    """Unhealthy deps carry the effective active mute. Discrimination: if
    the mute lookup is inverted to always None, the mute-until and
    comment assertions fail."""
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        try:
            daemon.catalog.record_dep_health(
                "iotaschool",
                "unhealthy",
                "2026-05-20T13:00:00.000Z",
                "exit 7: connection refused",
            )
            daemon.catalog.add_mute(
                "internal/dep:dependency_unhealthy:iotaschool",
                3600,
                "flapping, acked",
            )
            health = await daemon._op_health({})
        finally:
            daemon.connection.close()

        deps = {d["dependency_name"]: d for d in health["deps"]}
        assert deps["iotaschool"]["status"] == "unhealthy"
        assert deps["iotaschool"]["mute"]["until"].endswith("Z")
        assert deps["iotaschool"]["mute"]["comment"] == "flapping, acked"

    asyncio.run(driver())


def test_health_leaves_healthy_dep_unannotated(tmp_path) -> None:
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        try:
            daemon.catalog.record_dep_health(
                "skein", "healthy", "2026-05-20T13:00:00.000Z", "ok"
            )
            daemon.catalog.add_mute(
                "internal/dep:dependency_unhealthy:skein",
                3600,
                "should not render on healthy dep",
            )
            health = await daemon._op_health({})
        finally:
            daemon.connection.close()

        deps = {d["dependency_name"]: d for d in health["deps"]}
        assert "mute" not in deps["skein"]

    asyncio.run(driver())


def test_health_surfaces_digest_attempt_ladder_before_threshold(tmp_path) -> None:
    """Digest ladder state is visible before channel_health flips.
    Discrimination: if the digest-attempt reader returns an empty list,
    the daily/email lookup and attempts count assertions fail."""
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        try:
            for _ in range(MAX_RETRY_ATTEMPTS - 1):
                crossed = daemon.catalog.record_digest_send_failure(
                    "daily", "email", "smtp dead"
                )
                assert crossed is False
            health = await daemon._op_health({})
        finally:
            daemon.connection.close()

        attempts = {
            (row["pipe"], row["channel"]): row
            for row in health["channels"]["attempts"]
        }
        assert attempts[("daily", "email")]["attempts"] == MAX_RETRY_ATTEMPTS - 1
        assert attempts[("daily", "email")]["last_error"] == "smtp dead"
        assert health["channels"]["health"] == []

    asyncio.run(driver())


def test_muted_channel_unhealthy_is_silent_on_now_but_visible_in_health(tmp_path) -> None:
    """The muted internal/dispatch finding is a product choice; the rail
    is that channel_health still surfaces via health. Discrimination: if
    channel_health were mute-filtered out of the health response, the
    email row assertion fails."""
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        try:
            daemon.catalog.mark_channel_unhealthy("email", "smtp dead")
            daemon.connection.commit()
            daemon.catalog.write_internal_finding(
                "internal/dispatch",
                "channel_unhealthy",
                "email",
                "smtp dead",
                set(daemon.lodging.pipes),
            )
            daemon.catalog.add_mute(
                "internal/dispatch:channel_unhealthy:email",
                3600,
                "digest already being watched",
            )

            await daemon.pipe_drains["now"].drain_once()
            dispatches = [
                row["status"]
                for row in daemon.connection.execute(
                    "SELECT status FROM dispatches WHERE pipe = 'now'"
                )
            ]
            health = await daemon._op_health({})
        finally:
            daemon.connection.close()

        assert dispatches == ["muted"], dispatches
        channel_rows = {
            row["channel"]: row for row in health["channels"]["health"]
        }
        assert channel_rows["email"]["status"] == "unhealthy"
        assert channel_rows["email"]["last_error"] == "smtp dead"

    asyncio.run(driver())


def test_daemon_down_health_renders_new_dep_and_channel_surfaces(tmp_path) -> None:
    _write_lodging(tmp_path)
    state = tmp_path / "state"
    state.mkdir()
    connection = init_db(state / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    catalog.record_dep_health(
        "iotaschool",
        "unhealthy",
        "2026-05-20T13:00:00.000Z",
        "exit 7: connection refused",
    )
    catalog.add_mute(
        "internal/dep:dependency_unhealthy:iotaschool",
        3600,
        "flapping, acked",
    )
    catalog.record_digest_send_failure("daily", "email", "smtp dead")
    catalog.mark_channel_unhealthy("email", "smtp dead")
    connection.commit()
    connection.close()

    result = CliRunner().invoke(main, ["health", "--root", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "iotaschool: unhealthy" in result.output
    assert "muted until:" in result.output
    assert "channels:" in result.output
    assert "email: unhealthy" in result.output
    assert "daily/email: 1 attempts" in result.output
