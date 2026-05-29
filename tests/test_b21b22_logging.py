"""B21+B22 logging unification.

Proves the daemon's failure paths leave real ERROR lines in the single
canonical log file (state/angelus.log), not just INFO chatter -- the gap that
let the 2026-05-29 silent failure go unlogged.
"""

from __future__ import annotations

import asyncio
import logging

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.lodging import Channel, Pipe
from angelus.logging_config import (
    _HANDLER_MARK,
    configure_logging,
    log_path,
)
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db


@pytest.fixture
def reset_root_logging():
    """Restore the root logger after a test wires handlers onto it, so the
    file handler bound to a tmp_path does not leak into later tests."""
    root = logging.getLogger()
    saved_level = root.level
    yield
    for handler in list(root.handlers):
        if getattr(handler, _HANDLER_MARK, False):
            root.removeHandler(handler)
            handler.close()
    root.setLevel(saved_level)


def _now_drain(tmp_path) -> tuple[Catalog, PipeDrain, int]:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    pipe = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{type}:{entity}:{body}",
        channels=["push"],
    )
    channel = Channel(name="push", kind="push", command="notify-pat")
    drain = PipeDrain(catalog, pipe, {"push": channel}, tmp_path, {"now"})
    observation_id = catalog.write_observation(
        "scheduled/a", {}, {"source": "scheduled/a"}
    )
    finding_id = catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/a",
            "type": "down",
            "entity": "example",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now"},
    )
    return catalog, drain, finding_id


def _force_due(connection, finding_id: int) -> None:
    connection.execute(
        """
        UPDATE pipe_queues
        SET next_attempt_at = '2000-01-01T00:00:00.000Z'
        WHERE finding_id = ? AND pipe = 'now'
        """,
        (finding_id,),
    )
    connection.commit()


def test_failed_dispatch_writes_error_line_to_angelus_log(
    tmp_path, monkeypatch, reset_root_logging
) -> None:
    path = configure_logging(tmp_path, console=False)
    assert path == log_path(tmp_path)
    assert path == tmp_path / "state" / "angelus.log"

    catalog, drain, finding_id = _now_drain(tmp_path)

    async def fail_send(*_args, **_kwargs):
        raise RuntimeError("push broke")

    monkeypatch.setattr(pipe_runner, "send_push", fail_send)
    try:
        # Drive the immediate path through its retry ladder to exhaustion; the
        # final drain is the forced failed dispatch the daemon gives up on.
        for _ in range(5):
            asyncio.run(drain.drain_once())
            _force_due(catalog.connection, finding_id)
    finally:
        catalog.connection.close()

    contents = path.read_text(encoding="utf-8")
    error_lines = [line for line in contents.splitlines() if " ERROR " in line]

    # grep ERROR is non-empty (acceptance) ...
    assert error_lines, f"expected an ERROR line, got:\n{contents}"
    # ... and an ERROR line names the failure.
    assert any(
        "push broke" in line and "exhausted" in line for line in error_lines
    ), f"no ERROR line named the exhausted dispatch:\n{contents}"
    # The channel-unhealthy transition is logged too (WARNING).
    assert any(
        "channel push marked unhealthy" in line
        for line in contents.splitlines()
    ), contents


def test_configure_logging_is_idempotent(tmp_path, reset_root_logging) -> None:
    configure_logging(tmp_path, console=False)
    configure_logging(tmp_path, console=False)

    root = logging.getLogger()
    marked = [h for h in root.handlers if getattr(h, _HANDLER_MARK, False)]
    # One file handler, no duplicates from the second call.
    assert len(marked) == 1
