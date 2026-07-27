"""Unexpected task failures remain visible during bounded shutdown."""

from __future__ import annotations

import asyncio

from angelus.daemon import AngelusDaemon
from angelus.lodging.reloader import LodgingReloader


async def _task_failing_on_cancel(message: str) -> asyncio.Task[None]:
    started = asyncio.Event()

    async def fail_on_cancel() -> None:
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            raise RuntimeError(message) from None

    task = asyncio.create_task(fail_on_cancel())
    await started.wait()
    return task


def test_reloader_stop_logs_unexpected_task_failure(tmp_path, caplog) -> None:
    async def driver() -> None:
        reloader = LodgingReloader(object(), tmp_path)  # type: ignore[arg-type]
        reloader._task = await _task_failing_on_cancel("reload failed")

        with caplog.at_level("ERROR", logger="angelus.lodging.reloader"):
            await reloader.stop()

        assert reloader._task is None

    asyncio.run(driver())

    records = [
        record
        for record in caplog.records
        if record.name == "angelus.lodging.reloader"
    ]
    assert [record.getMessage() for record in records] == [
        "lodging reload task failed during shutdown"
    ]
    assert records[0].exc_info is not None
    assert records[0].exc_info[0] is RuntimeError


def test_cancel_pipe_loop_logs_unexpected_task_failure(caplog) -> None:
    async def driver() -> None:
        daemon = object.__new__(AngelusDaemon)
        daemon._pipe_loop_tasks = {
            "broken": await _task_failing_on_cancel("pipe loop failed")
        }

        with caplog.at_level("ERROR", logger="angelus.daemon"):
            await daemon._cancel_pipe_loop("broken")

        assert "broken" not in daemon._pipe_loop_tasks

    asyncio.run(driver())

    records = [
        record for record in caplog.records if record.name == "angelus.daemon"
    ]
    assert [record.getMessage() for record in records] == [
        "pipe loop broken failed during shutdown"
    ]
    assert records[0].exc_info is not None
    assert records[0].exc_info[0] is RuntimeError
