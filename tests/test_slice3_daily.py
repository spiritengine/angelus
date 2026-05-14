from __future__ import annotations

import asyncio
import os
import subprocess
from pathlib import Path

import pytest
from apscheduler.triggers.cron import CronTrigger

import angelus.channels.email as email_module
import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon, _make_trigger
from angelus.lodging import Channel, Pipe, load_lodging
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db


def _daily_pipe() -> Pipe:
    return Pipe(
        name="daily",
        cadence="0 8 * * *",
        render_kind="digest",
        template=None,
        channels=["push"],
        render={
            "preamble": [
                {
                    "kind": "structured",
                    "source": "suppressed_findings",
                    "template": "rate-limit-callout",
                },
                {
                    "kind": "structured",
                    "source": "open_incidents",
                    "template": "incident-status",
                },
            ],
            "body": {"kind": "llm", "mantle": "chronicler"},
        },
    )


def _now_pipe() -> Pipe:
    return Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{type}:{entity}:{body}",
        channels=["push"],
        rate_limit={
            "per_channel": "6/hr",
            "per_source": "4/hr",
            "overflow": "defer_to_daily",
        },
    )


def _write_templates(root: Path) -> None:
    (root / "render-templates").mkdir()
    (root / "render-templates" / "rate-limit-callout.j2").write_text(
        "Suppressed:\n{% for finding in suppressed_findings %}"
        "{{ finding.entity }} {{ finding.body_text }}\n{% endfor %}",
        encoding="utf-8",
    )
    (root / "render-templates" / "incident-status.j2").write_text(
        "Incidents:\n{% for incident in open_incidents %}"
        "{{ incident.entity }}\n{% endfor %}"
        "{% for closure in recent_closures %}closed {{ closure.entity }}\n{% endfor %}",
        encoding="utf-8",
    )


def _write_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "test.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "handlers" / "noop.py").write_text(
        "import json\nprint(json.dumps({'findings': [], 'new_state': {}}))\n",
        encoding="utf-8",
    )
    (root / "triagers" / "noop.yaml").write_text(
        "inputs:\n  source: scheduled/test\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      source: suppressed_findings\n"
        "      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n",
        encoding="utf-8",
    )
    _write_templates(root)


def _insert_sent_dispatches(
    catalog: Catalog, *, channel: str = "push", source: str = "scheduled/test", count: int
) -> None:
    for idx in range(count):
        catalog.record_dispatch("now", channel, [idx + 1000], "sent", source=source)


def test_cron_cadence_and_daemon_register_daily_drain(tmp_path) -> None:
    _write_lodging(tmp_path)
    assert isinstance(_make_trigger("0 8 * * *"), CronTrigger)

    daemon = AngelusDaemon(tmp_path)
    try:
        daemon._register_sources()
        job = daemon.scheduler.get_job("pipe:daily")
        assert job is not None
        assert isinstance(job.trigger, CronTrigger)
    finally:
        daemon.connection.close()


def test_rate_limit_per_channel_suppresses_and_routes_daily(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _insert_sent_dispatches(catalog, count=6)
    drain = PipeDrain(
        catalog,
        _now_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    finding_id = catalog.write_finding(
        None,
        {
            "source": "scheduled/new",
            "type": "down",
            "entity": "example",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now", "daily"},
    )
    monkeypatch.setattr(pipe_runner, "send_push", pytest.fail)

    try:
        asyncio.run(drain.drain_once())
        rows = {
            row["pipe"]: row["status"]
            for row in connection.execute(
                "SELECT pipe, status FROM pipe_queues WHERE finding_id = ?",
                (finding_id,),
            )
        }
    finally:
        connection.close()

    assert rows == {"now": "suppressed", "daily": "pending"}


def test_rate_limit_per_source_suppresses_and_routes_daily(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _insert_sent_dispatches(catalog, source="scheduled/test", count=4)
    drain = PipeDrain(
        catalog,
        _now_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    finding_id = catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "example",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now", "daily"},
    )
    monkeypatch.setattr(pipe_runner, "send_push", pytest.fail)

    try:
        asyncio.run(drain.drain_once())
        rows = {
            row["pipe"]: row["status"]
            for row in connection.execute(
                "SELECT pipe, status FROM pipe_queues WHERE finding_id = ?",
                (finding_id,),
            )
        }
    finally:
        connection.close()

    assert rows == {"now": "suppressed", "daily": "pending"}


def test_two_zone_render_dispatches_preamble_then_llm_body(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "site",
            "severity": "high",
            "target_pipes": ["daily"],
            "body": "structured fact",
        },
        {"daily"},
    )
    sent: list[str] = []

    async def fake_push(_channel, message: str, _workdir: Path) -> None:
        sent.append(message)

    async def fake_llm(self, _structured):
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert sent
    assert sent[0].index("Suppressed:") < sent[0].index("This is the chronicler body.")
    assert "Incidents:" in sent[0]


def test_suppressed_overflow_stays_out_of_llm_inputs(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    finding_id = catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "overflowed",
            "severity": "high",
            "target_pipes": ["now"],
            "body": "suppressed body",
        },
        {"now", "daily"},
    )
    catalog.suppress_pipe_item_to_daily(finding_id, "now")
    seen_inputs: list[dict] = []

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    async def fake_llm(self, structured):
        seen_inputs.append(structured)
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert [item["entity"] for item in seen_inputs[0]["suppressed_findings"]] == [
        "overflowed"
    ]
    assert seen_inputs[0]["findings_since_last_drain"] == []


def test_llm_nonzero_fallback_dispatches_and_emits_internal_finding(
    tmp_path, monkeypatch
) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    horizon = tmp_path / "horizon"
    horizon.write_text("#!/usr/bin/env sh\necho broken >&2\nexit 7\n", encoding="utf-8")
    horizon.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    sent: list[str] = []

    async def fake_push(_channel, message: str, _workdir: Path) -> None:
        sent.append(message)

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    drain = PipeDrain(
        catalog,
        _daily_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )

    try:
        asyncio.run(drain.drain_once())
        internal = connection.execute(
            "SELECT source, type, entity FROM findings WHERE source = 'internal/render'"
        ).fetchone()
        queued = connection.execute(
            """
            SELECT status FROM pipe_queues
            WHERE pipe = 'now' AND finding_id = (
                SELECT id FROM findings WHERE source = 'internal/render'
            )
            """
        ).fetchone()
    finally:
        connection.close()

    assert "LLM digest body unavailable — see structured data above." in sent[0]
    assert internal["type"] == "llm_render_failed"
    assert internal["entity"] == "daily"
    assert queued["status"] == "pending"


def test_llm_timeout_kills_subprocess_and_falls_back(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    pid_file = tmp_path / "horizon.pid"
    horizon = tmp_path / "horizon"
    horizon.write_text(
        "#!/usr/bin/env python3\n"
        "import os, sys, time\n"
        f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "time.sleep(30)\n",
        encoding="utf-8",
    )
    horizon.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    real_wait_for = pipe_runner.asyncio.wait_for

    async def short_wait(awaitable, timeout):
        return await real_wait_for(awaitable, timeout=0.2)

    monkeypatch.setattr(pipe_runner.asyncio, "wait_for", short_wait)
    sent: list[str] = []

    async def fake_push(_channel, message: str, _workdir: Path) -> None:
        sent.append(message)

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    drain = PipeDrain(
        catalog,
        _daily_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )

    try:
        asyncio.run(drain.drain_once())
        pid = int(pid_file.read_text(encoding="utf-8"))
        with pytest.raises(ProcessLookupError):
            os.kill(pid, 0)
        internal = connection.execute(
            "SELECT type FROM findings WHERE source = 'internal/render'"
        ).fetchone()
    finally:
        connection.close()

    assert "LLM digest body unavailable" in sent[0]
    assert internal["type"] == "llm_render_failed"


def test_pipe_state_updates_and_scopes_next_drain(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    structured_counts: list[int] = []

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    async def fake_llm(self, structured):
        structured_counts.append(len(structured["findings_since_last_drain"]))
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        catalog.write_finding(
            None,
            {
                "source": "scheduled/test",
                "type": "clearance",
                "entity": "site",
                "severity": "info",
                "target_pipes": ["daily"],
            },
            {"daily"},
        )
        asyncio.run(drain.drain_once())
        first_state = catalog.last_pipe_drain_at("daily")
        assert first_state is not None
        asyncio.run(drain.drain_once())
        second_state = catalog.last_pipe_drain_at("daily")
    finally:
        connection.close()

    assert structured_counts == [1, 0]
    assert second_state is not None
    assert second_state >= first_state


def test_email_channel_invokes_patbot_with_subject_and_stdin(tmp_path, monkeypatch) -> None:
    channel = Channel(
        name="email",
        kind="email",
        command="/home/user/projects/patbot-email/patbot-email",
        to="person@example.com",
    )
    calls: list[tuple[list[str], bytes]] = []

    def fake_run(args, input, check, stdout, stderr):
        calls.append((args, input))
        return subprocess.CompletedProcess(args, 0, stdout=b"", stderr=b"")

    monkeypatch.setattr(email_module.subprocess, "run", fake_run)

    asyncio.run(email_module.send_email(channel, "Angelus daily 2026-05-14", "body", tmp_path))

    assert calls == [
        (
            [
                "/home/user/projects/patbot-email/patbot-email",
                "send",
                "person@example.com",
                "Angelus daily 2026-05-14",
            ],
            b"body",
        )
    ]


def test_email_channel_loaded_from_repo_lodging() -> None:
    lodging = load_lodging(Path.cwd())

    assert lodging.channels["email"].kind == "email"
    assert lodging.channels["email"].to == "$env:ANGELUS_EMAIL_TO"
