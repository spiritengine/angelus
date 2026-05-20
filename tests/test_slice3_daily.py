from __future__ import annotations

import asyncio
import os
from datetime import UTC, datetime
from pathlib import Path

import pytest
from apscheduler.triggers.cron import CronTrigger

import angelus.channels.email as email_module
import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon, _make_trigger
from angelus.lodging import Channel, Pipe, load_lodging
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db


def _daily_pipe(inputs: list[str] | None = None) -> Pipe:
    return Pipe(
        name="daily",
        cadence="0 8 * * *",
        render_kind="digest",
        template=None,
        channels=["push"],
        render={
            "preamble": [
                {"kind": "structured", "template": "rate-limit-callout"},
                {"kind": "structured", "template": "incident-status"},
            ],
            "body": {
                "kind": "llm",
                "mantle": "chronicler",
                "inputs": inputs
                if inputs is not None
                else ["findings_since_last_drain", "open_incidents"],
            },
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
            "overflow": "daily",
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
        "    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n      - open_incidents\n",
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
        daemon._register_initial_jobs()
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

    async def fake_llm(self, _pipe, _structured):
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


def test_digest_partial_channel_failure_does_not_resend_success_channel(
    tmp_path, monkeypatch
) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        Pipe(
            name="daily",
            cadence="0 8 * * *",
            render_kind="digest",
            template=None,
            channels=["push", "email"],
            render=_daily_pipe().render,
        ),
        {
            "push": Channel(name="push", kind="push", command="notify-pat"),
            "email": Channel(
                name="email",
                kind="email",
                command="/home/user/projects/patbot-email/patbot-email",
                to="person@example.com",
            ),
        },
        tmp_path,
        {"now", "daily"},
    )
    finding_id = catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "site",
            "severity": "high",
            "target_pipes": ["daily"],
        },
        {"daily"},
    )
    push_messages: list[str] = []
    email_attempts = 0
    email_subjects: list[str] = []

    async def fake_push(_channel, message: str, _workdir: Path) -> None:
        push_messages.append(message)

    async def fake_email(_channel, subject: str, _body: str, _workdir: Path) -> None:
        nonlocal email_attempts
        email_attempts += 1
        email_subjects.append(subject)
        raise RuntimeError("email broke")

    async def fake_llm(self, _pipe, _structured):
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        asyncio.run(drain.drain_once())
        first_queue = connection.execute(
            """
            SELECT status, attempts, next_attempt_at
            FROM pipe_queues
            WHERE finding_id = ? AND pipe = 'daily'
            """,
            (finding_id,),
        ).fetchone()
        first_drain_at = catalog.last_pipe_drain_at("daily")
        failed_dispatches = list(
            connection.execute(
                """
                SELECT channel, status, last_error
                FROM dispatches
                WHERE pipe = 'daily' AND status = 'failed'
                ORDER BY id
                """
            )
        )
        internal = connection.execute(
            """
            SELECT type, entity FROM findings
            WHERE source = 'internal/dispatch'
            """
        ).fetchone()
        channel_health_rows = list(
            connection.execute("SELECT channel FROM channel_health")
        )
        asyncio.run(drain.drain_once())
        sent_dispatches = list(
            connection.execute(
                """
                SELECT channel, status
                FROM dispatches
                WHERE pipe = 'daily' AND status = 'sent'
                ORDER BY id
                """
            )
        )
    finally:
        connection.close()

    assert first_queue["status"] == "dispatched"
    assert first_queue["attempts"] == 0
    assert first_queue["next_attempt_at"] is None
    assert first_drain_at is not None
    assert len(push_messages) == 1
    assert email_attempts == 1
    assert [row["channel"] for row in failed_dispatches] == ["email"]
    assert failed_dispatches[0]["last_error"] == "email broke"
    assert internal["type"] == "channel_unhealthy"
    assert internal["entity"] == "email"
    assert channel_health_rows == []
    assert [row["channel"] for row in sent_dispatches] == ["push"]
    assert email_subjects
    today_utc = datetime.now(UTC).date().isoformat()
    assert email_subjects[0] == f"Angelus daily digest {today_utc} UTC"


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
    catalog.suppress_pipe_item_to(finding_id, "now", "daily")
    seen_inputs: list[dict] = []

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    async def fake_llm(self, _pipe, structured):
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


def test_empty_day_channel_failure_records_dispatch_and_emits_internal(
    tmp_path, monkeypatch
) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        Pipe(
            name="daily",
            cadence="0 8 * * *",
            render_kind="digest",
            template=None,
            channels=["email"],
            render=_daily_pipe().render,
        ),
        {
            "email": Channel(
                name="email",
                kind="email",
                command="/home/user/projects/patbot-email/patbot-email",
                to="person@example.com",
            )
        },
        tmp_path,
        {"now", "daily"},
    )

    async def fake_email(_channel, _subject: str, _body: str, _workdir: Path) -> None:
        raise RuntimeError("email broke")

    async def fake_llm(self, _pipe, _structured):
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        asyncio.run(drain.drain_once())
        dispatches = list(
            connection.execute(
                """
                SELECT channel, finding_ids, status, last_error
                FROM dispatches
                WHERE pipe = 'daily'
                """
            )
        )
        internal = connection.execute(
            "SELECT type, entity FROM findings WHERE source = 'internal/dispatch'"
        ).fetchone()
        channel_health_rows = list(
            connection.execute("SELECT channel FROM channel_health")
        )
    finally:
        connection.close()

    assert len(dispatches) == 1
    assert dispatches[0]["channel"] == "email"
    assert dispatches[0]["status"] == "failed"
    assert dispatches[0]["finding_ids"] == "[]"
    assert dispatches[0]["last_error"] == "email broke"
    assert internal["type"] == "channel_unhealthy"
    assert internal["entity"] == "email"
    assert channel_health_rows == []


def _digest_email_drain(catalog: Catalog, workdir: Path) -> PipeDrain:
    return PipeDrain(
        catalog,
        Pipe(
            name="daily",
            cadence="0 8 * * *",
            render_kind="digest",
            template=None,
            channels=["email"],
            render=_daily_pipe().render,
        ),
        {
            "email": Channel(
                name="email",
                kind="email",
                command="/home/user/projects/patbot-email/patbot-email",
                to="person@example.com",
            )
        },
        workdir,
        {"now", "daily"},
    )


def test_digest_channel_repeated_failure_marks_channel_unhealthy(
    tmp_path, monkeypatch
) -> None:
    from angelus.storage.catalog import MAX_RETRY_ATTEMPTS

    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = _digest_email_drain(catalog, tmp_path)

    async def fake_email(_channel, _subject: str, _body: str, _workdir: Path) -> None:
        raise RuntimeError("smtp dead")

    async def fake_llm(self, _pipe, _structured):
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)
    monkeypatch.setattr(pipe_runner, "_is_same_utc_day", lambda *_args: False)

    try:
        catalog.write_finding(
            None,
            {
                "source": "scheduled/test",
                "type": "down",
                "entity": "site",
                "severity": "high",
                "target_pipes": ["daily"],
            },
            {"daily"},
        )
        per_cycle_attempts: list[int] = []
        per_cycle_health: list[list[str]] = []
        for _ in range(MAX_RETRY_ATTEMPTS):
            asyncio.run(drain.drain_once())
            attempts_row = connection.execute(
                """
                SELECT attempts FROM digest_channel_attempts
                WHERE pipe = 'daily' AND channel = 'email'
                """
            ).fetchone()
            per_cycle_attempts.append(
                int(attempts_row["attempts"]) if attempts_row is not None else 0
            )
            per_cycle_health.append(
                [
                    row["channel"]
                    for row in connection.execute(
                        "SELECT channel FROM channel_health WHERE status = 'unhealthy'"
                    )
                ]
            )
    finally:
        connection.close()

    assert per_cycle_attempts == list(range(1, MAX_RETRY_ATTEMPTS + 1))
    assert per_cycle_health[: MAX_RETRY_ATTEMPTS - 1] == [
        [] for _ in range(MAX_RETRY_ATTEMPTS - 1)
    ]
    assert per_cycle_health[-1] == ["email"]


def test_digest_channel_success_resets_failure_counter(tmp_path, monkeypatch) -> None:
    from angelus.storage.catalog import MAX_RETRY_ATTEMPTS

    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = _digest_email_drain(catalog, tmp_path)
    cycle_outcomes = (
        ["fail"] * (MAX_RETRY_ATTEMPTS - 1)
        + ["ok"]
        + ["fail"] * (MAX_RETRY_ATTEMPTS - 1)
    )
    cycle_index = {"i": 0}

    async def fake_email(_channel, _subject: str, _body: str, _workdir: Path) -> None:
        outcome = cycle_outcomes[cycle_index["i"]]
        cycle_index["i"] += 1
        if outcome == "fail":
            raise RuntimeError("smtp dead")

    async def fake_llm(self, _pipe, _structured):
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)
    monkeypatch.setattr(pipe_runner, "_is_same_utc_day", lambda *_args: False)

    try:
        for cycle in range(len(cycle_outcomes)):
            catalog.write_finding(
                None,
                {
                    "source": "scheduled/test",
                    "type": "down",
                    "entity": f"site-cycle-{cycle}",
                    "severity": "high",
                    "target_pipes": ["daily"],
                },
                {"daily"},
            )
            asyncio.run(drain.drain_once())
        final_attempts_row = connection.execute(
            """
            SELECT attempts FROM digest_channel_attempts
            WHERE pipe = 'daily' AND channel = 'email'
            """
        ).fetchone()
        final_health = list(
            connection.execute(
                "SELECT channel FROM channel_health WHERE status = 'unhealthy'"
            )
        )
    finally:
        connection.close()

    assert cycle_index["i"] == len(cycle_outcomes)
    assert final_attempts_row is not None
    assert int(final_attempts_row["attempts"]) == MAX_RETRY_ATTEMPTS - 1
    assert final_health == []


def test_digest_attempts_send_even_when_channel_marked_unhealthy(
    tmp_path, monkeypatch
) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = _digest_email_drain(catalog, tmp_path)
    catalog.mark_channel_unhealthy("email", "marked-by-earlier-cycle")
    connection.commit()
    attempts: list[str] = []

    async def fake_email(_channel, subject: str, _body: str, _workdir: Path) -> None:
        attempts.append(subject)
        raise RuntimeError("still broken")

    async def fake_llm(self, _pipe, _structured):
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        catalog.write_finding(
            None,
            {
                "source": "scheduled/test",
                "type": "down",
                "entity": "site",
                "severity": "high",
                "target_pipes": ["daily"],
            },
            {"daily"},
        )
        asyncio.run(drain.drain_once())
        dispatch = connection.execute(
            """
            SELECT status, last_error
            FROM dispatches
            WHERE pipe = 'daily' AND channel = 'email'
            """
        ).fetchone()
    finally:
        connection.close()

    assert len(attempts) == 1
    assert dispatch is not None
    assert dispatch["status"] == "failed"
    assert dispatch["last_error"] == "still broken"


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
    closure_counts: list[int] = []
    finding_counts: list[int] = []

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    async def fake_llm(self, _pipe, structured):
        closure_counts.append(len(structured["recent_closures"]))
        finding_counts.append(len(structured["findings_since_last_drain"]))
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(pipe_runner, "_is_same_utc_day", lambda *_args: False)
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

    assert closure_counts == [1, 0]
    assert finding_counts == [0, 0]
    assert second_state is not None
    assert second_state >= first_state


def test_daily_drain_processes_all_pending_items(tmp_path, monkeypatch) -> None:
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
    llm_inputs: list[dict] = []

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    async def fake_llm(self, _pipe, structured):
        llm_inputs.append(structured)
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        for idx in range(25):
            catalog.write_finding(
                None,
                {
                    "source": "scheduled/test",
                    "type": "down",
                    "entity": f"site-{idx}",
                    "severity": "high",
                    "target_pipes": ["daily"],
                },
                {"daily"},
            )
        asyncio.run(drain.drain_once())
        dispatched = connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM pipe_queues
            WHERE pipe = 'daily' AND status = 'dispatched'
            """
        ).fetchone()
    finally:
        connection.close()

    assert len(llm_inputs) == 1
    assert len(llm_inputs[0]["findings_since_last_drain"]) == 25
    assert dispatched["n"] == 25


def test_email_channel_invokes_patbot_with_subject_and_stdin(tmp_path) -> None:
    capture_args = tmp_path / "args.txt"
    capture_stdin = tmp_path / "stdin.txt"
    script = tmp_path / "fake-patbot-email"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import sys\n"
        f"open({str(capture_args)!r}, 'w').write('\\n'.join(sys.argv))\n"
        f"open({str(capture_stdin)!r}, 'wb').write(sys.stdin.buffer.read())\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    channel = Channel(
        name="email",
        kind="email",
        command=str(script),
        to="person@example.com",
    )

    asyncio.run(email_module.send_email(channel, "Angelus daily 2026-05-14", "body", tmp_path))

    assert capture_args.read_text(encoding="utf-8").split("\n") == [
        str(script),
        "send",
        "person@example.com",
        "Angelus daily 2026-05-14",
    ]
    assert capture_stdin.read_bytes() == b"body"


def test_email_channel_kills_subprocess_on_timeout(tmp_path) -> None:
    pid_file = tmp_path / "patbot.pid"
    script = tmp_path / "hang-patbot-email"
    script.write_text(
        "#!/usr/bin/env python3\n"
        "import os, time\n"
        f"open({str(pid_file)!r}, 'w').write(str(os.getpid()))\n"
        "time.sleep(999)\n",
        encoding="utf-8",
    )
    script.chmod(0o755)
    channel = Channel(
        name="email",
        kind="email",
        command=str(script),
        to="person@example.com",
    )

    with pytest.raises(RuntimeError, match=r"email timed out after 0\.2s"):
        asyncio.run(
            email_module.send_email(
                channel, "subject", "body", tmp_path, timeout_seconds=0.2
            )
        )

    pid = int(pid_file.read_text(encoding="utf-8"))
    with pytest.raises(ProcessLookupError):
        os.kill(pid, 0)


def test_email_channel_loaded_from_repo_lodging() -> None:
    lodging = load_lodging(Path.cwd())

    assert lodging.channels["email"].kind == "email"
    assert lodging.channels["email"].to == "$env:ANGELUS_EMAIL_TO"


def test_digest_preamble_blocks_reject_source_field(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      source: suppressed_findings\n"
        "      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="do not accept source"):
        load_lodging(tmp_path)


def test_channel_command_is_required(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "channels" / "push.yaml").write_text("kind: push\n", encoding="utf-8")

    with pytest.raises(ValueError, match="non-empty string command"):
        load_lodging(tmp_path)


def test_digest_body_inputs_filter_chronicler_payload(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(inputs=["findings_since_last_drain"]),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    captured: dict[str, object] = {}

    real_create = pipe_runner.asyncio.create_subprocess_exec

    async def capture_create(*args, **kwargs):
        for idx, token in enumerate(args):
            if token == "--message" and idx + 1 < len(args):
                captured["message"] = args[idx + 1]
        return await real_create(
            "sh", "-c", "echo 'chronicler output text here.'", **kwargs
        )

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(
        pipe_runner.asyncio, "create_subprocess_exec", capture_create
    )

    catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "site",
            "severity": "high",
            "target_pipes": ["daily"],
        },
        {"daily"},
    )
    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    import json as _json

    payload_json = captured["message"].split("\n\n", 1)[1]
    payload = _json.loads(payload_json)
    assert list(payload.keys()) == ["findings_since_last_drain"]
    assert "open_incidents" not in payload
    assert "suppressed_findings" not in payload
    assert "recent_closures" not in payload


def test_digest_body_inputs_unknown_name_rejected(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - bogus_name\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="unknown name 'bogus_name'"):
        load_lodging(tmp_path)


def test_clearance_dedup_excludes_clearance_from_findings(tmp_path, monkeypatch) -> None:
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(
            inputs=["findings_since_last_drain", "recent_closures", "open_incidents"]
        ),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "site-a",
            "severity": "high",
            "target_pipes": ["daily"],
        },
        {"daily"},
    )
    catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "clearance",
            "entity": "site-b",
            "severity": "info",
            "target_pipes": ["daily"],
        },
        {"daily"},
    )
    seen: list[dict] = []

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    async def fake_llm(self, _pipe, structured):
        seen.append(structured)
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    findings = seen[0]["findings_since_last_drain"]
    closures = seen[0]["recent_closures"]
    assert [item["entity"] for item in findings] == ["site-a"]
    assert [item["type"] for item in findings] == ["down"]
    assert [item["entity"] for item in closures] == ["site-b"]


def test_overflow_defer_to_prefix_is_rejected(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n"
        "rate_limit:\n"
        "  per_channel: 6/hr\n  per_source: 4/hr\n  overflow: defer_to_daily\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="rate_limit.overflow references unknown pipe 'defer_to_daily'"
    ):
        load_lodging(tmp_path)


def test_overflow_unknown_pipe_is_rejected(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n"
        "rate_limit:\n"
        "  per_channel: 6/hr\n  per_source: 4/hr\n  overflow: nonexistent\n",
        encoding="utf-8",
    )

    with pytest.raises(
        ValueError, match="rate_limit.overflow references unknown pipe 'nonexistent'"
    ):
        load_lodging(tmp_path)


def test_overflow_bare_pipe_name_loads_cleanly(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n"
        "rate_limit:\n"
        "  per_channel: 6/hr\n  per_source: 4/hr\n  overflow: daily\n",
        encoding="utf-8",
    )

    lodging = load_lodging(tmp_path)

    assert lodging.pipes["now"].rate_limit["overflow"] == "daily"


def test_digest_body_kind_must_be_llm(tmp_path) -> None:
    _write_lodging(tmp_path)
    (tmp_path / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      template: rate-limit-callout\n"
        "  body:\n    kind: handlebars\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n",
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="body.kind must be 'llm'"):
        load_lodging(tmp_path)
