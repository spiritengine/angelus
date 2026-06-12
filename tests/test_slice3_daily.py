from __future__ import annotations

import asyncio
import os
from datetime import datetime
from pathlib import Path

import pytest
from apscheduler.triggers.cron import CronTrigger

import angelus.channels.email as email_module
import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon, _make_trigger
from angelus.lodging import Channel, Pipe, load_lodging
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db


def _daily_pipe(
    inputs: list[str] | None = None,
    channels: list[str] | None = None,
) -> Pipe:
    return Pipe(
        name="daily",
        cadence="0 7 * * *",
        render_kind="digest",
        template=None,
        channels=channels if channels is not None else ["push"],
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
        _daily_pipe(channels=["email"]),
        {"email": Channel(name="email", kind="email", command="true", to="x@y")},
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

    async def fake_email(_channel, _subject: str, body: str, _workdir: Path) -> None:
        sent.append(body)

    async def fake_llm(self, _pipe, _structured):
        return "This is the chronicler body.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert sent
    # The full long-form digest rides the email leg (push gets the compact
    # render). Body ordering reversed 2026-05-27: chronicler synthesis
    # paragraph leads, structured preamble items follow. The preamble was the
    # source of structured-data-twice-rendered messes; the llm is now
    # constrained to a short paragraph and the preamble owns items.
    assert sent[0].index("This is the chronicler body.") < sent[0].index("Suppressed:")
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
    # Subject format changed 2026-05-27 to local-time, screen-reader
    # friendly form: "Angelus Observances for <weekday> <month> <day>,
    # <year>". The exact rendered date depends on the test machine's
    # local TZ; assert against the prefix and the year.
    today_local = datetime.now().astimezone()
    expected_subject = (
        f"Angelus Observances for "
        f"{today_local.strftime('%A %B')} "
        f"{today_local.day}, {today_local.year}"
    )
    assert email_subjects[0] == expected_subject


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

    async def fake_email(_channel, _subject: str, body: str, _workdir: Path) -> None:
        sent.append(body)

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["email"]),
        {"email": Channel(name="email", kind="email", command="true", to="x@y")},
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

    # Footer wording flipped from "above" to "below" when the body order
    # reversed in the email cleanup pass (fell-r1 BLOCK #1).
    assert "LLM digest body unavailable — see structured data below." in sent[0]
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

    async def fake_email(_channel, _subject: str, body: str, _workdir: Path) -> None:
        sent.append(body)

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["email"]),
        {"email": Channel(name="email", kind="email", command="true", to="x@y")},
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
        # Open an incident for `site` first so the clearance has something to
        # close -- under the B30 recovery gate a clearance with nothing open
        # is a no-op. Route the opening `down` to `now`, not `daily`, so it
        # stays out of the daily findings_since_last_drain this test scopes.
        catalog.write_finding(
            None,
            {
                "source": "scheduled/test",
                "type": "down",
                "entity": "site",
                "severity": "high",
                "target_pipes": ["now"],
            },
            {"now", "daily"},
        )
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
    lodging = load_lodging(Path.cwd() / "examples" / "lodging")

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

    # The prompt rides a temp file now (it embeds the inputs JSON, which would
    # blow past the argv size limit), passed via --message-file; capture it by
    # reading that file rather than from argv.
    async def capture_create(*args, **kwargs):
        argv = list(args)
        captured["argv"] = argv
        idx = argv.index("--message-file")
        captured["message"] = Path(argv[idx + 1]).read_text(encoding="utf-8")

        class _Proc:
            returncode = 0

            async def communicate(self, stdin_bytes=None):
                return b'{"result": "chronicler output text here."}', b""

        return _Proc()

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

    # Chronicler prompt has a "Structured inputs (JSON):\n<json>" suffix.
    # The JSON is the last block in the prompt; split on the marker so
    # the test stays decoupled from the prose-rule list above.
    message = captured["message"]
    marker = "Structured inputs (JSON):\n"
    payload_json = message.rsplit(marker, 1)[1]
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
    # Open an incident for site-b first so its clearance has something to
    # close (B30 recovery gate). Routed to `now`, so it does not appear in the
    # daily findings_since_last_drain that this test asserts holds only site-a.
    catalog.write_finding(
        None,
        {
            "source": "scheduled/test",
            "type": "down",
            "entity": "site-b",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now", "daily"},
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


def test_llm_body_unwraps_horizon_cast_envelope(tmp_path, monkeypatch) -> None:
    # `horizon cast` stdout wraps the model output in a CLI envelope:
    # a "New strand created: ..." preamble, three help lines about
    # `cast --omlet`, a "Result: ..." line carrying the actual body,
    # and a footer block (Omlet:/Strand:/Status:/Bearing:/Duration:).
    # Pre-fix the runner returned the entire envelope as the digest
    # body; the friction-20260514-hxpb gap. This test pins that
    # `_render_llm_body` strictly returns the unwrapped model output.
    #
    # The stub branches on argv: with `--json`, it returns the
    # structured payload exactly as real `horizon cast --json` does;
    # without `--json`, it returns the legacy text envelope. This
    # makes the fix's choice of envelope discriminating both ways:
    #
    #   * dropping `--json` from the runner's argv -> stub emits
    #     legacy text -> json.loads fails -> fallback footer dispatched
    #     instead of the clean body -> this test fails.
    #   * keeping `--json` but skipping the parse and returning raw
    #     stdout -> body is the JSON blob (braces, escape codes) ->
    #     contains "result" / "omlet" markers -> this test fails.
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    horizon = tmp_path / "horizon"
    horizon.write_text(
        "#!/usr/bin/env python3\n"
        "import json, sys\n"
        "argv = sys.argv\n"
        "clean = (\n"
        "    'Digest body: nothing urgent across the watched surface. '\n"
        "    'One closure noted, otherwise quiet.'\n"
        ")\n"
        "if '--json' in argv:\n"
        "    sys.stdout.write(json.dumps({\n"
        "        'omlet': 'api_2026-05-20_00-00-00-000@agent-aaaa@turn-1',\n"
        "        'status': 'complete',\n"
        "        'result': clean,\n"
        "        'bearing': 'chronicler digest',\n"
        "        'duration': 1.23,\n"
        "        'tokens': None,\n"
        "        'actions': None,\n"
        "        'is_new_strand': True,\n"
        "    }))\n"
        "else:\n"
        "    sys.stdout.write(\n"
        "        'New strand created: api_2026-05-20_00-00-00-000\\n'\n"
        "        '\\n'\n"
        "        'Use cast --omlet <strand id> to talk to the latest version'\n"
        "        ' of this agent at any time in the future\\n'\n"
        "        'Use cast --omlet <full omlet ref> to resume this conversation'\n"
        "        ' from this exact point at any time in the future\\n'\n"
        "        'Prefer to use cast --omlet <strand> for normal'\n"
        "        ' conversational flow\\n'\n"
        "        '\\n'\n"
        "        f'Result: {clean}\\n'\n"
        "        '\\n'\n"
        "        'Omlet: api_2026-05-20_00-00-00-000@agent-aaaa@turn-1\\n'\n"
        "        'Strand: api_2026-05-20_00-00-00-000\\n'\n"
        "        'Status: complete\\n'\n"
        "        'Bearing: chronicler digest\\n'\n"
        "        'Duration: 1.23s\\n'\n"
        "    )\n",
        encoding="utf-8",
    )
    horizon.chmod(0o755)
    monkeypatch.setenv("PATH", f"{tmp_path}:{os.environ['PATH']}")
    sent: list[str] = []

    async def fake_email(_channel, _subject: str, body: str, _workdir: Path) -> None:
        sent.append(body)

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["email"]),
        {"email": Channel(name="email", kind="email", command="true", to="x@y")},
        tmp_path,
        {"now", "daily"},
    )

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
        internal = connection.execute(
            "SELECT type FROM findings WHERE source = 'internal/render'"
        ).fetchone()
    finally:
        connection.close()

    assert internal is None, "clean digest must not emit an internal/render finding"
    assert sent, "digest must dispatch when the chronicler returns a body"
    body = sent[0]
    clean = (
        "Digest body: nothing urgent across the watched surface. "
        "One closure noted, otherwise quiet."
    )
    assert clean in body
    for leak in (
        # Legacy text-envelope markers (inversion: drop --json from argv,
        # stub emits text, body contains the whole envelope).
        "New strand created:",
        "Use cast --omlet",
        "Result:",
        "Omlet:",
        "Strand:",
        "Status: complete",
        "Bearing:",
        "Duration:",
        # JSON-envelope markers (inversion: keep --json but skip parsing,
        # body contains the raw JSON blob with these keys).
        '"omlet"',
        '"result"',
        '"bearing"',
        '"duration"',
        '"is_new_strand"',
    ):
        assert leak not in body, f"{leak!r} leaked into digest body"


# --- email cleanup pass (2026-05-27) ---------------------------------------


def test_digest_subject_is_local_time_date_only(tmp_path, monkeypatch) -> None:
    """Subject was previously 'Angelus daily digest YYYY-MM-DD UTC'. Patrick
    asked for a screen-reader-friendly local-time form with the project's
    'Observances' vocabulary. Pins the exact format so an accidental
    revert (or a future strftime locale change) fails one targeted test."""
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
            render={
                "preamble": [
                    {"kind": "structured", "template": "rate-limit-callout"},
                    {"kind": "structured", "template": "incident-status"},
                ],
                "body": {
                    "kind": "llm",
                    "mantle": "chronicler",
                    "inputs": ["findings_since_last_drain", "open_incidents"],
                },
            },
        ),
        {"email": Channel(name="email", kind="email", command="true", to="x@y")},
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
        },
        {"daily"},
    )
    subjects: list[str] = []

    async def fake_email(_channel, subject: str, _body: str, _workdir: Path) -> None:
        subjects.append(subject)

    async def fake_llm(self, _pipe, _structured):
        return "body paragraph here.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)
    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert subjects, "digest must send to email"
    subject = subjects[0]
    assert subject.startswith("Angelus Observances for "), (
        f"new format must start with the project-vocabulary phrase; got {subject!r}"
    )
    # Date components must match local 'today' rendered with the same
    # format the code uses. Avoid asserting on TZ name (test runners
    # can be in any TZ) -- just verify weekday/month/day/year match.
    today_local = datetime.now().astimezone()
    expected = (
        f"Angelus Observances for "
        f"{today_local.strftime('%A %B')} "
        f"{today_local.day}, {today_local.year}"
    )
    assert subject == expected
    # No time, no UTC label -- regressing toward those is the historical
    # mess this test pins against.
    assert "UTC" not in subject
    # Pin "no clock time" by checking for ":" which would appear in any
    # HH:MM time. The date itself has no colons in our format.
    assert ":" not in subject


def test_structured_inputs_attach_local_timestamp_siblings(tmp_path) -> None:
    """Each finding/incident gets an `<field>_local` sibling for any
    known UTC timestamp field, so templates and the chronicler prompt
    can use human times without each renderer reimplementing
    conversion. The UTC originals must be preserved (downstream code
    and tests assume them)."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
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
            "body": "hi",
        },
        {"daily"},
    )
    try:
        structured = drain._structured_inputs(_daily_pipe(), None)
    finally:
        connection.close()

    findings = structured["findings_since_last_drain"]
    assert findings, "expected the test finding to be present"
    f = findings[0]
    assert "occurred_at" in f and isinstance(f["occurred_at"], str)
    assert "occurred_at_local" in f, (
        "missing _local sibling -- chronicler prompt expects these"
    )
    # The local string must look human-readable (weekday + date + clock).
    assert ":" in f["occurred_at_local"], (
        f"expected HH:MM in local timestamp; got {f['occurred_at_local']!r}"
    )

    incidents = structured["open_incidents"]
    assert incidents, "expected an open incident from the down finding"
    inc = incidents[0]
    assert "opened_at" in inc
    assert "opened_at_local" in inc


def test_chronicler_prompt_constrains_output_to_plain_paragraph(
    tmp_path, monkeypatch
) -> None:
    """The mess Patrick called out (markdown tables, headers, emoji in
    text/plain) came from a loose chronicler prompt. The new prompt
    explicitly forbids those constructs. Catch a future loosening of
    the rules-list by asserting the load-bearing phrases stay."""
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
    captured: dict[str, object] = {}

    # The prompt rides a temp file now, not argv; capture it from the
    # --message-file path.
    async def capture_create(*args, **kwargs):
        argv = list(args)
        captured["argv"] = argv
        idx = argv.index("--message-file")
        captured["message"] = Path(argv[idx + 1]).read_text(encoding="utf-8")

        class _Proc:
            returncode = 0

            async def communicate(self, stdin_bytes=None):
                return (
                    b'{"result": "body paragraph here, several characters wide."}',
                    b"",
                )

        return _Proc()

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

    # Pin the off-argv contract: the prompt must NOT ride argv (that path blew
    # past the OS arg-length limit and degraded the digest on 2026-06-01). It
    # travels via --message-file; the bare --message flag must be absent.
    argv = captured["argv"]
    assert "--message-file" in argv
    assert "--message" not in argv

    message = captured.get("message")
    assert message is not None, "expected the chronicler prompt to be captured"
    # Each of these must appear: they're the load-bearing constraints
    # that produced the mess on regression last cycle.
    for phrase in (
        "single short paragraph",
        "Plain text only",
        "No markdown",
        "no emoji",
        "Do not enumerate every item",
    ):
        assert phrase in message, (
            f"chronicler prompt missing constraint {phrase!r}"
        )


def test_digest_stages_chronicler_prompt_and_retains_it(tmp_path, monkeypatch) -> None:
    """The chronicler prompt is staged in state/digest-staging/ named
    <stamp>-<pipe>.txt and RETAINED after the drain (not a throwaway tmp
    file), so 'what did the digest ask for' is auditable after the fact."""
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

    async def capture_create(*args, **kwargs):
        class _Proc:
            returncode = 0

            async def communicate(self, stdin_bytes=None):
                return b'{"result": "chronicler output text here."}', b""

        return _Proc()

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        return None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(pipe_runner.asyncio, "create_subprocess_exec", capture_create)

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

    staging = tmp_path / "state" / "digest-staging"
    staged = list(staging.glob("*-daily.txt"))
    assert len(staged) == 1, f"expected one retained staged prompt, got {staged}"
    body = staged[0].read_text(encoding="utf-8")
    assert "Structured inputs (JSON):" in body
    # Fixed-width UTC stamp prefix so names sort chronologically.
    assert staged[0].name.endswith("-daily.txt")
    assert len(staged[0].name) == len("20260601T080000Z-daily.txt")


def test_prune_digest_staging_keeps_last_n(tmp_path) -> None:
    """_prune_digest_staging keeps only the most recent N staged prompts
    (lexical name order == chronological) and is best-effort."""
    staging = tmp_path / "digest-staging"
    staging.mkdir()
    names = [f"2026060{i}T080000Z-daily.txt" for i in range(1, 6)]
    for name in names:
        (staging / name).write_text("x", encoding="utf-8")
    pipe_runner._prune_digest_staging(staging, keep=2)
    remaining = sorted(p.name for p in staging.glob("*.txt"))
    assert remaining == names[-2:]
    # keep<=0 is a no-op guard (never wipes the folder).
    pipe_runner._prune_digest_staging(staging, keep=0)
    assert len(list(staging.glob("*.txt"))) == 2


def test_drain_message_body_synthesis_precedes_preamble(tmp_path, monkeypatch) -> None:
    """End-to-end ordering pin: the assembled message has the chronicler
    synthesis paragraph FIRST and the structured preamble items SECOND.
    Reverses the original (preamble, body) order; doc'd in
    _drain_digest with the 'two-voice mess' rationale."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["email"]),
        {"email": Channel(name="email", kind="email", command="true", to="x@y")},
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
            "body": "site went away",
        },
        {"daily"},
    )
    sent: list[str] = []

    async def fake_email(_channel, _subject: str, body: str, _workdir: Path) -> None:
        sent.append(body)

    async def fake_llm(self, _pipe, _structured):
        return "Synthesis: one site went down.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)
    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert sent, "digest must dispatch"
    message = sent[0]
    # Synthesis paragraph must appear before any preamble marker.
    body_marker = "Synthesis: one site went down."
    pre_marker_a = "Suppressed:"  # from rate-limit-callout test stub
    pre_marker_b = "Incidents:"  # from incident-status test stub
    assert message.index(body_marker) < message.index(pre_marker_a)
    assert message.index(body_marker) < message.index(pre_marker_b)


def test_chronicler_quiet_day_short_reply_is_not_rejected(
    tmp_path, monkeypatch
) -> None:
    """The chronicler prompt invites a one-sentence quiet-day reply.
    The prior `< 20` length threshold rejected exactly that compliant
    output and substituted the LLM_FALLBACK_FOOTER -- a false-failure.
    The new floor of 5 accepts short compliant replies like
    "All quiet." (10 chars) while still rejecting empty/<=4-char
    outputs. fell-r2 NIT on missing coverage for the threshold.

    Inverts to: revert the threshold to 20 and this test fails because
    "All quiet." is then rejected and the message contains the fallback
    footer instead."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["email"]),
        {"email": Channel(name="email", kind="email", command="true", to="x@y")},
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
        },
        {"daily"},
    )
    sent: list[str] = []

    async def fake_email(_channel, _subject: str, body: str, _workdir: Path) -> None:
        sent.append(body)

    real_create = pipe_runner.asyncio.create_subprocess_exec

    async def short_chronicler(*args, **kwargs):
        return await real_create(
            "sh",
            "-c",
            'echo \'{"result": "All quiet."}\'',
            **kwargs,
        )

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(
        pipe_runner.asyncio, "create_subprocess_exec", short_chronicler
    )

    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert sent, "digest must dispatch even with a short compliant body"
    body = sent[0]
    assert "All quiet." in body, (
        f"compliant short reply must reach the digest; got: {body!r}"
    )
    assert "LLM digest body unavailable" not in body, (
        "short compliant reply must NOT trigger the LLM fallback footer"
    )


def test_shipped_jinja_templates_render_one_bullet_per_line() -> None:
    """Regression for end-to-end review BLOCK #1 (2026-05-27): the shipped
    incident-status.j2 and rate-limit-callout.j2 had {% endif %} as the
    last tag on each body line. Combined with trim_blocks=True, that
    consumes the per-iteration newline and collapses every bullet onto
    a single line.

    Existing tests stub their own templates (see _write_templates above),
    so the shipped templates were never exercised. Tomorrow's 08:00 EDT
    digest would have rendered four open-incident bullets as one line.

    The fix uses inline `{{ ... if ... }}` expressions (which end with
    `}}`, not a block tag) so the per-iteration newline survives.
    """
    from jinja2 import Environment, FileSystemLoader, select_autoescape

    repo_root = Path(__file__).resolve().parent.parent
    env = Environment(
        loader=FileSystemLoader(repo_root / "examples" / "lodging" / "render-templates"),
        autoescape=select_autoescape(enabled_extensions=()),
        trim_blocks=True,
        lstrip_blocks=True,
    )

    incident_tmpl = env.get_template("incident-status.j2")
    out = incident_tmpl.render(
        open_incidents=[
            {
                "severity": "medium",
                "type": "down",
                "entity": "a.com",
                "opened_at_local": "Tue 2026-05-26 21:07 EDT",
            },
            {
                "severity": "medium",
                "type": "down",
                "entity": "b.com",
                "opened_at_local": "Tue 2026-05-26 21:07 EDT",
            },
            {
                "severity": "low",
                "type": "stale_pr",
                "entity": "c",
                "opened_at_local": "Wed 2026-05-27 03:00 EDT",
            },
        ],
        recent_closures=[],
        findings_since_last_drain=[],
    )
    bullet_lines = [line for line in out.splitlines() if line.startswith("- ")]
    assert len(bullet_lines) == 3, (
        "expected three bullets on separate lines; got rendered output "
        f"with {len(bullet_lines)} bullet-starting lines.\n"
        f"Rendered:\n{out!r}"
    )

    out2 = incident_tmpl.render(
        open_incidents=[],
        recent_closures=[
            {"entity": "x", "body_text": "closed-x"},
            {"entity": "y", "body_text": "closed-y"},
        ],
        findings_since_last_drain=[
            {"severity": "low", "type": "stale_pr", "entity": "z", "body_text": "f-z"},
            {"severity": "low", "type": "stale_pr", "entity": "w", "body_text": "f-w"},
        ],
    )
    bullets2 = [line for line in out2.splitlines() if line.startswith("- ")]
    assert len(bullets2) == 4, (
        f"expected 2 closures + 2 findings = 4 bullets; got {len(bullets2)}.\n"
        f"Rendered:\n{out2!r}"
    )

    rl_tmpl = env.get_template("rate-limit-callout.j2")
    out3 = rl_tmpl.render(
        suppressed_findings=[
            {"severity": "high", "type": "down", "entity": "p", "body_text": "p-down"},
            {"severity": "high", "type": "down", "entity": "q", "body_text": "q-down"},
        ]
    )
    rl_lines = [line for line in out3.splitlines() if line.startswith("- ")]
    assert len(rl_lines) == 2, (
        f"expected two suppressed-finding bullets on separate lines; "
        f"got {len(rl_lines)}.\nRendered:\n{out3!r}"
    )


def test_digest_additive_email_full_push_compact(tmp_path, monkeypatch) -> None:
    """Additive transports (2026-06-02): the email leg carries the full
    long-form digest (LLM synthesis + structured preamble); the push leg
    carries a compact summary with no LLM prose. One drain, two renders."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["email", "push"]),
        {
            "email": Channel(name="email", kind="email", command="true", to="x@y"),
            "push": Channel(name="push", kind="push", command="notify-pat"),
        },
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
            "body": "site went away",
        },
        {"daily"},
    )
    email_sent: list[str] = []
    push_sent: list[str] = []

    async def fake_email(_channel, _subject: str, body: str, _workdir: Path) -> None:
        email_sent.append(body)

    async def fake_push(_channel, message: str, _workdir: Path) -> None:
        push_sent.append(message)

    async def fake_llm(self, _pipe, _structured):
        return "Synthesis: one site went down.", None

    monkeypatch.setattr(pipe_runner, "send_email", fake_email)
    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)
    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert email_sent and push_sent
    # Email = full digest: LLM synthesis paragraph + structured preamble.
    assert "Synthesis: one site went down." in email_sent[0]
    assert "Incidents:" in email_sent[0]
    # Push = compact: heartbeat header + counts, NO LLM prose, NO raw preamble.
    compact = push_sent[0]
    assert compact.startswith("Angelus Observances for ")
    assert "new finding(s)" in compact
    assert "Synthesis: one site went down." not in compact
    assert "Suppressed:" not in compact


def test_compact_render_caps_sections_with_more_tail(tmp_path) -> None:
    """The compact push render lists at most N items per section and prints a
    '+K more' tail so a busy day cannot blow past telegram's message cap."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["push"]),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    cap = pipe_runner.DEFAULT_COMPACT_MAX_ITEMS_PER_SECTION
    structured = {
        "open_incidents": [
            {"severity": "high", "type": "down", "entity": f"e{i}"}
            for i in range(cap + 3)
        ],
        "findings_since_last_drain": [],
        "recent_closures": [],
        "suppressed_findings": [],
    }
    out = drain._render_compact("Angelus Observances for Day", structured)

    assert out.startswith("Angelus Observances for Day")
    assert f"{cap + 3} open incident(s)" in out
    listed = [line for line in out.splitlines() if line.startswith("high down: ")]
    assert len(listed) == cap
    assert "+3 more" in out


def test_digest_heartbeat_pings_when_url_set(tmp_path, monkeypatch) -> None:
    """A successful digest drain pings the dead-man URL exactly once."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["push"]),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    monkeypatch.setenv("ANGELUS_DIGEST_HEARTBEAT_URL", "http://hc.example/ping-uuid")
    pinged: list[str] = []

    def fake_get(url: str, _timeout: float) -> None:
        pinged.append(url)

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        pass

    monkeypatch.setattr(pipe_runner, "_get_url", fake_get)
    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()

    assert pinged == ["http://hc.example/ping-uuid"]


def test_digest_heartbeat_inert_when_url_unset(tmp_path, monkeypatch) -> None:
    """With no heartbeat URL the ping is skipped entirely (feature inert)."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["push"]),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    monkeypatch.delenv("ANGELUS_DIGEST_HEARTBEAT_URL", raising=False)

    def fail_get(_url: str, _timeout: float) -> None:
        raise AssertionError("_get_url must not be called when URL is unset")

    async def fake_push(_channel, _message: str, _workdir: Path) -> None:
        pass

    monkeypatch.setattr(pipe_runner, "_get_url", fail_get)
    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    try:
        asyncio.run(drain.drain_once())
    finally:
        connection.close()


def test_digest_heartbeat_failure_does_not_break_delivery(tmp_path, monkeypatch) -> None:
    """A failing dead-man ping must not turn a delivered digest into an error:
    the ping runs last, after the drain is recorded."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    _write_templates(tmp_path)
    drain = PipeDrain(
        catalog,
        _daily_pipe(channels=["push"]),
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    monkeypatch.setenv("ANGELUS_DIGEST_HEARTBEAT_URL", "http://hc.example/ping-uuid")
    push_sent: list[str] = []

    def boom_get(_url: str, _timeout: float) -> None:
        raise RuntimeError("healthcheck endpoint down")

    async def fake_push(_channel, message: str, _workdir: Path) -> None:
        push_sent.append(message)

    monkeypatch.setattr(pipe_runner, "_get_url", boom_get)
    monkeypatch.setattr(pipe_runner, "send_push", fake_push)
    try:
        asyncio.run(drain.drain_once())  # must not raise
        drained = connection.execute(
            "SELECT last_drain_at FROM pipe_state WHERE pipe_name = 'daily'"
        ).fetchone()
    finally:
        connection.close()

    assert push_sent, "digest must still be delivered despite a failed heartbeat ping"
    assert drained is not None and drained["last_drain_at"]


def test_get_url_rejects_non_http_scheme() -> None:
    """The dead-man ping URL is env-sourced; _get_url must reject non-http(s)
    schemes (e.g. file://) before opening anything."""
    with pytest.raises(RuntimeError, match="must be http"):
        pipe_runner._get_url("file:///etc/passwd", 1.0)
