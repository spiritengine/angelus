"""B18: a misconfigured daemon must not come up silently healthy.

At startup the daemon validates that every channel a pipe routes to has its
required env config present. The requirement is derived domain-agnostically
from each channel's `$env:NAME` markers -- nothing about email or a specific
var is hardcoded. On a missing var the daemon does NOT refuse to start (the
systemd unit is Restart=on-failure/RestartSec=5, so a nonzero exit would
crash-loop); it comes up in degraded mode, logs ERROR, and opens a
high-severity internal/config incident routed to `now` (push-only, off the
broken email transport). The clearance fires on the clean edge so the B30
emission gate re-arms.
"""

from __future__ import annotations

import logging
from pathlib import Path

from angelus.daemon import AngelusDaemon
from angelus.lodging import (
    Channel,
    Pipe,
    channel_env_requirements,
    missing_channel_config,
)
from angelus.lodging.config import Lodging


# --- pure helpers: requirements derived from `$env:` markers, not hardcoded ---


def test_channel_env_requirements_reads_the_env_marker() -> None:
    email = Channel(
        name="email", kind="email", command="/bin/true", to="$env:ANGELUS_EMAIL_TO"
    )
    assert channel_env_requirements(email) == ["ANGELUS_EMAIL_TO"]


def test_channel_env_requirements_empty_for_literal_config() -> None:
    push = Channel(name="push", kind="push", command="notify-pat", to=None)
    assert channel_env_requirements(push) == []


def test_validate_and_send_share_one_env_marker(monkeypatch) -> None:
    """The validate-time requirement and the send-time resolution must key off
    the SAME `$env:` marker, or the guard could pass while the send fails (or
    vice-versa) -- the silent-healthy hole B18 closes. Pin both to ENV_REF_PREFIX.
    """
    from angelus.channels.email import _resolve_to
    from angelus.lodging import ENV_REF_PREFIX

    channel = Channel(
        name="email", kind="email", command="/bin/true", to=f"{ENV_REF_PREFIX}FOO_ADDR"
    )
    # Same marker drives the requirement...
    assert channel_env_requirements(channel) == ["FOO_ADDR"]
    # ...and the send-time resolve: unset -> raises, set -> returns the value.
    monkeypatch.delenv("FOO_ADDR", raising=False)
    import pytest

    with pytest.raises(RuntimeError):
        _resolve_to(channel.to)
    monkeypatch.setenv("FOO_ADDR", "ops@example.com")
    assert _resolve_to(channel.to) == "ops@example.com"


def _lodging(*, email_in_pipe: bool) -> Lodging:
    channels = {
        "push": Channel(name="push", kind="push", command="notify-pat"),
        "email": Channel(
            name="email",
            kind="email",
            command="/bin/true",
            to="$env:ANGELUS_EMAIL_TO",
        ),
    }
    pipe_channels = ["push", "email"] if email_in_pipe else ["push"]
    pipes = {
        "now": Pipe(
            name="now",
            cadence="immediate",
            render_kind="dumb-alert",
            template="{type}:{entity}:{body}",
            channels=pipe_channels,
        ),
    }
    return Lodging(
        sources={}, triagers={}, pipes=pipes, channels=channels, dependencies={}
    )


def test_missing_channel_config_flags_unset_referenced_var(monkeypatch) -> None:
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)
    missing = missing_channel_config(_lodging(email_in_pipe=True))
    assert missing == {"email": ["ANGELUS_EMAIL_TO"]}


def test_missing_channel_config_clean_when_var_set(monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_EMAIL_TO", "ops@example.com")
    assert missing_channel_config(_lodging(email_in_pipe=True)) == {}


def test_missing_channel_config_ignores_unreferenced_channel(monkeypatch) -> None:
    # email exists as a channel file but no pipe routes to it -> not a startup
    # failure, because it cannot dispatch.
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)
    assert missing_channel_config(_lodging(email_in_pipe=False)) == {}


# --- daemon integration: degraded-mode finding, never silent-healthy ---------


def _write_lodging_with_email_pipe(root: Path) -> None:
    (root / "pipes").mkdir(parents=True)
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    # An email-routed pipe makes the email channel "referenced", so its missing
    # config is a startup failure. dumb-alert keeps the fixture small.
    (root / "pipes" / "alert.yaml").write_text(
        "cadence: immediate\nchannels: [email]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )
    (root / "channels" / "email.yaml").write_text(
        "kind: email\nto: $env:ANGELUS_EMAIL_TO\ncommand: /bin/true\n",
        encoding="utf-8",
    )


def _config_incidents(daemon: AngelusDaemon) -> list[dict]:
    return [
        i for i in daemon.catalog.open_incidents() if i["source"] == "internal/config"
    ]


def test_missing_email_env_opens_degraded_incident_and_logs_error(
    tmp_path, monkeypatch, caplog
) -> None:
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)
    _write_lodging_with_email_pipe(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        with caplog.at_level(logging.ERROR, logger="angelus.daemon"):
            daemon._validate_channel_config()

        # Loud: a high-severity internal/config incident is open for the email
        # channel -- the daemon is not silently healthy.
        incidents = _config_incidents(daemon)
        assert len(incidents) == 1
        assert incidents[0]["entity"] == "email"

        # The alarm rides `now` (push), never the broken email transport.
        finding = daemon.catalog.connection.execute(
            "SELECT severity, type FROM findings WHERE source = 'internal/config'"
        ).fetchone()
        assert finding["severity"] == "high"
        assert finding["type"] == "channel_config_missing"
        queued = daemon.catalog.connection.execute(
            "SELECT DISTINCT pipe FROM pipe_queues pq "
            "JOIN findings f ON f.id = pq.finding_id "
            "WHERE f.source = 'internal/config'"
        ).fetchall()
        assert [row["pipe"] for row in queued] == ["now"]

        # And an ERROR line names the failure.
        assert any(
            "missing required config" in r.getMessage() and "email" in r.getMessage()
            for r in caplog.records
            if r.levelno >= logging.ERROR
        )
    finally:
        daemon.connection.close()


def test_present_email_env_starts_clean(tmp_path, monkeypatch) -> None:
    monkeypatch.setenv("ANGELUS_EMAIL_TO", "ops@example.com")
    _write_lodging_with_email_pipe(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        daemon._validate_channel_config()
        assert _config_incidents(daemon) == []
    finally:
        daemon.connection.close()


def test_recovery_on_next_startup_clears_the_incident(tmp_path, monkeypatch) -> None:
    _write_lodging_with_email_pipe(tmp_path)

    # First start: config missing -> incident opens.
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)
    daemon = AngelusDaemon(tmp_path)
    try:
        daemon._validate_channel_config()
        assert len(_config_incidents(daemon)) == 1
    finally:
        daemon.connection.close()

    # Second start with the var now set: the clearance closes the incident and
    # the B30 emission gate re-arms.
    monkeypatch.setenv("ANGELUS_EMAIL_TO", "ops@example.com")
    daemon2 = AngelusDaemon(tmp_path)
    try:
        daemon2._validate_channel_config()
        assert _config_incidents(daemon2) == []
        clearances = [
            c
            for c in daemon2.catalog.clearance_findings_since(None)
            if c["source"] == "internal/config"
        ]
        assert len(clearances) == 1
        assert clearances[0]["entity"] == "email"
    finally:
        daemon2.connection.close()
