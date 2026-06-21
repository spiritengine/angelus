"""Digest-path send retry (issue-20260620-rnoh).

The digest path opens an internal/dispatch `channel_unhealthy` incident on the
FIRST failed send (unlike the immediate path, which ladders to a threshold). On
the low-traffic push channel nothing re-exercises the channel until the next
daily digest, so one transient 30s Telegram timeout latched a belfry DOWN page
for ~24h. _send_channel_with_retry re-attempts the transport a bounded number of
times before the failure handling runs, absorbing the transient case so no
incident opens.

Coverage:
  - a digest send that fails once then succeeds opens NO incident (retry absorbed
    it) and still counts dispatched.
  - a digest send that fails EVERY attempt still opens the internal/dispatch
    finding (the persistent-failure alarm is preserved).
  - fault injection is raised ONCE, not once per retry (B28 semantics intact).
"""

from __future__ import annotations

import asyncio
from pathlib import Path

import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon
from angelus.pipes import PipeDrain


def _write_lodging(root: Path) -> None:
    scheduled = root / "sources" / "scheduled"
    scheduled.mkdir(parents=True)
    (scheduled / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n", encoding="utf-8"
    )
    (root / "pipes").mkdir()
    # A digest pipe routing to a SINGLE channel (email) so one send is one attempt.
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 7 * * *'\nchannels: [email]\n"
        "render:\n  preamble: []\n  body:\n    kind: llm\n"
        "    mantle: chronicler\n    inputs:\n      - findings_since_last_drain\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: person@example.com\n", encoding="utf-8"
    )


class _FlakySender:
    """Async channel-sender double that fails its first ``fail_times`` calls with a
    transport-shaped RuntimeError, then succeeds."""

    def __init__(self, fail_times: int) -> None:
        self.fail_times = fail_times
        self.calls = 0

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls += 1
        if self.calls <= self.fail_times:
            raise RuntimeError(f"{channel.name} timed out after 30s")


def _setup(monkeypatch, fail_times: int) -> _FlakySender:
    sender = _FlakySender(fail_times)
    monkeypatch.setattr(pipe_runner, "send_email", sender)
    # No real backoff sleeps in the test.
    monkeypatch.setattr(pipe_runner, "DIGEST_SEND_BACKOFF_SECONDS", (0.0, 0.0))

    async def fake_llm(self, _pipe, _structured):
        return "synthesis paragraph.", None

    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)
    monkeypatch.setattr(pipe_runner, "_is_same_utc_day", lambda *_args: False)
    return sender


def _seed_finding(daemon: AngelusDaemon, pipe: str) -> int:
    observation_id = daemon.catalog.write_observation(
        "scheduled/watch", {}, {"source": "scheduled/watch"}
    )
    return daemon.catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/watch",
            "type": "down",
            "entity": "digest.example",
            "severity": "high",
            "target_pipes": [pipe],
        },
        set(daemon.lodging.pipes),
    )


def _internal_dispatch_count(daemon: AngelusDaemon) -> int:
    return int(
        daemon.catalog.connection.execute(
            "SELECT COUNT(*) AS n FROM findings WHERE source = 'internal/dispatch'"
        ).fetchone()["n"]
    )


def test_transient_digest_failure_is_absorbed_no_incident(tmp_path, monkeypatch) -> None:
    """One failed send then success: the retry absorbs it, so NO
    internal/dispatch channel_unhealthy finding is written (no belfry page) and
    the digest still ships."""
    _write_lodging(tmp_path)
    sender = _setup(monkeypatch, fail_times=1)  # fail once, succeed on retry

    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_finding(daemon, "daily")
        result = asyncio.run(daemon._op_drain({"pipe": "daily"}))

        assert sender.calls == 2, "the transport is retried after the first failure"
        assert result == {"pipe": "daily", "dispatched": 1, "failed": 0}
        assert _internal_dispatch_count(daemon) == 0, (
            "a transient blip must NOT open a channel_unhealthy incident"
        )
        assert not daemon.catalog.is_channel_unhealthy("email")
    finally:
        daemon.connection.close()


def test_persistent_digest_failure_still_opens_incident(tmp_path, monkeypatch) -> None:
    """All attempts fail: the retry exhausts and the failure handling runs as
    before -- the internal/dispatch alarm is preserved, not swallowed."""
    _write_lodging(tmp_path)
    sender = _setup(monkeypatch, fail_times=99)  # never succeeds

    daemon = AngelusDaemon(tmp_path)
    try:
        _seed_finding(daemon, "daily")
        result = asyncio.run(daemon._op_drain({"pipe": "daily"}))

        assert sender.calls == pipe_runner.DIGEST_SEND_ATTEMPTS, "all attempts used"
        assert result == {"pipe": "daily", "dispatched": 0, "failed": 1}
        assert _internal_dispatch_count(daemon) >= 1, (
            "a persistent failure still opens the channel_unhealthy alarm"
        )
    finally:
        daemon.connection.close()


def test_armed_fault_raised_once_not_per_retry(tmp_path, monkeypatch) -> None:
    """A B28 armed fault is raised ONCE up front and not retried: the transport is
    never reached, and exactly one failure is recorded (digest semantics intact)."""
    _write_lodging(tmp_path)
    sender = _setup(monkeypatch, fail_times=0)  # would succeed if ever called

    daemon = AngelusDaemon(tmp_path)
    try:
        daemon.faults.arm("email")
        _seed_finding(daemon, "daily")
        result = asyncio.run(daemon._op_drain({"pipe": "daily"}))

        assert sender.calls == 0, "an armed fault short-circuits before the transport"
        assert result == {"pipe": "daily", "dispatched": 0, "failed": 1}
        assert _internal_dispatch_count(daemon) >= 1
    finally:
        daemon.connection.close()
