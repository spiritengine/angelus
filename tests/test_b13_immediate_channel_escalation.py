"""B7 fell-r1 Finding 3 -- per-channel escalation on the immediate path.

B7 fans internal/* findings to every configured channel on _drain_immediate.
The retry/health counter it inherited was the single pipe_queues.attempts row
keyed (finding_id, pipe). Post-fan, N channels drove that one row, which broke
per-channel escalation two ways:

  (a) two failing channels inflated attempts +2 per drain, so an arbitrary
      channel hit MAX_RETRY_ATTEMPTS too fast;
  (b) when one channel SUCCEEDED, record_dispatch marked the pipe_queues row
      terminal ('dispatched'), so a co-fanned channel's failures never reached
      threshold and its internal/dispatch channel_unhealthy escalation never
      fired.

The fix splits the two concerns: per-finding redelivery stays on pipe_queues
(advanced once per drain, only when ZERO channels delivered the finding), while
per-channel health escalation moves to a dedicated per-(pipe, channel) counter
(immediate_channel_attempts, migration 0010) that accumulates a channel's
failures ACROSS findings and resets on a success -- exactly the digest path's
shape (digest_channel_attempts).

These tests pin: each channel ladders independently and marks ITSELF (not an
arbitrary peer); the +N shared-counter inflation is gone (one channel needs
exactly MAX_RETRY_ATTEMPTS failures); a co-fanned success no longer starves a
failing channel's escalation (defect b); a success resets the channel's counter;
and the load-bearing property survives -- a live channel still delivers while a
co-fanned channel fails.
"""

from __future__ import annotations

import asyncio

import angelus.pipes.runner as pipe_runner
from angelus.lodging import Channel, Pipe
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS

# The now-pipe shape: immediate, dumb-alert, push-only. The fan widens the
# *dispatch* set for internal findings to the channel registry without touching
# this config, so an internal finding reaches push AND email below.
NOW_PIPE = Pipe(
    name="now",
    cadence="immediate",
    render_kind="dumb-alert",
    template="{severity} {type}: {entity} {body}",
    channels=["push"],
)


def _drain(tmp_path) -> tuple[Catalog, PipeDrain]:
    """A now-pipe drain whose channel registry holds BOTH push and email."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    channels = {
        "push": Channel(name="push", kind="push", command="notify-pat"),
        "email": Channel(
            name="email", kind="email", command="patbot-email", to="x@example.com"
        ),
    }
    drain = PipeDrain(catalog, NOW_PIPE, channels, tmp_path, {"now"})
    return catalog, drain


class _Recorder:
    """Channel sender double. ``fail`` is mutable so a single recorder can flip
    between healthy and down across drains (used by the reset test)."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)
        if self.fail:
            raise RuntimeError(f"{channel.name} transport down")


def _write_internal(catalog: Catalog, entity: str) -> int:
    """A distinct internal/dispatch finding routed to `now` (fans to all)."""
    return catalog.write_internal_finding(
        "internal/dep", "down", entity, f"dependency {entity} is down", {"now"}
    )


def _channel_attempts(catalog: Catalog) -> dict[tuple[str, str], int]:
    """{(pipe, channel): attempts} from the immediate per-channel counter."""
    return {
        (row["pipe"], row["channel"]): row["attempts"]
        for row in catalog.connection.execute(
            "SELECT pipe, channel, attempts FROM immediate_channel_attempts"
        )
    }


def _unhealthy_channels(catalog: Catalog) -> set[str]:
    return {
        row["channel"]
        for row in catalog.connection.execute(
            "SELECT channel FROM channel_health WHERE status = 'unhealthy'"
        )
    }


def _escalation_findings(catalog: Catalog) -> list[str]:
    """Entity (channel name) of every internal/dispatch channel_unhealthy
    finding, in write order -- the escalation alert per channel."""
    return [
        row["entity"]
        for row in catalog.connection.execute(
            """
            SELECT entity FROM findings
            WHERE source = 'internal/dispatch' AND type = 'channel_unhealthy'
            ORDER BY id
            """
        )
    ]


# --------------------------------------------------------------------------
# (a) Two co-fanned channels each ladder to escalation independently. One
#     channel reaching threshold marks ITSELF, with correct attribution -- and
#     the +N shared-counter inflation is gone (exactly MAX_RETRY_ATTEMPTS
#     failures per channel, not fewer).
# --------------------------------------------------------------------------


def test_both_failing_channels_escalate_independently_at_threshold(
    tmp_path, monkeypatch
) -> None:
    """Both push and email fail on every finding. Each channel's per-(pipe,
    channel) counter climbs one-per-finding, independently, and crosses
    MAX_RETRY_ATTEMPTS at the SAME finding -- marking BOTH channels unhealthy
    with one escalation finding each, correctly attributed.

    Discrimination:
    - After MAX_RETRY_ATTEMPTS - 1 findings, NEITHER channel is unhealthy. Under
      the old shared (finding_id, pipe) counter, two failures per finding would
      have inflated the ladder +2 per finding and crossed threshold in roughly
      half as many findings, marking an arbitrary channel unhealthy early. This
      assertion fails under that inflation.
    - Each channel's counter equals the finding count exactly (one per finding),
      proving the per-channel grain.
    """
    catalog, drain = _drain(tmp_path)
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder(fail=True))
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder(fail=True))

    # One fewer finding than the threshold: each distinct finding fans to push
    # AND email, both fail, so each channel takes one failure per finding.
    for i in range(MAX_RETRY_ATTEMPTS - 1):
        _write_internal(catalog, f"dep-{i}")
        asyncio.run(drain.drain_once())

    # Neither channel has crossed yet, and each counter is exactly the failure
    # count -- no inflation, no early/arbitrary unhealthy flip.
    assert _unhealthy_channels(catalog) == set()
    assert _channel_attempts(catalog) == {
        ("now", "push"): MAX_RETRY_ATTEMPTS - 1,
        ("now", "email"): MAX_RETRY_ATTEMPTS - 1,
    }
    assert _escalation_findings(catalog) == []

    # The threshold-crossing finding: both channels hit MAX_RETRY_ATTEMPTS on
    # the same drain and each marks ITSELF unhealthy with its own escalation.
    _write_internal(catalog, "dep-final")
    asyncio.run(drain.drain_once())

    assert _unhealthy_channels(catalog) == {"push", "email"}
    # One escalation finding per channel, each naming the channel that crossed
    # (correct attribution -- not an arbitrary peer).
    assert sorted(_escalation_findings(catalog)) == ["email", "push"]


# --------------------------------------------------------------------------
# (b) The headline defect: a co-fanned SUCCESS no longer starves a failing
#     channel's escalation. The failing channel still ladders to threshold off
#     its own counter, across findings each delivered by the live channel.
# --------------------------------------------------------------------------


def test_failing_channel_escalates_despite_cofanned_success(
    tmp_path, monkeypatch
) -> None:
    """push fails every finding; email succeeds every finding. Each finding is
    DELIVERED over email (marked dispatched) -- yet push's per-channel counter
    accumulates across findings and push escalates at MAX_RETRY_ATTEMPTS.

    This is defect (b) directly. Under the old code email's success marked the
    finding's pipe_queues row terminal on the first drain, so push's failure
    counter (shared on that row) never advanced past 1 across findings and push
    NEVER escalated. Both assertions below invert under that behaviour:
    - push IS unhealthy after MAX_RETRY_ATTEMPTS findings (old: never);
    - email is NOT unhealthy (it delivered every time).
    It also discriminates against a (pipe, channel, finding_id) counter grain:
    each finding is delivered once and never re-attempted, so a per-finding key
    would reset to 1 each time and push would never reach threshold.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder(fail=True)
    email = _Recorder()  # live
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # One below threshold: push has failed MAX_RETRY_ATTEMPTS - 1 times, each
    # finding delivered by email. Not yet escalated.
    for i in range(MAX_RETRY_ATTEMPTS - 1):
        finding_id = _write_internal(catalog, f"dep-{i}")
        asyncio.run(drain.drain_once())
        # Each finding reached a live transport -> its pipe_queues row is
        # terminal (delivered), proving delivery did NOT depend on push.
        assert _queue_status(catalog, finding_id) == "dispatched"

    assert _unhealthy_channels(catalog) == set()
    assert _channel_attempts(catalog)[("now", "push")] == MAX_RETRY_ATTEMPTS - 1

    # The MAX_RETRY_ATTEMPTS-th push failure crosses the ladder. push escalates;
    # email -- which delivered every finding -- stays healthy.
    _write_internal(catalog, "dep-final")
    asyncio.run(drain.drain_once())

    assert "push" in _unhealthy_channels(catalog)
    assert "email" not in _unhealthy_channels(catalog)
    assert _escalation_findings(catalog) == ["push"]
    # Email delivered on every drain including the escalation one.
    assert email.calls == ["email"] * MAX_RETRY_ATTEMPTS


# --------------------------------------------------------------------------
# (c) A channel success resets its escalation counter (recovery edge).
# --------------------------------------------------------------------------


def test_channel_success_resets_its_counter(tmp_path, monkeypatch) -> None:
    """push fails (MAX_RETRY_ATTEMPTS - 1) findings, then succeeds once, then
    fails (MAX_RETRY_ATTEMPTS - 1) more. Because the success reset push's
    counter, the accumulated failures never reach threshold -- only CONSECUTIVE
    failures ladder.

    Discrimination: without the reset, 2 * (MAX_RETRY_ATTEMPTS - 1) failures is
    >= MAX_RETRY_ATTEMPTS, so push would be unhealthy. The post-reset counter is
    MAX_RETRY_ATTEMPTS - 1 (the second burst only), and push stays healthy.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder(fail=True)
    # email always succeeds so each finding is delivered and never blocks the
    # drain; this test is purely about push's counter.
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder())

    for i in range(MAX_RETRY_ATTEMPTS - 1):
        _write_internal(catalog, f"down-{i}")
        asyncio.run(drain.drain_once())
    assert _channel_attempts(catalog)[("now", "push")] == MAX_RETRY_ATTEMPTS - 1

    # push recovers for one finding -> its counter resets (row deleted).
    push.fail = False
    _write_internal(catalog, "recovered")
    asyncio.run(drain.drain_once())
    assert ("now", "push") not in _channel_attempts(catalog)

    # push fails again, one short of threshold from a clean counter.
    push.fail = True
    for i in range(MAX_RETRY_ATTEMPTS - 1):
        _write_internal(catalog, f"down-again-{i}")
        asyncio.run(drain.drain_once())

    # Counter reflects only the second burst -- the reset held -- so push is
    # still healthy despite 2*(MAX_RETRY_ATTEMPTS - 1) total failures.
    assert _channel_attempts(catalog)[("now", "push")] == MAX_RETRY_ATTEMPTS - 1
    assert "push" not in _unhealthy_channels(catalog)


# --------------------------------------------------------------------------
# (d) Load-bearing property: a live channel still delivers while a co-fanned
#     channel is failing, and the failing channel is NOT marked unhealthy on a
#     single failure (the escalation rework must not regress B7's delivery).
# --------------------------------------------------------------------------


def test_live_channel_delivers_while_cofanned_channel_fails(
    tmp_path, monkeypatch
) -> None:
    catalog, drain = _drain(tmp_path)
    push = _Recorder(fail=True)  # `now`'s own channel, attempted first
    email = _Recorder()  # fanned-in, live
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    finding_id = _write_internal(catalog, "speakbot")
    asyncio.run(drain.drain_once())

    # Both transports attempted; email delivered despite push failing first.
    assert push.calls == ["push"]
    assert email.calls == ["email"]
    assert ("email", "sent") in _dispatch_rows(catalog)
    # The finding is delivered -> pipe_queues terminal, so it does not redeliver.
    assert _queue_status(catalog, finding_id) == "dispatched"
    # A single failure does not flip push unhealthy (it needs the full ladder),
    # but it IS recorded on the per-channel counter for the next finding.
    assert "push" not in _unhealthy_channels(catalog)
    assert _channel_attempts(catalog)[("now", "push")] == 1


def test_undelivered_finding_stays_retryable_without_inflation(
    tmp_path, monkeypatch
) -> None:
    """When NO channel delivers a finding (both fail), the per-finding
    redelivery ladder advances exactly ONE step that drain -- not +1 per failed
    channel. Pins that the reconciliation is per-finding, not per-channel.

    Discrimination: two channels fail this drain. Under the old shared counter
    each failure advanced pipe_queues.attempts, so the row would read 2 after a
    single drain. The split path advances it once -> attempts == 1, and the
    finding stays retryable (status 'pending', next_attempt_at set).
    """
    catalog, drain = _drain(tmp_path)
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder(fail=True))
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder(fail=True))

    finding_id = _write_internal(catalog, "speakbot")
    asyncio.run(drain.drain_once())

    row = catalog.connection.execute(
        """
        SELECT attempts, status, next_attempt_at
        FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'
        """,
        (finding_id,),
    ).fetchone()
    assert row["attempts"] == 1, "per-finding ladder must advance once, not per channel"
    assert row["status"] == "pending", "undelivered finding stays retryable"
    assert row["next_attempt_at"] is not None
    # Yet BOTH channels recorded their own per-channel failure this same drain.
    assert _channel_attempts(catalog) == {
        ("now", "push"): 1,
        ("now", "email"): 1,
    }


def _queue_status(catalog: Catalog, finding_id: int) -> str | None:
    row = catalog.connection.execute(
        "SELECT status FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'",
        (finding_id,),
    ).fetchone()
    return None if row is None else row["status"]


def _dispatch_rows(catalog: Catalog) -> list[tuple[str, str]]:
    return [
        (row["channel"], row["status"])
        for row in catalog.connection.execute(
            "SELECT channel, status FROM dispatches ORDER BY id"
        )
    ]
