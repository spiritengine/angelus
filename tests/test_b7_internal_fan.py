"""B7 internal-findings-fan-all.

angelus's OWN failure findings (source ``internal/*``) route with
``target_pipes=["now"]``, and `now` carries a single channel (push). That makes
the system's distress signal share fate with one transport: if push is down,
the alert that something is wrong is swallowed silently -- the 2026-05-29
incident class. B7 fans internal/* findings to the UNION of every configured
channel on the immediate path, dispatched independently, so a channel being
down can't swallow the signal as long as one OTHER transport is live.

These tests pin the load-bearing property (a failure on one fanned channel
still delivers over another in the same drain), the domain-agnostic detection
(source prefix, never a channel name), the no-double-send dedup, and that the
fan does not leak onto ordinary findings or onto clearances (which page
nothing by design).
"""

from __future__ import annotations

import asyncio

import angelus.pipes.runner as pipe_runner
from angelus.lodging import Channel, Pipe
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db

# The real now-pipe shape: immediate, dumb-alert, push-only. The fan widens the
# *dispatch* set for internal findings without touching this config -- the
# point of doing it in the routing layer rather than the 11 write call sites.
NOW_PIPE = Pipe(
    name="now",
    cadence="immediate",
    render_kind="dumb-alert",
    template="{severity} {type}: {entity} {body}",
    channels=["push"],
)


def _drain(tmp_path) -> tuple[Catalog, PipeDrain]:
    """A now-pipe drain whose channel registry holds BOTH push and email.

    `now` references only push (channels=["push"]); email exists in the
    registry but is not one of `now`'s own channels. A non-internal finding
    must therefore reach push only, while an internal finding fans to push
    AND email -- exactly the asymmetry these tests exercise.
    """
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


def _dispatch_rows(catalog: Catalog) -> list[tuple[str, str]]:
    """(channel, status) for every dispatch row, in insertion order."""
    return [
        (row["channel"], row["status"])
        for row in catalog.connection.execute(
            "SELECT channel, status FROM dispatches ORDER BY id"
        )
    ]


class _Recorder:
    """Channel sender double: records each call, optionally raises.

    send_push/send_email are both ``async def fn(channel, ...)`` -- the first
    positional arg is the Channel either way -- so one recorder shape stands in
    for both. ``fail=True`` makes the channel a forced-down transport.
    """

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)
        if self.fail:
            raise RuntimeError(f"{channel.name} transport down")


# --------------------------------------------------------------------------
# Acceptance: an internal finding with one channel forced to fail still
# delivers over another channel in the same drain.
# --------------------------------------------------------------------------


def test_internal_finding_delivers_over_push_when_email_down(
    tmp_path, monkeypatch
) -> None:
    catalog, drain = _drain(tmp_path)
    push = _Recorder()
    email = _Recorder(fail=True)
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # A real internal failure finding: the config-integrity alarm. Routed to
    # `now` by write_internal_finding's target_pipes=["now"], same as in prod.
    catalog.write_internal_finding(
        "internal/config",
        "missing_env",
        "ANGELUS_EMAIL_TO",
        "ANGELUS_EMAIL_TO is unset",
        {"now"},
    )

    asyncio.run(drain.drain_once())

    # The fan reached BOTH transports (email was attempted, not skipped because
    # `now` only lists push) ...
    assert email.calls == ["email"], "email was never attempted -- no fan-out"
    assert push.calls == ["push"]
    # ... and push delivered despite email being down: the distress signal got
    # out over a live transport.
    assert ("push", "sent") in _dispatch_rows(catalog)


def test_internal_finding_delivers_over_email_when_push_fails_first(
    tmp_path, monkeypatch
) -> None:
    """The genuine independence proof: force the FIRST-attempted channel to
    fail and assert a LATER one still delivers.

    The fan is ordered pipe-channels-first, so push (now's own channel) is
    attempted before the fanned-in email. Making push the failing channel
    means the loop takes an exception on its very first iteration; if a
    failure aborted or skipped the rest of the loop, email would never be
    tried. It is, and it delivers -- the alert does not share fate with the
    channel that happened to be tried first.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder(fail=True)
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    catalog.write_internal_finding(
        "internal/dep",
        "down",
        "speakbot",
        "dependency speakbot is down",
        {"now"},
    )

    asyncio.run(drain.drain_once())

    assert push.calls == ["push"], "push (first channel) should have been tried"
    assert email.calls == ["email"], "email never reached -- first failure aborted the loop"
    assert ("email", "sent") in _dispatch_rows(catalog)


# --------------------------------------------------------------------------
# The fan is scoped to internal/* and must not leak onto ordinary findings.
# --------------------------------------------------------------------------


def test_non_internal_finding_does_not_fan(tmp_path, monkeypatch) -> None:
    catalog, drain = _drain(tmp_path)
    push = _Recorder()
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # An ordinary product finding from a scheduled source -- NOT internal/*.
    observation_id = catalog.write_observation(
        "scheduled/a", {}, {"source": "scheduled/a"}
    )
    catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/a",
            "type": "down",
            "entity": "example.com",
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now"},
    )

    asyncio.run(drain.drain_once())

    # Reaches the pipe's own channel only; the fan does not widen routine
    # alerts onto every transport.
    assert push.calls == ["push"]
    assert email.calls == [], "non-internal finding must not fan to email"


# --------------------------------------------------------------------------
# Unit coverage for the channel-selection mechanism itself.
# --------------------------------------------------------------------------


def _row(source: str) -> dict[str, str]:
    # _dispatch_channels only reads row["source"]; a dict row stands in for a
    # sqlite3.Row here, which supports the same __getitem__ access.
    return {"source": source}


def test_dispatch_channels_internal_fans_to_union_pipe_first(tmp_path) -> None:
    _catalog, drain = _drain(tmp_path)
    selected = drain._dispatch_channels(NOW_PIPE, _row("internal/render"), drain.channels)
    # Union of the pipe's channels and the registry, pipe-channels-first so the
    # urgent transport (push) leads.
    assert selected == ["push", "email"]


def test_dispatch_channels_non_internal_is_pipe_channels(tmp_path) -> None:
    _catalog, drain = _drain(tmp_path)
    selected = drain._dispatch_channels(NOW_PIPE, _row("scheduled/x"), drain.channels)
    assert selected == ["push"]


def test_dispatch_channels_dedups_overlap(tmp_path) -> None:
    """No double-send: a channel in both the pipe's list and the wider
    registry appears exactly once in the fan set."""
    _catalog, drain = _drain(tmp_path)
    # A pipe that already lists email; the registry also has email + push.
    pipe = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{body}",
        channels=["email", "push"],
    )
    selected = drain._dispatch_channels(pipe, _row("internal/config"), drain.channels)
    assert selected == ["email", "push"], "overlap must collapse to one entry each"
    assert len(selected) == len(set(selected))


def test_dispatch_channels_detection_is_prefix_not_channel_name(tmp_path) -> None:
    """Detection keys on the ``internal/`` source prefix, never a channel
    name -- so the rule stays domain-agnostic. A source that merely contains
    'internal' mid-string is NOT treated as internal."""
    _catalog, drain = _drain(tmp_path)
    assert drain._dispatch_channels(
        NOW_PIPE, _row("scheduled/internal-audit"), drain.channels
    ) == ["push"]
    assert drain._dispatch_channels(
        NOW_PIPE, _row("internal/triage"), drain.channels
    ) == ["push", "email"]


# --------------------------------------------------------------------------
# A clearance pages nothing and must not fan.
# --------------------------------------------------------------------------


def test_internal_clearance_does_not_fan_or_enqueue(tmp_path, monkeypatch) -> None:
    """write_internal_clearance routes with target_pipes=[] by design: its job
    is closing the incident (re-arming the emission gate), not paging. It must
    never enqueue on `now`, so it never reaches the fan at all."""
    catalog, drain = _drain(tmp_path)
    push = _Recorder()
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # Open an incident so the clearance has something to close (otherwise the
    # gate drops it to a no-op and there is nothing to assert about routing).
    catalog.write_internal_finding(
        "internal/dispatch",
        "channel_unhealthy",
        "email",
        "email delivery failing",
        {"now"},
    )
    clearance_id = catalog.write_internal_clearance(
        "internal/dispatch", "email", "email delivery recovered", {"now"}
    )

    # The clearance row exists (it closed the incident) but enqueued on no pipe.
    assert clearance_id
    queued = catalog.connection.execute(
        "SELECT COUNT(*) AS n FROM pipe_queues WHERE finding_id = ?",
        (clearance_id,),
    ).fetchone()["n"]
    assert queued == 0, "clearance must not enqueue on any pipe"

    asyncio.run(drain.drain_once())

    # Only the opening finding fanned (push + email); the clearance contributed
    # no sends of its own. So email was attempted exactly once (the opener),
    # never a second time for the clearance.
    assert email.calls == ["email"]
    assert push.calls == ["push"]
