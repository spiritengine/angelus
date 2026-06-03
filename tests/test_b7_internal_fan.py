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


# The PRODUCTION now-pipe shape from pipes/now.yaml: the same push-only pipe as
# NOW_PIPE but carrying the real rate_limit (per_channel 6/hr, per_source 4/hr,
# overflow: daily). The immediate-drain rate-limit check runs BEFORE the fan and
# shunts an over-budget finding off `now` onto the `daily` digest -- so without
# the internal bypass, an internal/* distress signal during an alert storm would
# never page immediately and never fan. Exercising that gate requires a pipe
# that actually carries rate_limit; NOW_PIPE omits it, which is precisely why the
# pre-bypass suite stayed green over this hole.
NOW_PIPE_RATE_LIMITED = Pipe(
    name="now",
    cadence="immediate",
    render_kind="dumb-alert",
    template="{severity} {type}: {entity} {body}",
    channels=["push"],
    rate_limit={"per_channel": "6/hr", "per_source": "4/hr", "overflow": "daily"},
)


def _rate_limited_drain(tmp_path) -> tuple[Catalog, PipeDrain]:
    """A now-pipe drain on the real rate-limited shape, knowing `daily` exists.

    known_pipes includes `daily` so suppression has a real overflow target to
    route to -- i.e. the pre-bypass behaviour (now -> suppressed, daily ->
    pending) is reachable, and a test can assert it does NOT happen.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    channels = {
        "push": Channel(name="push", kind="push", command="notify-pat"),
        "email": Channel(
            name="email", kind="email", command="patbot-email", to="x@example.com"
        ),
    }
    drain = PipeDrain(
        catalog, NOW_PIPE_RATE_LIMITED, channels, tmp_path, {"now", "daily"}
    )
    return catalog, drain


def _queue_rows(catalog: Catalog, finding_id: int) -> dict[str, str]:
    """{pipe: status} for one finding's pipe_queues rows."""
    return {
        row["pipe"]: row["status"]
        for row in catalog.connection.execute(
            "SELECT pipe, status FROM pipe_queues WHERE finding_id = ?",
            (finding_id,),
        )
    }


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
# The immediate rate-limit gate runs BEFORE the fan; internal findings must
# bypass it so a distress signal is never suppressed off `now` onto the daily
# digest. This is the path NOW_PIPE (no rate_limit) could never exercise.
# --------------------------------------------------------------------------


def test_internal_findings_over_budget_still_fan_not_suppressed(
    tmp_path, monkeypatch
) -> None:
    """Internal findings beyond BOTH the per_source and per_channel budgets
    still fan to every channel on the immediate path and are NOT routed to
    `daily`.

    This pins Finding 1: the rate-limit check (_over_rate_limit ->
    suppress_pipe_item_to) sits ahead of the fan and, on a hit, shunts the
    finding off `now` onto the once-a-day digest. For an internal/* finding
    that is the distress signal being silenced -- it never pages immediately
    and never fans, and the digest may itself be what is broken. The B30
    emission gate (one alert per open incident key) is the correct flood
    control for internal findings, so they must skip the rate limit entirely.

    Discrimination -- this FAILS against the pre-bypass code: the pre-load
    pushes the push channel to its 6/hr cap AND the internal/dep source to
    its 4/hr cap, so without the bypass _over_rate_limit returns True for the
    very first internal finding and every one is suppressed (now -> suppressed,
    daily -> pending) with push/email never called. With the bypass each
    finding fans to push AND email and its `now` row is marked dispatched. The
    two assertions below (fan happened; no daily routing) both invert under
    the pre-fix behaviour. Verified failing pre-fix and passing post-fix.
    """
    catalog, drain = _rate_limited_drain(tmp_path)
    push = _Recorder()
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # Drive BOTH gates over budget before the drain. record_dispatch stamps
    # dispatched_at at the clock's "now", so all six land inside the 1h window
    # the rate-limit check looks back over. Six push `sent` rows trip
    # per_channel (>= 6/hr); the same six, stamped source=internal/dep, also
    # trip per_source (>= 4/hr) for the findings below -- so the gate would
    # fire on whichever tooth is checked first.
    for idx in range(6):
        catalog.record_dispatch(
            "now", "push", [9000 + idx], "sent", source="internal/dep"
        )

    # Distinct internal findings: same source/type, different entity, so the
    # emission gate opens a separate incident for each and all three emit
    # (several deps down at once is routine, not a flood). Each routes
    # target_pipes=["now"] via write_internal_finding, exactly as in prod.
    finding_ids = [
        catalog.write_internal_finding(
            "internal/dep", "down", entity, f"dependency {entity} is down",
            {"now", "daily"},
        )
        for entity in ("speakbot", "skein", "belfry")
    ]

    asyncio.run(drain.drain_once())

    # Every internal finding fanned to BOTH transports despite both budgets
    # being blown -- one push + one email call per finding, in finding order.
    assert push.calls == ["push", "push", "push"]
    assert email.calls == ["email", "email", "email"]

    # And none was suppressed onto `daily`: each `now` row is dispatched (not
    # suppressed) and no overflow `daily` row was ever created. Pre-fix this
    # dict would read {"now": "suppressed", "daily": "pending"} for each.
    for finding_id in finding_ids:
        assert _queue_rows(catalog, finding_id) == {"now": "dispatched"}


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
