"""B13 transport-failover-fixer -- content still gets out when its channel is down.

When a NON-internal finding is routed to a channel that is degraded -- already
is_channel_unhealthy (skipped today), or it crosses its per-channel failure
threshold on this drain -- PipeDrain._drain_immediate delivers the finding over
that channel's configured `backup` so >=1 delivery happens THIS drain, and the
degraded channel is alarmed separately by the EXISTING internal/dispatch
escalation (B13 adds the failover DELIVERY only, never a second alarm).

internal/* findings are excluded: B7 already fans them to every configured
channel, so they reach a live transport without a per-channel backup and must
not be double-handled by failover.

These tests pin:
  - the acceptance: a finding whose email channel crosses threshold is delivered
    over its push backup AND the channel-degraded alarm is raised;
  - an already-unhealthy primary routes straight to the backup (single drain);
  - no double-delivery when the primary itself delivers (backup untouched);
  - a backup that is also unhealthy is not delivered to -- the finding stays
    retryable, no crash;
  - the cross-ref validation (missing backup, self-backup, cycle) fails the load;
  - internal findings are unaffected -- still fan, no failover double-handling.
"""

from __future__ import annotations

import asyncio

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.lodging import (
    Channel,
    Lodging,
    Pipe,
    load_lodging,
    parse_channel,
    validate_cross_refs,
)
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS

# An immediate dumb-alert pipe routing to email ONLY. email declares push as its
# backup, so a degraded email fails a finding over to push. This is the
# acceptance shape (email->push) expressed domain-agnostically: nothing in the
# runner or loader names email/push; the policy lives entirely in config.
ALERT_PIPE = Pipe(
    name="now",
    cadence="immediate",
    render_kind="dumb-alert",
    template="{severity} {type}: {entity} {body}",
    channels=["email"],
)


def _drain(tmp_path) -> tuple[Catalog, PipeDrain]:
    """A now-pipe drain routing to email (backup=push), with push in the registry."""
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    channels = {
        "email": Channel(
            name="email",
            kind="email",
            command="patbot-email",
            to="x@example.com",
            backup="push",
        ),
        "push": Channel(name="push", kind="push", command="notify-pat"),
    }
    drain = PipeDrain(catalog, ALERT_PIPE, channels, tmp_path, {"now"})
    return catalog, drain


class _Recorder:
    """Channel sender double. ``fail`` is mutable so one recorder can flip
    between healthy and down across drains."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)
        if self.fail:
            raise RuntimeError(f"{channel.name} transport down")


def _write_finding(catalog: Catalog, entity: str) -> int:
    """A NON-internal product finding routed to `now` (no B7 fan)."""
    observation_id = catalog.write_observation(
        "scheduled/a", {}, {"source": "scheduled/a"}
    )
    return catalog.write_finding(
        observation_id,
        {
            "source": "scheduled/a",
            "type": "down",
            "entity": entity,
            "severity": "high",
            "target_pipes": ["now"],
        },
        {"now"},
    )


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


def _sent_count(catalog: Catalog, channel: str) -> int:
    row = catalog.connection.execute(
        "SELECT COUNT(*) AS n FROM dispatches WHERE channel = ? AND status = 'sent'",
        (channel,),
    ).fetchone()
    return int(row["n"])


def _unhealthy_channels(catalog: Catalog) -> set[str]:
    return {
        row["channel"]
        for row in catalog.connection.execute(
            "SELECT channel FROM channel_health WHERE status = 'unhealthy'"
        )
    }


def _escalation_entities(catalog: Catalog) -> list[str]:
    """Entity (channel name) of every internal/dispatch channel_unhealthy
    finding -- the per-channel degraded alarm, in write order."""
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
# (a) Acceptance: force email to fail until it crosses its threshold; the
#     crossing finding is delivered over the push backup AND the
#     channel-degraded alarm (internal/dispatch) is raised for email.
# --------------------------------------------------------------------------


def test_acceptance_email_degraded_delivers_over_push_and_alarms(
    tmp_path, monkeypatch
) -> None:
    """email fails every drain. Below threshold a finding is NOT failed over --
    a single transient blip retries via the per-finding ladder, matching the
    substrate's "degraded" definition (only a channel past threshold, or already
    unhealthy, fails over). On the MAX_RETRY_ATTEMPTS-th failure email crosses
    the threshold: the internal/dispatch alarm fires AND that finding is
    delivered over email's push backup in the SAME drain.

    Discrimination:
    - push delivers exactly once -- on the threshold-crossing drain, not on the
      sub-threshold ones. A failover that triggered on every email failure
      (ignoring the degraded gate) would have push.calls longer than 1; a
      failover that never triggered would leave push.calls empty and the
      crossing finding undelivered. Only the degraded-gated failover yields one.
    - the crossing finding's queue row is 'dispatched' (delivered), while email
      is marked unhealthy and exactly one internal/dispatch alarm names email.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder()
    email = _Recorder(fail=True)
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # Sub-threshold failures: each finding fails on email, is NOT failed over
    # (email not yet degraded), and stays retryable. push is untouched.
    for i in range(MAX_RETRY_ATTEMPTS - 1):
        _write_finding(catalog, f"sub-{i}")
        asyncio.run(drain.drain_once())
    assert push.calls == [], "failover must not fire before email is degraded"
    assert _unhealthy_channels(catalog) == set()
    assert _escalation_entities(catalog) == []

    # The threshold-crossing finding: email crosses, alarms, and this finding
    # is delivered over the push backup THIS drain.
    crossing_id = _write_finding(catalog, "crossing")
    asyncio.run(drain.drain_once())

    # Delivered over push -> the crossing finding's queue row is terminal.
    assert push.calls == ["push"], "the crossing finding must fail over to push once"
    assert ("push", "sent") in _dispatch_rows(catalog)
    assert _queue_status(catalog, crossing_id) == "dispatched"
    # email is alarmed separately by the EXISTING internal/dispatch escalation
    # (not a second, B13-specific alarm) and marked unhealthy.
    assert "email" in _unhealthy_channels(catalog)
    assert _escalation_entities(catalog) == ["email"]
    # email was attempted on every drain (it is the primary, not skipped).
    assert email.calls == ["email"] * MAX_RETRY_ATTEMPTS


# --------------------------------------------------------------------------
# (b) An already-unhealthy primary routes straight to its backup -- one drain,
#     no threshold drive needed. The primary is SKIPPED (never attempted).
# --------------------------------------------------------------------------


def test_already_unhealthy_primary_routes_straight_to_backup(
    tmp_path, monkeypatch
) -> None:
    """email is unhealthy before the drain, so it is skipped; the finding is
    delivered over the push backup in a single drain.

    Discrimination: without failover, a skipped sole channel leaves the finding
    pending and never reaches push -- so push.calls == ["push"] and the
    'dispatched' status both invert. The email-never-attempted assertion
    proves the skip path (not an attempt-and-fail) drove the failover.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder()
    email = _Recorder(fail=True)  # would fail if attempted -- it must be skipped
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    catalog.mark_channel_unhealthy("email", "smtp route down")
    finding_id = _write_finding(catalog, "example.com")
    asyncio.run(drain.drain_once())

    assert email.calls == [], "an unhealthy primary is skipped, not attempted"
    assert push.calls == ["push"], "the finding fails over straight to the backup"
    assert ("push", "sent") in _dispatch_rows(catalog)
    assert _queue_status(catalog, finding_id) == "dispatched"
    # No alarm was raised on THIS drain: email was already unhealthy (its alarm
    # fired when it first crossed); failover delivers, it does not re-alarm.
    assert _escalation_entities(catalog) == []


# --------------------------------------------------------------------------
# (c) No double-delivery: when the primary itself delivers, the backup is never
#     touched. The contract is ">=1 delivery", never a duplicate page.
# --------------------------------------------------------------------------


def test_no_double_delivery_when_primary_succeeds(tmp_path, monkeypatch) -> None:
    """email (healthy) delivers the finding; push (its backup) is never invoked.

    Discrimination: a failover that ran unconditionally -- rather than only when
    the finding is still undelivered -- would also send over push, so
    push.calls == [] is the load-bearing assertion. A single push 'sent' row
    would mean the finding paged twice.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder()
    email = _Recorder()  # healthy -> delivers
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    finding_id = _write_finding(catalog, "example.com")
    asyncio.run(drain.drain_once())

    assert email.calls == ["email"], "the primary delivered"
    assert push.calls == [], "the backup must not be used when the primary delivers"
    assert _sent_count(catalog, "push") == 0
    assert _queue_status(catalog, finding_id) == "dispatched"


# --------------------------------------------------------------------------
# (d) A backup that is itself unhealthy is not delivered to. The finding can't
#     be delivered anywhere this drain, so it stays retryable -- no crash, no
#     budget burned (dead-lettering is B15, out of scope).
# --------------------------------------------------------------------------


def test_backup_also_unhealthy_finding_stays_retryable(tmp_path, monkeypatch) -> None:
    """email (primary) AND push (its backup) are both unhealthy before the drain.
    email is skipped; the failover target push is skipped too (never deliver to
    an unhealthy backup). Nothing is attempted, so the finding stays pending
    with its redelivery budget untouched.

    Discrimination:
    - push.calls == [] proves the unhealthy backup is not sent over -- a
      failover that ignored the backup's health would invoke push (it would
      even "succeed" here, masking the bug).
    - attempts == 0 / status 'pending' / next_attempt_at None proves a pure-skip
      drain does not burn the per-finding ladder (last_error stayed None), so
      the finding is retried in full on a later drain.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder()  # would "deliver" if wrongly attempted
    email = _Recorder(fail=True)
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    catalog.mark_channel_unhealthy("email", "smtp down")
    catalog.mark_channel_unhealthy("push", "pushd down")
    finding_id = _write_finding(catalog, "example.com")
    asyncio.run(drain.drain_once())  # must not raise

    assert email.calls == [] and push.calls == [], "no unhealthy channel is sent over"
    row = catalog.connection.execute(
        """
        SELECT attempts, status, next_attempt_at
        FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'
        """,
        (finding_id,),
    ).fetchone()
    assert row["attempts"] == 0, "a pure-skip drain must not burn the redelivery budget"
    assert row["status"] == "pending", "the undeliverable finding stays retryable"
    assert row["next_attempt_at"] is None, "no backoff scheduled -- nothing was tried"


# --------------------------------------------------------------------------
# (d') Following the chain: a degraded primary whose first backup is unhealthy
#      reaches the next healthy channel on the chain.
# --------------------------------------------------------------------------


def test_failover_follows_chain_past_unhealthy_backup(tmp_path, monkeypatch) -> None:
    """email -> push(unhealthy) -> push2(healthy). The finding fails over past
    the dead first backup to the live second one. Pins "follow the chain to the
    first healthy channel" rather than giving up at the first backup.

    Discrimination: a single-hop failover would stop at push (unhealthy) and
    leave the finding undelivered, so the 'dispatched' status and push2's single
    send both invert. push2 shares the push kind, but push is unhealthy and so
    is never invoked -- the recorder it shares is only ever called for push2.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    channels = {
        "email": Channel(
            name="email", kind="email", command="c", to="x@e", backup="push"
        ),
        "push": Channel(name="push", kind="push", command="c", backup="push2"),
        "push2": Channel(name="push2", kind="push", command="c"),
    }
    drain = PipeDrain(catalog, ALERT_PIPE, channels, tmp_path, {"now"})
    push = _Recorder()  # both push and push2 are push-kind -> share this sender
    email = _Recorder(fail=True)
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    catalog.mark_channel_unhealthy("email", "smtp down")  # primary degraded
    catalog.mark_channel_unhealthy("push", "first backup down")  # skip past it
    finding_id = _write_finding(catalog, "example.com")
    asyncio.run(drain.drain_once())

    # email skipped (unhealthy), push skipped (unhealthy first backup), push2
    # delivered -- so the only send_push call is for push2.
    assert email.calls == []
    assert push.calls == ["push2"], "failover walked the chain to the live channel"
    assert ("push2", "sent") in _dispatch_rows(catalog)
    assert _queue_status(catalog, finding_id) == "dispatched"


# --------------------------------------------------------------------------
# (e) internal/* findings are unaffected: they still fan to every channel (B7)
#     and are never double-handled by failover.
# --------------------------------------------------------------------------


def test_internal_findings_unaffected_by_failover(tmp_path, monkeypatch) -> None:
    """An internal/* finding with email degraded still reaches a live transport
    via the B7 fan (push), and failover does NOT additionally route email's
    backup -- push is sent exactly once, not twice.

    Internal findings fan to the UNION of all channels, so email's backup is
    always already in the fan; combined with failover's not-delivered guard and
    the attempted-set, an internal finding can never double-page. This test
    guards that property end-to-end: a regression that ran failover for internal
    findings (or counted the skipped primary as a degraded primary) would
    produce a second push send here.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder()
    email = _Recorder(fail=True)
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    catalog.mark_channel_unhealthy("email", "smtp down")  # primary of the fan
    # Internal finding fans to push AND email (B7); email is skipped, push wins.
    catalog.write_internal_finding(
        "internal/dep", "down", "speakbot", "dependency speakbot is down", {"now"}
    )
    asyncio.run(drain.drain_once())

    assert email.calls == [], "the unhealthy fanned channel is skipped"
    assert push.calls == ["push"], "delivered once over the fan, never doubled by failover"
    assert _sent_count(catalog, "push") == 1
    # No NEW degraded-channel alarm: email was already unhealthy, and the
    # internal finding's skip does not re-alarm or fail over.
    assert _escalation_entities(catalog) == []


# --------------------------------------------------------------------------
# (f) Cross-ref validation: a backup must exist, must not be the channel itself,
#     and the chain must not cycle -- each fails the load.
# --------------------------------------------------------------------------


def _lodging(channels: list[Channel]) -> Lodging:
    return Lodging(
        sources={},
        triagers={},
        pipes={},
        channels={c.name: c for c in channels},
        dependencies={},
    )


def test_backup_missing_channel_fails_validation() -> None:
    errors = validate_cross_refs(
        _lodging([Channel(name="email", kind="email", command="c", to="x", backup="ghost")])
    )
    assert any("missing channel" in e and "ghost" in e for e in errors)


def test_backup_self_reference_fails_validation() -> None:
    errors = validate_cross_refs(
        _lodging([Channel(name="email", kind="email", command="c", to="x", backup="email")])
    )
    assert any("itself" in e for e in errors)


def test_backup_cycle_fails_validation() -> None:
    a = Channel(name="a", kind="push", command="c", backup="b")
    b = Channel(name="b", kind="push", command="c", backup="a")
    errors = validate_cross_refs(_lodging([a, b]))
    assert any("cycle" in e for e in errors)


def test_valid_backup_chain_passes_validation() -> None:
    a = Channel(name="a", kind="push", command="c", backup="b")
    b = Channel(name="b", kind="push", command="c", backup="c")
    c = Channel(name="c", kind="push", command="c")
    assert validate_cross_refs(_lodging([a, b, c])) == []


def test_backup_cycle_fails_the_load(tmp_path) -> None:
    """End-to-end: a cyclic backup chain on disk makes load_lodging raise, so a
    misconfigured deploy crashes startup loudly rather than coming up with a
    failover loop waiting to spin at runtime."""
    (tmp_path / "channels").mkdir()
    (tmp_path / "channels" / "a.yaml").write_text(
        "kind: push\ncommand: 'true'\nbackup: b\n", encoding="utf-8"
    )
    (tmp_path / "channels" / "b.yaml").write_text(
        "kind: push\ncommand: 'true'\nbackup: a\n", encoding="utf-8"
    )
    with pytest.raises(ValueError, match="cycle"):
        load_lodging(tmp_path)


def test_backup_parsed_from_yaml(tmp_path) -> None:
    """parse_channel reads the optional backup field (and leaves it None when
    absent) -- the loader-side half of the failover wiring."""
    path = tmp_path / "email.yaml"
    path.write_text(
        "kind: email\nto: person@example.com\ncommand: 'true'\nbackup: push\n",
        encoding="utf-8",
    )
    assert parse_channel(path).backup == "push"

    bare = tmp_path / "push.yaml"
    bare.write_text("kind: push\ncommand: 'true'\n", encoding="utf-8")
    assert parse_channel(bare).backup is None
