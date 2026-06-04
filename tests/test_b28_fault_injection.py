"""B28 fault injection -- force ONE channel's dispatch to fail on demand.

A scoped, first-class seam (``angelus/faults.py`` + ``PipeDrain._send_channel``)
that arms a channel to fail so the REAL detection/failover/escalation machinery
(per-channel health escalation, B13 failover, B14 ladder, B15 dead-letter) runs
without touching real channel config. An injected fault must be indistinguishable
downstream from a genuine transport failure -- that is what makes it a faithful
exercise of the pipeline rather than a test-only double.

The acceptance (master brief): "a control op/flag forces email dispatch to fail
and detection/fixers respond, with real config untouched."

These tests pin:
  - the registry's env parsing (ANGELUS_FAULT_INJECT) and in-memory semantics;
  - the acceptance: an armed channel fails a real drain, detection records it,
    and clearing it lets the next drain deliver;
  - the env-flag construction path (no daemon) arms the drain;
  - one channel at a time: arming email leaves push untouched;
  - real config is untouched -- the fault is an overlay, not a config edit;
  - an injected fault walks the FULL B14 ladder to rung 3 identically to a real
    failure, with the real sender never invoked (the strongest test);
  - the control op arms/clears/lists and rejects an unknown channel;
  - armed faults surface in `angelus health`, and none-armed renders 'none'.
"""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import angelus.pipes.runner as pipe_runner
from angelus.clock import FakeClock
from angelus.daemon import AngelusDaemon
from angelus.faults import FAULT_INJECT_ENV, FaultRegistry
from angelus.lodging import Channel, Pipe
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db
from angelus.storage.catalog import MAX_RETRY_ATTEMPTS

PINNED = datetime(2026, 1, 15, 12, 0, 0, tzinfo=UTC)

# Clears the longest backoff so the same finding re-arms across drains -- the
# mechanism test_b14 uses to walk a finding's ladder to exhaustion.
_PAST_BACKOFF = timedelta(hours=9)


class _Recorder:
    """Channel sender double that SUCCEEDS unless told otherwise. The whole
    point of B28 is that the failure comes from the injected fault, not from
    this double -- so it records calls and (by default) returns normally; if it
    is ever called for an armed channel, the test fails because the fault should
    have short-circuited the send before reaching it."""

    def __init__(self) -> None:
        self.calls: list[str] = []

    async def __call__(self, channel, *_args, **_kwargs):
        self.calls.append(channel.name)


def _channels(*, backup: str | None = None) -> dict[str, Channel]:
    return {
        "email": Channel(
            name="email", kind="email", command="patbot-email", to="x@e", backup=backup
        ),
        "push": Channel(name="push", kind="push", command="notify-pat"),
    }


def _drain(
    tmp_path,
    *,
    channels: dict[str, Channel],
    pipe_channels: list[str],
    faults: FaultRegistry | None = None,
    max_delivery_attempts: int | None = None,
) -> tuple[Catalog, PipeDrain, FakeClock]:
    """A now-pipe on a FakeClock, with an explicit fault registry. No channel
    name is hardcoded in the runner -- routing and faults are entirely config."""
    clock = FakeClock(PINNED)
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path, clock=clock)
    pipe = Pipe(
        name="now",
        cadence="immediate",
        render_kind="dumb-alert",
        template="{severity} {type}: {entity} {body}",
        channels=pipe_channels,
        max_delivery_attempts=max_delivery_attempts,
    )
    drain = PipeDrain(
        catalog, pipe, channels, tmp_path, {"now"}, clock=clock, faults=faults
    )
    return catalog, drain, clock


def _write_finding(catalog: Catalog, entity: str) -> int:
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


def _immediate_attempts(catalog: Catalog, channel: str) -> int:
    """How many consecutive immediate failures the per-channel ladder has
    recorded for ``channel`` -- the detection signal a single failed drain
    leaves behind before the unhealthy threshold is crossed."""
    for row in catalog.immediate_channel_attempts():
        if row["channel"] == channel:
            return row["attempts"]
    return 0


def _open_incidents(catalog: Catalog) -> list[tuple[str, str, str]]:
    return [
        (row["source"], row["type"], row["entity"])
        for row in catalog.connection.execute(
            "SELECT source, type, entity FROM incidents "
            "WHERE status = 'open' ORDER BY id"
        )
    ]


def _exhausted_entities(catalog: Catalog) -> list[str]:
    return [
        row["entity"]
        for row in catalog.connection.execute(
            "SELECT entity FROM findings WHERE source = 'internal/delivery' "
            "AND type = 'delivery_exhausted' ORDER BY id"
        )
    ]


# --------------------------------------------------------------------------
# Registry: env parsing + in-memory semantics.
# --------------------------------------------------------------------------


def test_from_env_parses_comma_separated_with_whitespace(monkeypatch) -> None:
    """ANGELUS_FAULT_INJECT is split on commas with each name trimmed and empty
    entries dropped, so a sloppily-typed value arms exactly the named channels.

    Discrimination: "email, , push," has whitespace, a blank entry, and a
    trailing comma -- the result is exactly {email, push}. A parser that did not
    trim would arm " push"; one that did not drop empties would arm "".
    """
    monkeypatch.setenv(FAULT_INJECT_ENV, "email, , push,")
    assert FaultRegistry.from_env().armed() == ["email", "push"]


def test_from_env_unset_arms_nothing(monkeypatch) -> None:
    """Unset or empty env arms no faults -- the feature is inert until armed.

    Discrimination: both the missing-var and empty-string cases yield []. A
    parser that split "" on "," would produce [""] and arm a phantom channel.
    """
    monkeypatch.delenv(FAULT_INJECT_ENV, raising=False)
    assert FaultRegistry.from_env().armed() == []
    monkeypatch.setenv(FAULT_INJECT_ENV, "")
    assert FaultRegistry.from_env().armed() == []


def test_registry_arm_clear_clear_all_are_in_memory(monkeypatch) -> None:
    """arm/clear/clear_all/is_armed behave as a plain in-memory set, and clear
    of an un-armed channel is an idempotent no-op (not an error).

    Discrimination: the armed() snapshot tracks each mutation; clearing a
    never-armed name leaves the set unchanged rather than raising.
    """
    registry = FaultRegistry()
    registry.arm("email")
    registry.arm("push")
    assert registry.armed() == ["email", "push"]
    assert registry.is_armed("email")
    registry.clear("nonexistent")  # idempotent: no raise, no change
    assert registry.armed() == ["email", "push"]
    registry.clear("email")
    assert registry.armed() == ["push"]
    registry.clear_all()
    assert registry.armed() == []


def test_raise_if_armed_raises_transport_shaped_error() -> None:
    """raise_if_armed raises a RuntimeError (the same TYPE a real sender raises,
    so downstream cannot tell them apart) with a distinguishable message, and is
    a silent no-op for an un-armed channel.

    Discrimination: the armed channel raises RuntimeError carrying
    'fault-injected' and the channel name; the un-armed channel returns None.
    """
    registry = FaultRegistry(["email"])
    with pytest.raises(RuntimeError, match="fault-injected: email forced failure"):
        registry.raise_if_armed("email")
    assert registry.raise_if_armed("push") is None


# --------------------------------------------------------------------------
# Acceptance: an armed channel fails a real drain (detection responds), and
# clearing it lets the next drain deliver. Real config untouched throughout.
# --------------------------------------------------------------------------


def test_acceptance_armed_email_fails_then_clear_delivers(tmp_path, monkeypatch) -> None:
    """Arm email on the in-process seam, run a REAL drain: the email send is
    forced to fail, the real sender is never invoked, and detection records the
    failure on the per-channel immediate ladder. Clear the fault and run a fresh
    drain: the send now SUCCEEDS and the finding is dispatched.

    Discrimination:
    - while armed, send_email is never called (calls == []) yet the per-channel
      immediate-attempt counter is 1 -- the failure was injected AND detected;
    - after clearing, the same email channel delivers (send_email called, queue
      'dispatched'). An implementation that ignored the fault would deliver on
      the first drain; one that never cleared would still fail the second.
    """
    faults = FaultRegistry()
    catalog, drain, clock = _drain(
        tmp_path, channels=_channels(), pipe_channels=["email"], faults=faults
    )
    email = _Recorder()
    push = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", push)

    faults.arm("email")
    failing = _write_finding(catalog, "armed.example")
    asyncio.run(drain.drain_once())

    assert email.calls == [], "the real sender must be short-circuited by the fault"
    assert _immediate_attempts(catalog, "email") == 1, "detection recorded the failure"
    assert _queue_status(catalog, failing) == "pending", "undelivered, still retryable"

    # Clear the fault -> the very same channel now delivers a fresh finding.
    faults.clear("email")
    clock.advance(_PAST_BACKOFF)
    delivered = _write_finding(catalog, "cleared.example")
    asyncio.run(drain.drain_once())

    assert "email" in email.calls, "the cleared channel delivers over the real sender"
    assert _queue_status(catalog, delivered) == "dispatched", "the finding got out"


def test_env_flag_arms_drain_at_construction(tmp_path, monkeypatch) -> None:
    """Setting ANGELUS_FAULT_INJECT before building the drain arms the channel
    with NO daemon -- the no-daemon path B27 scenario fixtures consume. The
    drain's default registry is read from the env at construction.

    Discrimination: email (armed via env) is never sent; push (un-armed) is. The
    finding still delivers over push. If from_env were read at import time, or
    not at all, the env set here would not arm the drain and email would send.
    """
    monkeypatch.setenv(FAULT_INJECT_ENV, "email")
    monkeypatch.setattr(pipe_runner, "send_email", (email := _Recorder()))
    monkeypatch.setattr(pipe_runner, "send_push", (push := _Recorder()))
    # faults=None -> PipeDrain builds its registry from the env we just set.
    catalog, drain, _clock = _drain(
        tmp_path, channels=_channels(), pipe_channels=["email", "push"]
    )

    finding_id = _write_finding(catalog, "example.com")
    asyncio.run(drain.drain_once())

    assert email.calls == [], "env-armed email must not be sent"
    assert push.calls == ["push"], "un-armed push still delivers"
    assert _queue_status(catalog, finding_id) == "dispatched", "delivered over push"


def test_arming_one_channel_does_not_affect_the_other(tmp_path, monkeypatch) -> None:
    """One channel at a time: in a pipe routing to BOTH email and push, arming
    email fails only email; push delivers. Arming push instead (email cleared)
    inverts which one fails.

    Discrimination: with email armed, email.calls is empty while push delivers;
    after swapping the armed channel, push.calls stops growing and email
    delivers. A registry that armed globally (not per-channel) would fail both.
    """
    faults = FaultRegistry()
    catalog, drain, clock = _drain(
        tmp_path,
        channels=_channels(),
        pipe_channels=["email", "push"],
        faults=faults,
    )
    email = _Recorder()
    push = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", push)

    faults.arm("email")
    first = _write_finding(catalog, "first.example")
    asyncio.run(drain.drain_once())
    assert email.calls == [], "armed email did not send"
    assert push.calls == ["push"], "un-armed push delivered"
    assert _queue_status(catalog, first) == "dispatched", "delivered over push"

    # Swap: now push is armed and email is live.
    faults.clear("email")
    faults.arm("push")
    clock.advance(_PAST_BACKOFF)
    second = _write_finding(catalog, "second.example")
    asyncio.run(drain.drain_once())
    assert push.calls == ["push"], "push did not send again (now armed)"
    assert email.calls == ["email"], "email (now un-armed) delivered"
    assert _queue_status(catalog, second) == "dispatched", "delivered over email"


def test_real_channel_config_untouched_by_fault(tmp_path, monkeypatch) -> None:
    """Arming and draining must NOT mutate the channel config -- the fault is an
    in-memory overlay, never a config edit. The Channel object is unchanged and
    still equal to a freshly-built copy of the same config.

    Discrimination: the channel dict is captured before arming and asserted
    identical (same object, equal value) after a drain that injected a fault. An
    implementation that 'disabled' a channel by editing its config object (e.g.
    flipping a field) would diverge from the fresh reference here.
    """
    faults = FaultRegistry()
    channels = _channels(backup="push")
    before = Channel(name="email", kind="email", command="patbot-email", to="x@e", backup="push")
    catalog, drain, _clock = _drain(
        tmp_path, channels=channels, pipe_channels=["email"], faults=faults
    )
    monkeypatch.setattr(pipe_runner, "send_email", _Recorder())
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    faults.arm("email")
    _write_finding(catalog, "example.com")
    asyncio.run(drain.drain_once())

    assert drain.channels is channels, "the channels dict is the same object"
    assert drain.channels["email"] == before, "the channel config is byte-for-byte unchanged"
    assert faults.is_armed("email"), "the fault lives in the registry, not the config"


# --------------------------------------------------------------------------
# The strongest test: an injected fault walks the FULL B14 ladder to rung 3,
# identically to a real transport failure, with the real sender never invoked.
# --------------------------------------------------------------------------


def test_injected_fault_walks_b14_ladder_to_rung3(tmp_path, monkeypatch) -> None:
    """email is the now-pipe's sole channel (no backup) and is ARMED -- not
    monkeypatched to fail. Across MAX_RETRY_ATTEMPTS drains the finding walks
    rung 1 (retry/backoff), the per-channel ladder marks email unhealthy
    (internal/dispatch channel_unhealthy), and rung 3 fires a durable
    internal/delivery delivery_exhausted incident -- the exact ladder
    test_b14's acceptance pins, reached purely through the B28 seam.

    Discrimination: the real send_email recorder is NEVER called (calls == [])
    -- every failure came from the injected fault -- yet BOTH the per-channel
    and per-finding terminal signals fire identically to a genuine outage. This
    is what proves an injected fault is indistinguishable from a real transport
    failure to the rest of the pipeline. If _send_channel consulted the registry
    AFTER dispatching (or the error did not flow through the normal handler),
    either the recorder would record calls or the ladder would not reach rung 3.
    """
    faults = FaultRegistry(["email"])
    catalog, drain, clock = _drain(
        tmp_path, channels=_channels(), pipe_channels=["email"], faults=faults
    )
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    finding_id = _write_finding(catalog, "example.com")
    for i in range(MAX_RETRY_ATTEMPTS):
        if i:
            clock.advance(_PAST_BACKOFF)
        asyncio.run(drain.drain_once())

    assert email.calls == [], "the real sender was never invoked -- failure was injected"
    assert _queue_status(catalog, finding_id) == "dead_letter", "the finding exhausted"
    open_incidents = _open_incidents(catalog)
    assert ("internal/dispatch", "channel_unhealthy", "email") in open_incidents, (
        "the per-channel ladder marked email unhealthy, same as a real outage"
    )
    assert ("internal/delivery", "delivery_exhausted", str(finding_id)) in open_incidents, (
        "rung 3 fired the durable per-finding incident"
    )
    assert _exhausted_entities(catalog) == [str(finding_id)]


def test_clearing_fault_lets_channel_recover(tmp_path, monkeypatch) -> None:
    """After email is marked unhealthy by an injected fault, clearing the fault
    and replaying lets the channel recover and deliver -- the injected outage is
    fully reversible through the normal recovery path, not a permanent break.

    max_delivery_attempts=2 exhausts the per-finding ladder while leaving the
    per-channel counter below the unhealthy threshold, so the replayed finding's
    channel is eligible on the post-clear drain.

    Discrimination: the finding is 'dead_letter' with the rung-3 incident open
    while armed; after clearing + replay the next drain delivers (send_email
    called, queue 'dispatched') and the incident clears. If the fault were not
    cleared, the replay drain would re-fail and the finding would never deliver.
    """
    faults = FaultRegistry(["email"])
    catalog, drain, clock = _drain(
        tmp_path,
        channels=_channels(),
        pipe_channels=["email"],
        faults=faults,
        max_delivery_attempts=2,
    )
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_email", email)
    monkeypatch.setattr(pipe_runner, "send_push", _Recorder())

    finding_id = _write_finding(catalog, "example.com")
    for i in range(2):
        if i:
            clock.advance(_PAST_BACKOFF)
        asyncio.run(drain.drain_once())
    assert _queue_status(catalog, finding_id) == "dead_letter", "exhausted while armed"
    assert ("internal/delivery", "delivery_exhausted", str(finding_id)) in _open_incidents(
        catalog
    )
    assert email.calls == [], "the real sender never ran while armed"

    # Clear the fault and replay the dead-lettered finding -> it delivers.
    faults.clear("email")
    assert catalog.replay_finding(finding_id, {"now"})["outcome"] == "requeued"
    clock.advance(_PAST_BACKOFF)
    asyncio.run(drain.drain_once())

    assert email.calls and email.calls[-1] == "email", (
        "the recovered channel delivered over the real sender"
    )
    assert _queue_status(catalog, finding_id) == "dispatched"
    assert ("internal/delivery", "delivery_exhausted", str(finding_id)) not in _open_incidents(
        catalog
    ), "a successful redelivery clears the rung-3 incident"


# --------------------------------------------------------------------------
# Control op: arm / list / clear / clear_all over the real control layer, and
# rejection of an unknown channel.
# --------------------------------------------------------------------------


def _write_lodging(root: Path) -> None:
    """Minimal lodging with two channels (push, email) so the control op's
    configured-channel validation has real names to accept and reject."""
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n", encoding="utf-8"
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )
    (root / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: person@example.com\n", encoding="utf-8"
    )


async def _ask(sock_path: Path, payload: dict) -> dict:
    reader, writer = await asyncio.open_unix_connection(str(sock_path))
    try:
        writer.write((json.dumps(payload) + "\n").encode())
        await writer.drain()
        line = await reader.readline()
    finally:
        writer.close()
        await asyncio.gather(writer.wait_closed(), return_exceptions=True)
    return json.loads(line.decode("utf-8"))


def test_op_fault_inject_arm_list_clear_round_trip(tmp_path) -> None:
    """The fault_inject control op arms, lists, clears, and clear_all's over a
    real socket round-trip, and the armed set drives the live PipeDrain through
    the daemon's shared registry.

    Discrimination: each op returns the resulting armed set; after arming both
    channels and clearing one, the list is exactly the remaining one; clear_all
    empties it. The daemon's PipeDrain sees the same registry object, so the
    list reflects what would actually fail at drain time.
    """
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            armed = await _ask(
                daemon.socket_path,
                {"op": "fault_inject", "args": {"action": "arm", "channel": "email"}},
            )
            assert armed == {"ok": True, "result": {"armed": ["email"]}}
            await _ask(
                daemon.socket_path,
                {"op": "fault_inject", "args": {"action": "arm", "channel": "push"}},
            )
            listed = await _ask(
                daemon.socket_path, {"op": "fault_inject", "args": {"action": "list"}}
            )
            assert listed["result"] == {"armed": ["email", "push"]}
            # The live drain shares the registry the op mutated.
            assert daemon.pipe_drains["now"].faults.armed() == ["email", "push"]

            cleared = await _ask(
                daemon.socket_path,
                {"op": "fault_inject", "args": {"action": "clear", "channel": "email"}},
            )
            assert cleared["result"] == {"armed": ["push"]}
            emptied = await _ask(
                daemon.socket_path,
                {"op": "fault_inject", "args": {"action": "clear_all"}},
            )
            assert emptied["result"] == {"armed": []}
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


def test_op_fault_inject_rejects_unknown_channel(tmp_path) -> None:
    """Arming a name that is not a configured channel is a structured error, not
    a silent no-op -- a typo must surface loudly, never arm nothing quietly.

    Discrimination: arming 'sms' (not configured) returns ok=False naming the
    unknown channel; a missing action and a missing channel are likewise
    rejected. A handler that armed any string would return ok=True here, leaving
    a phantom fault that never fires.
    """
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            unknown = await _ask(
                daemon.socket_path,
                {"op": "fault_inject", "args": {"action": "arm", "channel": "sms"}},
            )
            assert unknown["ok"] is False
            assert "unknown channel: sms" in unknown["error"]
            assert daemon.faults.armed() == [], "a rejected arm must not arm anything"

            bad_action = await _ask(
                daemon.socket_path,
                {"op": "fault_inject", "args": {"action": "bogus"}},
            )
            assert bad_action["ok"] is False
            assert "action" in bad_action["error"]

            no_channel = await _ask(
                daemon.socket_path, {"op": "fault_inject", "args": {"action": "arm"}}
            )
            assert no_channel["ok"] is False
            assert "channel" in no_channel["error"]
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


# --------------------------------------------------------------------------
# Health visibility: an armed fault is surfaced in `angelus health`; none-armed
# renders 'none'.
# --------------------------------------------------------------------------


def test_health_surfaces_armed_fault(tmp_path) -> None:
    """An armed fault appears in the health surface's fault_injection section, so
    an armed fault on the live daemon is impossible to silently forget. A fresh
    daemon (nothing armed) surfaces an empty list.

    Discrimination: health returns armed == ['email'] after arming and == []
    before. A health surface that omitted the section entirely would KeyError
    here; one that read it from sqlite would never see the in-memory fault.
    """
    _write_lodging(tmp_path)

    async def driver() -> None:
        daemon = AngelusDaemon(tmp_path)
        await daemon.control.start()
        try:
            fresh = await _ask(daemon.socket_path, {"op": "health"})
            assert fresh["result"]["fault_injection"] == {"armed": []}
            await _ask(
                daemon.socket_path,
                {"op": "fault_inject", "args": {"action": "arm", "channel": "email"}},
            )
            after = await _ask(daemon.socket_path, {"op": "health"})
            assert after["result"]["fault_injection"] == {"armed": ["email"]}
        finally:
            await daemon.control.stop()
            daemon.connection.close()

    asyncio.run(driver())


def test_daemon_seeds_faults_from_env(tmp_path, monkeypatch) -> None:
    """A daemon constructed with ANGELUS_FAULT_INJECT set comes up with the
    channel already armed -- the seam a scenario harness uses to bring the
    daemon up failing -- and surfaces it in health.

    Discrimination: daemon.faults.armed() and the health surface both report
    ['email'] right after construction, with no control op sent. If the daemon
    did not seed from env, both would be empty.
    """
    monkeypatch.setenv(FAULT_INJECT_ENV, "email")
    _write_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    try:
        assert daemon.faults.armed() == ["email"]
        assert daemon.pipe_drains["now"].faults is daemon.faults, (
            "the drain shares the daemon's seeded registry"
        )
    finally:
        daemon.connection.close()


def test_render_fault_injection_screen_reader_format(capsys) -> None:
    """The CLI renders armed faults one channel per line under a plain header,
    and 'none' when empty -- screen-reader friendly, no tables/columns.

    Discrimination: the armed render emits a 'fault injection:' header followed
    by one indented channel per line; the empty render emits 'none'. A tabular
    or comma-joined renderer would put multiple channels on one line.
    """
    from angelus.cli import _render_fault_injection

    _render_fault_injection({"armed": ["email", "push"]})
    out = capsys.readouterr().out
    assert "fault injection:\n  email\n  push\n" in out

    _render_fault_injection({})
    assert "fault injection:\n  none\n" in capsys.readouterr().out
