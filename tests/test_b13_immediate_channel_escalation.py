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
from pathlib import Path

import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon
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


def _drain_three(tmp_path) -> tuple[Catalog, PipeDrain]:
    """A now-pipe drain over THREE channels for the skip+fail+succeed mix.

    Only push and email kinds exist (_send_channel supports no others), so the
    skipped channel shares the push kind with the live one: push_backup is
    pre-marked unhealthy and therefore never invokes its sender, so routing two
    push-kind channels through the one send_push double is unambiguous -- only
    the live `push` ever calls it.
    """
    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)
    channels = {
        "push": Channel(name="push", kind="push", command="notify-pat"),
        "email": Channel(
            name="email", kind="email", command="patbot-email", to="x@example.com"
        ),
        "push_backup": Channel(
            name="push_backup", kind="push", command="notify-pat"
        ),
    }
    drain = PipeDrain(catalog, NOW_PIPE, channels, tmp_path, {"now"})
    return catalog, drain


def _write_lodging(root: Path) -> None:
    """On-disk lodging for the cross-restart test, which needs a real
    AngelusDaemon (its startup clear methods + orphan reconcile). Mirrors the
    `now`(push) + `daily`(email) shape other daemon tests use; the immediate
    drain under test is `now`, fanning internal findings to push AND email."""
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [email]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )
    (root / "channels" / "email.yaml").write_text(
        "kind: email\ncommand: 'true'\nto: person@example.com\n",
        encoding="utf-8",
    )


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


# --------------------------------------------------------------------------
# (d') The atomic-mark crash window. When the first fanned channel delivers,
#     record_dispatch(mark_queue=True) marks the finding's pipe_queues row
#     'dispatched' in the SAME transaction as the 'sent' dispatch insert --
#     BEFORE the next fanned channel is attempted. That ordering is the only
#     thing closing a SIGKILL window: a later channel's send (e.g. SMTP) can
#     take seconds, and a crash in that gap must not leave a committed 'sent'
#     row beside a still-'pending' queue row, or the finding re-drains and
#     RE-DELIVERS to the first channel on restart (duplicate page).
#
#     This is the ONE property no other test pins. The post-loop
#     mark_pipe_items_dispatched backstop re-asserts the terminal state at the
#     end of every in-process happy-path drain, so reverting the success branch
#     to mark_queue=False leaves the *final* row status 'dispatched' and every
#     other test still green -- the regression is invisible to any assertion
#     that only inspects state AFTER the loop. The only way to see it is to
#     observe the row's status mid-loop, at the instant the SECOND fanned
#     channel is attempted: with mark_queue=True it is already 'dispatched';
#     with mark_queue=False it is still 'pending' (the masked bug shape).
# --------------------------------------------------------------------------


def test_first_channel_success_marks_dispatched_before_next_channel_attempted(
    tmp_path, monkeypatch
) -> None:
    """push (fanned first) delivers; email (fanned second) probes the finding's
    pipe_queues status the instant it is attempted and CAPTURES it. The captured
    value must be 'dispatched' -- which can only hold if push's success marked
    the row atomically inside the channel loop (mark_queue=True on the success-
    branch record_dispatch) rather than deferring to the post-loop backstop.

    The probe records the status rather than asserting it: an AssertionError
    raised inside the sender would be swallowed by the channel loop's broad
    `except Exception` (it would be miscounted as an email transport failure),
    so the discriminating check must run in the OUTER scope after the drain.

    Discrimination: revert that record_dispatch to mark_queue=False and the
    probe captures 'pending' (the backstop has not run yet), so `seen` reads
    ['pending'] and the outer assertion fails. No other test catches that
    revert, because they all inspect the row after the loop, where the backstop
    has already re-asserted 'dispatched' and masked it.
    """
    catalog, drain = _drain(tmp_path)
    seen: list[str | None] = []

    push = _Recorder()  # `now`'s own channel, fanned first -> delivers
    finding_id: int | None = None

    async def email_probe(channel, *_args, **_kwargs):
        # push already delivered this drain; capture the queue row's status at
        # the instant this second fanned channel is attempted. Returning (no
        # raise) means email itself "delivers" -- this probe is non-failing so
        # nothing is swallowed and the captured value reaches the outer assert.
        seen.append(_queue_status(catalog, finding_id))

    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email_probe)

    finding_id = _write_internal(catalog, "speakbot")
    asyncio.run(drain.drain_once())

    # email WAS attempted exactly once (so the probe ran), and at that instant
    # push's success had ALREADY marked the queue row 'dispatched' atomically.
    assert push.calls == ["push"]
    assert seen == ["dispatched"]
    assert _queue_status(catalog, finding_id) == "dispatched"


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


def _open_dispatch_incidents(catalog: Catalog, entity: str) -> list[dict]:
    """Open internal/dispatch incidents for one channel (entity), oldest first."""
    return [
        incident
        for incident in catalog.open_incidents()
        if incident["source"] == "internal/dispatch" and incident["entity"] == entity
    ]


# --------------------------------------------------------------------------
# (e) Finding 2 -- a pure-skip drain never burns the finding's redelivery
#     budget. When EVERY fanned channel is unhealthy and skipped, no channel
#     is attempted (last_error stays None), so the reconciliation must leave
#     the pipe_queues row entirely untouched. This pins the single
#     `elif last_error is not None:` guard that distinguishes "skipped" from
#     "attempted-and-failed".
# --------------------------------------------------------------------------


def test_pure_skip_drain_leaves_finding_redelivery_budget_untouched(
    tmp_path, monkeypatch
) -> None:
    """Both fanned channels are unhealthy before the drain, so both are SKIPPED
    and neither sender is invoked. The finding was never attempted, so its
    per-finding redelivery ladder must not advance: the pipe_queues row stays
    exactly as enqueued (status 'pending', attempts 0, next_attempt_at NULL),
    and no per-channel counter is created.

    Discrimination: this fails if the reconciliation's
    `elif last_error is not None:` guard is weakened to an unconditional
    `else: record_pipe_finding_undelivered(...)`. Under that weakening a
    skip-only drain (delivered False, last_error None) would still advance the
    ladder -> attempts becomes 1 and next_attempt_at is set, so both assertions
    below invert. The empty-sender-calls assertion separately proves the
    channels were skipped, not attempted-and-swallowed.
    """
    catalog, drain = _drain(tmp_path)
    push = _Recorder()  # would SUCCEED if ever called -- it must not be
    email = _Recorder()
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # Both fanned channels down before the finding is ever drained.
    catalog.mark_channel_unhealthy("push", "pushd hung")
    catalog.mark_channel_unhealthy("email", "smtp refused")

    finding_id = _write_internal(catalog, "speakbot")
    asyncio.run(drain.drain_once())

    row = catalog.connection.execute(
        """
        SELECT attempts, status, next_attempt_at
        FROM pipe_queues WHERE finding_id = ? AND pipe = 'now'
        """,
        (finding_id,),
    ).fetchone()
    assert row["attempts"] == 0, "a skip-only drain must not burn the redelivery budget"
    assert row["status"] == "pending", "the finding stays pending for a later drain"
    assert row["next_attempt_at"] is None, "no backoff was scheduled -- nothing was tried"
    # No channel was attempted, so no per-channel counter exists either.
    assert _channel_attempts(catalog) == {}
    assert push.calls == [] and email.calls == []


# --------------------------------------------------------------------------
# (f) Finding 3a -- one drain exercising all three channel outcomes at once:
#     a skipped (pre-unhealthy) channel, a failing channel, and a live channel
#     that delivers. Pins that the three are accounted independently in a
#     single pass.
# --------------------------------------------------------------------------


def test_single_drain_mixes_skip_fail_and_deliver(tmp_path, monkeypatch) -> None:
    """push (live) delivers, email fails, push_backup is pre-unhealthy and
    skipped. After one drain: the finding is delivered (row 'dispatched'), the
    failing channel's counter is +1, the skipped channel's counter is absent
    (never attempted), and delivery happened on the live channel only.

    Discrimination:
    - delivery on the live channel while a co-fanned channel fails is B7's
      load-bearing property; send_push.calls == ["push"] (NOT including the
      skipped push_backup) proves the skip guard held -- a broken skip would
      attempt push_backup through the same send_push double and append it.
    - the skipped channel has no per-channel counter, so a skip is not miscounted
      as a failure.
    """
    catalog, drain = _drain_three(tmp_path)
    push = _Recorder()  # live; the pipe's own channel, attempted first
    email = _Recorder(fail=True)  # fanned-in, down
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    # push_backup is unhealthy before the drain -> skipped, not attempted.
    catalog.mark_channel_unhealthy("push_backup", "backup pushd down")

    finding_id = _write_internal(catalog, "speakbot")
    asyncio.run(drain.drain_once())

    # Delivered over the live channel -> the single pipe_queues row is terminal.
    assert _queue_status(catalog, finding_id) == "dispatched"
    assert ("push", "sent") in _dispatch_rows(catalog)
    # Live channel delivered; the skipped backup never invoked its sender.
    assert push.calls == ["push"]
    assert email.calls == ["email"]
    # The failing channel laddered by one; the skipped one has no counter at all;
    # the live channel's counter was reset by its success.
    attempts = _channel_attempts(catalog)
    assert attempts[("now", "email")] == 1
    assert ("now", "push_backup") not in attempts
    assert ("now", "push") not in attempts
    # A single failure does not flip the failing channel unhealthy either.
    assert _unhealthy_channels(catalog) == {"push_backup"}


# --------------------------------------------------------------------------
# (g) Finding 3b -- end-to-end cross-restart recovery. A channel escalated via
#     the per-channel counter (unhealthy + internal/dispatch incident) is fully
#     re-armed by the daemon's startup clear sequence: counter wiped, channel
#     healthy, incident reconciled, and a single post-restart failure starts the
#     ladder from zero rather than re-crossing immediately.
# --------------------------------------------------------------------------


def test_escalated_channel_clears_and_rearms_across_restart(
    tmp_path, monkeypatch
) -> None:
    """Drive push to MAX_RETRY_ATTEMPTS failures over the REAL daemon drain
    (each finding delivered by a live email, so this is the defect-(b) shape):
    push goes unhealthy and an internal/dispatch incident opens. Then run the
    exact startup clear sequence (clear_channel_health +
    clear_digest_channel_attempts + clear_immediate_channel_attempts +
    _reconcile_orphaned_internal_incidents) the daemon runs on boot, and assert
    the channel is fully re-armed.

    Discrimination:
    - if clear_immediate_channel_attempts were NOT part of startup, push's
      counter would survive at MAX_RETRY_ATTEMPTS and the first post-restart
      failure would re-cross the threshold immediately; the single-failure
      assertion (counter == 1, push still healthy) fails.
    - if the internal/dispatch reconcile did not run, the B30 gate would stay
      armed-open and the re-escalation would NOT write a fresh incident; the
      new-incident-id assertion fails.
    """
    _write_lodging(tmp_path)
    push = _Recorder(fail=True)
    email = _Recorder()  # live -> each finding is delivered, push counter climbs
    monkeypatch.setattr(pipe_runner, "send_push", push)
    monkeypatch.setattr(pipe_runner, "send_email", email)

    daemon = AngelusDaemon(tmp_path)
    try:
        drain = daemon.pipe_drains["now"]
        known_pipes = set(daemon.lodging.pipes)

        # Escalate push to threshold: MAX_RETRY_ATTEMPTS distinct findings, each
        # delivered by email while push fails, so push's per-channel counter
        # accumulates across findings and crosses on the last drain.
        for i in range(MAX_RETRY_ATTEMPTS):
            daemon.catalog.write_internal_finding(
                "internal/dep", "down", f"dep-{i}", f"{i} down", known_pipes
            )
            asyncio.run(drain.drain_once())

        assert daemon.catalog.is_channel_unhealthy("push")
        opened = _open_dispatch_incidents(daemon.catalog, "push")
        assert len(opened) == 1
        first_incident_id = opened[0]["id"]
        assert _channel_attempts(daemon.catalog)[("now", "push")] == MAX_RETRY_ATTEMPTS

        # The threshold-crossing drain wrote the internal/dispatch finding mid-
        # drain (after the pending-row snapshot), so it is itself still pending.
        # Flush it now -- push is unhealthy so it is SKIPPED on push (counter
        # unchanged) while email delivers it -- leaving no leftover pending
        # finding to muddy the isolated single post-restart failure below.
        asyncio.run(drain.drain_once())
        assert _channel_attempts(daemon.catalog)[("now", "push")] == MAX_RETRY_ATTEMPTS

        # --- simulate restart: the exact startup clear sequence (daemon.run) ---
        daemon.catalog.clear_channel_health()
        daemon.catalog.clear_digest_channel_attempts()
        daemon.catalog.clear_immediate_channel_attempts()
        daemon._reconcile_orphaned_internal_incidents()

        # Counter wiped, channel healthy again, incident reconciled closed.
        assert ("now", "push") not in _channel_attempts(daemon.catalog)
        assert not daemon.catalog.is_channel_unhealthy("push")
        assert _open_dispatch_incidents(daemon.catalog, "push") == []

        # A SINGLE post-restart push failure starts the ladder from zero: counter
        # == 1, push stays healthy, no fresh escalation yet.
        daemon.catalog.write_internal_finding(
            "internal/dep", "down", "after-restart", "still down", known_pipes
        )
        asyncio.run(drain.drain_once())
        assert _channel_attempts(daemon.catalog)[("now", "push")] == 1
        assert not daemon.catalog.is_channel_unhealthy("push")
        assert _open_dispatch_incidents(daemon.catalog, "push") == []

        # The lifecycle re-arms cleanly: driving the remaining failures to
        # threshold re-escalates and opens a NEW incident (gate re-armed by the
        # reconcile clearance), distinct from the pre-restart one.
        for i in range(MAX_RETRY_ATTEMPTS - 1):
            daemon.catalog.write_internal_finding(
                "internal/dep", "down", f"rearm-{i}", "down again", known_pipes
            )
            asyncio.run(drain.drain_once())

        assert daemon.catalog.is_channel_unhealthy("push")
        reopened = _open_dispatch_incidents(daemon.catalog, "push")
        assert len(reopened) == 1
        assert reopened[0]["id"] != first_incident_id
    finally:
        daemon.connection.close()
