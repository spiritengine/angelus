"""S6 missing_env_degraded_startup -- B18's contract, brief-20260612-p3ct.

A daemon that comes up misconfigured must come up LOUD, and the distress signal
must not ride the very transport that is broken. B18 is the founding
2026-05-29 lesson generalized: a channel whose required env config is absent
(the ``$env:NAME`` marker form) does not crash-loop the daemon (systemd would
restart it every 5s and never reach a live transport) and does not start
silently healthy -- it starts in DEGRADED mode, logs ERROR, and opens a
high-severity internal/config incident routed to `now`. B7 then fans that
internal finding to EVERY configured channel, so the alarm reaches Patrick over
push even though the email transport it is reporting on is the broken one
(don't-share-fate).

This scenario replays that end to end on one root. email's recipient is
``$env:ANGELUS_EMAIL_TO_S6`` and the harness is constructed with that var UNSET
(the B18 startup validation runs in the harness's _startup_register, so
"construct with the env unset" IS the degraded startup -- no harness method to
call by hand). The `now` pipe is routed to (email, push) so email is a DECLARED
channel of the alarm's pipe, not merely an incidental B7 fan target -- "fans to
push AND email" is then literally true on the dispatch ledger.

The don't-share-fate property is made observable on that ledger rather than
incidental. email's leg genuinely FAILS -- for two coincident reasons: its
recipient is the unset $env marker (so the send cannot resolve a destination)
AND it is armed at the fault seam (the rule-2 way to express a down transport).
The LOAD-BEARING element is not the email fault, though: removing the fault does
NOT change the outcome, because the unset-recipient already fails email's send.
What carries the property is PUSH being an independent LIVE transport. So the
scenario asserts on the ledger that email FAILED while push SENT, and that the
alarm content (channel_config_missing) reached dispatches() -- delivered over
push, not over the broken email it reports on. It also asserts a degraded
internal/config incident is open, an ERROR line names the failure, and belfry
failure_surface is red. Then it sets the var, clears the fault, and rebuilds the
harness on the same root (restart semantics): the startup validation finds the
config present, fires the clearance that closes the incident, and belfry goes
green.

Two mechanisms, two mutation-verifications:

  - The DEGRADED-STARTUP EMISSION (B18) is pinned by the incident assertion.
    Reverting it -- the ``write_internal_finding("internal/config", ...)`` branch
    in ``_validate_channel_config`` made into a bare ``pass`` so a missing-env
    channel comes up with no incident -- turns this scenario red: no
    internal/config incident opens, the alarm never dispatches, and belfry stays
    green over a misconfigured daemon (the exact silent-startup failure B18
    exists to prevent). Confirmed red with that edit; restored after. (The ERROR
    log line is emitted by a separate ``LOGGER.error`` just above the suppressed
    write, so it still fires under this mutation -- the log-line assertion is not
    what the mutation kills; the incident/dispatch/belfry trio is.)

  - The DON'T-SHARE-FATE delivery is pinned by the ledger assertion, and the
    load-bearing mutation is breaking the LIVE transport (NOT removing the inert
    email fault). Arming push at the fault seam as well -- ANGELUS_FAULT_INJECT
    = "email,push" -- leaves the alarm with no live transport: both legs fail,
    dispatched drops to 0, no ('push','sent') row, and the alarm never reaches
    dispatches(). Confirmed red with that edit; restored after.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from datetime import UTC, datetime

import pytest

from angelus.sim import SimHarness

_ENV_VAR = "ANGELUS_EMAIL_TO_S6"


def _config_incidents(sim) -> list[dict]:
    """Open internal/config channel_config_missing incidents -- the degraded
    startup alarm."""
    return [
        incident
        for incident in sim.open_incidents()
        if incident["source"] == "internal/config"
    ]


def _dispatch_rows(db_path) -> list[tuple[str, str]]:
    """(channel, status) for every dispatches row, oldest first -- the
    channel-level receipt the dry-run log cannot disambiguate (a failed send
    writes a ledger row but no dry-run log line)."""
    connection = sqlite3.connect(db_path)
    try:
        return [
            (row[0], row[1])
            for row in connection.execute(
                "SELECT channel, status FROM dispatches ORDER BY id"
            )
        ]
    finally:
        connection.close()


@pytest.mark.scenario
def test_missing_env_degraded_startup(
    tmp_path, monkeypatch, reference_lodging, belfry, belfry_paths, caplog
):
    # The daily pipe routes to email, whose recipient is an $env marker -- so
    # the email channel is "referenced" and its missing env is a startup fault.
    reference_lodging(
        tmp_path,
        sources=(("scheduled/watch", "1h"),),
        status_code=503,
        daily_max_interval=None,
        email_env_var=_ENV_VAR,
        # Route `now` to email AND push so email is a DECLARED channel of the
        # alarm's pipe, attempted before push -- "B7 fans to push AND email" is
        # then literally true on the ledger, and the alarm surviving over push
        # while the email leg fails is observable rather than incidental.
        now_channels=("email", "push"),
    )
    # The var is UNSET -> degraded startup. email is ALSO faulted so the broken
    # transport genuinely fails and the fan's delivery over push is observable.
    monkeypatch.delenv(_ENV_VAR, raising=False)
    monkeypatch.setenv("ANGELUS_FAULT_INJECT", "email")

    paths = belfry_paths(tmp_path)
    start = datetime.now(UTC)

    async def scenario() -> None:
        # -- DEGRADED STARTUP -------------------------------------------------
        # Constructing the harness runs the B18 validation (_startup_register);
        # capture its ERROR line.
        with caplog.at_level(logging.ERROR, logger="angelus.daemon"):
            with SimHarness(tmp_path, start) as sim:
                # Loud: a high-severity internal/config incident is open for email.
                config = _config_incidents(sim)
                assert len(config) == 1, sim.open_incidents()
                assert config[0]["entity"] == "email", config[0]

                # The distress signal must actually GET OUT, and NOT over the
                # broken transport it reports on. Drain `now`: the internal/config
                # finding fans (B7) to email AND push. email is down (faulted at
                # the seam, and independently un-sendable -- its recipient env is
                # the unset $env marker), so its leg FAILS; push is the live
                # transport that carries the alarm.
                summary = await sim.drain("now")
                assert summary.dispatched >= 1, summary
                # Don't-share-fate, observable on the dispatch LEDGER: the broken
                # email leg failed, and push delivered the alarm. If the fan let
                # the distress signal ride email's fate, push would not show a
                # 'sent' row and the alarm would never reach the operator.
                rows = _dispatch_rows(paths.db_path)
                assert ("email", "failed") in rows, rows
                assert ("push", "sent") in rows, rows
                # And the alarm CONTENT got out (the dry-run log carries only
                # successful sends -- email's failed leg wrote no line, so this
                # line is push's).
                assert any(
                    "channel_config_missing" in line for line in sim.dispatches()
                ), sim.dispatches()

                # belfry sees the daemon's own self-reported config failure: red.
                # Called AFTER the drain so its watermark advances past the
                # degraded-startup failed email dispatch (edge-trigger), leaving
                # only the incident to drive the next tick -- which the recovery
                # below closes, so the post-fix tick reads green.
                surface = belfry.failure_surface(paths.db_path, paths.failcheck)
                assert surface is not None and "internal/config" in surface, surface

        # An ERROR line names the failing channel.
        assert any(
            "missing required config" in record.getMessage()
            and "email" in record.getMessage()
            for record in caplog.records
            if record.levelno >= logging.ERROR
        ), [r.getMessage() for r in caplog.records]

        # -- RECOVERY: fix the config and restart -----------------------------
        # Set the var, clear the fault, rebuild on the same root. The B18
        # validation now finds the config present and fires the clearance that
        # closes the incident; the gate re-arms.
        monkeypatch.setenv(_ENV_VAR, "ops@example.com")
        monkeypatch.delenv("ANGELUS_FAULT_INJECT", raising=False)
        with SimHarness(tmp_path, start) as sim:
            assert _config_incidents(sim) == [], (
                f"a fixed config must clear the incident on restart: {sim.open_incidents()}"
            )
            assert belfry.failure_surface(paths.db_path, paths.failcheck) is None, (
                "belfry must go green once the config is present"
            )

    asyncio.run(scenario())
