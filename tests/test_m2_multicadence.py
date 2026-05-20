"""M2 slice 3: three-source / multi-cadence rig (brief-20260520-tqov §3.5,
§6 slice 3; supersedes brief-20260513-cz9x Item 1 cadences per §5b Q6).

Three lodged sources at three distinct cadences exercise the production
multi-source / multi-cadence surface. The two synthetic canaries lodged
under sources/scheduled/ ship with no-op echo check commands; the tests
below override those commands in tmp_path with controllable variants
that read a JSON status fixture so the test can drive concrete up->down
transitions deterministically. APScheduler is NOT real-cadence-driven
in the tests: source fires are invoked directly via daemon._fire_source
(the same code path APScheduler dispatches to), so a 1h / 4h production
cadence runs in milliseconds here without faking time. The test
cadences in tmp_path (1s / 2s / 3s) are merely distinct intervals; the
production lodging cadences (1h / 4h / 15m) reflect operator urgency
per Q6 -- iotaschool is closer to an archive than a going concern, so
4h-late would be fine, which is exactly the slack the 1h / 4h tier
gives the canaries.

Discrimination: open incidents are upserted under the partial unique
index idx_incidents_one_open_per_entity on (source, type, entity)
WHERE status = 'open' (migrations/0001_initial_v3_1.sql) and the
matching ON CONFLICT (source, type, entity) clause in
Catalog._upsert_incident. Both canaries here emit findings on entity
'canary-pipeline' from distinct source_refs; the test asserts TWO open
incidents land on that entity, one per source. Dropping 'source' from
the conflict target -- the "dedup per-URL" inversion the original M2
Item 1 plan motivated -- collapses both findings into a single
incident; the canary-count assertion fails. Discrimination evidence:
locally inverting the ON CONFLICT clause in catalog.py to
(type, entity) reduces the canary incident count from 2 to 1, the
discriminating assertion fires, and the test fails.
"""

from __future__ import annotations

import asyncio
import json
import shutil
from pathlib import Path

import angelus.pipes.runner as pipe_runner
from angelus.daemon import AngelusDaemon
from angelus.pipes import PipeDrain

PROJECT_ROOT = Path(__file__).resolve().parents[1]


def _write_canary_lodging(root: Path) -> Path:
    """Mirror the production lodging shape in tmp_path with controllable
    source check commands. Each source's check `cat`s a JSON fixture the
    test rewrites between fires to drive transitions; this is the
    simulated-time mechanism the brief calls for in §6 slice 3 (no
    real-sleep for the cadence interval). The production-cadence YAMLs
    (1h / 4h) are checked into sources/scheduled/ separately; the tmp
    cadences (1s/2s/3s) are merely distinct intervals so each source is
    a separate APScheduler job -- no fire actually waits on them.

    Returns the directory holding the JSON fixtures so the test can
    rewrite them between fires.
    """
    state_canary = root / "state-canary-fixtures"
    state_canary.mkdir(parents=True)

    (root / "sources" / "scheduled").mkdir(parents=True)

    iotaschool_fixture = state_canary / "iotaschool.json"
    _write_fixture(
        iotaschool_fixture,
        {"url": "https://iotaschool.com", "status_code": 200},
    )
    (root / "sources" / "scheduled" / "iotaschool-watch.yaml").write_text(
        "cadence: 1s\ncheck:\n  kind: shell\n"
        f"  command: 'cat {iotaschool_fixture}'\n",
        encoding="utf-8",
    )

    hourly_fixture = state_canary / "canary-hourly.json"
    _write_fixture(
        hourly_fixture,
        {
            "source_ref": "scheduled/canary-hourly",
            "entity": "canary-pipeline",
            "url": "file:///dev/null",
            "status_code": 200,
        },
    )
    (root / "sources" / "scheduled" / "canary-hourly.yaml").write_text(
        "cadence: 2s\ncheck:\n  kind: shell\n"
        f"  command: 'cat {hourly_fixture}'\n",
        encoding="utf-8",
    )

    fourhour_fixture = state_canary / "canary-4hourly.json"
    _write_fixture(
        fourhour_fixture,
        {
            "source_ref": "scheduled/canary-4hourly",
            "entity": "canary-pipeline",
            "url": "file:///dev/null",
            "status_code": 200,
        },
    )
    (root / "sources" / "scheduled" / "canary-4hourly.yaml").write_text(
        "cadence: 3s\ncheck:\n  kind: shell\n"
        f"  command: 'cat {fourhour_fixture}'\n",
        encoding="utf-8",
    )

    # Triagers: copy the real lodged YAMLs + handlers from the project
    # root so we exercise the production triager surface, not a test
    # double. The handler shape is what gets shipped; faking it here
    # would leave the production handler unexercised.
    (root / "triagers").mkdir()
    (root / "triagers" / "handlers").mkdir()
    for handler_name in ("canary_watch.py", "dead_link.py"):
        shutil.copy(
            PROJECT_ROOT / "triagers" / "handlers" / handler_name,
            root / "triagers" / "handlers" / handler_name,
        )
    for triager_yaml in (
        "dead-link.yaml",
        "canary-hourly-watch.yaml",
        "canary-4hourly-watch.yaml",
    ):
        shutil.copy(
            PROJECT_ROOT / "triagers" / triager_yaml,
            root / "triagers" / triager_yaml,
        )

    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n"
        "  template: '[now] {source} {type}:{entity}'\n",
        encoding="utf-8",
    )
    (root / "pipes" / "daily.yaml").write_text(
        "cadence: '0 8 * * *'\nchannels: [push]\n"
        "render:\n"
        "  preamble:\n"
        "    - kind: structured\n      template: incident-status\n"
        "  body:\n    kind: llm\n    mantle: chronicler\n"
        "    inputs:\n      - findings_since_last_drain\n      - open_incidents\n",
        encoding="utf-8",
    )

    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: 'true'\n", encoding="utf-8"
    )

    (root / "render-templates").mkdir()
    (root / "render-templates" / "incident-status.j2").write_text(
        "Incidents:\n{% for incident in open_incidents %}"
        "{{ incident.source }} {{ incident.entity }}\n{% endfor %}",
        encoding="utf-8",
    )

    return state_canary


def _write_fixture(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload), encoding="utf-8")


async def _drive_triage(daemon: AngelusDaemon) -> int:
    """Run one full triage pass synchronously. Mirrors the body of
    AngelusDaemon._triage_loop for one iteration without starting the
    background task: take the ready observations for each triager, mark
    them processing, and run the triager. Returns count of observations
    processed -- which the test asserts on so the rest of the assertions
    can trust the triage step actually ran."""
    processed = 0
    for triager in list(daemon.lodging.triagers.values()):
        rows = daemon.catalog.ready_observations_for(
            triager.name, triager.source_ref
        )
        for row in rows:
            daemon.catalog.mark_triage_processing(row["id"], triager.name)
            await daemon._run_triager(row, triager.name)
            processed += 1
    return processed


def test_three_source_multi_cadence_dedup_discriminates_on_source_ref(
    tmp_path, monkeypatch,
) -> None:
    """Three sources fire across multiple ticks; the discrimination
    target is the (source, type, entity) incident upsert key (per the
    idx_incidents_one_open_per_entity partial unique index). Both
    canaries emit findings on entity 'canary-pipeline' from distinct
    source_refs -- two open incidents must result. The original M2 Item
    1 plan motivated this with "three URLs at three different
    cadences"; the active discriminating shape is two findings sharing
    an entity but distinct source_refs, which is what would silently
    collide under a "dedup per (type, entity)" simplification.

    Discrimination (verified locally): inverting Catalog._upsert_incident's
    ON CONFLICT clause from (source, type, entity) to (type, entity)
    (and the matching partial unique index) collapses both canary
    findings into a single incident; the assertion
    `len(canary_incidents) == 2` fails with len 1.

    Also pinned: within-source dedup via the triager state machine. A
    repeat fire on canary-hourly with status_code still 503 sees
    prior_state.last_status == 503 and emits NO new finding -- no new
    incident, no new dispatch. (The catalog _upsert_incident's ON
    CONFLICT clause would update the existing incident even if the
    triager emitted a finding; the triager-state mechanism prevents the
    duplicate finding from ever being written. Both layers cooperate.)
    """
    state_canary = _write_canary_lodging(tmp_path)
    monkeypatch.setenv("ANGELUS_DRY_RUN", "1")
    daemon = AngelusDaemon(tmp_path)

    sent_push: list[str] = []
    daily_digest_sent: list[str] = []

    async def fake_push_now_drain(_channel, message, _workdir):
        sent_push.append(message)

    async def fake_llm(self, _pipe, _structured):
        # Render-only stub: we are not testing chronicler output here,
        # only that the digest pipe drains and the structured inputs
        # carry the right findings. A real chronicler would burn a
        # horizon cast per test run.
        daily_digest_sent.append("digest body rendered")
        return "stub digest body for multicadence test.", None

    monkeypatch.setattr(pipe_runner, "send_push", fake_push_now_drain)
    monkeypatch.setattr(PipeDrain, "_render_llm_body", fake_llm)

    async def driver() -> None:
        try:
            # Tick 1: all three sources fire with status 200. The triager
            # for each records last_status = 200 in triager_state and
            # emits no findings (no transition).
            await daemon._fire_source("scheduled/canary-hourly")
            await daemon._fire_source("scheduled/canary-4hourly")
            await daemon._fire_source("scheduled/iotaschool-watch")
            triaged = await _drive_triage(daemon)
            assert triaged == 3, (
                f"expected 3 observations triaged at tick 1, got {triaged}"
            )
            assert daemon.catalog.open_incidents() == [], (
                "no findings should emit on first tick (all status 200)"
            )

            # Mutate the fixtures: all three sources go down.
            _write_fixture(
                state_canary / "canary-hourly.json",
                {
                    "source_ref": "scheduled/canary-hourly",
                    "entity": "canary-pipeline",
                    "url": "file:///dev/null",
                    "status_code": 503,
                },
            )
            _write_fixture(
                state_canary / "canary-4hourly.json",
                {
                    "source_ref": "scheduled/canary-4hourly",
                    "entity": "canary-pipeline",
                    "url": "file:///dev/null",
                    "status_code": 503,
                },
            )
            _write_fixture(
                state_canary / "iotaschool.json",
                {"url": "https://iotaschool.com", "status_code": 503},
            )

            # Tick 2: all three sources fire with status 503. Each
            # triager sees its prior_state.last_status == 200 and emits
            # a down finding (one per source).
            await daemon._fire_source("scheduled/canary-hourly")
            await daemon._fire_source("scheduled/canary-4hourly")
            await daemon._fire_source("scheduled/iotaschool-watch")
            triaged = await _drive_triage(daemon)
            assert triaged == 3, (
                f"expected 3 observations triaged at tick 2, got {triaged}"
            )

            incidents = daemon.catalog.open_incidents()
            # --- DISCRIMINATING ASSERTION ---
            # The two canaries share entity 'canary-pipeline' but have
            # distinct source_refs. Under the current
            # idx_incidents_one_open_per_entity (source, type, entity)
            # WHERE status = 'open', they upsert into TWO rows. Dropping
            # 'source' from the conflict key would collapse them into
            # ONE.
            canary_incidents = [
                i for i in incidents if i["entity"] == "canary-pipeline"
            ]
            assert len(canary_incidents) == 2, (
                "expected two open incidents on 'canary-pipeline' "
                "(one per source_ref); got "
                f"{len(canary_incidents)} -- this is the per-(source, "
                "type, entity) dedup discrimination"
            )
            canary_sources = {i["source"] for i in canary_incidents}
            assert canary_sources == {
                "scheduled/canary-hourly",
                "scheduled/canary-4hourly",
            }
            iotaschool_incidents = [
                i for i in incidents if i["entity"] == "iotaschool.com"
            ]
            assert len(iotaschool_incidents) == 1
            assert iotaschool_incidents[0]["source"] == (
                "scheduled/iotaschool-watch"
            )

            # Drain now-pipe: immediate cadence drains every pending
            # finding queued to 'now' -- one per source = three sends.
            await daemon.pipe_drains["now"].drain_once()
            assert len(sent_push) == 3, (
                "now-pipe drains immediately on each source-fire's "
                f"down-finding; expected 3 sent, got {len(sent_push)}"
            )
            sent_after_now = list(sent_push)

            # Tick 3: canary-hourly fires AGAIN with status_code still
            # 503. The triager sees prior_state.last_status == 503 and
            # emits NO new finding -- the within-source dedup property.
            # If the triager state machine were broken (always emitting
            # on a non-200), this would write a fresh finding and a
            # subsequent now-drain would push a 4th dispatch.
            await daemon._fire_source("scheduled/canary-hourly")
            triaged = await _drive_triage(daemon)
            assert triaged == 1, (
                "the third canary-hourly fire produces one observation"
            )

            canary_hourly_findings = list(daemon.connection.execute(
                "SELECT id FROM findings WHERE source = ?",
                ("scheduled/canary-hourly",),
            ))
            assert len(canary_hourly_findings) == 1, (
                "within-source dedup: a second canary-hourly down-fire "
                "must not write a new finding while the prior down is "
                f"still the current state; got "
                f"{len(canary_hourly_findings)} findings"
            )

            # Drain now-pipe again: no new dispatches (no new findings
            # queued).
            await daemon.pipe_drains["now"].drain_once()
            assert sent_push == sent_after_now, (
                "no new dispatches after the within-source-dedup tick"
            )

            # Drain digest-pipe: drain-in-expected-order = the daily
            # picks up everything queued for 'daily' since last drain
            # (which is None on a fresh daemon -- all-time). Only the
            # canary_watch handler targets both 'now' and 'daily'; the
            # dead-link iotaschool handler routes its down-findings to
            # 'now' only, so the digest sees the two canary findings
            # but not the iotaschool down-finding. (This asymmetry is a
            # property of the existing dead-link handler we are
            # deliberately not modifying in this slice -- the
            # iotaschool cadence-relaxation follow-up records it.)
            await daemon.pipe_drains["daily"].drain_once()
            assert daily_digest_sent == ["digest body rendered"], (
                "daily digest drains once and renders a single body "
                f"on tick 3; got {daily_digest_sent}"
            )

            # The digest's findings_since_last_drain MUST carry both
            # canary findings as distinct rows. Under the per-(source,
            # type, entity) dedup contract, the two canary findings are
            # independent rows; under a "dedup per (type, entity)" or
            # per-URL inversion the two would still write to the
            # findings table (findings are per-write, not deduped), so
            # the discrimination at the findings_for_pipe_since level
            # is weaker than the incident-count discrimination above --
            # but it confirms the digest picks up the cross-cadence
            # findings without losing either canary.
            findings_in_digest = daemon.catalog.findings_for_pipe_since(
                "daily", None, exclude_types=("clearance",)
            )
            sources_in_digest = {f["source"] for f in findings_in_digest}
            assert sources_in_digest == {
                "scheduled/canary-hourly",
                "scheduled/canary-4hourly",
            }, (
                "digest must carry one finding per canary source -- "
                f"got sources {sources_in_digest}"
            )
        finally:
            daemon.connection.close()

    asyncio.run(driver())
