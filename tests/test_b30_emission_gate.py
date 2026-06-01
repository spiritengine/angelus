"""B30: idempotent finding emission with a recovery gate.

Pins the emission/recovery gate added to Catalog.write_finding and the
per-source clearance wiring that the gate depends on. The gate makes findings
edge-triggered on incident transitions: a non-clearance finding emits only
when it OPENS a new incident, a clearance emits only when it CLOSES an open
one. A source that can open an incident but never emit a clearance would go
silent forever under the gate, so the audit test below guards that every
internal failure source has a wired clearance.
"""

from __future__ import annotations

import asyncio
import json
import re
from pathlib import Path

from angelus.daemon import AngelusDaemon
from angelus.lodging import Channel, Pipe
from angelus.lodging.reloader import LodgingReloader
from angelus.pipes import PipeDrain
from angelus.pipes.runner import DEFAULT_DIGEST_MAX_ITEMS_PER_INPUT, _cap_digest_input
from angelus.storage import Catalog, init_db

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _catalog(tmp_path: Path) -> Catalog:
    connection = init_db(tmp_path / "angelus.sqlite3")
    return Catalog(connection, tmp_path)


def _down(entity: str = "site") -> dict:
    return {
        "source": "scheduled/test",
        "type": "down",
        "entity": entity,
        "severity": "high",
        "target_pipes": ["now", "daily"],
    }


def _clearance(entity: str = "site") -> dict:
    return {
        "source": "scheduled/test",
        "type": "clearance",
        "entity": entity,
        "severity": "info",
        "target_pipes": ["now", "daily"],
    }


def _finding_count(catalog: Catalog, source: str, finding_type: str) -> int:
    return catalog.connection.execute(
        "SELECT COUNT(*) AS n FROM findings WHERE source = ? AND type = ?",
        (source, finding_type),
    ).fetchone()["n"]


def _queue_count(catalog: Catalog) -> int:
    return catalog.connection.execute(
        "SELECT COUNT(*) AS n FROM pipe_queues"
    ).fetchone()["n"]


# --- idempotency ----------------------------------------------------------


def test_repeat_while_incident_open_writes_no_row_or_enqueue(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    known = {"now", "daily"}
    try:
        first_id = catalog.write_finding(None, _down(), known)
        assert first_id >= 1
        rows_after_open = _finding_count(catalog, "scheduled/test", "down")
        queue_after_open = _queue_count(catalog)
        assert rows_after_open == 1
        assert queue_after_open == 2  # now + daily

        # Repeat the identical finding while its incident is open. The gate
        # drops it entirely: no new findings row, no new pipe_queues row, and
        # the call hands back the existing opening finding_id so the int
        # contract holds.
        repeat_id = catalog.write_finding(None, _down(), known)
        assert repeat_id == first_id
        assert _finding_count(catalog, "scheduled/test", "down") == 1
        assert _queue_count(catalog) == 2
    finally:
        catalog.connection.close()


def test_no_op_clearance_returns_sentinel_and_writes_nothing(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    try:
        # A clearance with no open incident to close is a no-op: returns the
        # _NO_FINDING sentinel (0) and writes neither a row nor a queue item.
        result = catalog.write_finding(None, _clearance(), {"now", "daily"})
        assert result == 0
        assert _finding_count(catalog, "scheduled/test", "clearance") == 0
        assert _queue_count(catalog) == 0
    finally:
        catalog.connection.close()


# --- re-arm: open -> clearance -> close -> reopen -------------------------


def test_recovery_re_arms_the_gate_and_reopen_emits_once(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    known = {"now", "daily"}
    try:
        catalog.write_finding(None, _down(), known)
        catalog.write_finding(None, _down(), known)  # dropped
        assert _finding_count(catalog, "scheduled/test", "down") == 1
        assert len(catalog.open_incidents()) == 1

        # Clearance closes the incident (a real transition) and emits.
        cleared = catalog.write_finding(None, _clearance(), known)
        assert cleared >= 1
        assert _finding_count(catalog, "scheduled/test", "clearance") == 1
        assert catalog.open_incidents() == []

        # The condition must CLEAR before it can re-alert. The next failure
        # finds no open incident, opens a fresh one, and emits exactly one
        # more `down` finding -- proving the gate re-armed rather than going
        # silent forever.
        reopened = catalog.write_finding(None, _down(), known)
        assert reopened >= 1
        assert _finding_count(catalog, "scheduled/test", "down") == 2
        assert len(catalog.open_incidents()) == 1
    finally:
        catalog.connection.close()


# --- per-source clearance: the reloader self-clears on recovery -----------


def _write_min_lodging(root: Path) -> None:
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "watch.yaml").write_text(
        "cadence: 1h\ncheck:\n  kind: shell\n  command: 'echo {}'\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "handlers" / "noop.py").write_text(
        "import json\nprint(json.dumps({'findings': [], 'new_state': {}}))\n",
        encoding="utf-8",
    )
    (root / "triagers" / "noop.yaml").write_text(
        "inputs:\n  source: scheduled/watch\n"
        "handler:\n  kind: python\n  path: triagers/handlers/noop.py\n",
        encoding="utf-8",
    )
    (root / "pipes").mkdir()
    (root / "pipes" / "now.yaml").write_text(
        "cadence: immediate\nchannels: [push]\n"
        "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
        encoding="utf-8",
    )
    (root / "channels").mkdir()
    (root / "channels" / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n",
        encoding="utf-8",
    )


def test_reloader_emits_clearance_on_recovery_and_closes_incident(tmp_path) -> None:
    _write_min_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)
    now_pipe = tmp_path / "pipes" / "now.yaml"
    try:
        # Reject the pipe by referencing a channel that does not exist.
        now_pipe.write_text(
            "cadence: immediate\nchannels: [push, log]\n"
            "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
            encoding="utf-8",
        )
        reloader.event_queue.put(str(now_pipe))
        asyncio.run(reloader.process_pending_events())

        opens = [
            i for i in daemon.catalog.open_incidents()
            if i["source"] == "internal/lodging"
        ]
        assert len(opens) == 1
        assert opens[0]["entity"] == "pipes/now.yaml"

        # Recovery: drop the missing channel so the pipe loads OK on retry.
        (tmp_path / "channels" / "log.yaml").write_text(
            "kind: push\ncommand: 'echo log'\n", encoding="utf-8"
        )
        reloader.event_queue.put(str(tmp_path / "channels" / "log.yaml"))
        asyncio.run(reloader.process_pending_events())

        # The reloader emitted a clearance, which closed the lodging incident.
        assert [
            i for i in daemon.catalog.open_incidents()
            if i["source"] == "internal/lodging"
        ] == []
        clearances = [
            c for c in daemon.catalog.clearance_findings_since(None)
            if c["source"] == "internal/lodging"
        ]
        assert len(clearances) == 1
        assert clearances[0]["entity"] == "pipes/now.yaml"
    finally:
        daemon.connection.close()


# --- the flood scenario: persistent failure -> exactly ONE finding --------


def test_persistent_reload_failure_emits_one_finding_not_n(tmp_path) -> None:
    _write_min_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    reloader = LodgingReloader(daemon, tmp_path, debounce_seconds=0.0)
    bad = tmp_path / "pipes" / "now.yaml"
    try:
        # A pipe that fails cross-ref validation on every poll -- the exact
        # shape of the 2026-06-01 stale-daemon flood (a file the running
        # daemon rejects ~1/sec for hours).
        bad.write_text(
            "cadence: immediate\nchannels: [nonexistent]\n"
            "render:\n  kind: dumb-alert\n  template: '{type}:{entity}:{body}'\n",
            encoding="utf-8",
        )
        # Poll it 50 times the way the runtime would: re-enqueue + process.
        # Each process call also runs _retry_rejected, so the failing file is
        # actually handled ~2x per loop -- ~100 reject events total.
        for _ in range(50):
            reloader.event_queue.put(str(bad))
            asyncio.run(reloader.process_pending_events())

        # Without the gate this table held ~114k rows. With it: exactly one
        # internal/lodging finding -- the open transition -- and one open
        # incident carrying the (now long) duration.
        assert _finding_count(daemon.catalog, "internal/lodging", "cross_ref_broken") == 1
        opens = [
            i for i in daemon.catalog.open_incidents()
            if i["source"] == "internal/lodging"
        ]
        assert len(opens) == 1
    finally:
        daemon.connection.close()


# --- digest hard-cap backstop --------------------------------------------


def test_cap_digest_input_truncates_with_marker() -> None:
    items = [{"type": "down", "entity": f"e{i}", "body_text": "x"} for i in range(1000)]
    capped = _cap_digest_input(items, "findings_since_last_drain")
    # max_items kept + one marker row.
    assert len(capped) == DEFAULT_DIGEST_MAX_ITEMS_PER_INPUT + 1
    marker = capped[-1]
    omitted = 1000 - DEFAULT_DIGEST_MAX_ITEMS_PER_INPUT
    assert marker["entity"] == f"[{omitted} more findings_since_last_drain omitted]"
    assert str(omitted) in marker["body_text"]
    # The kept items are the original prefix, untouched.
    assert capped[0]["entity"] == "e0"


def test_cap_digest_input_passthrough_under_budget() -> None:
    items = [{"entity": f"e{i}"} for i in range(5)]
    assert _cap_digest_input(items, "open_incidents") is items


def test_structured_inputs_bounds_a_flood_with_marker(tmp_path, monkeypatch) -> None:
    catalog = _catalog(tmp_path)
    pipe = Pipe(
        name="daily",
        cadence="0 8 * * *",
        render_kind="digest",
        template=None,
        channels=["push"],
        render={"preamble": [], "body": {"kind": "llm", "mantle": "chronicler",
                                          "inputs": ["findings_since_last_drain"]}},
    )
    drain = PipeDrain(
        catalog,
        pipe,
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path,
        {"now", "daily"},
    )
    flood = [
        {"source": "internal/lodging", "type": "load_failed", "entity": "pipes/daily.yaml",
         "severity": "high", "body_text": "boom", "occurred_at": "2026-06-01T00:00:00.000Z"}
        for _ in range(50_000)
    ]
    monkeypatch.setattr(catalog, "findings_for_pipe_since", lambda *a, **k: flood)
    try:
        structured = drain._structured_inputs(pipe, None)
        capped = structured["findings_since_last_drain"]
        assert len(capped) == DEFAULT_DIGEST_MAX_ITEMS_PER_INPUT + 1
        assert "more findings_since_last_drain omitted" in capped[-1]["entity"]
        # The JSON the chronicler prompt embeds is now small -- far below the
        # 15MB that triggered the incident (the uncapped flood would be ~5MB+).
        as_json = json.dumps(structured, default=str)
        assert len(as_json) < 500_000
    finally:
        catalog.connection.close()


# --- audit guard: no internal source can open without a wired clearance ---


def _internal_sources(call_name: str) -> set[str]:
    """Source strings passed as the first arg to `call_name` across angelus/.

    Matches the multi-line call shape used in the codebase, e.g.
        self.catalog.write_internal_finding(
            "internal/dep",
            ...
    """
    pattern = re.compile(call_name + r"\(\s*[\"']([^\"']+)[\"']")
    sources: set[str] = set()
    for path in (_REPO_ROOT / "angelus").rglob("*.py"):
        sources.update(pattern.findall(path.read_text(encoding="utf-8")))
    return sources


def test_every_internal_failure_source_has_a_wired_clearance() -> None:
    """Guard: every internal source that can OPEN an incident must also have
    a clearance wired, or it goes silent forever under the emission gate.

    This fails the moment someone adds a new write_internal_finding source
    without a matching write_internal_clearance -- the load-bearing risk B30
    is built around. External/triager sources (http_status, gh_actions_status,
    gh_stale_pr, canary_watch) emit their own type='clearance' findings on the
    recovery edge and are covered by their handler tests, so they are out of
    scope for this internal-source guard.
    """
    openers = _internal_sources("write_internal_finding")
    clearers = _internal_sources("write_internal_clearance")
    assert openers, "expected to find internal failure sources to audit"
    missing = openers - clearers
    assert not missing, (
        "internal sources that open an incident but have no wired clearance "
        f"(would go silent forever under the gate): {sorted(missing)}"
    )
