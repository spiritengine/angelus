"""The informational delivery lane (migration 0017, PLAN_informational_path.md).

A finding is either a CONDITION (opens/refreshes an incident, the unchanged
default path) or INFORMATIONAL (a one-shot content delivery -- seed, reminder, a
heads-up from another system -- that is delivered once and opens NO incident).
These tests pin: informational opens no incident, the lane-keyed deliver-once
dedup, the lane filter on findings_for_pipe_since, the digest rendering it under
Updates (and NOT as an incident finding), and the engine deriving the lane from
the finding TYPE so a mixed-emitting triager keeps store_corrupt a condition.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from angelus.storage import Catalog, init_db

_REPO_ROOT = Path(__file__).resolve().parent.parent


def _catalog(tmp_path) -> Catalog:
    connection = init_db(tmp_path / "angelus.sqlite3")
    return Catalog(connection, tmp_path)


def _info(entity: str = "seed-1", *, dedup_key: str | None = None, title: str = "T") -> dict:
    f = {
        "source": "scheduled/informational",
        "type": "info",
        "entity": entity,
        "severity": "low",
        "target_pipes": ["daily"],
        "body": {"title": title, "body": "details", "source": "build-bot"},
    }
    if dedup_key is not None:
        f["dedup_key"] = dedup_key
    return f


def _down(entity: str = "site") -> dict:
    return {
        "source": "scheduled/test",
        "type": "down",
        "entity": entity,
        "severity": "high",
        "target_pipes": ["daily"],
    }


# --- no incident ----------------------------------------------------------


def test_informational_opens_no_incident(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    fid = catalog.write_finding(None, _info(), {"daily"}, lane="informational")
    assert fid > 0
    # The row exists and is laned informational...
    row = catalog.connection.execute(
        "SELECT lane, status FROM findings WHERE id = ?", (fid,)
    ).fetchone()
    assert row["lane"] == "informational"
    assert row["status"] == "ready"
    # ...but it opened NO incident, so it is absent from open_incidents().
    assert catalog.open_incidents() == []
    assert (
        catalog.connection.execute("SELECT COUNT(*) AS n FROM incidents").fetchone()["n"]
        == 0
    )


def test_condition_still_opens_incident(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    catalog.write_finding(None, _down(), {"daily"})  # default lane=condition
    assert len(catalog.open_incidents()) == 1


def test_unknown_lane_raises(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    with pytest.raises(ValueError):
        catalog.write_finding(None, _info(), {"daily"}, lane="bogus")


# --- deliver-once dedup ---------------------------------------------------


def test_informational_repeat_dropped_returns_existing(tmp_path) -> None:
    """The at-least-once triage retry re-emits the same finding; the second
    write is dropped and returns the first finding's id (no second row)."""
    catalog = _catalog(tmp_path)
    first = catalog.write_finding(None, _info(dedup_key="k1"), {"daily"}, lane="informational")
    second = catalog.write_finding(None, _info(dedup_key="k1"), {"daily"}, lane="informational")
    assert second == first
    n = catalog.connection.execute(
        "SELECT COUNT(*) AS n FROM findings WHERE dedup_key = 'k1'"
    ).fetchone()["n"]
    assert n == 1


def test_distinct_informational_keys_both_write(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    a = catalog.write_finding(None, _info(dedup_key="a"), {"daily"}, lane="informational")
    b = catalog.write_finding(None, _info(dedup_key="b"), {"daily"}, lane="informational")
    assert a != b


def test_informational_dedup_ignores_writing_rows(tmp_path) -> None:
    """A crashed half-write leaves a `writing` row; the retry must NOT be
    dropped against it (else the item is silently lost). Only delivered
    (`ready`) rows gate the retry."""
    catalog = _catalog(tmp_path)
    # Simulate a crashed half-write: a writing-status informational row.
    catalog.connection.execute(
        """
        INSERT INTO findings (observation_id, source, type, entity, dedup_key,
            target_pipes, status, severity, occurred_at, created_at, lane)
        VALUES (NULL, 'scheduled/informational', 'info', 'seed-1', 'k1',
            '["daily"]', 'writing', 'low', '2026-01-01T00:00:00.000Z',
            '2026-01-01T00:00:00.000Z', 'informational')
        """
    )
    catalog.connection.commit()
    # The retry should still deliver: a fresh ready row, not a drop.
    fid = catalog.write_finding(None, _info(dedup_key="k1"), {"daily"}, lane="informational")
    ready = catalog.connection.execute(
        "SELECT id FROM findings WHERE dedup_key='k1' AND status='ready'"
    ).fetchall()
    assert len(ready) == 1
    assert ready[0]["id"] == fid


# --- lane filter on the pipe read ----------------------------------------


def test_pipe_read_lane_filter_partitions(tmp_path) -> None:
    catalog = _catalog(tmp_path)
    catalog.write_finding(None, _down("a"), {"daily"})  # condition
    catalog.write_finding(None, _info("seed", dedup_key="k1"), {"daily"}, lane="informational")

    cond = catalog.findings_for_pipe_since("daily", None, lane="condition")
    info = catalog.findings_for_pipe_since("daily", None, lane="informational")
    both = catalog.findings_for_pipe_since("daily", None)

    assert [f["type"] for f in cond] == ["down"]
    assert [f["type"] for f in info] == ["info"]
    assert len(both) == 2


# --- digest render --------------------------------------------------------


def test_digest_renders_informational_under_updates_not_incidents(tmp_path) -> None:
    """An informational finding routed to daily renders in the Updates section
    of the preamble and is absent from open_incidents / the condition list."""
    from angelus.lodging import Channel, Pipe
    from angelus.pipes import PipeDrain

    catalog = _catalog(tmp_path)
    # Use the example lodging render-templates so updates-cards.j2 resolves.
    workdir = _REPO_ROOT / "examples" / "lodging"

    catalog.write_finding(
        None,
        {
            "source": "scheduled/informational",
            "type": "info",
            "entity": "seed-1",
            "severity": "low",
            "target_pipes": ["daily"],
            "body": {"title": "VulMask seed", "body": "write the piece", "source": "pitch"},
        },
        {"daily"},
        lane="informational",
    )

    pipe = Pipe(
        name="daily",
        cadence="0 7 * * *",
        render_kind="digest",
        template=None,
        channels=["email"],
        render={
            "preamble": [
                {"kind": "structured", "template": "incident-status"},
                {"kind": "structured", "template": "updates-cards"},
            ],
        },
    )
    channels = {"email": Channel(name="email", kind="email", command="true")}
    drain = PipeDrain(catalog, pipe, channels, workdir, {"daily"})

    structured = drain._structured_inputs(pipe, None)
    assert [f["type"] for f in structured["informational_since_last_drain"]] == ["info"]
    assert structured["findings_since_last_drain"] == []  # not in the condition list
    assert structured["open_incidents"] == []  # never an incident

    preamble = drain._render_preamble(pipe, structured)
    assert "Updates (1):" in preamble
    assert "VulMask seed" in preamble
    assert "(pitch)" in preamble
    # The compact (push) leg headlines it without severity/type framing.
    compact = drain._render_compact("Subject", structured)
    assert "Updates (1):" in compact
    assert "VulMask seed" in compact
    assert "1 update(s)" in compact


# --- config: informational_types -----------------------------------------


def test_informational_types_defaults_empty_when_no_file(tmp_path) -> None:
    from angelus.lodging.config import _load_informational_types

    assert _load_informational_types(tmp_path) == frozenset()


def test_informational_types_parsed(tmp_path) -> None:
    from angelus.lodging.config import _load_informational_types

    (tmp_path / "informational.yaml").write_text(
        "types:\n  - info\n  - seed\n  - reminder\n", encoding="utf-8"
    )
    assert _load_informational_types(tmp_path) == frozenset({"info", "seed", "reminder"})


def test_informational_types_malformed_rejected(tmp_path) -> None:
    from angelus.lodging.config import _load_informational_types

    (tmp_path / "informational.yaml").write_text(
        "types:\n  - info\n  - 7\n", encoding="utf-8"
    )
    with pytest.raises(ValueError):
        _load_informational_types(tmp_path)

    (tmp_path / "informational.yaml").write_text("types: notalist\n", encoding="utf-8")
    with pytest.raises(ValueError):
        _load_informational_types(tmp_path)


# --- daemon wiring: lane derived from TYPE, mixed-emitting triager ---------

_PROBE_HANDLER = '''\
import json, sys
req = json.load(sys.stdin)
src = req["triager"]["source_ref"]
findings = [
    {"source": src, "type": "info", "entity": "u1", "severity": "low",
     "target_pipes": [], "body": {"title": "an update"}},
    {"source": src, "type": "store_corrupt", "entity": "the-store",
     "severity": "high", "target_pipes": [], "body": {"text": "torn"}},
]
json.dump({"findings": findings, "new_state": {}}, sys.stdout)
'''


def _write_min_lodging(root: Path) -> None:
    (root / "informational.yaml").write_text("types:\n  - info\n", encoding="utf-8")
    (root / "sources" / "scheduled").mkdir(parents=True)
    (root / "sources" / "scheduled" / "probe.yaml").write_text(
        "cadence: 4h\ncheck:\n  kind: shell\n  command: \"true\"\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers").mkdir(parents=True)
    (root / "triagers" / "probe-watch.yaml").write_text(
        "inputs:\n  source: scheduled/probe\n"
        "handler:\n  kind: python\n  path: triagers/handlers/probe.py\n",
        encoding="utf-8",
    )
    (root / "triagers" / "handlers" / "probe.py").write_text(_PROBE_HANDLER, encoding="utf-8")


def test_daemon_lanes_by_type_mixed_triager(tmp_path) -> None:
    """The daemon derives the lane from the finding TYPE: a triager that emits
    both an informational `info` finding AND a real `store_corrupt` condition
    lanes each correctly -- info opens no incident, store_corrupt does."""
    import asyncio

    from angelus.daemon import AngelusDaemon

    _write_min_lodging(tmp_path)
    daemon = AngelusDaemon(tmp_path)
    assert daemon.lodging.informational_types == frozenset({"info"})

    async def drive() -> None:
        obs_id = daemon.catalog.write_observation(
            "scheduled/probe", {"state": 1}, {"source": "scheduled/probe"}
        )
        triager = daemon.lodging.triagers["probe-watch"]
        rows = daemon.catalog.ready_observations_for(triager.name, triager.source_ref)
        daemon.catalog.mark_triage_processing(rows[0]["id"], triager.name)
        await daemon._run_triager(rows[0], triager.name)

    asyncio.run(drive())

    rows = {
        r["type"]: r["lane"]
        for r in daemon.connection.execute("SELECT type, lane FROM findings")
    }
    assert rows == {"info": "informational", "store_corrupt": "condition"}
    # The condition opened an incident; the informational did not.
    incidents = daemon.connection.execute(
        "SELECT type FROM incidents WHERE status='open'"
    ).fetchall()
    assert [i["type"] for i in incidents] == ["store_corrupt"]


# --- render robustness ----------------------------------------------------


def _updates_drain(tmp_path):
    from angelus.lodging import Channel, Pipe
    from angelus.pipes import PipeDrain

    catalog = _catalog(tmp_path)
    pipe = Pipe(
        name="daily",
        cadence="0 7 * * *",
        render_kind="digest",
        template=None,
        channels=["email"],
        render={"preamble": [{"kind": "structured", "template": "updates-cards"}]},
    )
    channels = {"email": Channel(name="email", kind="email", command="true")}
    return PipeDrain(catalog, pipe, channels, _REPO_ROOT / "examples" / "lodging", {"daily"})


def test_updates_empty_renders_blank(tmp_path) -> None:
    drain = _updates_drain(tmp_path)
    structured = {"informational_since_last_drain": []}
    assert drain._render_preamble(drain.pipe, structured) == ""


def test_updates_multi_item_does_not_collapse(tmp_path) -> None:
    """The trim_blocks footgun: each card must stay on its own line."""
    drain = _updates_drain(tmp_path)
    structured = {
        "informational_since_last_drain": [
            {"body": {"title": "first"}, "body_text": "", "entity": "a"},
            {"body": {"title": "second"}, "body_text": "", "entity": "b"},
            {"body": {}, "body_text": "third via body_text", "entity": "c"},
            {"body": {}, "body_text": "", "entity": "fourth-via-entity"},
        ]
    }
    out = drain._render_preamble(drain.pipe, structured)
    assert "Updates (4):" in out
    assert "- first" in out
    assert "- second" in out
    # Fallback chain: body_text then entity.
    assert "third via body_text" in out
    assert "fourth-via-entity" in out
    # Each card on its own line (no collapse).
    card_lines = [ln for ln in out.splitlines() if ln.startswith("- ")]
    assert len(card_lines) == 4
