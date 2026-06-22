"""The shared informational inbox: store, batch drip, generic triager, `inform`
CLI (the producer front door for the lane).

End-to-end shape: a producer drops a record (via `inform` or a direct append),
the drip drains EVERY pending record into one batch observation, the generic
triager fans it out into one `info` finding per record (source-namespaced
dedup_key, digest-only routing), and the engine lanes those informational.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path

import pytest

_REPO_ROOT = Path(__file__).resolve().parent.parent
_BIN = _REPO_ROOT / "examples" / "lodging" / "bin"
_HANDLER = _REPO_ROOT / "examples" / "lodging" / "triagers" / "handlers" / "informational.py"


def _load(name: str):
    spec = importlib.util.spec_from_file_location(name, _BIN / f"{name}.py")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ins = _load("informational_store")


# --- store ---------------------------------------------------------------


def test_make_record_validates(tmp_path) -> None:
    with pytest.raises(ValueError):
        ins.make_record(source="", title="x")
    with pytest.raises(ValueError):
        ins.make_record(source="s", title="   ")
    r = ins.make_record(source="s", title="hello")
    assert r["emitted_at"] is None
    assert r["id"]


def test_add_and_load_roundtrip(tmp_path) -> None:
    store = tmp_path / "i.jsonl"
    ins.add_record(store, ins.make_record(source="a", title="t1"))
    ins.add_record(store, ins.make_record(source="b", title="t2"))
    records = ins.load_records(store)
    assert {r["title"] for r in records} == {"t1", "t2"}


def test_drain_all_batches_then_idle(tmp_path) -> None:
    store = tmp_path / "i.jsonl"
    for i in range(3):
        ins.add_record(store, ins.make_record(source="a", title=f"t{i}", record_id=f"id{i}"))
    emitted: list[dict] = []
    drained, malformed = ins.drain_all_emitting(store, emitted.append)
    assert drained == 3 and malformed == 0
    assert len(emitted) == 1  # ONE batch observation
    assert emitted[0]["type"] == "batch"
    assert len(emitted[0]["records"]) == 3
    # All stamped emitted; a second drain finds nothing.
    emitted2: list[dict] = []
    drained2, _ = ins.drain_all_emitting(store, emitted2.append)
    assert drained2 == 0 and emitted2 == []


def test_torn_row_is_quarantined_not_fatal(tmp_path) -> None:
    store = tmp_path / "i.jsonl"
    ins.add_record(store, ins.make_record(source="a", title="good", record_id="g1"))
    # Append a torn line by hand.
    with open(store, "a", encoding="utf-8") as fh:
        fh.write("{not json\n")
    records, malformed = ins._parse_store(store)
    assert [r["id"] for r in records] == ["g1"]
    assert len(malformed) == 1
    # The good record still drains; corruption is drain-gated.
    emitted: list[dict] = []
    drained, mal = ins.drain_all_emitting(store, emitted.append)
    assert drained == 1 and mal == 1


def test_prune_keeps_unemitted(tmp_path) -> None:
    store = tmp_path / "i.jsonl"
    # 2 emitted + 1 pending; retain=1 should drop the oldest emitted only.
    ins.add_record(store, {**ins.make_record(source="a", title="old", record_id="o"),
                           "emitted_at": "2026-01-01T00:00:00.000Z"})
    ins.add_record(store, {**ins.make_record(source="a", title="new", record_id="n"),
                           "emitted_at": "2026-02-01T00:00:00.000Z"})
    ins.add_record(store, ins.make_record(source="a", title="pending", record_id="p"))
    removed = ins.prune_emitted(store, retain=1)
    assert removed == 1
    ids = {r["id"] for r in ins.load_records(store)}
    assert "o" not in ids and "n" in ids and "p" in ids  # pending never pruned


# --- handler fan-out -----------------------------------------------------


def _run_handler(observation: dict) -> list[dict]:
    req = {
        "observation": observation,
        "prior_state": {},
        "triager": {
            "source_ref": "scheduled/informational",
            "metadata": {"target_pipe": "daily", "urgent_pipe": "now"},
        },
    }
    out = subprocess.run(
        [sys.executable, str(_HANDLER)], input=json.dumps(req), capture_output=True, text=True
    )
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)["findings"]


def test_handler_fans_batch_to_info_findings(tmp_path) -> None:
    obs = {
        "type": "batch",
        "records": [
            {"id": "a1", "source": "build-bot", "title": "green", "severity": "low"},
            {"id": "seed-1", "source": "pitch", "title": "VulMask", "body": "x", "severity": "high"},
            {"id": "seed-1", "source": "ci", "title": "same id", "severity": "low"},
        ],
    }
    findings = _run_handler(obs)
    assert [f["type"] for f in findings] == ["info", "info", "info"]
    # All route ONLY to the digest -- informational never pages, even at high sev.
    assert all(f["target_pipes"] == ["daily"] for f in findings)
    # Source-namespaced dedup_key: the two seed-1 from different producers differ.
    assert {f["dedup_key"] for f in findings} == {"build-bot:a1", "pitch:seed-1", "ci:seed-1"}


def test_handler_store_corrupt_is_condition(tmp_path) -> None:
    findings = _run_handler(
        {"type": "store_corrupt", "entity": "informational-store", "malformed_lines": 2}
    )
    assert len(findings) == 1
    f = findings[0]
    assert f["type"] == "store_corrupt"  # NOT in informational_types -> condition
    assert f["target_pipes"] == ["now", "daily"]  # urgent + floor
    assert f["severity"] == "high"


def test_handler_idle_emits_nothing(tmp_path) -> None:
    assert _run_handler({"type": "idle"}) == []


def test_handler_skips_non_dict_record(tmp_path) -> None:
    """A malformed records shape must not crash the whole batch."""
    findings = _run_handler(
        {"type": "batch", "records": ["not-a-dict", {"id": "ok", "source": "s", "title": "t"}]}
    )
    assert [f["entity"] for f in findings] == ["ok"]


def test_batch_state_is_source_namespaced(tmp_path) -> None:
    """The collapse token must be as discriminating as the finding dedup_key:
    same id, different source -> DIFFERENT state, so the daemon does not collapse
    the second producer's update away."""
    a = ins.batch_observation([{"id": "status", "source": "a", "title": "x"}])
    b = ins.batch_observation([{"id": "status", "source": "b", "title": "y"}])
    assert a["state"] != b["state"]
    assert a["state"] == "batch:a:status"
    assert b["state"] == "batch:b:status"


# --- inform CLI ----------------------------------------------------------


def test_inform_cli_add_and_ls(tmp_path) -> None:
    store = tmp_path / "i.jsonl"
    env = {"INFORMATIONAL_STORE": str(store), "PATH": "/usr/bin:/bin"}
    r = subprocess.run(
        [sys.executable, str(_BIN / "inform"), "add", "--source", "bot", "hello world"],
        capture_output=True, text=True, env={**env},
    )
    assert r.returncode == 0, r.stderr
    assert "from bot" in r.stdout
    records = ins.load_records(store)
    assert len(records) == 1 and records[0]["title"] == "hello world"
    assert records[0]["source"] == "bot"

    ls = subprocess.run(
        [sys.executable, str(_BIN / "inform"), "ls"], capture_output=True, text=True, env={**env}
    )
    assert "pending bot hello world" in ls.stdout


def test_inform_cli_explicit_id_is_idempotent_marker(tmp_path) -> None:
    store = tmp_path / "i.jsonl"
    env = {"INFORMATIONAL_STORE": str(store)}
    subprocess.run(
        [sys.executable, str(_BIN / "inform"), "add", "--source", "ci", "--id", "flake-1", "t"],
        capture_output=True, text=True, env={**env}, check=True,
    )
    records = ins.load_records(store)
    assert records[0]["id"] == "flake-1"


# --- end to end through the engine ---------------------------------------


def test_intake_delivers_informational_via_example_lodging(tmp_path) -> None:
    """A dropped record, dripped + triaged through the SHIPPED example lodging,
    lands as an informational finding (no incident) routed to daily."""
    import asyncio
    import shutil

    from angelus.daemon import AngelusDaemon

    # Copy the example lodging into tmp so the daemon's state/ db is written
    # there, not into the shipped examples tree.
    lodging = tmp_path / "lodging"
    shutil.copytree(_REPO_ROOT / "examples" / "lodging", lodging)
    daemon = AngelusDaemon(lodging)
    assert "info" in daemon.lodging.informational_types

    obs = {
        "type": "batch",
        "records": [{"id": "u1", "source": "bot", "title": "an update", "severity": "low"}],
        "source_ref": "scheduled/informational",
        "state": "batch:u1",
        "entity": "informational-batch",
    }

    async def drive() -> None:
        obs_id = daemon.catalog.write_observation("scheduled/informational", obs, {})
        triager = daemon.lodging.triagers["informational-watch"]
        rows = daemon.catalog.ready_observations_for(triager.name, triager.source_ref)
        daemon.catalog.mark_triage_processing(rows[0]["id"], triager.name)
        await daemon._run_triager(rows[0], triager.name)

    asyncio.run(drive())

    rows = daemon.connection.execute("SELECT type, lane FROM findings").fetchall()
    assert [(r["type"], r["lane"]) for r in rows] == [("info", "informational")]
    assert daemon.catalog.open_incidents() == []  # never an incident
