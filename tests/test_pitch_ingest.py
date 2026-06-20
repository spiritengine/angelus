"""Pitch -> seed-store ingest adapter (brief-20260619-opi5 option C, write side).

pitch_ingest.py parses the plain text the ``pitch`` mill chain emits and appends
one seed per block to the store that the drip drains. Coverage:
  - a multi-seed sample (mixed URGENCY) -> correct rows, correct severity
    mapping (HOT -> high, WARM/MONITOR -> low).
  - "No seeds today." (and empty input) -> store untouched.
  - a malformed block (missing CANNON) among good ones -> skipped, the good ones
    still ingest, no crash.
  - idempotency: ingesting the same text twice -> no duplicate rows.
  - the ingested rows are valid seed rows and drain through the real drip.
"""

from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
LODGING = REPO_ROOT / "examples" / "lodging"
BIN = LODGING / "bin"
INGEST = BIN / "pitch_ingest.py"
DRIP = BIN / "pitch_seed_drip.py"


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


ss = _load_module("seed_store", BIN / "seed_store.py")
ingest_mod = _load_module("pitch_ingest", INGEST)


def _drip(store: Path) -> dict:
    result = subprocess.run(
        [sys.executable, str(DRIP), str(store)],
        capture_output=True,
        text=True,
        check=True,
    )
    return json.loads(result.stdout)


# A realistic three-block sample with mixed urgency plus the NOISE FILTERED
# trailer the chain appends.
SAMPLE = """\
EVENT: Skills shipped with no supply-chain guard
SOURCE: https://example.com/skills
DATE: 2026-06-20
MARKERS: S1, S4, S7
CANNON: 2. Bad Skillz audit cannon
MOVE: Ship a scanner that flags malicious skill manifests.
URGENCY: HOT (48h)
SCOOP CHECK: not scooped
---
EVENT: New Wayland a11y protocol draft landed
SOURCE: https://example.com/wayland
DATE: 2026-06-19
MARKERS: S2
CANNON: 5. Tine/Wayland-a11y cannon
MOVE: Prototype an accessibility bridge against the draft.
URGENCY: WARM (5 days)
SCOOP CHECK: n
---
EVENT: Mass-LLM eval harness gap reported
SOURCE: https://example.com/eval
DATE: 2026-06-18
MARKERS: S3, S9
CANNON: 3. mass-LLM-audit cannon
MOVE: Build a batched eval runner for the reported gap.
URGENCY: MONITOR
SCOOP CHECK: n
---
NOISE FILTERED: 9 (commentary-only, no cannon, already-shipped)
"""


def _run_ingest(store: Path, text: str) -> subprocess.CompletedProcess:
    """Drive the real CLI over stdin (the deployed invocation path)."""
    return subprocess.run(
        [sys.executable, str(INGEST), str(store)],
        input=text,
        capture_output=True,
        text=True,
        check=True,
    )


def _run_ingest_stdin_bytes(store: Path, data: bytes) -> subprocess.CompletedProcess:
    """Drive the real CLI over stdin with RAW BYTES (the deployed path).

    text=False so ``input`` is fed to the child's stdin as bytes verbatim -- this
    is how an invalid-UTF-8 byte reaches the CLI's stdin read. stdout/stderr come
    back as bytes; callers decode leniently for assertions.
    """
    return subprocess.run(
        [sys.executable, str(INGEST), str(store)],
        input=data,
        capture_output=True,
        check=True,
    )


def _run_ingest_file(store: Path, input_file: Path) -> subprocess.CompletedProcess:
    """Drive the real CLI over an INPUT_FILE argument (the file path)."""
    return subprocess.run(
        [sys.executable, str(INGEST), str(store), str(input_file)],
        capture_output=True,
        text=True,
        check=True,
    )


# --------------------------------------------------------------------------
# multi-seed parse + severity mapping
# --------------------------------------------------------------------------


def test_multi_seed_sample_ingests_with_correct_severity(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ingested, skipped, duplicate = ingest_mod.ingest(store, SAMPLE)
    assert (ingested, skipped, duplicate) == (3, 0, 0)

    seeds = ss.load_seeds(store)
    by_cannon = {s["cannon"]: s for s in seeds}
    assert len(seeds) == 3

    hot = by_cannon["2. Bad Skillz audit cannon"]
    assert hot["event"] == "Skills shipped with no supply-chain guard"
    assert hot["build_move"] == "Ship a scanner that flags malicious skill manifests."
    assert hot["severity"] == "high", "HOT -> high (jumps to the now pipe)"

    warm = by_cannon["5. Tine/Wayland-a11y cannon"]
    assert warm["severity"] == "low", "WARM -> low (daily batch)"

    monitor = by_cannon["3. mass-LLM-audit cannon"]
    assert monitor["severity"] == "low", "MONITOR -> low (daily batch)"

    # The id is the (cannon, event) digest seed_store keys dedup on.
    assert hot["id"] == ss.seed_id(hot["cannon"], hot["event"])


def test_missing_or_unknown_urgency_maps_low(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    text = (
        "EVENT: urgency line absent entirely\n"
        "CANNON: 1. some cannon\n"
        "MOVE: build the thing\n"
        "---\n"
        "EVENT: urgency is gibberish\n"
        "CANNON: 1. other cannon\n"
        "MOVE: build the other thing\n"
        "URGENCY: whenever-ish\n"
    )
    ingested, skipped, duplicate = ingest_mod.ingest(store, text)
    assert (ingested, skipped, duplicate) == (2, 0, 0)
    assert {s["severity"] for s in ss.load_seeds(store)} == {"low"}


# --------------------------------------------------------------------------
# "No seeds today." / empty -> no-op
# --------------------------------------------------------------------------


def test_no_seeds_today_leaves_store_untouched(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ingested, skipped, duplicate = ingest_mod.ingest(store, "No seeds today.\n")
    assert (ingested, skipped, duplicate) == (0, 0, 0)
    assert not store.exists(), "a no-seed scan writes nothing to the store"
    assert ss.load_seeds(store) == []


def test_empty_input_is_a_noop(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    assert ingest_mod.ingest(store, "") == (0, 0, 0)
    assert ingest_mod.ingest(store, "\n   \n") == (0, 0, 0)
    assert not store.exists()


# --------------------------------------------------------------------------
# malformed block among good ones -> skipped, not fatal
# --------------------------------------------------------------------------


def test_malformed_block_missing_cannon_is_skipped(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    text = (
        "EVENT: good one\n"
        "CANNON: 1. real cannon\n"
        "MOVE: build it\n"
        "URGENCY: WARM\n"
        "---\n"
        "EVENT: this block has no cannon\n"
        "MOVE: build something\n"
        "URGENCY: HOT (48h)\n"
        "---\n"
        "EVENT: another good one\n"
        "CANNON: 2. real cannon\n"
        "MOVE: build it too\n"
        "URGENCY: MONITOR\n"
    )
    ingested, skipped, duplicate = ingest_mod.ingest(store, text)
    assert ingested == 2, "the two complete blocks ingest"
    assert skipped == 1, "the block missing CANNON is skipped, not fatal"
    assert duplicate == 0
    events = {s["event"] for s in ss.load_seeds(store)}
    assert events == {"good one", "another good one"}
    assert "this block has no cannon" not in events


def test_cli_skips_malformed_and_warns(tmp_path: Path) -> None:
    """The CLI path stays exit-0 over a malformed block and warns on stderr."""
    store = tmp_path / "seeds.jsonl"
    text = (
        "EVENT: good\n"
        "CANNON: 1. c\n"
        "MOVE: m\n"
        "---\n"
        "EVENT: no move, no cannon\n"
    )
    result = _run_ingest(store, text)
    assert result.returncode == 0
    assert "skipping malformed block" in result.stderr
    assert "1 seed(s) ingested" in result.stderr
    assert len(ss.load_seeds(store)) == 1


# --------------------------------------------------------------------------
# a SYSTEMIC store-write failure is NOT swallowed -> crash loud, non-zero
# --------------------------------------------------------------------------
#
# The per-block guard catches DATA defects (a toxic field value make_seed rejects)
# only. append_seed's failures are SYSTEMIC -- disk full, permission denied, an
# flock failure in write_seeds -- and must NOT be turned into a counted "skipped
# block" + exit 0, which would silently drop a parseable seed with no operator
# alert (the exact miss-a-seed failure this drip exists to prevent). A store-write
# failure must propagate: the run crashes loud and non-zero. Re-running is
# idempotent (append_seed dedups), so crash-then-fix-then-rerun loses nothing.


def test_store_write_failure_is_not_swallowed(tmp_path: Path, monkeypatch) -> None:
    store = tmp_path / "seeds.jsonl"
    text = (
        "EVENT: a perfectly good block\n"
        "CANNON: 1. real cannon\n"
        "MOVE: build it\n"
        "URGENCY: WARM\n"
    )

    def boom(*_args, **_kwargs):
        raise OSError(28, "No space left on device")

    # Simulate a systemic store-write failure (disk-full / permission denied).
    monkeypatch.setattr(ingest_mod.seed_store, "append_seed", boom)

    # The block is well-formed -- the only failure is the store write -- so it must
    # PROPAGATE, not be caught and counted as a skip with a clean return.
    import pytest

    with pytest.raises(OSError):
        ingest_mod.ingest(store, text)


def test_cli_store_write_failure_exits_nonzero(tmp_path: Path) -> None:
    """The deployed CLI path must exit NON-ZERO on a store-write failure.

    Drives the real CLI against a store path whose parent is a regular FILE, so
    write_seeds' mkdir/open inside append_seed raises OSError -- a systemic write
    failure the adapter must not swallow. The run must exit non-zero (the seed is
    NOT silently counted as skipped with exit 0).
    """
    blocker = tmp_path / "not-a-dir"
    blocker.write_text("i am a file, not a directory\n")
    store = blocker / "seeds.jsonl"  # parent is a file -> mkdir/open fails
    text = (
        "EVENT: a perfectly good block\n"
        "CANNON: 1. real cannon\n"
        "MOVE: build it\n"
        "URGENCY: WARM\n"
    )
    result = subprocess.run(
        [sys.executable, str(INGEST), str(store)],
        input=text,
        capture_output=True,
        text=True,
    )
    assert result.returncode != 0, "a store-write failure must crash loud, not exit 0"
    assert "1 seed(s) ingested" not in result.stdout + result.stderr, (
        "the dropped seed must not be reported as a clean success"
    )


# --------------------------------------------------------------------------
# invalid-UTF-8 byte in a block value -> skipped loud, later blocks still ingest
# --------------------------------------------------------------------------
#
# pitch_ingest reads with errors="surrogateescape", so an invalid byte becomes a
# lone surrogate that parses cleanly and passes the required-field check -- then
# seed_store.seed_id's strict-UTF-8 encode raises UnicodeEncodeError on it. Before
# the per-block guard, that aborted the whole run: the toxic block crashed the
# loop, every block AFTER it never ingested, the summary never printed, exit 1.
# These assert the run stays exit-0, the good block AFTER the toxic one ingests,
# and the toxic block is skipped + counted -- via BOTH the file path and stdin.

# A toxic block (invalid byte 0xff in EVENT) FOLLOWED BY a good block. The good
# block must survive the toxic one to prove no partial drop of later blocks.
TOXIC_THEN_GOOD = (
    b"EVENT: toxic \xff event\n"
    b"SOURCE: https://example.com/toxic\n"
    b"CANNON: 9. toxic cannon\n"
    b"MOVE: build the toxic thing\n"
    b"URGENCY: WARM\n"
    b"---\n"
    b"EVENT: good block after the toxic one\n"
    b"SOURCE: https://example.com/good\n"
    b"CANNON: 8. good cannon\n"
    b"MOVE: build the good thing\n"
    b"URGENCY: HOT (48h)\n"
)


def test_toxic_byte_skipped_later_block_ingests_stdin(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    result = _run_ingest_stdin_bytes(store, TOXIC_THEN_GOOD)
    assert result.returncode == 0, "a toxic byte must not crash the stdin read/run"
    stderr = result.stderr.decode("utf-8", errors="replace")
    assert "skipping unstorable block" in stderr, "the toxic block is skipped loud"
    assert "1 seed(s) ingested, 1 skipped" in stderr, "summary printed; toxic counted"

    seeds = ss.load_seeds(store)
    assert len(seeds) == 1, "only the good block landed; later block not dropped"
    assert seeds[0]["event"] == "good block after the toxic one"
    assert seeds[0]["severity"] == "high"


def test_toxic_byte_skipped_later_block_ingests_file(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    input_file = tmp_path / "scan.md"
    input_file.write_bytes(TOXIC_THEN_GOOD)

    result = _run_ingest_file(store, input_file)
    assert result.returncode == 0, "a toxic byte must not crash the file-path run"
    assert "skipping unstorable block" in result.stderr, "the toxic block is skipped loud"
    assert "1 seed(s) ingested, 1 skipped" in result.stderr

    seeds = ss.load_seeds(store)
    assert len(seeds) == 1, "only the good block landed; later block not dropped"
    assert seeds[0]["event"] == "good block after the toxic one"


# --------------------------------------------------------------------------
# severity: HOT escalates from ANY token position; truly unknown stays low
# --------------------------------------------------------------------------


def test_severity_hot_matches_any_token_position() -> None:
    # Canonical and decorated HOT markers all escalate -- the LLM does not always
    # lead with the bare token.
    assert ingest_mod._severity("HOT (48h)") == "high"
    assert ingest_mod._severity("🔥 HOT") == "high"
    assert ingest_mod._severity("Critical HOT") == "high"
    assert ingest_mod._severity("Critical/HOT") == "high"
    assert ingest_mod._severity("URGENT - HOT") == "high"
    assert ingest_mod._severity("hot (48h)") == "high", "case-insensitive"

    # No HOT token -> low. A substring is not a token, so HOTEL does not escalate;
    # truly unknown / missing / WARM / MONITOR never false-escalate to a page.
    assert ingest_mod._severity("whenever") == "low"
    assert ingest_mod._severity("HOTEL booking") == "low"
    assert ingest_mod._severity("WARM (5 days)") == "low"
    assert ingest_mod._severity("MONITOR") == "low"
    assert ingest_mod._severity("") == "low"
    assert ingest_mod._severity(None) == "low"


# --------------------------------------------------------------------------
# idempotency: same text twice -> no duplicate rows
# --------------------------------------------------------------------------


def test_ingest_is_idempotent(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    first = ingest_mod.ingest(store, SAMPLE)
    assert first == (3, 0, 0)

    # The same scan again: append_seed dedups on the (cannon, event) id, so no new
    # rows land -- every block reports as a duplicate, none ingested.
    second = ingest_mod.ingest(store, SAMPLE)
    assert second == (0, 0, 3), "re-ingesting the same scan adds nothing"
    assert len(ss.load_seeds(store)) == 3, "no duplicate rows in the store"


# --------------------------------------------------------------------------
# the ingested rows are valid + drain through the real drip
# --------------------------------------------------------------------------


def test_ingested_rows_are_valid_and_drain(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ingest_mod.ingest(store, SAMPLE)

    seeds, malformed = ss._parse_store(store)
    assert malformed == [], "ingested rows are clean (no torn lines)"
    assert len(seeds) == 3
    assert all(ss._is_seed_row(s) for s in seeds), "every ingested row is a valid seed"

    # The oldest unemitted seed drains through the real drip script as a seed
    # observation whose state is its id -- proof the rows are drain-ready.
    obs = _drip(store)
    assert obs["type"] == "seed"
    assert obs["state"] == obs["seed_id"]
    assert obs["cannon"] in {s["cannon"] for s in seeds}

    # That seed is now stamped emitted; the next drip drains the next one.
    emitted = [s for s in ss.load_seeds(store) if s["emitted_at"] is not None]
    assert len(emitted) == 1
    assert _drip(store)["type"] == "seed"
