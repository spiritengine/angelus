"""Pitch -> seed-store ingest adapter (brief-20260619-opi5 option C, write side).

pitch_ingest.py parses the plain text the ``pitch`` mill chain emits and appends
one seed per block to the store that the drip drains. Coverage:
  - a multi-seed sample -> correct rows (event/source/date), correct dedup id.
  - source/date optional -> a block with only EVENT still ingests.
  - "Nothing notable today." (and empty input) -> store untouched.
  - a malformed block (missing EVENT) among good ones -> skipped, the good ones
    still ingest, no crash.
  - a systemic store-write failure is NOT swallowed (crash loud, non-zero).
  - an invalid-UTF-8 byte in a block value -> skipped loud, later blocks ingest.
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


# A realistic three-block sample plus the NOISE FILTERED trailer the chain
# appends.
SAMPLE = """\
EVENT: New frontier model shipped with a longer context window
SOURCE: https://example.com/model
DATE: 2026-06-20
---
EVENT: Wayland a11y protocol draft landed
SOURCE: https://example.com/wayland
DATE: 2026-06-19
---
EVENT: Prediction-market platform added a new market class
SOURCE: https://example.com/market
DATE: 2026-06-18
---
NOISE FILTERED: 9 (commentary-only, no primary source, already covered)
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
# multi-seed parse -> event/source/date rows
# --------------------------------------------------------------------------


def test_multi_seed_sample_ingests(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ingested, skipped, duplicate = ingest_mod.ingest(store, SAMPLE)
    assert (ingested, skipped, duplicate) == (3, 0, 0)

    seeds = ss.load_seeds(store)
    by_event = {s["event"]: s for s in seeds}
    assert len(seeds) == 3

    first = by_event["New frontier model shipped with a longer context window"]
    assert first["source"] == "https://example.com/model"
    assert first["date"] == "2026-06-20"
    # The id is the event digest seed_store keys dedup on.
    assert first["id"] == ss.seed_id(first["event"])


def test_source_and_date_optional(tmp_path: Path) -> None:
    """EVENT is the only required field; a block with no SOURCE/DATE still ingests
    (they default to empty strings)."""
    store = tmp_path / "seeds.jsonl"
    text = (
        "EVENT: a change with no source line\n"
        "---\n"
        "EVENT: a change with only a date\n"
        "DATE: 2026-06-20\n"
    )
    ingested, skipped, duplicate = ingest_mod.ingest(store, text)
    assert (ingested, skipped, duplicate) == (2, 0, 0)
    seeds = {s["event"]: s for s in ss.load_seeds(store)}
    assert seeds["a change with no source line"]["source"] == ""
    assert seeds["a change with no source line"]["date"] == ""
    assert seeds["a change with only a date"]["date"] == "2026-06-20"


# --------------------------------------------------------------------------
# "Nothing notable today." / empty -> no-op
# --------------------------------------------------------------------------


def test_nothing_notable_leaves_store_untouched(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    ingested, skipped, duplicate = ingest_mod.ingest(store, "Nothing notable today.\n")
    assert (ingested, skipped, duplicate) == (0, 0, 0)
    assert not store.exists(), "a no-seed scan writes nothing to the store"
    assert ss.load_seeds(store) == []


def test_empty_input_is_a_noop(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    assert ingest_mod.ingest(store, "") == (0, 0, 0)
    assert ingest_mod.ingest(store, "\n   \n") == (0, 0, 0)
    assert not store.exists()


# --------------------------------------------------------------------------
# malformed block (missing EVENT) among good ones -> skipped, not fatal
# --------------------------------------------------------------------------


def test_malformed_block_missing_event_is_skipped(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    text = (
        "EVENT: good one\n"
        "SOURCE: https://example.com/a\n"
        "---\n"
        "SOURCE: https://example.com/no-event\n"
        "DATE: 2026-06-20\n"
        "---\n"
        "EVENT: another good one\n"
        "SOURCE: https://example.com/b\n"
    )
    ingested, skipped, duplicate = ingest_mod.ingest(store, text)
    assert ingested == 2, "the two complete blocks ingest"
    assert skipped == 1, "the block missing EVENT is skipped, not fatal"
    assert duplicate == 0
    events = {s["event"] for s in ss.load_seeds(store)}
    assert events == {"good one", "another good one"}


def test_cli_skips_malformed_and_warns(tmp_path: Path) -> None:
    """The CLI path stays exit-0 over a malformed block and warns on stderr."""
    store = tmp_path / "seeds.jsonl"
    text = (
        "EVENT: good\n"
        "SOURCE: https://example.com/g\n"
        "---\n"
        "SOURCE: https://example.com/no-event\n"
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
    text = "EVENT: a perfectly good block\nSOURCE: https://example.com/x\n"

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
    text = "EVENT: a perfectly good block\nSOURCE: https://example.com/x\n"
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
    b"---\n"
    b"EVENT: good block after the toxic one\n"
    b"SOURCE: https://example.com/good\n"
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


# A toxic byte in the now-STORED SOURCE (an OPTIONAL field), followed by a good
# block. EVENT is strict-encoded via seed_id; SOURCE/DATE are strict-encoded at
# ingest too so a bad byte there is a graceful skip-loud here -- NOT a clean
# ingest that later trips the drain's surrogate quarantine and pages
# store_corrupt (urgent) over an optional field.
TOXIC_SOURCE_THEN_GOOD = (
    b"EVENT: a clean event line\n"
    b"SOURCE: https://example.com/\xff/toxic\n"
    b"DATE: 2026-06-20\n"
    b"---\n"
    b"EVENT: good block after the toxic source\n"
    b"SOURCE: https://example.com/good\n"
)


def test_toxic_byte_in_source_skipped_at_ingest(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    result = _run_ingest_stdin_bytes(store, TOXIC_SOURCE_THEN_GOOD)
    assert result.returncode == 0, "a toxic SOURCE byte must not crash the run"
    stderr = result.stderr.decode("utf-8", errors="replace")
    assert "skipping unstorable block" in stderr, "the toxic-source block is skipped loud"
    assert "1 seed(s) ingested, 1 skipped" in stderr, "summary printed; toxic counted"

    seeds = ss.load_seeds(store)
    assert len(seeds) == 1, "only the clean block landed; the toxic-source block skipped"
    assert seeds[0]["event"] == "good block after the toxic source"
    # Nothing toxic reached the store, so a later drain raises no store_corrupt.
    _, malformed = ss._parse_store(store)
    assert malformed == [], "no torn row left behind -> no drain-time corruption page"


# --------------------------------------------------------------------------
# idempotency: same text twice -> no duplicate rows
# --------------------------------------------------------------------------


def test_ingest_is_idempotent(tmp_path: Path) -> None:
    store = tmp_path / "seeds.jsonl"
    first = ingest_mod.ingest(store, SAMPLE)
    assert first == (3, 0, 0)

    # The same scan again: append_seed dedups on the event id, so no new rows
    # land -- every block reports as a duplicate, none ingested.
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
    assert obs["event"] in {s["event"] for s in seeds}

    # That seed is now stamped emitted; the next drip drains the next one.
    emitted = [s for s in ss.load_seeds(store) if s["emitted_at"] is not None]
    assert len(emitted) == 1
    assert _drip(store)["type"] == "seed"
