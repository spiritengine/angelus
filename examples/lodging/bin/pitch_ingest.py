#!/usr/bin/env python3
"""Pitch -> seed-store ingest adapter.

Turns the plain-text output of the ``pitch`` mill chain into rows in the seed
store that the drip source (sources/scheduled/pitch-seeds.yaml) drains. This is
the WRITE side of brief-20260619-opi5 option C: Pitch scans on its own schedule
and writes ``scanner_today.md``; this adapter parses that text and appends one
seed per block via seed_store.append_seed. The drip then drains the oldest
unemitted seed per tick. The two sides share seed_store.py and never run in the
same process.

Usage:
    pitch_ingest.py STORE_PATH [INPUT_FILE]

Reads the pitch text from INPUT_FILE, or from stdin when it is omitted. Appends
each parseable block to the JSONL store at STORE_PATH (the live deployment points
this at ``state/pitch-seeds.jsonl`` under the lodging root).

The pitch text format (what ``mill crank pitch`` emits)
-------------------------------------------------------
Either the literal line ``Nothing notable today.`` (a no-op here), or up to ten
blocks delimited by lines of ``---``, each block::

    EVENT: [one line -- what changed]
    SOURCE: [primary url]
    DATE: [when it happened]

followed by a trailing ``NOISE FILTERED: [count, reasons]`` line (ignored -- it
carries none of the seed fields, so it parses to zero recognized fields and is
skipped silently, like any blank/non-block chunk).

Mapping to a seed (seed_store.make_seed):
    event <- EVENT      source <- SOURCE      date <- DATE
    seed_store keys dedup on the event, so re-ingesting the same scan is
    idempotent.

Defensive contract
-------------------
LLM output is not trusted to be well-formed. A chunk that carries at least one
recognized field but is missing EVENT (the one field a seed needs) is a
MALFORMED block: it is skipped with a stderr warning and counted, never fatal --
the remaining blocks still ingest. A chunk with no recognized fields at all (a
blank chunk, the NOISE FILTERED trailer, the ``Nothing notable today.`` line) is
not a block and is skipped silently. Dedup is automatic (append_seed is
idempotent on the event id), so re-running the adapter on the same text adds
nothing. A one-line summary (N ingested, M skipped, D duplicate) goes to stderr.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

# seed_store lives beside this script under <lodging>/bin/; make it importable
# without depending on the lodging root being on sys.path (mirrors
# pitch_seed_drip.py).
sys.path.insert(0, str(Path(__file__).resolve().parent))

import seed_store  # noqa: E402  (path injected above)

# The recognized field keys of a pitch block. A chunk that contains none of these
# is not a block (it is the NOISE FILTERED trailer, a blank chunk, or the
# "Nothing notable today." line) and is skipped silently. EVENT is the only field
# a seed actually needs; SOURCE and DATE are parsed-but-optional context.
FIELD_KEYS = frozenset({"EVENT", "SOURCE", "DATE"})

# The fields make_seed needs to build a seed. A chunk that looks like a block (has
# at least one recognized field) but lacks any of these is malformed -> skipped.
REQUIRED_FIELDS = ("EVENT",)

# A delimiter line: three or more dashes, nothing else (after stripping).
_DELIMITER = re.compile(r"-{3,}")


def _split_blocks(text: str) -> list[str]:
    """Split the pitch text into chunks on lines that are only dashes.

    A leading delimiter yields an empty first chunk and a trailing delimiter (or
    the NOISE FILTERED trailer past the last one) yields a chunk with no
    recognized fields; both are dropped downstream by _parse_block returning an
    empty dict, so the split does not need to special-case them.
    """
    chunks: list[str] = []
    current: list[str] = []
    for line in text.splitlines():
        if _DELIMITER.fullmatch(line.strip()):
            chunks.append("\n".join(current))
            current = []
        else:
            current.append(line)
    chunks.append("\n".join(current))
    return chunks


def _parse_block(chunk: str) -> dict[str, str]:
    """Extract the recognized ``KEY: value`` fields from one chunk.

    Lines that are not ``KEY: value`` with a recognized KEY are ignored (a wrapped
    continuation, a stray blank line, the NOISE FILTERED trailer). The KEY match
    is case-insensitive and whitespace-tolerant so minor LLM drift still parses.
    A later duplicate key wins (last write), which does not arise in well-formed
    output but is harmless.
    """
    fields: dict[str, str] = {}
    for line in chunk.splitlines():
        key, sep, value = line.partition(":")
        if not sep:
            continue
        key = key.strip().upper()
        if key in FIELD_KEYS:
            fields[key] = value.strip()
    return fields


def ingest(store: str | Path, text: str) -> tuple[int, int, int]:
    """Parse pitch text and append each good block to the store.

    Returns (ingested, skipped, duplicate): newly-appended seeds, malformed
    blocks skipped, and blocks whose event id was already in the store
    (append_seed dedup). Never raises on malformed block CONTENT -- a bad block
    (missing field, or a toxic field value make_seed rejects) is skipped, not
    fatal. It DOES propagate a SYSTEMIC store-write failure (append_seed raising
    on disk-full/permission/flock): that is not a per-block defect and must crash
    loud rather than vanish into a counted skip, so a parseable seed is never
    silently dropped. Re-running is idempotent, so a crash loses nothing.
    """
    ingested = 0
    skipped = 0
    duplicate = 0
    for chunk in _split_blocks(text):
        fields = _parse_block(chunk)
        if not fields:
            # No recognized fields: a blank chunk, the NOISE FILTERED trailer, or
            # the "Nothing notable today." line. Not a block -- skipped silently.
            continue
        missing = [name for name in REQUIRED_FIELDS if not fields.get(name)]
        if missing:
            sys.stderr.write(
                "pitch_ingest: skipping malformed block (missing "
                f"{', '.join(missing)})\n"
            )
            skipped += 1
            continue
        # Build the seed under a NARROW guard that catches only DATA-driven
        # failures -- a toxic field value that cannot become a seed. The file/stdin
        # read uses errors="surrogateescape", so an invalid UTF-8 byte in a field
        # value decodes to a lone surrogate that parses cleanly and passes the
        # required-field check above -- then seed_store.seed_id does a STRICT UTF-8
        # encode (hashlib.sha256(...encode())) and raises UnicodeEncodeError on that
        # surrogate. That is a property of THIS block's bytes, so it is skip-loud-
        # and-continue: unguarded it would abort the whole loop (blocks before the
        # toxic one already persisted, every block after it never ingests, summary
        # never prints), exactly like the missing-field skip. ValueError/TypeError
        # are caught alongside it as the other shapes a bad field value could take
        # if make_seed grows validation -- same data-classification class.
        #
        # append_seed is deliberately OUTSIDE this guard. Its failures are SYSTEMIC,
        # not per-block: disk full, permission denied, an flock failure in
        # write_seeds. Swallowing those would turn a store-write failure into a
        # counted "skipped block" + exit 0 -- silently dropping a parseable seed
        # with no operator alert, the exact miss-a-seed failure this drip exists to
        # prevent (and unlike seed_store's broad catch, this adapter has no
        # preserved-line + store_corrupt alert path -- only a stderr line cron may
        # not surface). So a store-write failure PROPAGATES: the run crashes loud
        # and non-zero. Re-running is idempotent (append_seed dedups on the
        # event id), so crash-then-fix-then-rerun loses nothing.
        try:
            seed = seed_store.make_seed(
                event=fields["EVENT"],
                source=fields.get("SOURCE", ""),
                date=fields.get("DATE", ""),
            )
            # seed_id (inside make_seed) strict-encodes EVENT, so a lone surrogate
            # there raises UnicodeEncodeError and is skipped-loud below. SOURCE and
            # DATE are now STORED too, but make_seed does not encode them -- so
            # strict-encode them here to keep the same graceful skip. Without this
            # a toxic byte in SOURCE/DATE ingests "clean", then trips the drain's
            # surrogate quarantine -> a store_corrupt urgent page over an optional
            # field. The encoded bytes are discarded; we only want the raise.
            seed["source"].encode()
            seed["date"].encode()
        except (UnicodeEncodeError, ValueError, TypeError) as exc:
            sys.stderr.write(
                "pitch_ingest: skipping unstorable block "
                f"(EVENT: {fields.get('EVENT')!r}): {type(exc).__name__}: {exc}\n"
            )
            skipped += 1
            continue
        added = seed_store.append_seed(store, seed)
        if added:
            ingested += 1
        else:
            duplicate += 1
    return ingested, skipped, duplicate


def main(argv: list[str]) -> int:
    if len(argv) < 2:
        sys.stderr.write("usage: pitch_ingest.py STORE_PATH [INPUT_FILE]\n")
        return 2
    store = argv[1]
    if len(argv) > 2:
        text = Path(argv[2]).read_text(encoding="utf-8", errors="surrogateescape")
    else:
        # Read stdin via the raw buffer with errors="surrogateescape" to match the
        # file path. sys.stdin.read() decodes STRICTLY, so an invalid UTF-8 byte in
        # the piped pitch text raises UnicodeDecodeError on the read itself -- before
        # the per-block guard in ingest() can ever see it -- crashing the deployed
        # invocation (the live caller pipes the scan over stdin). Decoding the raw
        # bytes leniently turns the bad byte into a lone surrogate the per-block
        # guard then skips loud, exactly as the file path does.
        text = sys.stdin.buffer.read().decode("utf-8", errors="surrogateescape")

    ingested, skipped, duplicate = ingest(store, text)
    sys.stderr.write(
        f"pitch_ingest: {ingested} seed(s) ingested, {skipped} skipped, "
        f"{duplicate} duplicate\n"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv))
