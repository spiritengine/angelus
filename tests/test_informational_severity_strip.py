"""The chronicler must not frame informational items by their routing severity.

A HOT build seed is stored with severity="high" purely so it can escalate to the
`now` pipe (pitch_ingest maps URGENCY HOT -> "high"). Left in the JSON the
chronicler reads, that field made the digest synthesis describe a build seed as
"high-severity" -- incident framing on an opportunity. _render_llm_body drops
`severity` from the informational items it feeds the chronicler (only);
operational inputs keep it, and the shared structured data is untouched.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import angelus.pipes.runner as pipe_runner
from angelus.lodging import Pipe, Channel
from angelus.pipes import PipeDrain
from angelus.storage import Catalog, init_db


def _daily_pipe() -> Pipe:
    return Pipe(
        name="daily",
        cadence="0 8 * * *",
        render_kind="digest",
        template=None,
        channels=["push"],
        render={
            "preamble": [],
            "body": {
                "kind": "llm",
                "mantle": "chronicler",
                "inputs": [
                    "findings_since_last_drain",
                    "informational_since_last_drain",
                ],
            },
        },
    )


class _FakeProc:
    def __init__(self, stdout: bytes):
        self._stdout = stdout
        self.returncode = 0

    async def communicate(self):
        return self._stdout, b""

    def kill(self):  # pragma: no cover - happy path never reaps
        pass

    async def wait(self):  # pragma: no cover
        return 0


def test_chronicler_input_drops_informational_severity(tmp_path, monkeypatch):
    # Hermetic: don't shell real git for deploy staleness.
    monkeypatch.setattr(pipe_runner, "deploy_staleness", lambda *a, **k: None)

    connection = init_db(tmp_path / "angelus.sqlite3")
    catalog = Catalog(connection, tmp_path)

    # Operational finding (keeps severity) + an informational seed whose HOT
    # routing maps to severity="high" (must NOT reach the chronicler as severity).
    catalog.write_finding(
        None,
        {"source": "scheduled/test", "type": "down", "entity": "site",
         "severity": "high", "target_pipes": ["daily"]},
        {"daily"},
    )
    catalog.write_finding(
        None,
        {"source": "scheduled/pitch", "type": "seed", "entity": "ShareLock",
         "severity": "high", "target_pipes": ["daily"]},
        {"daily"},
        lane="informational",
    )

    pipe = _daily_pipe()
    drain = PipeDrain(
        catalog, pipe,
        {"push": Channel(name="push", kind="push", command="notify-pat")},
        tmp_path, {"now", "daily"},
    )
    structured = drain._structured_inputs(pipe, None)

    captured: dict[str, str] = {}

    async def fake_exec(*argv, **kwargs):
        argv = list(argv)
        mf = argv[argv.index("--message-file") + 1]
        captured["prompt"] = Path(mf).read_text(encoding="utf-8")
        return _FakeProc(
            json.dumps({"result": "All quiet; a build seed arrived."}).encode()
        )

    monkeypatch.setattr(asyncio, "create_subprocess_exec", fake_exec)

    try:
        output, err = asyncio.run(drain._render_llm_body(pipe, structured))
    finally:
        connection.close()

    assert err is None
    assert output == "All quiet; a build seed arrived."

    # The JSON the chronicler was actually handed.
    payload = json.loads(captured["prompt"].split("Structured inputs (JSON):\n", 1)[1])

    info = payload["informational_since_last_drain"]
    assert len(info) == 1
    assert info[0]["entity"] == "ShareLock"
    assert "severity" not in info[0]                       # dropped for the chronicler

    ops = payload["findings_since_last_drain"]
    assert len(ops) == 1
    assert ops[0]["severity"] == "high"                    # operational keeps it

    # Shared structured data is untouched -- the preamble still renders severity.
    assert structured["informational_since_last_drain"][0]["severity"] == "high"
