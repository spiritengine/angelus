"""B16 one-env-file: state/angelus.env is the single source of truth for
non-secret config, loaded identically by the daemon and the belfry.

Covers the parser, the non-override precedence rule (explicit env > file), the
missing-file no-op, the incident-reproducing acceptance (ANGELUS_EMAIL_TO unset
in the shell but present in the file still yields a usable recipient), and that
belfry's dependency-free loader behaves the same way.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

from angelus.channels.email import _resolve_to
import angelus.envfile as envfile
from angelus.envfile import (
    env_file_path,
    load_env_file,
    parse_env_file,
    resolve_op_refs,
)

REPO_ROOT = Path(__file__).resolve().parents[1]
BELFRY_PATH = REPO_ROOT / "belfry" / "belfry.py"


def _load_belfry():
    spec = importlib.util.spec_from_file_location("belfry_under_test_b16", BELFRY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _write_env(root: Path, body: str) -> None:
    (root / "state").mkdir(parents=True, exist_ok=True)
    env_file_path(root).write_text(body, encoding="utf-8")


def test_parse_handles_comments_blanks_export_and_quotes():
    parsed = parse_env_file(
        "\n"
        "# a comment\n"
        "ANGELUS_EMAIL_TO=you@example.com\n"
        "   \n"
        "export ANGELUS_BELFRY_SUCCESS_URL=https://hc-ping.com/abc\n"
        'QUOTED="quoted value"\n'
        "SINGLE='single value'\n"
        "no_equals_sign_line\n"
        "ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC=18000\n"
    )
    assert parsed == {
        "ANGELUS_EMAIL_TO": "you@example.com",
        "ANGELUS_BELFRY_SUCCESS_URL": "https://hc-ping.com/abc",
        "QUOTED": "quoted value",
        "SINGLE": "single value",
        "ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC": "18000",
    }


def test_load_applies_missing_names(tmp_path, monkeypatch):
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)
    _write_env(tmp_path, "ANGELUS_EMAIL_TO=from-file@example.com\n")

    applied = load_env_file(tmp_path)

    import os

    assert os.environ["ANGELUS_EMAIL_TO"] == "from-file@example.com"
    assert applied == {"ANGELUS_EMAIL_TO": "from-file@example.com"}


def test_explicit_env_wins_over_file(tmp_path, monkeypatch):
    # Precedence: a name already set in the environment is never overwritten.
    monkeypatch.setenv("ANGELUS_EMAIL_TO", "from-shell@example.com")
    _write_env(tmp_path, "ANGELUS_EMAIL_TO=from-file@example.com\n")

    applied = load_env_file(tmp_path)

    import os

    assert os.environ["ANGELUS_EMAIL_TO"] == "from-shell@example.com"
    assert "ANGELUS_EMAIL_TO" not in applied


def test_missing_file_is_a_noop(tmp_path):
    assert load_env_file(tmp_path) == {}


def test_recipient_resolves_from_file_when_shell_unset(tmp_path, monkeypatch):
    # Incident reproducer: ANGELUS_EMAIL_TO is absent from the shell (the
    # daemon was relaunched outside systemd) but present in state/angelus.env.
    # After loading, the email channel still resolves a usable recipient.
    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)
    with pytest.raises(RuntimeError):
        _resolve_to("$env:ANGELUS_EMAIL_TO")

    _write_env(tmp_path, "ANGELUS_EMAIL_TO=daily@example.com\n")
    load_env_file(tmp_path)

    assert _resolve_to("$env:ANGELUS_EMAIL_TO") == "daily@example.com"


def test_committed_example_parses_and_lists_the_documented_keys():
    example = (REPO_ROOT / "state" / "angelus.env.example").read_text(encoding="utf-8")
    parsed = parse_env_file(example)
    # The three uncommented keys are the live defaults a fresh copy needs.
    assert "ANGELUS_EMAIL_TO" in parsed
    assert "ANGELUS_BELFRY_SUCCESS_URL" in parsed
    assert "ANGELUS_BELFRY_DOWN_URL" in parsed


def test_belfry_loader_matches_semantics(tmp_path, monkeypatch):
    belfry = _load_belfry()
    state = tmp_path / "state"
    state.mkdir()
    (state / "angelus.env").write_text(
        "ANGELUS_EMAIL_TO=from-file@example.com\n"
        "ANGELUS_BELFRY_DOWN_URL=https://hc-ping.com/down\n",
        encoding="utf-8",
    )

    monkeypatch.delenv("ANGELUS_EMAIL_TO", raising=False)
    monkeypatch.setenv("ANGELUS_BELFRY_DOWN_URL", "https://hc-ping.com/shell-wins")

    applied = belfry.load_env_file(state)

    import os

    # Missing name filled from the file...
    assert os.environ["ANGELUS_EMAIL_TO"] == "from-file@example.com"
    assert applied == {"ANGELUS_EMAIL_TO": "from-file@example.com"}
    # ...explicitly-set name preserved (precedence).
    assert os.environ["ANGELUS_BELFRY_DOWN_URL"] == "https://hc-ping.com/shell-wins"


def test_belfry_missing_file_is_a_noop(tmp_path):
    belfry = _load_belfry()
    assert belfry.load_env_file(tmp_path / "state") == {}


# --- op:// secret-ref resolution (daemon-only; belfry never calls this) -------

def test_resolve_op_refs_resolves_with_token(monkeypatch):
    monkeypatch.setattr(envfile, "_op_read", lambda ref: "https://hc-ping.com/uuid")
    env = {
        "OP_SERVICE_ACCOUNT_TOKEN": "ops_tok",
        "ANGELUS_DIGEST_HEARTBEAT_URL": "op://angelus/healthchecks-digest/credential",
        "ANGELUS_EMAIL_TO": "x@example.com",
    }
    resolved = resolve_op_refs(env)
    assert env["ANGELUS_DIGEST_HEARTBEAT_URL"] == "https://hc-ping.com/uuid"
    assert env["ANGELUS_EMAIL_TO"] == "x@example.com"  # literal untouched
    assert resolved == {
        "ANGELUS_DIGEST_HEARTBEAT_URL": "op://angelus/healthchecks-digest/credential"
    }


def test_resolve_op_refs_no_token_unsets_ref(monkeypatch):
    # Fail-safe: a ref with no service-account token is unset, never left as a
    # literal "op://..." that a consumer would treat as a real value.
    called = []
    monkeypatch.setattr(envfile, "_op_read", lambda ref: called.append(ref) or "x")
    env = {"ANGELUS_DIGEST_HEARTBEAT_URL": "op://angelus/healthchecks-digest/credential"}
    resolved = resolve_op_refs(env)
    assert "ANGELUS_DIGEST_HEARTBEAT_URL" not in env
    assert resolved == {}
    assert called == []  # never shells out without a token


def test_resolve_op_refs_read_failure_unsets(monkeypatch):
    def boom(ref):
        raise RuntimeError("op read failed")

    monkeypatch.setattr(envfile, "_op_read", boom)
    env = {
        "OP_SERVICE_ACCOUNT_TOKEN": "ops_tok",
        "ANGELUS_DIGEST_HEARTBEAT_URL": "op://angelus/healthchecks-digest/credential",
    }
    resolved = resolve_op_refs(env)
    assert "ANGELUS_DIGEST_HEARTBEAT_URL" not in env  # fail-safe unset
    assert resolved == {}


def test_resolve_op_refs_noop_without_refs(monkeypatch):
    monkeypatch.setattr(
        envfile, "_op_read", lambda ref: (_ for _ in ()).throw(AssertionError("called"))
    )
    env = {"ANGELUS_EMAIL_TO": "x@example.com"}
    assert resolve_op_refs(env) == {}
    assert env == {"ANGELUS_EMAIL_TO": "x@example.com"}


def test_op_read_missing_binary_raises(monkeypatch):
    monkeypatch.setattr(envfile.shutil, "which", lambda _name: None)
    with pytest.raises(RuntimeError, match="not on PATH"):
        envfile._op_read("op://angelus/healthchecks-digest/credential")


def test_op_read_empty_value_raises(monkeypatch):
    import subprocess as sp

    monkeypatch.setattr(envfile.shutil, "which", lambda _name: "/usr/bin/op")
    monkeypatch.setattr(
        envfile.subprocess,
        "run",
        lambda *a, **k: sp.CompletedProcess(a[0], 0, stdout="\n", stderr=""),
    )
    with pytest.raises(RuntimeError, match="empty"):
        envfile._op_read("op://angelus/healthchecks-digest/credential")


def test_op_read_nonzero_surfaces_stderr(monkeypatch):
    import subprocess as sp

    monkeypatch.setattr(envfile.shutil, "which", lambda _name: "/usr/bin/op")
    monkeypatch.setattr(
        envfile.subprocess,
        "run",
        lambda *a, **k: sp.CompletedProcess(a[0], 1, stdout="", stderr="vault not found"),
    )
    with pytest.raises(RuntimeError, match="vault not found"):
        envfile._op_read("op://angelus/healthchecks-digest/credential")


def test_op_read_preserves_internal_whitespace(monkeypatch):
    # Only the trailing newline op appends is stripped; a value with significant
    # surrounding spaces (a future password) survives.
    import subprocess as sp

    monkeypatch.setattr(envfile.shutil, "which", lambda _name: "/usr/bin/op")
    monkeypatch.setattr(
        envfile.subprocess,
        "run",
        lambda *a, **k: sp.CompletedProcess(a[0], 0, stdout="  pa ss  \n", stderr=""),
    )
    assert envfile._op_read("op://v/i/f") == "  pa ss  "
