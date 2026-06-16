"""Tests for angelus.provenance (deploy-identity / belfry drift rework,
brief-20260613-3spy piece 2).

The install-provenance readers are the trust root for the running-version
snapshot belfry compares against the pin, so they must extract the commit_id /
repo from a well-formed PEP 610 direct_url.json and FAIL-SOFT (return None,
never raise) on every malformed or absent shape. deploy_staleness is the
digest-only behind-master FYI: it must emit only when the pin trails master
past the threshold and never when the repo can't be located.
"""

from __future__ import annotations

import importlib.metadata
import subprocess
from pathlib import Path
from unittest.mock import patch

from angelus import provenance


class _Dist:
    """Minimal stand-in for importlib.metadata.Distribution: only read_text
    (direct_url.json) is exercised. A None payload simulates an absent file
    (read_text returns None), a string a present one."""

    def __init__(self, payload):
        self._payload = payload

    def read_text(self, name):
        assert name == "direct_url.json"
        return self._payload


def _with_dist(payload):
    return patch.object(
        importlib.metadata, "distribution", return_value=_Dist(payload)
    )


# ---------------------------------------------------------------------------
# installed_commit_id
# ---------------------------------------------------------------------------

def test_installed_commit_id_from_well_formed_direct_url() -> None:
    """A git+file install records vcs_info.commit_id -> that sha verbatim."""
    payload = (
        '{"url": "file:///home/p/projects/angelus", '
        '"vcs_info": {"vcs": "git", "commit_id": "deadbeef0001"}}'
    )
    with _with_dist(payload):
        assert provenance.installed_commit_id() == "deadbeef0001"


def test_installed_commit_id_package_not_found_is_none() -> None:
    """No installed distribution -> None, never raises."""
    with patch.object(
        importlib.metadata,
        "distribution",
        side_effect=importlib.metadata.PackageNotFoundError,
    ):
        assert provenance.installed_commit_id() is None


def test_installed_commit_id_missing_or_empty_direct_url_is_none() -> None:
    """An absent direct_url.json (read_text -> None) and an empty one both
    yield None."""
    for payload in (None, ""):
        with _with_dist(payload):
            assert provenance.installed_commit_id() is None


def test_installed_commit_id_malformed_or_no_vcs_info_is_none() -> None:
    """Unparseable JSON, a non-object document, a direct_url with no vcs_info
    (an editable/dev tree), and an empty/non-string commit_id all fail soft."""
    for payload in (
        "not json at all",                              # unparseable
        "[1, 2, 3]",                                    # JSON but not an object
        '{"url": "file:///x", "dir_info": {"editable": true}}',  # editable, no vcs
        '{"url": "file:///x", "vcs_info": {"vcs": "git"}}',      # no commit_id
        '{"url": "file:///x", "vcs_info": {"commit_id": ""}}',   # empty commit_id
        '{"url": "file:///x", "vcs_info": {"commit_id": 5}}',    # non-string
        '{"url": "file:///x", "vcs_info": "notadict"}',          # vcs_info wrong type
    ):
        with _with_dist(payload):
            assert provenance.installed_commit_id() is None, payload


# ---------------------------------------------------------------------------
# installed_repo
# ---------------------------------------------------------------------------

def test_installed_repo_from_file_url() -> None:
    """The file:// url decodes to the engine repo path."""
    payload = (
        '{"url": "file:///home/p/projects/angelus", '
        '"vcs_info": {"commit_id": "abc"}}'
    )
    with _with_dist(payload):
        assert provenance.installed_repo() == Path("/home/p/projects/angelus")


def test_installed_repo_malformed_is_none() -> None:
    """A non-file scheme, non-string url, bad authority, missing url, and an
    absent direct_url all return None."""
    for payload in (
        '{"url": "git+file:///home/p/angelus"}',   # scheme not file
        '{"url": 123}',                            # non-string
        '{"url": "file://["}',                     # bad authority -> ValueError
        '{"no_url": "x"}',                         # missing url
        None,                                      # no direct_url file
    ):
        with _with_dist(payload):
            assert provenance.installed_repo() is None, payload


# ---------------------------------------------------------------------------
# deploy_staleness (behind-master digest FYI)
# ---------------------------------------------------------------------------

def _git(repo: Path, *args: str) -> str:
    env = {
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
    }
    return subprocess.run(
        ["git", "-C", str(repo), *args],
        check=True,
        capture_output=True,
        text=True,
        env={**_os_environ(), **env},
    ).stdout.strip()


def _os_environ() -> dict:
    import os

    return dict(os.environ)


def _make_engine_repo(tmp_path: Path) -> Path:
    """A repo carrying the engine fingerprint (_is_engine_repo): .git, the
    angelus package marker, and a Makefile."""
    repo = tmp_path / "engine"
    repo.mkdir()
    _git(repo, "init", "-q", "-b", "master")
    (repo / "Makefile").write_text("deploy:\n\t@true\n", encoding="utf-8")
    (repo / "angelus").mkdir()
    (repo / "angelus" / "__init__.py").write_text("", encoding="utf-8")
    return repo


def _commit(repo: Path, message: str, when_epoch: int) -> str:
    """Commit the current tree at a fixed author+committer date, return the sha."""
    iso = f"@{when_epoch} +0000"  # git's raw-epoch date form
    import os

    env = {
        **os.environ,
        "GIT_AUTHOR_NAME": "t",
        "GIT_AUTHOR_EMAIL": "t@t",
        "GIT_COMMITTER_NAME": "t",
        "GIT_COMMITTER_EMAIL": "t@t",
        "GIT_AUTHOR_DATE": iso,
        "GIT_COMMITTER_DATE": iso,
    }
    subprocess.run(
        ["git", "-C", str(repo), "add", "-A"], check=True, capture_output=True
    )
    subprocess.run(
        ["git", "-C", str(repo), "commit", "-q", "-m", message],
        check=True,
        capture_output=True,
        env=env,
    )
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()


# A day in seconds for readable fixtures.
_DAY = 86400


def test_deploy_staleness_emits_when_pin_trails_master_past_threshold(
    tmp_path,
) -> None:
    """Installed pin 3 commits behind master, oldest undeployed commit 10 days
    old, now beyond the 7d threshold -> a {commits_behind, oldest_days} dict."""
    repo = _make_engine_repo(tmp_path)
    now = 100 * _DAY
    (repo / "a").write_text("1", encoding="utf-8")
    pin = _commit(repo, "pinned base", now - 20 * _DAY)
    # Three undeployed commits; the OLDEST is 10 days before now.
    (repo / "a").write_text("2", encoding="utf-8")
    _commit(repo, "undeployed 1", now - 10 * _DAY)
    (repo / "a").write_text("3", encoding="utf-8")
    _commit(repo, "undeployed 2", now - 5 * _DAY)
    (repo / "a").write_text("4", encoding="utf-8")
    _commit(repo, "undeployed 3", now - 1 * _DAY)

    result = provenance.deploy_staleness(now, repo=repo, installed_sha=pin)
    assert result == {"commits_behind": 3, "oldest_days": 10}


def test_deploy_staleness_none_within_threshold(tmp_path) -> None:
    """Pin behind master but the OLDEST undeployed commit is younger than 7d ->
    no finding (master-ahead is the normal post-cutover state)."""
    repo = _make_engine_repo(tmp_path)
    now = 100 * _DAY
    (repo / "a").write_text("1", encoding="utf-8")
    pin = _commit(repo, "pinned base", now - 6 * _DAY)
    (repo / "a").write_text("2", encoding="utf-8")
    _commit(repo, "undeployed", now - 3 * _DAY)

    assert provenance.deploy_staleness(now, repo=repo, installed_sha=pin) is None


def test_deploy_staleness_none_when_pin_at_master(tmp_path) -> None:
    """Pin == master HEAD -> nothing behind -> None (no negative/zero counts)."""
    repo = _make_engine_repo(tmp_path)
    now = 100 * _DAY
    (repo / "a").write_text("1", encoding="utf-8")
    head = _commit(repo, "only commit", now - 30 * _DAY)
    assert provenance.deploy_staleness(now, repo=repo, installed_sha=head) is None


def test_deploy_staleness_none_when_repo_invalid(tmp_path) -> None:
    """A path that is not a valid engine checkout (no .git/angelus/Makefile) ->
    None, fail-soft. Guards against handing a stale/codeless path to git."""
    not_engine = tmp_path / "lodging"
    not_engine.mkdir()
    assert (
        provenance.deploy_staleness(
            100 * _DAY, repo=not_engine, installed_sha="abc"
        )
        is None
    )


def test_deploy_staleness_none_when_repo_unlocatable(tmp_path, monkeypatch) -> None:
    """No repo passed and provenance can't locate one (installed_repo -> None,
    e.g. an editable/dev tree) -> None, never raises, never a finding."""
    monkeypatch.setattr(provenance, "installed_repo", lambda: None)
    assert provenance.deploy_staleness(100 * _DAY) is None


def test_deploy_staleness_none_when_pin_unknown(tmp_path, monkeypatch) -> None:
    """A valid repo but no installed pin sha (installed_commit_id -> None) ->
    None: nothing to diff against."""
    repo = _make_engine_repo(tmp_path)
    monkeypatch.setattr(provenance, "installed_commit_id", lambda: None)
    assert provenance.deploy_staleness(100 * _DAY, repo=repo) is None


def test_deploy_staleness_none_on_git_failure(tmp_path) -> None:
    """An unknown installed sha makes `git rev-list <bad>..master` exit
    non-zero -> fail-soft None, not a crash."""
    repo = _make_engine_repo(tmp_path)
    now = 100 * _DAY
    (repo / "a").write_text("1", encoding="utf-8")
    _commit(repo, "base", now - 30 * _DAY)
    assert (
        provenance.deploy_staleness(now, repo=repo, installed_sha="0" * 40) is None
    )


def test_is_engine_repo_malformed_path_is_false() -> None:
    """A path the stdlib stat calls choke on (an embedded NUL raises ValueError)
    must return False, not propagate -- the prelude runs BEFORE deploy_staleness'
    git try-block, so a raise here would crash the digest."""
    from pathlib import Path

    assert provenance._is_engine_repo(Path("bad\x00path")) is False


def test_deploy_staleness_none_on_malformed_repo_path() -> None:
    """End-to-end fail-soft: a malformed repo path returns None (via the
    hardened _is_engine_repo) rather than raising into the digest."""
    from pathlib import Path

    assert (
        provenance.deploy_staleness(
            100 * _DAY, repo=Path("bad\x00path"), installed_sha="abc"
        )
        is None
    )
