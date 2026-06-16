"""Install provenance for the running angelus distribution.

Post-cutover the daemon runs a NON-EDITABLE pip install of the engine
(`pip install git+file://<repo>@<sha>`). pip records PEP 610 provenance for
that install in ``direct_url.json``: the ``file://`` URL it was installed from
and, under ``vcs_info``, the exact ``commit_id`` it snapshotted. That commit id
IS the running code's sha -- provably, not inferred from process age.

Two readers live here, both fail-soft (they return ``None`` rather than raise,
so neither a daemon boot nor a digest render ever crashes on a missing or
malformed provenance record):

  * ``installed_commit_id()`` -- the sha the live install was built from. The
    daemon snapshots this to ``state/running-version`` once at boot (see
    daemon.run); belfry later compares that snapshot against the on-disk pin to
    detect code drift.
  * ``installed_repo()`` -- the engine repo the install came from, used by the
    daily digest's behind-master staleness finding to count undeployed commits.

This module mirrors the provenance-reading strategy in deploy/sre_runner.py,
which deliberately keeps its OWN copy: sre_runner runs from <lodging>/bin and
must stay import-independent of the angelus package (it has to work when the
installed code is broken). This copy is the one the in-process daemon imports.
Editable/dev checkouts carry no ``direct_url.json`` (or a ``dir_info``-only one
with no ``vcs_info``); every reader here returns ``None`` for that case, which
each caller treats as "no pin to compare against."
"""

from __future__ import annotations

import importlib.metadata
import json
import subprocess
import urllib.parse
from pathlib import Path

# Default age (days) the oldest un-deployed commit must exceed before the daily
# digest surfaces a behind-master staleness line. Master running ahead of the
# pin is the NORMAL post-cutover state (restarts are no longer deploys), so a
# short lag is uninteresting; 7d matches the 3spy staleness-digest threshold.
DEFAULT_STALENESS_THRESHOLD_DAYS = 7
_SECONDS_PER_DAY = 86400.0


def _direct_url() -> dict | None:
    """Parsed ``direct_url.json`` for the installed ``angelus`` distribution.

    Returns the decoded JSON object, or ``None`` on any failure: the package
    is not installed (PackageNotFoundError), the file is absent/unreadable
    (OSError), it is empty, or it does not parse / is not an object. Never
    raises -- callers depend on that to stay fail-soft.
    """
    try:
        dist = importlib.metadata.distribution("angelus")
        raw = dist.read_text("direct_url.json")
    except (importlib.metadata.PackageNotFoundError, OSError):
        return None
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    if not isinstance(data, dict):
        return None
    return data


def installed_commit_id() -> str | None:
    """The git sha the running angelus install was built from, or ``None``.

    Reads ``direct_url.json``'s ``vcs_info.commit_id`` -- the ref pip resolved
    and snapshotted for ``pip install git+file://<repo>@<sha>``. Returns
    ``None`` (never raises) on any failure: package not installed, missing or
    malformed direct_url, or no ``vcs_info`` (an editable/dev tree, or a
    non-VCS install). A non-empty string is returned verbatim; an empty or
    non-string commit_id is treated as absent.
    """
    data = _direct_url()
    if data is None:
        return None
    vcs_info = data.get("vcs_info")
    if not isinstance(vcs_info, dict):
        return None
    commit_id = vcs_info.get("commit_id")
    if not isinstance(commit_id, str) or not commit_id:
        return None
    return commit_id


def installed_repo() -> Path | None:
    """The engine repo the running install came from, or ``None``.

    Reads ``direct_url.json``'s ``url`` (a ``file://<repo>`` URL for the
    non-editable install) and returns it as a filesystem path. Returns
    ``None`` (never raises) when there is no provenance, the url is missing,
    not a string, not a ``file://`` URL, or unparseable. The caller is
    expected to validate that the path is a real engine checkout before using
    it -- this only decodes the recorded location.
    """
    data = _direct_url()
    if data is None:
        return None
    url = data.get("url", "")
    if not isinstance(url, str) or not url:
        return None
    try:
        parsed = urllib.parse.urlsplit(url)
    except ValueError:
        return None
    if parsed.scheme != "file" or not parsed.path:
        return None
    return Path(urllib.parse.unquote(parsed.path))


def _is_engine_repo(path: Path) -> bool:
    """Structural fingerprint of an engine checkout: a git tree carrying the
    angelus package and the Makefile. Guards against a provenance url that
    points somewhere stale, codeless (the lodging root), or half-removed --
    the same conflation that has bitten the belt repeatedly.

    Fail-soft: a malformed path (e.g. an embedded NUL, which makes the stdlib
    stat calls raise ValueError) returns ``False`` rather than propagating, so
    the digest staleness path can never crash on a weird provenance url."""
    try:
        return (
            path.is_dir()
            and (path / ".git").exists()
            and (path / "angelus" / "__init__.py").is_file()
            and (path / "Makefile").is_file()
        )
    except (OSError, ValueError):
        return False


def deploy_staleness(
    now_epoch: float,
    *,
    repo: Path | None = None,
    installed_sha: str | None = None,
    master_ref: str = "master",
    threshold_days: int = DEFAULT_STALENESS_THRESHOLD_DAYS,
) -> dict | None:
    """Behind-master staleness for the daily digest, or ``None``.

    When the installed pin trails ``master_ref`` by a commit older than
    ``threshold_days``, returns ``{"commits_behind": N, "oldest_days": M}`` --
    the data for a LOW-SEVERITY, never-pages digest line ("installed N commits
    behind master, oldest M days"). DIGEST-ONLY by construction: this reads
    git and returns a plain dict; it never writes a finding row, opens an
    incident, or flips belfry red. Master-ahead-of-pin is the normal
    post-cutover state, so this is an FYI, not an alarm.

    Fail-soft -- returns ``None`` (never raises) whenever:
      * the engine repo cannot be located from provenance or is not a valid
        engine checkout (e.g. an editable/dev tree, the codeless lodging root);
      * the installed pin sha is unknown (no provenance / editable);
      * git is unavailable, the ref does not resolve, or output is unparseable;
      * the pin is at or ahead of master (nothing behind);
      * the oldest un-deployed commit is younger than the threshold.

    ``repo`` and ``installed_sha`` are injectable for tests; in production both
    default to the install's PEP 610 provenance.
    """
    repo = repo if repo is not None else installed_repo()
    if repo is None or not _is_engine_repo(repo):
        return None
    installed_sha = (
        installed_sha if installed_sha is not None else installed_commit_id()
    )
    if not installed_sha:
        return None
    span = f"{installed_sha}..{master_ref}"
    try:
        count_proc = subprocess.run(
            ["git", "-C", str(repo), "rev-list", "--count", span],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if count_proc.returncode != 0:
            return None
        commits_behind = int(count_proc.stdout.strip())
        if commits_behind <= 0:
            return None
        # Oldest un-deployed commit = first entry of <installed>..<master> with
        # --reverse (oldest first); %ct is the committer epoch. Its age is how
        # long the stalest undeployed change has waited -- the figure the digest
        # reports as "oldest M days".
        log_proc = subprocess.run(
            ["git", "-C", str(repo), "log", "--reverse", "--format=%ct", span],
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if log_proc.returncode != 0:
            return None
        oldest_epoch = float(log_proc.stdout.strip().splitlines()[0])
    except (ValueError, IndexError, OSError, subprocess.SubprocessError):
        return None
    oldest_days = (now_epoch - oldest_epoch) / _SECONDS_PER_DAY
    if oldest_days < threshold_days:
        return None
    return {"commits_behind": commits_behind, "oldest_days": int(oldest_days)}
