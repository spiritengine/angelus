#!/usr/bin/env python3
"""The angelus release path: a deliberate, preflighted, reversible deploy.

Spec: brief-20260613-3spy (release-path v3). This script is the meat behind
`make deploy`; operators and agents never touch pip/systemctl/sqlite directly.

Why it exists -- the incident pattern this closes:
  * 2026-05-29: a hand-launched daemon drifted from the systemd unit's config.
  * 2026-06-07: a wedge/restart loop.
  * 2026-06-12: migration 0015 dropped+rebuilt `observations` under
    PRAGMA foreign_keys=ON against a LIVE db whose findings referenced
    observation rows -> IntegrityError -> outage. Every restart silently
    deployed dev-tree master and ran its migrations against the live db
    unpreflighted.

The fix: a release is a normal non-editable `pip install git+file://<repo>@<sha>`
(pip snapshots the ref into site-packages; restarts stop being deploys), gated
by a migration dry-run that runs the TARGET ref's migrations against a
backup-API copy of the live db BEFORE the live db is ever touched.

Stdlib only, and it imports nothing from the `angelus` package: this must run
when the installed code is broken. The target ref's own code (migrations
runner, config loader) is exercised by shelling a subprocess whose cwd is a
tmp checkout of that ref -- ref-consistency, not the dev working tree.

Recovery model is idempotence, NOT a state machine. Every step is rerunnable;
a deploy that died anywhere is fixed by running `make deploy REF=<sha>` again
with the same ref. On any failure this reports loudly and LEAVES the belfry
hold in place to self-expire (see ANGELUS_BELFRY_HOLD_MAX_AGE_SEC in belfry) --
belfry will not race the operator's rerun with its own restart, and a forgotten
hold is overridden + alerted after the max age.

Every external touchpoint is parameterized via env (real defaults in
build_config); the test suite drives the whole lifecycle against a fake prod.
"""

from __future__ import annotations

import argparse
import os
import re
import shlex
import shutil
import sqlite3
import subprocess
import sys
import sysconfig
import tarfile
import tempfile
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from io import BytesIO
from pathlib import Path


# The engine repo this deploy.py ships in. Derived from __file__ so the deploy
# logic deploying ref X is itself the current dev-tree copy -- no
# self-replacement bootstrap. `make deploy` runs from the dev tree on purpose.
CODE_ROOT = Path(__file__).resolve().parent.parent

# Real deployment defaults. The live daemon's working directory (state/, logs,
# sqlite, control socket) is the lodging root, NOT this repo (see CLAUDE.md).
DEFAULT_LODGING = Path.home() / "projects" / "angelus-lodging"
DEFAULT_DEFAULT_REF = "master"
DEFAULT_SYSTEMCTL = "systemctl"
DEFAULT_SYSTEMCTL_SCOPE = "--user"  # matches belfry's `systemctl --user`
DEFAULT_UNIT = "angelus"
DEFAULT_HEALTH_CMD = "angelus health"
DEFAULT_NOTIFY_CMD = "notify-pat"
DEFAULT_BACKUPS_KEEP = 5
DEFAULT_VERIFY_TIMEOUT_SEC = 60
DEFAULT_VERIFY_POLL_SEC = 2
# The unit (angelus.service) is Type=simple, Restart=on-failure, RestartSec=5.
# systemd marks a Type=simple unit "active" the instant it forks -- before the
# daemon has initialised -- so a crash-at-startup (the 0015 incident class)
# cycles active(~1-2s)->activating(auto-restart waits RestartSec)->active and is
# briefly "active" on every loop. verify must therefore confirm the unit stays
# continuously active for a span EXCEEDING RestartSec, so a crash-restart inside
# the window is always caught (either an activating poll or NRestarts>0). 8 > 5.
DEFAULT_VERIFY_MIN_ACTIVE_SEC = 8
DEFAULT_SYSTEMCTL_TIMEOUT_SEC = 30

# Mirror belfry's hold sentinel filename/env so deploy WRITES where belfry
# READS. ANGELUS_BELFRY_HOLD_PATH (belfry's own override) wins for both sides.
DEFAULT_HOLD_FILENAME = "belfry-hold"

MIGRATION_RE = re.compile(r"^\d{4}_.+\.sql$")

# Subprocess run in the tmp checkout (cwd-first import => the target ref's
# angelus + its migrations dir). The argv is the absolute path to the
# backup-API copy of the live db; init_db applies the target migrations to it.
_DRYRUN_SNIPPET = (
    "import sys\n"
    "from angelus.storage import init_db\n"
    "conn = init_db(sys.argv[1])\n"
    "conn.close()\n"
)

# Post-install check: the FRESHLY INSTALLED package must be able to LOCATE its
# bundled migrations. The dry-run above proves the migrations APPLY, but it runs
# from the tmp checkout (cwd-import), so it can't see whether the installed wheel
# actually carries migrations -- the exact gap that crash-looped the 2026-06-13
# cutover (finding-20260613-mbfc). This imports the installed package and exits
# nonzero if it resolves a migrations dir with no .sql in it.
#
# argv[1] is cfg.repo (the dev tree). The snippet first proves it imported the
# INSTALLED copy, not the dev tree: if either the imported angelus package or its
# resolved migrations dir lives UNDER the repo, some env/cwd path let the dev
# tree shadow the install, so it fails closed (exit 8) instead of reporting a
# false pass. The caller sanitizes PYTHONPATH so this should never trip, but the
# guard is self-validating if a future env path slips through (codex fell-r1).
_INSTALLED_MIGRATIONS_SNIPPET = (
    "import sys\n"
    "from pathlib import Path\n"
    "import angelus\n"
    "from angelus.storage.migrations import DEFAULT_MIGRATIONS_DIR as d\n"
    "repo = Path(sys.argv[1]).resolve()\n"
    "migdir = Path(str(d)).resolve()\n"
    "pkg = Path(angelus.__file__).resolve()\n"
    "if migdir.is_relative_to(repo) or pkg.is_relative_to(repo):\n"
    "    print('post-install check imported the DEV TREE under '\n"
    "          f'{repo} (angelus={pkg}, migrations={migdir}), not the '\n"
    "          'installed package', file=sys.stderr)\n"
    "    sys.exit(8)\n"
    "sys.exit(0 if any(migdir.glob('*.sql')) else 9)\n"
)

# Config-integrity (B18) under the target ref against the LIVE lodging. Replays
# the daemon's own env-then-load sequence and fails closed if any pipe-routed
# channel is missing the $env config it needs. argv is the lodging root.
_CONFIG_CHECK_SNIPPET = (
    "import sys\n"
    "from pathlib import Path\n"
    "from angelus.envfile import load_env_file\n"
    "from angelus.lodging.config import load_lodging, missing_channel_config\n"
    "root = Path(sys.argv[1])\n"
    "load_env_file(root)\n"
    "lodging = load_lodging(root)\n"
    "missing = missing_channel_config(lodging)\n"
    "if missing:\n"
    "    for ch in sorted(missing):\n"
    "        print(f'channel {ch!r} missing env: '\n"
    "              + ', '.join(missing[ch]), file=sys.stderr)\n"
    "    sys.exit(7)\n"
)


class DeployError(Exception):
    """A deploy-aborting failure. Carries an operator-facing message."""


# ---------------------------------------------------------------------------
# Logging (operator-facing; stdout for progress, stderr for failures).
# ---------------------------------------------------------------------------


def _now_iso() -> str:
    return datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def log(message: str) -> None:
    print(f"{_now_iso()} deploy: {message}", flush=True)


def err(message: str) -> None:
    print(f"{_now_iso()} deploy: {message}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# Configuration: every external touchpoint resolved from env with a real
# default. One frozen object threaded through the steps so tests can build a
# fake prod by setting env and nothing is read from the environment twice.
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Config:
    repo: Path
    lodging: Path
    state: Path
    live_db: Path
    backups_dir: Path
    deploys_log: Path
    stamp_file: Path
    hold_file: Path
    bin_dir: Path
    default_ref: str
    systemctl: str
    systemctl_scope: list[str]
    unit: str
    deploy_python: str
    pip_python: str
    pip_install_args: list[str]
    pip_prefix: str | None
    health_cmd: list[str]
    notify_cmd: str
    operator: str
    backups_keep: int
    verify_timeout_sec: int
    verify_poll_sec: int
    verify_min_active_sec: int
    systemctl_timeout_sec: int


def _env_path(name: str, default: Path) -> Path:
    raw = os.environ.get(name)
    return Path(raw).expanduser() if raw else default


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except ValueError:
        err(f"invalid {name}={raw!r}; using default {default}")
        return default


def _hold_file(state: Path) -> Path:
    # Honour belfry's own override so writer and reader never diverge.
    override = os.environ.get("ANGELUS_BELFRY_HOLD_PATH")
    if override:
        return Path(override).expanduser()
    return state / DEFAULT_HOLD_FILENAME


def build_config() -> Config:
    repo = _env_path("ANGELUS_DEPLOY_REPO", CODE_ROOT).resolve()
    lodging = _env_path("ANGELUS_DEPLOY_LODGING", DEFAULT_LODGING).resolve()
    state = lodging / "state"
    operator = (
        os.environ.get("ANGELUS_DEPLOY_OPERATOR")
        or os.environ.get("USER")
        or os.environ.get("LOGNAME")
        or "unknown"
    )
    scope_raw = os.environ.get("ANGELUS_DEPLOY_SYSTEMCTL_SCOPE", DEFAULT_SYSTEMCTL_SCOPE)
    health_raw = os.environ.get("ANGELUS_DEPLOY_HEALTH_CMD", DEFAULT_HEALTH_CMD)
    # Flags between `pip install` and the URL. Default --force-reinstall makes a
    # same-sha rerun (recovery) re-snapshot the ref. Fully overridable: the test
    # suite installs into an isolated --prefix with --ignore-installed so it
    # never touches the env's own angelus.
    pip_args_raw = os.environ.get(
        "ANGELUS_DEPLOY_PIP_INSTALL_ARGS", "--force-reinstall"
    )
    pip_prefix = os.environ.get("ANGELUS_DEPLOY_PIP_PREFIX") or None
    return Config(
        repo=repo,
        lodging=lodging,
        state=state,
        live_db=state / "angelus.sqlite3",
        backups_dir=state / "backups",
        deploys_log=state / "deploys.log",
        stamp_file=state / "installed-version",
        hold_file=_hold_file(state),
        bin_dir=lodging / "bin",
        default_ref=os.environ.get("ANGELUS_DEPLOY_DEFAULT_REF", DEFAULT_DEFAULT_REF),
        systemctl=os.environ.get("ANGELUS_DEPLOY_SYSTEMCTL", DEFAULT_SYSTEMCTL),
        systemctl_scope=shlex.split(scope_raw),
        unit=os.environ.get("ANGELUS_DEPLOY_UNIT", DEFAULT_UNIT),
        deploy_python=os.environ.get("ANGELUS_DEPLOY_PYTHON", sys.executable),
        pip_python=os.environ.get("ANGELUS_DEPLOY_PIP_PYTHON", sys.executable),
        pip_install_args=shlex.split(pip_args_raw),
        pip_prefix=pip_prefix,
        health_cmd=shlex.split(health_raw),
        notify_cmd=os.environ.get("ANGELUS_DEPLOY_NOTIFY", DEFAULT_NOTIFY_CMD),
        operator=operator,
        backups_keep=_env_int("ANGELUS_DEPLOY_BACKUPS_KEEP", DEFAULT_BACKUPS_KEEP),
        verify_timeout_sec=_env_int(
            "ANGELUS_DEPLOY_VERIFY_TIMEOUT_SEC", DEFAULT_VERIFY_TIMEOUT_SEC
        ),
        verify_poll_sec=_env_int(
            "ANGELUS_DEPLOY_VERIFY_POLL_SEC", DEFAULT_VERIFY_POLL_SEC
        ),
        verify_min_active_sec=_env_int(
            "ANGELUS_DEPLOY_VERIFY_MIN_ACTIVE_SEC", DEFAULT_VERIFY_MIN_ACTIVE_SEC
        ),
        systemctl_timeout_sec=_env_int(
            "ANGELUS_DEPLOY_SYSTEMCTL_TIMEOUT_SEC", DEFAULT_SYSTEMCTL_TIMEOUT_SEC
        ),
    )


# ---------------------------------------------------------------------------
# git: ref resolution and a side-effect-free tmp checkout of the target tree.
# ---------------------------------------------------------------------------


def resolve_ref(repo: Path, ref: str) -> str:
    """Resolve a ref (branch/tag/sha/HEAD) to a full commit sha in `repo`."""
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "rev-parse", "--verify", f"{ref}^{{commit}}"],
            check=False,
            capture_output=True,
            text=True,
        )
    except OSError as exc:
        raise DeployError(f"git not available to resolve {ref!r}: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.strip() or f"exit {result.returncode}"
        raise DeployError(f"cannot resolve ref {ref!r} in {repo}: {detail}")
    return result.stdout.strip()


def make_tmp_checkout(repo: Path, sha: str, dest: Path) -> None:
    """Materialise the tree at `sha` into `dest` via `git archive` + tarfile.

    `git archive` is side-effect-free (no worktree admin files, no lock on the
    repo's working tree, nothing to clean up beyond rmtree of dest). The
    extracted tree carries the target ref's angelus package, migrations dir, and
    belt layer -- everything preflight and the belt copy need, ref-consistent.
    """
    try:
        result = subprocess.run(
            ["git", "-C", str(repo), "archive", "--format=tar", sha],
            check=False,
            capture_output=True,
        )
    except OSError as exc:
        raise DeployError(f"git archive failed for {sha}: {exc}") from exc
    if result.returncode != 0:
        detail = result.stderr.decode("utf-8", "replace").strip()
        raise DeployError(f"git archive {sha} failed: {detail or 'nonzero exit'}")
    dest.mkdir(parents=True, exist_ok=True)
    with tarfile.open(fileobj=BytesIO(result.stdout)) as tar:
        # filter="data" (3.12+, backported to 3.11.4) rejects path-traversal and
        # other unsafe members; harmless for a git archive but the secure
        # default. Fall back for older 3.11.x where the kwarg is absent.
        try:
            tar.extractall(dest, filter="data")
        except TypeError:
            tar.extractall(dest)


# ---------------------------------------------------------------------------
# sqlite: backup-API copy (correct against a live WAL writer) and restore.
# ---------------------------------------------------------------------------


def backup_api_copy(src_db: Path, dst_db: Path) -> None:
    """Copy `src_db` to `dst_db` via the sqlite3 backup API.

    Never cp the wal/shm trio: a plain file copy of a live WAL database can
    capture a torn page set. The backup API reads a transactionally-consistent
    snapshot. The source is opened read-only so the live db is never mutated.
    """
    dst_db.parent.mkdir(parents=True, exist_ok=True)
    uri = f"file:{src_db}?mode=ro"
    src = sqlite3.connect(uri, uri=True)
    try:
        dst = sqlite3.connect(str(dst_db))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def restore_db(cfg: Config, backup_path: Path) -> None:
    """Replace the live db with `backup_path` (unit MUST be stopped first).

    Removes any stale -wal/-shm sidecars and rebuilds the live db from the
    backup via the backup API so the result is a single clean file the
    older-code daemon can open. Accepts the seconds-scale data-loss window the
    rollback guard documents (up-on-old beats down-on-new for an alerter).
    """
    for suffix in ("-wal", "-shm"):
        Path(str(cfg.live_db) + suffix).unlink(missing_ok=True)
    cfg.live_db.unlink(missing_ok=True)
    backup_api_copy_plain(backup_path, cfg.live_db)


def backup_api_copy_plain(src_db: Path, dst_db: Path) -> None:
    """Backup-API copy from a normal (already-static) source file.

    The source is opened read-only (mode=ro URI) so a restore can never mutate
    the backup it is restoring FROM -- the backup stays a faithful artefact even
    if the copy is interrupted or the source path is reused.
    """
    dst_db.parent.mkdir(parents=True, exist_ok=True)
    uri = f"file:{src_db}?mode=ro"
    src = sqlite3.connect(uri, uri=True)
    try:
        dst = sqlite3.connect(str(dst_db))
        try:
            src.backup(dst)
        finally:
            dst.close()
    finally:
        src.close()


def applied_versions(db_path: Path) -> set[str]:
    """schema_migrations versions recorded in the live db (empty if none)."""
    if not db_path.exists():
        return set()
    uri = f"file:{db_path}?mode=ro"
    con = sqlite3.connect(uri, uri=True)
    try:
        rows = con.execute("SELECT version FROM schema_migrations").fetchall()
    except sqlite3.Error:
        # No schema_migrations table => a fresh/uninitialised db, not "ahead".
        return set()
    finally:
        con.close()
    return {row[0] for row in rows}


def readable_applied_versions(db_path: Path) -> set[str] | None:
    """schema_migrations versions in a BACKUP, or None if it can't be read.

    Distinct from applied_versions, which returns an empty set both for a fresh
    db and for a corrupt/unreadable one. That conflation is wrong for restore-
    source SELECTION: a corrupt newest backup would read as the empty set, which
    is trivially a subset of any target, so it would be chosen over a valid older
    subset backup and only fail loudly at the copy. Here None means "cannot be
    trusted as a restore source" (corrupt / not-a-database / unreadable) so the
    caller skips it; an empty set still means a genuinely readable db with no
    recorded migrations.
    """
    if not db_path.exists():
        return None
    uri = f"file:{db_path}?mode=ro"
    try:
        con = sqlite3.connect(uri, uri=True)
    except sqlite3.Error:
        return None
    try:
        # PRAGMA schema_version reads page 1 (the db header), so a corrupt or
        # non-sqlite file errors HERE rather than being mistaken for empty.
        con.execute("PRAGMA schema_version").fetchone()
    except sqlite3.Error:
        con.close()
        return None
    try:
        rows = con.execute("SELECT version FROM schema_migrations").fetchall()
    except sqlite3.Error:
        # Readable sqlite db, but no schema_migrations table => no migrations.
        return set()
    finally:
        con.close()
    return {row[0] for row in rows}


def target_versions(checkout: Path) -> set[str]:
    """Migration filenames present in the target ref's migrations dir.

    Migrations live inside the package (angelus/migrations/) so they ship in the
    wheel; the checkout mirrors that layout, so read them there -- the old
    top-level migrations/ path no longer exists post-move (finding-20260613-mbfc).
    """
    mig = checkout / "angelus" / "migrations"
    if not mig.is_dir():
        return set()
    return {p.name for p in mig.iterdir() if p.is_file() and MIGRATION_RE.match(p.name)}


# ---------------------------------------------------------------------------
# Lifecycle steps.
# ---------------------------------------------------------------------------


def hold_belfry(cfg: Config) -> None:
    """Touch the belfry hold sentinel so belfry suppresses its restart action.

    Idempotent: a rerun refreshes the mtime. Writes a human-readable body for
    operators reading the file; belfry only cares about the mtime.
    """
    cfg.hold_file.parent.mkdir(parents=True, exist_ok=True)
    cfg.hold_file.write_text(
        f"{_now_iso()} deploy hold by {cfg.operator}\n", encoding="utf-8"
    )
    now = time.time()
    os.utime(cfg.hold_file, (now, now))
    log(f"belfry hold acquired: {cfg.hold_file}")


def release_hold(cfg: Config) -> None:
    """Remove the belfry hold sentinel (only on a fully successful deploy)."""
    try:
        cfg.hold_file.unlink(missing_ok=True)
        log("belfry hold released")
    except OSError as exc:
        err(f"could not release belfry hold {cfg.hold_file}: {exc}")


def _run_in_checkout(cfg: Config, checkout: Path, snippet: str, arg: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [cfg.deploy_python, "-c", snippet, arg],
        cwd=str(checkout),
        check=False,
        capture_output=True,
        text=True,
    )


def preflight(cfg: Config, checkout: Path) -> None:
    """Fail-closed preflight. Mutates nothing in prod; raises on any failure.

    (a) Migration dry-run: backup-API copy the live db, then run the TARGET
        ref's init_db against the copy from the tmp checkout (target migrations
        + target runner). This alone would have prevented the 0015 outage.
    (b) Config-integrity (B18) under target code against the live lodging.
    """
    # (a) migration dry-run against a throwaway backup-API copy.
    with tempfile.TemporaryDirectory(prefix="angelus-dryrun-") as tmp:
        copy_path = Path(tmp) / "dryrun.sqlite3"
        if cfg.live_db.exists():
            backup_api_copy(cfg.live_db, copy_path)
        else:
            # No live db yet (first-ever deploy): dry-run against an empty db,
            # which still exercises the full migration chain on the target ref.
            copy_path.touch()
        result = _run_in_checkout(cfg, checkout, _DRYRUN_SNIPPET, str(copy_path))
        if result.returncode != 0:
            detail = (result.stderr.strip() or result.stdout.strip()
                      or f"exit {result.returncode}")
            raise DeployError(
                "preflight migration dry-run FAILED -- target migrations do not "
                f"apply cleanly to a copy of the live db:\n{detail}"
            )
    log("preflight (a): migration dry-run clean")

    # (b) config-integrity check under target code against the live lodging.
    if cfg.lodging.exists():
        result = _run_in_checkout(
            cfg, checkout, _CONFIG_CHECK_SNIPPET, str(cfg.lodging)
        )
        if result.returncode != 0:
            detail = (result.stderr.strip() or result.stdout.strip()
                      or f"exit {result.returncode}")
            raise DeployError(
                "preflight config-integrity check FAILED under target code:\n"
                f"{detail}"
            )
        log("preflight (b): config-integrity clean")
    else:
        err(f"lodging root {cfg.lodging} absent; skipping config-integrity check")


def existing_backups(cfg: Config) -> list[Path]:
    """Backup files newest-first (names are time-ordered, so reverse-sorted)."""
    if not cfg.backups_dir.is_dir():
        return []
    backups = [
        p
        for p in cfg.backups_dir.iterdir()
        if p.is_file() and p.name.startswith("angelus-") and p.suffix == ".sqlite3"
    ]
    return sorted(backups, key=lambda p: p.name, reverse=True)


def backup_live_db(cfg: Config) -> Path | None:
    """Timestamped backup-API copy of the live db; prune to backups_keep.

    Returns the backup path, or None if there is no live db yet (first deploy).
    """
    if not cfg.live_db.exists():
        log("no live db to back up (first deploy)")
        return None
    cfg.backups_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    dest = cfg.backups_dir / f"angelus-{stamp}.sqlite3"
    backup_api_copy(cfg.live_db, dest)
    log(f"backed up live db -> {dest}")
    prune_backups(cfg)
    return dest


def prune_backups(cfg: Config) -> None:
    """Keep only the newest `backups_keep` backups."""
    backups = existing_backups(cfg)
    for stale in backups[cfg.backups_keep :]:
        try:
            stale.unlink()
            log(f"pruned old backup {stale.name}")
        except OSError as exc:
            err(f"could not prune backup {stale}: {exc}")


def pip_install(cfg: Config, sha: str) -> None:
    """Non-editable install of the target ref: `pip install git+file://repo@sha`.

    pip clones the repo to a tmp dir internally and builds from there, so the
    dev working tree is never touched and need not be clean. --force-reinstall
    makes a same-sha rerun (recovery) idempotent.
    """
    url = f"git+file://{cfg.repo}@{sha}"
    cmd = [cfg.pip_python, "-m", "pip", "install", *cfg.pip_install_args]
    if cfg.pip_prefix:
        cmd += ["--prefix", cfg.pip_prefix]
    cmd.append(url)
    log(f"pip install {url}")
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        detail = (result.stderr.strip() or result.stdout.strip()
                  or f"exit {result.returncode}")
        raise DeployError(f"pip install of {sha} failed:\n{detail}")


def verify_installed_migrations(cfg: Config) -> None:
    """Confirm the FRESHLY INSTALLED package can locate its bundled migrations.

    This is the deterministic guard for the 2026-06-13 class: a non-editable
    install whose wheel omitted migrations crash-loops the daemon at startup
    (finding-20260613-mbfc). The dry-run preflight runs the target migrations
    from the tmp CHECKOUT, so it validates their CONTENT but never the INSTALLED
    package's ability to FIND them. This runs after pip_install and before the
    restart, so the packaging gap fails the deploy loudly here instead of via the
    crash-loop verify backstop.

    The probe must resolve angelus to what pip just installed, NOT the dev tree,
    regardless of the inherited environment. Two things could shadow the install:
    a stray ./angelus in the deploy's launch directory (handled by the neutral
    cwd), and an inherited PYTHONPATH pointing at the repo (common in a dev shell;
    `make deploy` inherits the operator's shell env). So the probe env is
    sanitized per install mode:

      * NO-PREFIX mode (real prod: pip installs into pip_python's own env) --
        REMOVE any inherited PYTHONPATH so the import resolves ONLY from the
        installed env's site-packages. Without this, PYTHONPATH=<repo> makes the
        probe import the DEV TREE, which always finds migrations package-relative,
        and the guard false-passes even when the installed wheel omitted them
        (codex fell-r1).
      * PREFIX mode (the test harness, and any --prefix deploy) -- pip placed the
        package under cfg.pip_prefix, which is NOT on pip_python's default path,
        so SET PYTHONPATH to EXACTLY that purelib (computed with the interpreter
        that runs the probe, so it matches where pip placed the package). Replace,
        never prepend-to-inherited: an inherited entry ahead of purelib could
        still shadow the install.

    The snippet is itself self-validating: it fails closed if the imported
    angelus resolves under cfg.repo (passed as argv), so a dev-tree import can
    never report a false pass even if some future env path slips through.
    """
    env = dict(os.environ)
    if cfg.pip_prefix:
        purelib = sysconfig.get_path(
            "purelib", vars={"base": cfg.pip_prefix, "platbase": cfg.pip_prefix}
        )
        env["PYTHONPATH"] = purelib
    else:
        env.pop("PYTHONPATH", None)
    with tempfile.TemporaryDirectory(prefix="angelus-migcheck-") as tmp:
        result = subprocess.run(
            [cfg.pip_python, "-c", _INSTALLED_MIGRATIONS_SNIPPET, str(cfg.repo)],
            cwd=tmp,
            env=env,
            check=False,
            capture_output=True,
            text=True,
        )
    if result.returncode != 0:
        detail = (result.stderr.strip() or result.stdout.strip()
                  or f"exit {result.returncode}")
        raise DeployError(
            "post-install check FAILED -- the installed angelus package cannot "
            "locate its bundled migrations. A restart would crash-loop the daemon "
            f"(FileNotFoundError on the migrations dir):\n{detail}"
        )
    log("post-install: installed package can locate its migrations")


def copy_belt(cfg: Config, checkout: Path) -> None:
    """Copy belfry.py + sre_runner.py FROM THE TMP CHECKOUT to <lodging>/bin.

    The belt layer (cron points at <lodging>/bin) deploys WITH the release but
    stays import-independent of site-packages. The source is the tmp checkout
    of the target ref, NOT the dev working tree -- ref-consistency. Idempotent
    (overwrites).

    Each file is written atomically (copy to a sibling .tmp, then os.replace):
    cron can fire mid-deploy under the deploy's own active hold, and a kill
    during a plain copy would leave a half-written, unparseable <lodging>/bin
    file. The temp-then-rename means a reader only ever sees the old whole file
    or the new whole file, and a leftover .tmp from a killed run is overwritten
    on rerun.
    """
    cfg.bin_dir.mkdir(parents=True, exist_ok=True)
    pairs = [
        (checkout / "belfry" / "belfry.py", cfg.bin_dir / "belfry.py"),
        (checkout / "deploy" / "sre_runner.py", cfg.bin_dir / "sre_runner.py"),
    ]
    for src, dst in pairs:
        if not src.exists():
            raise DeployError(f"belt source missing in checkout: {src}")
        tmp = dst.with_name(dst.name + ".tmp")
        shutil.copy2(src, tmp)
        os.replace(tmp, dst)  # atomic within the same directory/filesystem
        log(f"deployed belt: {dst}")


def _systemctl(cfg: Config, verb: str) -> subprocess.CompletedProcess[str]:
    cmd = [cfg.systemctl, *cfg.systemctl_scope, verb, cfg.unit]
    return subprocess.run(
        cmd,
        check=False,
        capture_output=True,
        text=True,
        timeout=cfg.systemctl_timeout_sec,
    )


def restart_unit(cfg: Config) -> None:
    # reset-failed IMMEDIATELY before the controlled restart. This deterministically
    # zeroes NRestarts and clears any failed/auto-restart state on every systemd
    # version, so verify's NRestarts==0 gate becomes a true "did the daemon crash
    # since WE (re)started it" signal -- independent of the unit's prior history.
    # It is load-bearing for the commonest deploy: fixing an ALREADY crash-looping
    # daemon. On this host (systemd 245) a plain `restart` issued while the unit is
    # in SubState=auto-restart (between a crash and its RestartSec re-fire) leaves
    # NRestarts NONZERO even after the new process is healthy (measured NRestarts=2,
    # 3spy fell-r2), which would false-FAIL a healthy recovery deploy. reset-failed
    # removes that version-dependent assumption. We are about to restart regardless.
    reset = _systemctl(cfg, "reset-failed")
    if reset.returncode != 0:
        # Non-fatal: reset-failed is a no-op (rc 0) on a non-failed unit, so a
        # nonzero here means a deeper systemctl problem the restart below surfaces.
        # If the counter is somehow not cleared, verify fails CLOSED (never passes
        # a flapping unit), which the rerun-after-fix model already covers.
        err(
            f"reset-failed {cfg.unit} returned nonzero (continuing to restart): "
            f"{reset.stderr.strip() or reset.returncode}"
        )
    result = _systemctl(cfg, "restart")
    if result.returncode != 0:
        detail = result.stderr.strip() or f"exit {result.returncode}"
        raise DeployError(f"systemctl restart {cfg.unit} failed: {detail}")
    log(f"restarted unit {cfg.unit} (reset-failed first)")


def stop_unit(cfg: Config) -> None:
    result = _systemctl(cfg, "stop")
    if result.returncode != 0:
        detail = result.stderr.strip() or f"exit {result.returncode}"
        raise DeployError(f"systemctl stop {cfg.unit} failed: {detail}")
    log(f"stopped unit {cfg.unit}")


def start_unit(cfg: Config) -> None:
    result = _systemctl(cfg, "start")
    if result.returncode != 0:
        detail = result.stderr.strip() or f"exit {result.returncode}"
        raise DeployError(f"systemctl start {cfg.unit} failed: {detail}")
    log(f"started unit {cfg.unit}")


_SHOW_PROPS = (
    "ActiveState",
    "NRestarts",
    "ExecMainStatus",
    "ActiveEnterTimestampMonotonic",
)


def unit_show(cfg: Config) -> dict[str, str] | None:
    """Read the unit's supervisor state via `systemctl show -p ...`.

    The process supervisor is the source of truth for liveness. `is-active`
    alone is blind to a crash loop: it reads "active" during each brief alive
    window of a daemon that forks then crashes. The richer properties expose the
    loop -- NRestarts climbs on each on-failure restart, ActiveState dips to
    activating, and ActiveEnterTimestampMonotonic jumps on every re-entry.

    Returns the parsed KEY=VALUE properties, or None if the query could not run.
    """
    cmd = [cfg.systemctl, *cfg.systemctl_scope, "show"]
    for prop in _SHOW_PROPS:
        cmd += ["-p", prop]
    cmd.append(cfg.unit)
    try:
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            timeout=cfg.systemctl_timeout_sec,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        err(f"systemctl show could not run: {exc}")
        return None
    if result.returncode != 0:
        return None
    props: dict[str, str] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep:
            props[key.strip()] = value.strip()
    return props


def health_clean(cfg: Config) -> bool:
    try:
        result = subprocess.run(
            cfg.health_cmd,
            cwd=str(cfg.lodging),
            check=False,
            capture_output=True,
            text=True,
            timeout=cfg.systemctl_timeout_sec,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        err(f"health check could not run: {exc}")
        return False
    return result.returncode == 0


def verify(cfg: Config) -> None:
    """Confirm SUSTAINED liveness from the process supervisor, or time out.

    First-active is NOT a liveness signal (see DEFAULT_VERIFY_MIN_ACTIVE_SEC):
    a crash-looping daemon reads "active" during each alive window. Neither is
    `angelus health` rc -- it exits 0 even when the daemon is DOWN (it falls
    through to the read-only sqlite fallback, cli.py health()), so it can only
    mask a crash loop, never reveal one. It is read here ONLY for the failure
    diagnostic; it does not make verify pass.

    The gate, all from `systemctl show`: ActiveState=="active", NRestarts==0
    (no on-failure restart since our explicit (re)start reset the counter),
    ExecMainStatus in {unset, 0}, AND the unit stays continuously active --
    same ActiveEnterTimestampMonotonic, never dipping to activating -- for a
    span >= verify_min_active_sec (which exceeds RestartSec). A crash-restart
    inside that span re-enters active (timestamp jumps, NRestarts climbs), which
    resets the sustained-active clock so verify never passes on a flapping unit.
    """
    deadline = time.time() + cfg.verify_timeout_sec
    need = cfg.verify_min_active_sec
    active_since: float | None = None  # wall time the current active span began
    active_enter: str | None = None  # ActiveEnterTimestampMonotonic of that span
    clean_polls = 0  # consecutive clean polls within the current active span
    last = "no check ran"
    while True:
        props = unit_show(cfg)
        if props is None:
            active_since = None
            clean_polls = 0
            last = "systemctl show could not be read"
        else:
            state = props.get("ActiveState", "")
            nrestarts_raw = props.get("NRestarts", "0")
            exec_status = props.get("ExecMainStatus", "")
            enter = props.get("ActiveEnterTimestampMonotonic", "")
            try:
                nrestarts = int(nrestarts_raw)
            except ValueError:
                nrestarts = -1
            clean = (
                state == "active"
                and nrestarts == 0
                and exec_status in ("", "0")
            )
            if clean:
                if active_since is None or enter != active_enter:
                    # First clean poll, or the unit re-entered active between
                    # polls (a crash-restart we'd otherwise miss): (re)start the
                    # sustained-active clock from now.
                    active_since = time.time()
                    active_enter = enter
                    clean_polls = 1
                else:
                    clean_polls += 1
                held = time.time() - active_since
                # Require >=2 confirming polls in the SAME active span (so a
                # single first-active reading can never pass) AND a wall span of
                # at least need seconds (which exceeds RestartSec, so an
                # auto-restart inside the span dips ActiveState/bumps NRestarts
                # and resets this clock before the span can mature).
                if clean_polls >= 2 and held >= need:
                    log(
                        f"verify: unit active and stable for >={need}s across "
                        f"{clean_polls} polls (NRestarts={nrestarts}, "
                        f"ExecMainStatus={exec_status or 'running'})"
                    )
                    return
                last = (
                    f"active, holding {held:.0f}/{need}s "
                    f"polls={clean_polls} (NRestarts={nrestarts})"
                )
            else:
                active_since = None
                clean_polls = 0
                # health rc is diagnostic only -- it never gates a pass.
                last = (
                    f"ActiveState={state!r} NRestarts={nrestarts_raw} "
                    f"ExecMainStatus={exec_status!r} "
                    f"health_rc_clean={health_clean(cfg)}"
                )
        if time.time() >= deadline:
            break
        time.sleep(cfg.verify_poll_sec)
    raise DeployError(
        f"verify FAILED within {cfg.verify_timeout_sec}s -- the daemon did not "
        f"stay up (last: {last}); rerun `make deploy REF=<sha>` once the cause "
        "is fixed"
    )


def read_stamp(cfg: Config) -> str:
    """The currently-installed sha recorded by the last deploy, or 'unknown'."""
    try:
        return cfg.stamp_file.read_text(encoding="utf-8").strip() or "unknown"
    except FileNotFoundError:
        return "unknown"
    except OSError:
        return "unknown"


def write_stamp(cfg: Config, sha: str) -> None:
    """Record the installed sha (Spin 2's health `code:` line reads this)."""
    cfg.state.mkdir(parents=True, exist_ok=True)
    cfg.stamp_file.write_text(f"{sha}\n", encoding="utf-8")


def append_deploys_log(
    cfg: Config,
    from_version: str,
    to_sha: str,
    duration_sec: float,
    backup_path: Path | None,
    restored_path: Path | None,
) -> None:
    cfg.state.mkdir(parents=True, exist_ok=True)
    parts = [
        _now_iso(),
        f"from={from_version}",
        f"to={to_sha}",
        f"operator={cfg.operator}",
        f"duration={duration_sec:.1f}s",
        f"backup={backup_path if backup_path else 'none'}",
    ]
    if restored_path is not None:
        parts.append(f"restored={restored_path}")
    line = " ".join(parts) + "\n"
    with cfg.deploys_log.open("a", encoding="utf-8") as fh:
        fh.write(line)
    log(f"recorded deploy in {cfg.deploys_log}")


def notify(cfg: Config, message: str) -> None:
    """Best-effort operator notification; never raises."""
    try:
        subprocess.run([cfg.notify_cmd, message], check=False, capture_output=True)
    except OSError:
        # notify command absent (e.g. notify-pat not installed): swallow.
        pass


# ---------------------------------------------------------------------------
# Commands.
# ---------------------------------------------------------------------------


def cmd_deploy(cfg: Config, ref: str, restore_backup: bool) -> int:
    started = time.time()
    sha = resolve_ref(cfg.repo, ref)
    short = sha[:12]
    from_version = read_stamp(cfg)
    log(f"deploy target {ref} -> {short} (from {from_version[:12]})")

    # Hold belfry first: from here on a failure leaves the hold to self-expire.
    hold_belfry(cfg)
    tmp_root = tempfile.mkdtemp(prefix="angelus-deploy-")
    checkout = Path(tmp_root) / "checkout"
    try:
        make_tmp_checkout(cfg.repo, sha, checkout)

        # Preflight (fail-closed, prod untouched).
        preflight(cfg, checkout)

        # Rollback guard: live schema ahead of the target ref's migrations.
        ahead = applied_versions(cfg.live_db) - target_versions(checkout)
        restore_source: Path | None = None
        if ahead:
            if not restore_backup:
                raise DeployError(
                    "rollback guard: live db schema_migrations is AHEAD of the "
                    f"target ref ({', '.join(sorted(ahead))} not in target "
                    "migrations). Old code must not run a newer schema. Rerun "
                    "with --restore-backup to restore the freshest pre-deploy "
                    "backup before starting the unit (accepts a seconds-scale "
                    "data-loss window)."
                )
            # Pick the freshest backup that exists BEFORE this run's own backup
            # AND whose own schema is at-or-below the target ref. Backups are
            # taken at each deploy's START, so after several deploys the freshest
            # backup can itself be schema-AHEAD of the rollback target (e.g.
            # rolling back two deploys). Restoring it would put old code on a
            # newer schema -- the exact hazard this guard exists to block -- so
            # skip ahead-of-target backups and refuse loudly if none qualify.
            prior = existing_backups(cfg)
            if not prior:
                raise DeployError(
                    "rollback guard: --restore-backup requested but no "
                    f"pre-deploy backup exists in {cfg.backups_dir}"
                )
            target = target_versions(checkout)
            skipped: list[str] = []
            for candidate in prior:  # newest-first
                versions = readable_applied_versions(candidate)
                if versions is None:
                    # Corrupt/unreadable: NOT an empty-set subset match. Skip it
                    # so a valid older subset backup can still be selected.
                    err(f"rollback: skipping unreadable/corrupt backup "
                        f"{candidate.name}")
                    skipped.append(f"{candidate.name} (unreadable/corrupt)")
                    continue
                if versions <= target:
                    restore_source = candidate
                    break
                skipped.append(f"{candidate.name} (schema-ahead)")
            if restore_source is None:
                raise DeployError(
                    "rollback guard: --restore-backup requested but no pre-deploy "
                    f"backup in {cfg.backups_dir} is both readable AND schema "
                    "at-or-below the target ref. Restoring a schema-ahead backup "
                    "would put old code on a newer schema; an unreadable backup "
                    f"can't be trusted as a restore source. Skipped: "
                    f"{'; '.join(skipped)}. Deploy a ref whose migrations cover an "
                    "existing backup, or restore manually."
                )
            log(f"rollback: will restore {restore_source} before start")

        # Backup the live db (timestamped, pruned to keep N).
        backup_path = backup_live_db(cfg)

        # Install the target ref, non-editable.
        pip_install(cfg, sha)

        # Post-install: the installed package must be able to find its bundled
        # migrations BEFORE we restart onto it (else the daemon crash-loops).
        verify_installed_migrations(cfg)

        # Deploy the belt layer from the tmp checkout (ref-consistent).
        copy_belt(cfg, checkout)

        # Bring the unit onto the new code. The restore path stops, restores,
        # then starts so the older code never opens the newer-schema db.
        if restore_source is not None:
            stop_unit(cfg)
            restore_db(cfg, restore_source)
            # Re-verify against the live db we are about to start old code on:
            # the chosen backup was subset-checked, but confirm the RESTORED
            # state is genuinely at-or-below target before the unit comes up.
            still_ahead = applied_versions(cfg.live_db) - target_versions(checkout)
            if still_ahead:
                raise DeployError(
                    "rollback guard: live db is STILL schema-ahead after "
                    f"restoring {restore_source.name} ({', '.join(sorted(still_ahead))} "
                    "not in target migrations); refusing to start old code on a "
                    "newer schema"
                )
            start_unit(cfg)
        else:
            restart_unit(cfg)

        verify(cfg)

        # Record + stamp + release the hold (only now, on full success).
        write_stamp(cfg, sha)
        append_deploys_log(
            cfg,
            from_version,
            sha,
            time.time() - started,
            backup_path,
            restore_source,
        )
        release_hold(cfg)
    except DeployError as exc:
        _fail(cfg, str(exc))
        return 1
    except Exception as exc:  # pragma: no cover - defensive catch-all
        _fail(cfg, f"unexpected error: {exc!r}")
        return 1
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)

    log(f"deploy complete: {short} (from {from_version[:12]})")
    return 0


def _fail(cfg: Config, message: str) -> None:
    """Report a deploy failure loudly and LEAVE the hold to self-expire."""
    err(f"DEPLOY FAILED: {message}")
    err(
        "hold left in place to self-expire; prod state is unchanged unless a "
        "step past backup ran. Fix the cause and rerun `make deploy` (idempotent)."
    )
    notify(cfg, f"angelus deploy FAILED: {message}")


def cmd_check(cfg: Config, ref: str) -> int:
    """Preflight only -- prod untouched, no hold, no install."""
    sha = resolve_ref(cfg.repo, ref)
    log(f"deploy-check {ref} -> {sha[:12]} (preflight only, prod untouched)")
    tmp_root = tempfile.mkdtemp(prefix="angelus-check-")
    checkout = Path(tmp_root) / "checkout"
    try:
        make_tmp_checkout(cfg.repo, sha, checkout)
        preflight(cfg, checkout)
        ahead = applied_versions(cfg.live_db) - target_versions(checkout)
        if ahead:
            err(
                "deploy-check: live schema is AHEAD of target "
                f"({', '.join(sorted(ahead))}); deploy would need --restore-backup"
            )
            return 1
    except DeployError as exc:
        err(f"deploy-check FAILED: {exc}")
        return 1
    finally:
        shutil.rmtree(tmp_root, ignore_errors=True)
    log("deploy-check: preflight clean")
    return 0


def cmd_status(cfg: Config) -> int:
    installed = read_stamp(cfg)
    print(f"installed: {installed}")
    try:
        master = resolve_ref(cfg.repo, cfg.default_ref)
        print(f"{cfg.default_ref}:   {master}")
        if installed not in ("unknown", master) and installed:
            behind = subprocess.run(
                ["git", "-C", str(cfg.repo), "rev-list", "--count",
                 f"{installed}..{master}"],
                check=False, capture_output=True, text=True,
            )
            if behind.returncode == 0:
                print(f"behind:    {behind.stdout.strip()} commit(s)")
        elif installed == master:
            print("behind:    0 commit(s) (up to date)")
    except DeployError as exc:
        print(f"{cfg.default_ref}:   <unresolved: {exc}>")

    print("last deploys:")
    try:
        lines = cfg.deploys_log.read_text(encoding="utf-8").splitlines()
        for line in lines[-5:]:
            print(f"  {line}")
    except FileNotFoundError:
        print("  (no deploys.log yet)")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    parser = argparse.ArgumentParser(prog="deploy.py", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    p_deploy = sub.add_parser("deploy", help="install + restart a ref")
    p_deploy.add_argument("ref", nargs="?", default=None)
    p_deploy.add_argument(
        "--restore-backup",
        action="store_true",
        help="restore the freshest pre-deploy backup (required to roll back "
        "past applied migrations)",
    )

    p_check = sub.add_parser("check", help="preflight only; prod untouched")
    p_check.add_argument("ref", nargs="?", default=None)

    sub.add_parser("status", help="installed version vs master + recent deploys")

    args = parser.parse_args(argv)
    cfg = build_config()

    if args.command == "deploy":
        ref = args.ref or cfg.default_ref
        return cmd_deploy(cfg, ref, args.restore_backup)
    if args.command == "check":
        ref = args.ref or cfg.default_ref
        return cmd_check(cfg, ref)
    if args.command == "status":
        return cmd_status(cfg)
    parser.error(f"unknown command {args.command!r}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
