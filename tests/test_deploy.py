"""Release-path tests: deploy/deploy.py lifecycle + belfry hold latch.

Spec: brief-20260613-3spy. The fixtures build a fake prod -- a tmp lodging, a
live-shaped sqlite at the 0014-with-findings incident shape, a tmp git repo as
the "dev repo", a systemctl shim on PATH (records calls, flips an active/failed
state file), and a pip --prefix into a tmp site-packages -- so the whole
lifecycle runs without touching the real install, unit, or crontab.

The load-bearing assertions:
  * happy path: step order, deploys.log written, hold released;
  * preflight failure (the real 0015 FK shape) leaves the live db byte-identical
    and exits nonzero with the hold left (and the failure IS the production
    IntegrityError -- mutation-verified separately);
  * schema-ahead rollback guard: refuses without --restore-backup, restores with;
  * rerun-after-kill at each step converges;
  * hold latch: suppresses restart, leaves alerting unchanged, max-age override.
"""

from __future__ import annotations

import hashlib
import importlib.util
import os
import shutil
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_PATH = REPO_ROOT / "deploy" / "deploy.py"
BELFRY_PATH = REPO_ROOT / "belfry" / "belfry.py"
SRC_MIGRATIONS = REPO_ROOT / "migrations"

# Migration filenames in order, so a "live db at version N" / "target ref at
# version N" can be built by selecting a prefix of this list.
ALL_MIGRATIONS = sorted(p.name for p in SRC_MIGRATIONS.glob("0*.sql"))
M0014 = "0014_source_sla.sql"
M0015 = "0015_observation_consumed.sql"


# ---------------------------------------------------------------------------
# Module loaders (register in sys.modules so deploy.py's frozen-annotation
# dataclass resolves under importlib, same as a normal import would).
# ---------------------------------------------------------------------------


def _load_module(name: str, path: Path):
    spec = importlib.util.spec_from_file_location(name, path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def deploy():
    return _load_module("deploy_under_test", DEPLOY_PATH)


@pytest.fixture
def belfry():
    return _load_module("belfry_under_test_deploy", BELFRY_PATH)


# ---------------------------------------------------------------------------
# git / db / repo builders.
# ---------------------------------------------------------------------------


_GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "test",
    "GIT_AUTHOR_EMAIL": "test@example.com",
    "GIT_COMMITTER_NAME": "test",
    "GIT_COMMITTER_EMAIL": "test@example.com",
}

_MINIMAL_PYPROJECT = """\
[build-system]
requires = ["setuptools>=69", "wheel"]
build-backend = "setuptools.build_meta"

[project]
name = "angelus"
version = "0.1.0"
dependencies = []

[project.scripts]
angelus = "angelus.cli:main"

[tool.setuptools.packages.find]
include = ["angelus*"]
"""


def _migrations_upto(version: str) -> list[str]:
    return [name for name in ALL_MIGRATIONS if name <= version]


def _git(repo: Path, *args: str) -> None:
    subprocess.run(["git", "-C", str(repo), *args], check=True, env=_GIT_ENV,
                   capture_output=True)


def build_dev_repo(dest: Path, upto: str) -> str:
    """A git repo standing in for the engine dev repo, at migration `upto`.

    Carries the real angelus package + the migration prefix + the real belt
    files, so the dry-run / config-check / belt-copy all run the genuine target
    code. Returns the HEAD sha.
    """
    dest.mkdir(parents=True, exist_ok=True)
    shutil.copytree(REPO_ROOT / "angelus", dest / "angelus")
    (dest / "migrations").mkdir()
    for name in _migrations_upto(upto):
        shutil.copy(SRC_MIGRATIONS / name, dest / "migrations" / name)
    (dest / "belfry").mkdir()
    shutil.copy(BELFRY_PATH, dest / "belfry" / "belfry.py")
    (dest / "deploy").mkdir()
    shutil.copy(REPO_ROOT / "deploy" / "sre_runner.py", dest / "deploy" / "sre_runner.py")
    (dest / "pyproject.toml").write_text(_MINIMAL_PYPROJECT, encoding="utf-8")
    _git(dest, "init", "-q")
    _git(dest, "add", "-A")
    _git(dest, "commit", "-q", "-m", f"angelus at {upto}")
    out = subprocess.run(["git", "-C", str(dest), "rev-parse", "HEAD"],
                         check=True, capture_output=True, text=True, env=_GIT_ENV)
    return out.stdout.strip()


def _build_db(path: Path, upto: str, *, with_findings: bool) -> None:
    """Build a sqlite db migrated through `upto` using the REAL migration runner.

    with_findings seeds an observation + a finding referencing it -- the
    0014-with-findings incident shape whose FK makes 0015's DROP TABLE fail.
    """
    sys.path.insert(0, str(REPO_ROOT))
    from angelus.storage.migrations import init_db  # real runner
    tmp_dir = path.parent / f".mig-{path.name}"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    for name in _migrations_upto(upto):
        shutil.copy(SRC_MIGRATIONS / name, tmp_dir / name)
    conn = init_db(path, tmp_dir)
    try:
        if with_findings:
            conn.execute(
                "INSERT INTO observations (id, source, status) "
                "VALUES (1, 'scheduled/x', 'ready')"
            )
            conn.execute(
                "INSERT INTO findings "
                "(id, observation_id, source, type, entity, dedup_key, "
                " target_pipes, status) "
                "VALUES (1, 1, 'scheduled/x', 't', 'e', 'dk', '[\"now\"]', 'ready')"
            )
            conn.commit()
    finally:
        conn.close()
    shutil.rmtree(tmp_dir, ignore_errors=True)


# ---------------------------------------------------------------------------
# Fake-prod fixture: lodging + shims + env wiring.
# ---------------------------------------------------------------------------


class FakeProd:
    def __init__(self, deploy_mod, lodging: Path, calls_file: Path,
                 state_file: Path, notify_file: Path, prefix: Path,
                 scenario_file: Path, nrestarts_file: Path):
        self.deploy = deploy_mod
        self.lodging = lodging
        self.state = lodging / "state"
        self.live_db = self.state / "angelus.sqlite3"
        self.backups = self.state / "backups"
        self.deploys_log = self.state / "deploys.log"
        self.stamp = self.state / "installed-version"
        self.hold = self.state / "belfry-hold"
        self.bin = lodging / "bin"
        self.calls_file = calls_file
        self.state_file = state_file
        self.notify_file = notify_file
        self.prefix = prefix
        self.scenario_file = scenario_file
        self.nrestarts_file = nrestarts_file

    def crash_loop(self) -> None:
        """Make the systemctl shim model a daemon that crashes at startup: it
        reads "active" on the first poll then flips to activating/auto-restart
        with NRestarts climbing -- the 0015 incident shape verify must catch."""
        self.scenario_file.write_text("crashloop")

    def set_nrestarts(self, n: int) -> None:
        """Seed the unit's PERSISTENT NRestarts counter -- the unit entering the
        deploy mid crash-loop (a prior on-failure restart history). The shim
        retains this across `restart` and zeroes it only on `reset-failed`."""
        self.nrestarts_file.write_text(f"{n}\n")

    def systemctl_calls(self) -> list[str]:
        if not self.calls_file.exists():
            return []
        return [ln for ln in self.calls_file.read_text().splitlines() if ln.strip()]

    def notified(self) -> bool:
        return self.notify_file.exists() and bool(self.notify_file.read_text().strip())

    def config(self):
        return self.deploy.build_config()


@pytest.fixture
def fake_prod(deploy, tmp_path, monkeypatch):
    lodging = tmp_path / "lodging"
    (lodging / "state").mkdir(parents=True)

    # Shim bin on PATH: systemctl, health, notify.
    shim_bin = tmp_path / "shimbin"
    shim_bin.mkdir()
    calls_file = tmp_path / "systemctl-calls.txt"
    state_file = tmp_path / "unit-state.txt"
    notify_file = tmp_path / "notify.txt"
    scenario_file = tmp_path / "unit-scenario.txt"  # "crashloop" => flapping unit
    poll_file = tmp_path / "show-poll.txt"  # per-(re)start poll counter
    nrestarts_file = tmp_path / "unit-nrestarts.txt"  # PERSISTENT NRestarts counter

    # The shim models the seam verify reads: `systemctl show -p ...`. A healthy
    # unit shows ActiveState=active, ExecMainStatus=0 with a fixed
    # ActiveEnterTimestampMonotonic (continuously active) and NRestarts read from
    # a PERSISTENT counter. In "crashloop" mode it reads active on the first poll
    # then flips to activating with NRestarts climbing and the enter-timestamp
    # jumping -- the crash-at-startup shape that first-active gating would wrongly
    # pass.
    #
    # NRestarts retention is the load-bearing behaviour (3spy fell-r2): a real
    # `restart` issued while the unit is mid auto-restart does NOT flush NRestarts
    # (measured =2 on this host's systemd 245), so the shim RETAINS NRestarts
    # across `restart` and zeroes it ONLY on `reset-failed`. `start` (from a
    # stopped/dead unit, the rollback path) zeroes it as real start-from-dead
    # does. A (re)start resets the poll counter.
    systemctl = shim_bin / "systemctl"
    systemctl.write_text(
        "#!/bin/sh\n"
        f'echo "$@" >> "{calls_file}"\n'
        f'scenario=$(cat "{scenario_file}" 2>/dev/null || echo healthy)\n'
        "for a in \"$@\"; do\n"
        "  case \"$a\" in\n"
        f'    reset-failed) echo 0 > "{nrestarts_file}"; exit 0;;\n'
        f'    restart) echo active > "{state_file}"; echo 0 > "{poll_file}"; exit 0;;\n'
        f'    start) echo active > "{state_file}"; echo 0 > "{poll_file}"; echo 0 > "{nrestarts_file}"; exit 0;;\n'
        f'    stop) echo inactive > "{state_file}"; exit 0;;\n'
        f'    is-active) cat "{state_file}" 2>/dev/null || echo unknown; exit 0;;\n'
        "    show)\n"
        f'      n=$(cat "{poll_file}" 2>/dev/null || echo 0); n=$((n+1)); echo "$n" > "{poll_file}";\n'
        f'      st=$(cat "{state_file}" 2>/dev/null || echo unknown);\n'
        f'      base=$(cat "{nrestarts_file}" 2>/dev/null || echo 0);\n'
        '      if [ "$scenario" = crashloop ] && [ "$n" -gt 1 ]; then\n'
        '        echo "ActiveState=activating"; echo "NRestarts=$((base+n-1))";\n'
        '        echo "ExecMainStatus=1"; echo "ActiveEnterTimestampMonotonic=$((1000+n*5000000))";\n'
        "      else\n"
        '        echo "ActiveState=$st"; echo "NRestarts=$base";\n'
        '        echo "ExecMainStatus=0"; echo "ActiveEnterTimestampMonotonic=1000";\n'
        "      fi\n"
        "      exit 0;;\n"
        "  esac\n"
        "done\n"
        "exit 0\n"
    )
    systemctl.chmod(0o755)

    health = shim_bin / "angelus-health"
    health.write_text("#!/bin/sh\nexit 0\n")
    health.chmod(0o755)

    notify = shim_bin / "notify-shim"
    notify.write_text("#!/bin/sh\n" f'echo "$@" >> "{notify_file}"\n' "exit 0\n")
    notify.chmod(0o755)

    monkeypatch.setenv("PATH", f"{shim_bin}{os.pathsep}{os.environ['PATH']}")

    prefix = tmp_path / "prefix"

    # Wire every deploy.py touchpoint at the fake prod.
    monkeypatch.setenv("ANGELUS_DEPLOY_LODGING", str(lodging))
    monkeypatch.setenv("ANGELUS_DEPLOY_SYSTEMCTL", "systemctl")
    monkeypatch.setenv("ANGELUS_DEPLOY_SYSTEMCTL_SCOPE", "--user")
    monkeypatch.setenv("ANGELUS_DEPLOY_UNIT", "angelus-test")
    monkeypatch.setenv("ANGELUS_DEPLOY_HEALTH_CMD", str(health))
    monkeypatch.setenv("ANGELUS_DEPLOY_NOTIFY", str(notify))
    monkeypatch.setenv("ANGELUS_DEPLOY_PIP_PREFIX", str(prefix))
    monkeypatch.setenv(
        "ANGELUS_DEPLOY_PIP_INSTALL_ARGS",
        "--ignore-installed --no-deps --no-build-isolation",
    )
    monkeypatch.setenv("ANGELUS_DEPLOY_VERIFY_TIMEOUT_SEC", "10")
    monkeypatch.setenv("ANGELUS_DEPLOY_VERIFY_POLL_SEC", "0")
    # The sustained-active span verify requires. 0 means "two consecutive clean
    # polls" against this deterministic shim (real default is 8s > RestartSec);
    # the crashloop scenario flips state before a second clean poll lands.
    monkeypatch.setenv("ANGELUS_DEPLOY_VERIFY_MIN_ACTIVE_SEC", "0")
    monkeypatch.setenv("ANGELUS_DEPLOY_OPERATOR", "tester")
    # Belfry's hold path env must NOT leak from a prior test.
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_PATH", raising=False)

    return FakeProd(deploy, lodging, calls_file, state_file, notify_file, prefix,
                    scenario_file, nrestarts_file)


def _set_repo(monkeypatch, repo: Path) -> None:
    monkeypatch.setenv("ANGELUS_DEPLOY_REPO", str(repo))


# Session-scoped dev repos (built once; pip caches the wheel per sha so reruns
# are cheap). Migration content is what varies between scenarios.
@pytest.fixture(scope="session")
def dev_repo_0014(tmp_path_factory) -> tuple[Path, str]:
    dest = tmp_path_factory.mktemp("dev0014") / "repo"
    sha = build_dev_repo(dest, M0014)
    return dest, sha


@pytest.fixture(scope="session")
def dev_repo_0015(tmp_path_factory) -> tuple[Path, str]:
    dest = tmp_path_factory.mktemp("dev0015") / "repo"
    sha = build_dev_repo(dest, M0015)
    return dest, sha


# A migration that orphans an FK reference: under the FK-deferred runner (czir,
# migration 0015 was made to apply cleanly on a live findings-bearing DB) this
# DELETE succeeds with foreign_keys OFF, but the runner's pre-commit
# `PRAGMA foreign_key_check` then catches the dangling finding and raises the
# same sqlite3.IntegrityError class the 2026-06-12 outage raised. It is the
# stand-in for "a migration that would corrupt the live DB", which preflight
# must catch before touching prod -- 0015 itself no longer qualifies now that
# the runner is FK-safe.
_BREAKING_MIGRATION_NAME = "9999_test_orphan_finding_fk.sql"
_BREAKING_MIGRATION_SQL = "DELETE FROM observations;\n"


@pytest.fixture(scope="session")
def dev_repo_breaking(tmp_path_factory) -> tuple[Path, str]:
    """Dev repo at 0015 plus a migration that fails the FK-safe runner's
    foreign_key_check on a findings-bearing DB -- the preflight failure shape
    post-czir."""
    dest = tmp_path_factory.mktemp("devbreak") / "repo"
    build_dev_repo(dest, M0015)
    (dest / "migrations" / _BREAKING_MIGRATION_NAME).write_text(
        _BREAKING_MIGRATION_SQL, encoding="utf-8"
    )
    _git(dest, "add", "-A")
    _git(dest, "commit", "-q", "-m", "add FK-orphaning migration")
    out = subprocess.run(
        ["git", "-C", str(dest), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True, env=_GIT_ENV,
    )
    return dest, out.stdout.strip()


def _digest(path: Path) -> str | None:
    if not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _db_digests(state: Path) -> str | None:
    # Hash only the main db file. A read-only backup-API open of a WAL database
    # can create empty -wal/-shm sidecars without mutating any data; the main
    # file's bytes are the prod-data-untouched invariant.
    return _digest(state / "angelus.sqlite3")


# ===========================================================================
# Happy path.
# ===========================================================================


def test_happy_path_order_log_and_hold_released(
    fake_prod, dev_repo_0014, monkeypatch
):
    repo, sha = dev_repo_0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)
    pre = _db_digests(fake_prod.state)

    # Record high-level step order: cmd_deploy looks each step up as a module
    # global, so wrapping the module attribute captures the real call order.
    order: list[str] = []
    dep = fake_prod.deploy
    for step in ("hold_belfry", "preflight", "backup_live_db", "pip_install",
                 "copy_belt", "restart_unit", "verify", "append_deploys_log",
                 "release_hold"):
        original = getattr(dep, step)

        def make_wrapper(name, func):
            def wrapper(*a, **k):
                order.append(name)
                return func(*a, **k)
            return wrapper

        monkeypatch.setattr(dep, step, make_wrapper(step, original))

    rc = dep.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc == 0

    # Order: hold first, preflight before any mutation, backup before pip,
    # belt before restart, verify before log, hold released last.
    assert order == [
        "hold_belfry", "preflight", "backup_live_db", "pip_install",
        "copy_belt", "restart_unit", "verify", "append_deploys_log",
        "release_hold",
    ]

    # deploys.log line records the deploy.
    assert fake_prod.deploys_log.exists()
    log_line = fake_prod.deploys_log.read_text().strip()
    assert f"to={sha}" in log_line
    assert "operator=tester" in log_line
    assert "duration=" in log_line and "backup=" in log_line

    # Hold released; stamp written; backup + belt present.
    assert not fake_prod.hold.exists()
    assert fake_prod.stamp.read_text().strip() == sha
    assert list(fake_prod.backups.glob("angelus-*.sqlite3"))
    assert (fake_prod.bin / "belfry.py").exists()
    assert (fake_prod.bin / "sre_runner.py").exists()

    # The live db survived (backup-API copy is read-only) -- same bytes.
    assert _db_digests(fake_prod.state) == pre

    # A restart (not stop/start) drove the no-rollback path; verify polled the
    # supervisor via `systemctl show` (not the crash-blind is-active).
    calls = " | ".join(fake_prod.systemctl_calls())
    assert "restart angelus-test" in calls
    assert "show" in calls and "angelus-test" in calls


# ===========================================================================
# Verify gate: a crash-looping daemon must FAIL verify (the 0015 startup-crash
# class). systemd marks Type=simple "active" the instant it forks, so a daemon
# that execs then crashes reads "active" on the first poll; first-active gating
# would wrongly release the hold on a flapping unit.
# ===========================================================================


def test_verify_fails_on_crash_looping_daemon(
    fake_prod, dev_repo_0014, monkeypatch
):
    repo, sha = dev_repo_0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)
    # The unit enters the deploy ALREADY crash-looping (NRestarts>0) and the new
    # code KEEPS crashing: the deploy's reset-failed zeroes the prior count, but
    # the unit climbs NRestarts above 0 again post-reset and flips to activating.
    # reset-failed must not mask a genuinely still-crashing unit.
    fake_prod.set_nrestarts(3)
    fake_prod.crash_loop()
    # Short verify window so the failing poll loop doesn't spin long (poll=0).
    monkeypatch.setenv("ANGELUS_DEPLOY_VERIFY_TIMEOUT_SEC", "1")

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=False)

    # verify must FAIL: the unit never sustains active with NRestarts==0.
    assert rc == 1
    # Mutation note: against the old first-active gate this returns 0 (the unit
    # reads "active" + health exits 0 on the first poll), so this assertion is
    # the red that proves the new sustained-liveness gate is doing the work.

    # The deploy did NOT declare success: hold left to self-expire, no stamp,
    # no deploys.log line, operator notified loudly.
    assert fake_prod.hold.exists()
    assert not fake_prod.stamp.exists()
    assert not fake_prod.deploys_log.exists()
    assert fake_prod.notified()
    # verify did reach the supervisor-state poll (not is-active).
    assert "show" in " | ".join(fake_prod.systemctl_calls())


def test_verify_passes_recovery_from_crash_loop(
    fake_prod, dev_repo_0014, monkeypatch
):
    """The commonest fix-deploy: the daemon is ALREADY crash-looping when the
    deploy runs (NRestarts>0), and the NEW code is healthy. The host's systemd
    245 does NOT flush NRestarts on a `restart` issued mid auto-restart, so the
    deploy issues `reset-failed` immediately before the restart; that zeroes
    NRestarts so verify's NRestarts==0 gate reads a true "crashed since we
    started it" signal and PASSES.

    Mutation-verify: delete the reset-failed call in deploy.restart_unit and this
    goes red -- the retained NRestarts>0 trips the gate so verify never passes
    and cmd_deploy returns 1. (The shim RETAINS NRestarts across restart and
    zeroes only on reset-failed, so the test is not vacuous.)"""
    repo, sha = dev_repo_0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)
    # Enter mid crash-loop: NRestarts already >0, retained across restart. The
    # new code is healthy (scenario stays 'healthy'), so it stops crashing.
    fake_prod.set_nrestarts(2)

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc == 0

    # reset-failed was issued IMMEDIATELY before the controlled restart.
    calls = fake_prod.systemctl_calls()
    reset_idx = next(
        i for i, c in enumerate(calls) if "reset-failed angelus-test" in c
    )
    restart_idx = next(
        i for i, c in enumerate(calls) if "restart angelus-test" in c
    )
    assert reset_idx < restart_idx

    # The deploy declared success: hold released, stamp + log written.
    assert not fake_prod.hold.exists()
    assert fake_prod.stamp.read_text().strip() == sha
    assert f"to={sha}" in fake_prod.deploys_log.read_text()


# ===========================================================================
# Preflight failure: a migration that would corrupt the live DB.
# Post-czir the runner is FK-safe, so 0015 itself no longer fails; the target
# here carries an extra migration that orphans an FK reference, which the
# runner's foreign_key_check rejects with the same IntegrityError class.
# ===========================================================================


def test_preflight_failure_leaves_prod_byte_identical(
    fake_prod, dev_repo_breaking, monkeypatch
):
    repo, sha = dev_repo_breaking  # target carries an FK-orphaning migration
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)  # live at 0014 + FK
    pre = _db_digests(fake_prod.state)

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc == 1

    # Live db byte-identical: preflight ran before any mutation.
    assert _db_digests(fake_prod.state) == pre
    # Nothing downstream of preflight happened.
    assert not fake_prod.backups.exists() or not list(
        fake_prod.backups.glob("angelus-*.sqlite3")
    )
    assert not fake_prod.deploys_log.exists()
    assert not fake_prod.stamp.exists()
    assert not (fake_prod.bin / "belfry.py").exists()
    # The unit was never touched (no restart/stop/start).
    calls = " ".join(fake_prod.systemctl_calls())
    assert "restart" not in calls and "stop" not in calls and "start" not in calls
    # Hold LEFT to self-expire; operator notified loudly.
    assert fake_prod.hold.exists()
    assert fake_prod.notified()


def test_preflight_failure_is_the_production_integrity_error(
    fake_prod, dev_repo_breaking, monkeypatch, capsys
):
    """The preflight failure carries the genuine IntegrityError (the FK-safe
    runner's foreign_key_check raising on the orphaned finding), not some
    incidental error -- ties the byte-identical test to a real FK violation."""
    repo, sha = dev_repo_breaking
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc == 1
    captured = capsys.readouterr()
    assert "IntegrityError" in captured.err
    assert "migration dry-run FAILED" in captured.err


def test_mutation_skipping_dryrun_hits_integrity_error(
    fake_prod, dev_repo_breaking, monkeypatch
):
    """Mutation verification: without the dry-run guard, the target migrations
    applied to the LIVE db raise the IntegrityError. This is the red the
    preflight test would go if its guard were removed -- proving the test isn't
    vacuously green."""
    repo, _sha = dev_repo_breaking
    _build_db(fake_prod.live_db, M0014, with_findings=True)

    # Apply the TARGET ref's migrations directly to the live db (what a restart
    # would do with no preflight). The FK-orphaning migration deletes the
    # observation the finding references; the runner's pre-commit
    # foreign_key_check catches the dangling finding and raises.
    sys.path.insert(0, str(REPO_ROOT))
    from angelus.storage.migrations import init_db
    with pytest.raises(sqlite3.IntegrityError):
        init_db(fake_prod.live_db, repo / "migrations").close()


# ===========================================================================
# Rollback guard: schema ahead of the target ref.
# ===========================================================================


def _seed_prior_backup(fake_prod, version: str) -> Path:
    fake_prod.backups.mkdir(parents=True, exist_ok=True)
    backup = fake_prod.backups / "angelus-20200101T000000Z.sqlite3"
    _build_db(backup, version, with_findings=False)
    return backup


def _seed_named_backup(fake_prod, name: str, version: str) -> Path:
    """Seed a pre-deploy backup with an explicit (time-ordered) filename so a
    multi-deploy backup history can be built: existing_backups sorts by name."""
    fake_prod.backups.mkdir(parents=True, exist_ok=True)
    backup = fake_prod.backups / name
    _build_db(backup, version, with_findings=False)
    return backup


def test_schema_ahead_refuses_without_restore_backup(
    fake_prod, dev_repo_0014, monkeypatch
):
    repo, sha = dev_repo_0014  # target only has 0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0015, with_findings=False)  # live AHEAD (0015)
    pre = _db_digests(fake_prod.state)

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc == 1
    # Prod untouched, hold left, operator notified.
    assert _db_digests(fake_prod.state) == pre
    assert fake_prod.hold.exists()
    assert fake_prod.notified()
    # No unit action.
    assert "restart" not in " ".join(fake_prod.systemctl_calls())


def test_schema_ahead_restores_with_flag(fake_prod, dev_repo_0014, monkeypatch):
    repo, sha = dev_repo_0014  # rolling back to 0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0015, with_findings=False)  # live ahead at 0015
    prior = _seed_prior_backup(fake_prod, M0014)  # freshest pre-deploy backup
    prior_digest = _digest(prior)

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=True)
    assert rc == 0

    # Live db restored to the 0014 backup: 0015 no longer applied.
    applied = fake_prod.deploy.applied_versions(fake_prod.live_db)
    assert M0015 not in applied
    assert M0014 in applied

    # Rollback path is stop -> (restore) -> start, not restart.
    calls = fake_prod.systemctl_calls()
    joined = " | ".join(calls)
    assert "stop angelus-test" in joined and "start angelus-test" in joined
    assert "restart angelus-test" not in joined
    stop_idx = next(i for i, c in enumerate(calls) if "stop" in c)
    start_idx = next(i for i, c in enumerate(calls) if c.strip().startswith("--user start"))
    assert stop_idx < start_idx

    # deploys.log names the restored backup; hold released.
    log_line = fake_prod.deploys_log.read_text()
    assert f"restored={prior}" in log_line
    assert not fake_prod.hold.exists()
    # Sanity: the prior backup file itself was not mutated by the restore.
    assert _digest(prior) == prior_digest


def test_schema_ahead_picks_subset_backup_not_freshest(
    fake_prod, dev_repo_0014, monkeypatch
):
    """Multi-deploy history: the FRESHEST backup is itself schema-ahead of the
    rollback target (backups are taken at each deploy's START). The freshest is
    the wrong choice -- it would put old code on a newer schema. The guard must
    pick the freshest backup whose schema is a subset of the target ref's."""
    repo, sha = dev_repo_0014  # target only has 0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0015, with_findings=False)  # live ahead at 0015
    ahead_backup = _seed_named_backup(
        fake_prod, "angelus-20200102T000000Z.sqlite3", M0015)  # freshest, ahead
    target_backup = _seed_named_backup(
        fake_prod, "angelus-20200101T000000Z.sqlite3", M0014)  # older, at target

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=True)
    assert rc == 0

    # Restored the 0014 backup (subset of target), NOT the freshest 0015 one.
    log_line = fake_prod.deploys_log.read_text()
    assert f"restored={target_backup}" in log_line
    assert f"restored={ahead_backup}" not in log_line
    applied = fake_prod.deploy.applied_versions(fake_prod.live_db)
    assert M0015 not in applied and M0014 in applied
    assert not fake_prod.hold.exists()


def test_schema_ahead_refuses_when_only_backup_is_ahead(
    fake_prod, dev_repo_0014, monkeypatch
):
    """If no pre-deploy backup is at-or-below the target ref, --restore-backup
    refuses loudly rather than restoring a schema-ahead backup blindly."""
    repo, sha = dev_repo_0014  # target only has 0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0015, with_findings=False)  # live ahead at 0015
    pre = _db_digests(fake_prod.state)
    _seed_named_backup(fake_prod, "angelus-20200101T000000Z.sqlite3", M0015)  # ahead

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=True)
    assert rc == 1
    # Refused before any unit action: prod untouched, hold left, notified.
    assert _db_digests(fake_prod.state) == pre
    assert fake_prod.hold.exists()
    assert fake_prod.notified()
    calls = " ".join(fake_prod.systemctl_calls())
    assert "stop" not in calls and "start" not in calls


def test_schema_ahead_skips_corrupt_backup_picks_older_valid(
    fake_prod, dev_repo_0014, monkeypatch
):
    """The FRESHEST backup is corrupt/unreadable; an older backup is valid and
    at-or-below the target. A corrupt backup's schema can't be read, so it must
    be SKIPPED -- not treated as an empty (trivially-subset) set and selected,
    which would mask the valid older candidate and only fail at the copy. The
    guard falls through to the older valid subset backup."""
    repo, sha = dev_repo_0014  # target only has 0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0015, with_findings=False)  # live ahead at 0015
    # Newest backup: corrupt garbage (not a sqlite database).
    fake_prod.backups.mkdir(parents=True, exist_ok=True)
    corrupt = fake_prod.backups / "angelus-20200103T000000Z.sqlite3"
    corrupt.write_bytes(b"this is not a sqlite database\n" * 8)
    # Older backup: valid, at the rollback target (0014).
    valid = _seed_named_backup(
        fake_prod, "angelus-20200101T000000Z.sqlite3", M0014)

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=True)
    assert rc == 0

    # The older VALID subset backup was chosen, NOT the corrupt newest one.
    log_line = fake_prod.deploys_log.read_text()
    assert f"restored={valid}" in log_line
    assert f"restored={corrupt}" not in log_line
    applied = fake_prod.deploy.applied_versions(fake_prod.live_db)
    assert M0015 not in applied and M0014 in applied
    assert not fake_prod.hold.exists()


# ===========================================================================
# Idempotence: rerun-after-kill at each step converges.
# ===========================================================================


@pytest.mark.parametrize(
    "kill_at",
    ["preflight", "backup_live_db", "pip_install", "copy_belt", "restart_unit"],
)
def test_rerun_after_kill_converges(fake_prod, dev_repo_0014, monkeypatch, kill_at):
    repo, sha = dev_repo_0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)
    dep = fake_prod.deploy

    # First run: blow up AT a given step (simulating a kill mid-deploy). The
    # step itself runs (its side effects, if any, land) then raises.
    original = getattr(dep, kill_at)

    def boom(*a, **k):
        original(*a, **k)
        raise dep.DeployError(f"simulated kill after {kill_at}")

    monkeypatch.setattr(dep, kill_at, boom)
    rc1 = dep.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc1 == 1
    assert fake_prod.hold.exists()  # failure leaves the hold

    # Rerun (unpatched): converges to a clean, fully-deployed state.
    monkeypatch.setattr(dep, kill_at, original)
    rc2 = dep.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc2 == 0
    assert not fake_prod.hold.exists()
    assert fake_prod.stamp.read_text().strip() == sha
    assert (fake_prod.bin / "belfry.py").exists()
    assert (fake_prod.bin / "sre_runner.py").exists()
    assert f"to={sha}" in fake_prod.deploys_log.read_text()
    # Backups never grow past the keep limit across reruns.
    assert len(list(fake_prod.backups.glob("angelus-*.sqlite3"))) <= 5


def test_rerun_after_torn_belt_converges(fake_prod, dev_repo_0014, monkeypatch):
    """copy_belt is the genuinely non-atomic step the parametrized kill-AFTER
    test can't probe for a kill-DURING. A kill mid-copy (cron can fire under the
    deploy's own hold) must at worst leave a stale whole file + a leftover .tmp;
    a clean rerun must converge to the full, correct belt with no torn file and
    no .tmp residue. (A half-finished pip_install is made idempotent separately
    by --ignore-installed on rerun; see ANGELUS_DEPLOY_PIP_INSTALL_ARGS.)"""
    repo, sha = dev_repo_0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)
    # Pre-write the torn state a kill mid-copy_belt would leave behind.
    fake_prod.bin.mkdir(parents=True, exist_ok=True)
    (fake_prod.bin / "belfry.py").write_text("# torn / truncated\n")
    (fake_prod.bin / "belfry.py.tmp").write_text("# partial write\n")

    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc == 0

    # Converged: belfry.py is the full real target file; .tmp residue gone.
    deployed = (fake_prod.bin / "belfry.py").read_bytes()
    expected = (repo / "belfry" / "belfry.py").read_bytes()
    assert deployed == expected
    assert not (fake_prod.bin / "belfry.py.tmp").exists()
    assert (fake_prod.bin / "sre_runner.py").exists()
    assert not (fake_prod.bin / "sre_runner.py.tmp").exists()


def test_backup_prune_keeps_five(fake_prod, dev_repo_0014, monkeypatch):
    repo, sha = dev_repo_0014
    _set_repo(monkeypatch, repo)
    _build_db(fake_prod.live_db, M0014, with_findings=True)
    # Pre-seed 7 old backups; a deploy adds one and prunes to 5.
    fake_prod.backups.mkdir(parents=True)
    for i in range(7):
        (fake_prod.backups / f"angelus-2020010{i}T000000Z.sqlite3").write_bytes(b"x")
    rc = fake_prod.deploy.cmd_deploy(fake_prod.config(), sha, restore_backup=False)
    assert rc == 0
    assert len(list(fake_prod.backups.glob("angelus-*.sqlite3"))) == 5


# ===========================================================================
# Belfry hold latch (driven against belfry's real functions).
# ===========================================================================


def _write_hold(state: Path, age_sec: float) -> Path:
    state.mkdir(parents=True, exist_ok=True)
    hold = state / "belfry-hold"
    hold.write_text("deploy hold\n", encoding="utf-8")
    when = time.time() - age_sec
    os.utime(hold, (when, when))
    return hold


def test_hold_status_classification(belfry, tmp_path, monkeypatch):
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_PATH", raising=False)
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_MAX_AGE_SEC", raising=False)
    state = tmp_path / "state"
    state.mkdir()
    # Absent.
    assert belfry.hold_status(state)[0] == "absent"
    # Fresh -> active.
    _write_hold(state, age_sec=5)
    assert belfry.hold_status(state)[0] == "active"
    # Old -> stale.
    _write_hold(state, age_sec=5000)
    assert belfry.hold_status(state)[0] == "stale"


def test_hold_max_age_env_failsafe(belfry, monkeypatch):
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_MAX_AGE_SEC", raising=False)
    assert belfry.hold_max_age_sec() == belfry.DEFAULT_HOLD_MAX_AGE_SEC
    monkeypatch.setenv("ANGELUS_BELFRY_HOLD_MAX_AGE_SEC", "garbage")
    assert belfry.hold_max_age_sec() == belfry.DEFAULT_HOLD_MAX_AGE_SEC  # fail-safe
    monkeypatch.setenv("ANGELUS_BELFRY_HOLD_MAX_AGE_SEC", "60")
    assert belfry.hold_max_age_sec() == 60


def _run_belfry_main(belfry, root: Path, monkeypatch, pid_alive: bool):
    """Drive belfry.main against a fake root, intercepting the side-effecting
    seams (pings, restart, pid check) so only the decision logic is exercised."""
    restarts = {"n": 0}
    pings: list[str] = []

    monkeypatch.setattr(belfry, "ping_env", lambda name: pings.append(name) or True)
    monkeypatch.setattr(belfry, "notify", lambda reason: True)

    def fake_restart() -> bool:
        restarts["n"] += 1
        return True

    monkeypatch.setattr(belfry, "restart_daemon", fake_restart)
    monkeypatch.setattr(belfry, "verify_recovery", lambda pid_file: True)
    # pid_failure returns a reason string when DEAD, None when alive.
    monkeypatch.setattr(
        belfry, "pid_failure",
        lambda pid_file: None if pid_alive else "pid: process not running"
    )
    # Silence the live-daemon checks that would otherwise read sqlite.
    for name in ("wedge_failure", "failure_surface", "drift_failure",
                 "stale_deployment", "sla_failure", "source_overdue_failure"):
        monkeypatch.setattr(belfry, name, lambda *a, **k: None)
    monkeypatch.setattr(belfry, "touch_sentinel", lambda path: None)

    rc = belfry.main([str(root)])
    return rc, restarts["n"], pings


def test_hold_suppresses_restart_alerting_unchanged(belfry, tmp_path, monkeypatch):
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_PATH", raising=False)
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_MAX_AGE_SEC", raising=False)
    root = tmp_path
    state = root / "state"
    _write_hold(state, age_sec=5)  # fresh, active hold

    rc, restarts, pings = _run_belfry_main(belfry, root, monkeypatch, pid_alive=False)

    # Restart SUPPRESSED by the active hold...
    assert restarts == 0
    # ...but alerting is unchanged: the DOWN ping still fired.
    assert "ANGELUS_BELFRY_DOWN_URL" in pings


def test_no_hold_restarts_dead_daemon(belfry, tmp_path, monkeypatch):
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_PATH", raising=False)
    root = tmp_path
    (root / "state").mkdir()
    rc, restarts, pings = _run_belfry_main(belfry, root, monkeypatch, pid_alive=False)
    # No hold => the dead daemon is restarted as normal.
    assert restarts == 1
    assert "ANGELUS_BELFRY_DOWN_URL" in pings


def test_stale_hold_overrides_loudly_and_resumes_restart(
    belfry, tmp_path, monkeypatch, capsys
):
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_PATH", raising=False)
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_MAX_AGE_SEC", raising=False)
    root = tmp_path
    state = root / "state"
    _write_hold(state, age_sec=5000)  # older than the 1800s default => stale

    rc, restarts, pings = _run_belfry_main(belfry, root, monkeypatch, pid_alive=False)

    # Stale hold is overridden: restart RESUMES.
    assert restarts == 1
    # It is surfaced as its own DOWN reason, logged loudly.
    captured = capsys.readouterr()
    assert "deploy-hold-stale" in captured.err
    assert "ANGELUS_BELFRY_DOWN_URL" in pings


def test_stale_hold_alerts_even_when_daemon_healthy(
    belfry, tmp_path, monkeypatch, capsys
):
    """An abandoned (stale) hold pings DOWN even with a healthy daemon, so a
    deploy that died without releasing its lock is surfaced."""
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_PATH", raising=False)
    monkeypatch.delenv("ANGELUS_BELFRY_HOLD_MAX_AGE_SEC", raising=False)
    root = tmp_path
    state = root / "state"
    _write_hold(state, age_sec=5000)

    rc, restarts, pings = _run_belfry_main(belfry, root, monkeypatch, pid_alive=True)
    # Daemon healthy => no restart, but the stale hold still pings DOWN.
    assert restarts == 0
    assert "ANGELUS_BELFRY_DOWN_URL" in pings
    assert "deploy-hold-stale" in capsys.readouterr().err
