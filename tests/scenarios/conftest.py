"""Shared scaffolding for the B27 scenario fixtures (tests/scenarios/).

A scenario replays ONE real incident end to end on a single root: the daemon
side through the B26 SimHarness step primitives (fire_source / run_triage /
drain / advance), the belfry side through the REAL belfry check functions read
against that same root's sqlite/state. The point each scenario proves is that
the detection ladder fired WHILE the coarse liveness checks stayed green --
every production incident here had the shape "something is red-silent while the
green checks are green".

Conventions every scenario in this package honors (stated here once):

  1. RED AND GREEN are both mandatory. A scenario asserts BOTH that the
     detecting check returns a reason string naming the failure AND that the
     checks which must not fire return None. Proving only that the failure
     happened is not a finished scenario -- it must prove the machinery noticed
     AND that the coarse checks were not fooled.

  2. Channel failure ONLY via the fault seam. Arm ``ANGELUS_FAULT_INJECT`` (or a
     ``FaultRegistry``) before harness construction; never monkeypatch a sender.
     Under the harness's guaranteed dry-run a send otherwise SUCCEEDS (it writes
     a dispatches.log line), so a fault is the only correct way to make a
     channel fail in a scenario.

  3. Clocks. Belfry checks that take an explicit ``now`` (sla_failure,
     source_overdue_failure) are called with ``sim.clock.now()`` so the SLA math
     is wholly in sim time. The real-time checks (wedge_failure,
     within_startup_grace) read the wall clock and /proc directly and cannot be
     told a sim instant -- so a scenario exercises them with the test_belfry.py
     conventions: it pins the sim START at ~real-now so a fire stamps a
     wall-fresh watch_state heartbeat, writes the pid file by hand, and
     monkeypatches process_start_epoch to choose the daemon's apparent age.

  4. Every scenario is MUTATION-VERIFIED: reverting the fix/detection it guards
     must turn the scenario red. The mutation applied and its red result are
     recorded in each scenario module's docstring.

The two factories below mean no scenario hand-rolls lodging or belfry paths.
``reference_lodging`` generalizes test_b26_sim_harness._write_lodging
(parameterized source count/cadences, literal-vs-env-marker email, backup
wiring, per-pipe delivery SLA). ``belfry_paths`` follows test_belfry.py's
state-dir / pid / sentinel / failcheck / restart-log conventions.
"""

from __future__ import annotations

import importlib.util
import json
import shutil
import sqlite3
from datetime import UTC, datetime
from pathlib import Path

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[2]
BELFRY_PATH = PROJECT_ROOT / "belfry" / "belfry.py"

# The belfry env vars a scenario must not inherit from the ambient environment
# (mirrors test_belfry._clear_belfry_env) plus the two seams a scenario arms
# deliberately: a leftover fault or dry-run override from another test would
# silently change a scenario's meaning.
_SCENARIO_ENV_VARS = [
    "ANGELUS_BELFRY_NOTIFY_COMMAND",
    "ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC",
    "ANGELUS_BELFRY_SUCCESS_URL",
    "ANGELUS_BELFRY_DOWN_URL",
    "ANGELUS_BELFRY_MAX_RESTARTS",
    "ANGELUS_BELFRY_RESTART_WINDOW_SEC",
    "ANGELUS_BELFRY_RECOVER_WAIT_SEC",
    "ANGELUS_BELFRY_SENTINEL_PATH",
    "ANGELUS_BELFRY_FAILCHECK_PATH",
    "ANGELUS_BELFRY_NEEDS_SRE_PATH",
    "ANGELUS_BELFRY_RESTART_PATH",
    "ANGELUS_BELFRY_FIXERS_LOG_PATH",
    "ANGELUS_BELFRY_STARTUP_GRACE_SEC",
    "ANGELUS_BELFRY_TICK_INTERVAL_SEC",
    "ANGELUS_SYSTEMD_UNIT",
    "ANGELUS_FAULT_INJECT",
]


@pytest.fixture(autouse=True)
def _clear_scenario_env(monkeypatch):
    """No scenario inherits belfry/fault env from the ambient process or a
    sibling test -- each arms exactly what it means to."""
    for var in _SCENARIO_ENV_VARS:
        monkeypatch.delenv(var, raising=False)


@pytest.fixture
def belfry():
    """The belfry module, loaded the test_belfry.py way (belfry/belfry.py is a
    standalone script, not an installed package). Scenarios call its check
    functions against the sim root and monkeypatch its module-level seams
    (systemd_main_pid, process_start_epoch)."""
    spec = importlib.util.spec_from_file_location("belfry_under_test", BELFRY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


# --------------------------------------------------------------------------
# reference_lodging: the generalized _write_lodging.
# --------------------------------------------------------------------------


def _slug(source_ref: str) -> str:
    return source_ref.rsplit("/", 1)[-1]


def _write_reference_lodging(
    root: Path,
    *,
    sources: tuple[tuple[str, str], ...] = (("scheduled/watch", "1h"),),
    status_code: int = 503,
    daily_max_interval: str | None = "24h",
    daily_channels: tuple[str, ...] = ("email",),
    now_channels: tuple[str, ...] = ("push",),
    email_to: str = "person@example.com",
    email_env_var: str | None = None,
    email_backup: str | None = None,
) -> dict[str, Path]:
    """Write a self-contained lodging and return the per-source fixture paths
    (keyed by source_ref) so a scenario can rewrite a status between fires.

    Generalizes test_b26_sim_harness._write_lodging:
      - ``sources``: one ``(source_ref, cadence)`` per shell source; each gets
        its own JSON status fixture, source yaml, and a triager wired to the
        real canary_watch handler (which emits a `down` finding on the up->down
        edge, routed to BOTH `now` and `daily`).
      - ``daily_max_interval``: the daily digest pipe's delivery SLA (B2); None
        omits it so no pipe_sla row is tracked (a scenario that does not want
        the out-of-band SLA to fire).
      - ``daily_channels``: the digest's channels (default email-only; pass
        ("email", "push") for the additive transports shape).
      - ``now_channels``: the immediate `now` pipe's channels (default push-only,
        the production shape). A scenario exercising the B13 immediate-path
        failover ladder routes `now` to ("email",) with ``email_backup="push"``
        so a degraded email primary fails over to the push backup (the digest
        path does not honour ``backup`` -- see runner._drain_digest -- so the
        failover ladder lives only on the immediate path).
      - ``email_to`` / ``email_env_var``: the email channel's recipient as a
        literal address, or as a B18 ``$env:NAME`` marker (env-marker form, for
        the degraded-startup class) when ``email_env_var`` is given.
      - ``email_backup``: the B13 failover backup channel name on email.
    """
    fixtures_dir = root / "fixtures"
    fixtures_dir.mkdir(parents=True, exist_ok=True)
    (root / "triagers" / "handlers").mkdir(parents=True, exist_ok=True)
    shutil.copy(
        PROJECT_ROOT
        / "examples" / "lodging" / "triagers" / "handlers" / "canary_watch.py",
        root / "triagers" / "handlers" / "canary_watch.py",
    )

    fixtures: dict[str, Path] = {}
    for source_ref, cadence in sources:
        slug = _slug(source_ref)
        entity = f"{slug}.example"
        fixture = fixtures_dir / f"{slug}.json"
        fixture.write_text(
            json.dumps(
                {
                    "source_ref": source_ref,
                    "entity": entity,
                    "url": f"https://{entity}",
                    "status_code": status_code,
                }
            ),
            encoding="utf-8",
        )
        fixtures[source_ref] = fixture
        source_path = root / "sources" / source_ref
        source_path.parent.mkdir(parents=True, exist_ok=True)
        source_path.with_suffix(".yaml").write_text(
            f"cadence: {cadence}\ncheck:\n  kind: shell\n  command: 'cat {fixture}'\n",
            encoding="utf-8",
        )
        (root / "triagers" / f"{slug}.yaml").write_text(
            f"inputs:\n  source: {source_ref}\n"
            "handler:\n  kind: python\n  path: triagers/handlers/canary_watch.py\n",
            encoding="utf-8",
        )

    pipes = root / "pipes"
    pipes.mkdir(exist_ok=True)
    (pipes / "now.yaml").write_text(
        "cadence: immediate\nchannels: [" + ", ".join(now_channels) + "]\n"
        "render:\n  kind: dumb-alert\n"
        "  template: '[now] {source} {type}:{entity}'\n",
        encoding="utf-8",
    )
    daily_lines = ["cadence: '0 8 * * *'"]
    if daily_max_interval is not None:
        daily_lines.append(f"max_interval: {daily_max_interval}")
    daily_lines.append("channels: [" + ", ".join(daily_channels) + "]")
    daily_lines.append(
        "render:\n  preamble: []\n  body:\n    kind: llm\n"
        "    mantle: chronicler\n    inputs:\n      - findings_since_last_drain"
    )
    (pipes / "daily.yaml").write_text("\n".join(daily_lines) + "\n", encoding="utf-8")

    channels = root / "channels"
    channels.mkdir(exist_ok=True)
    (channels / "push.yaml").write_text(
        "kind: push\ncommand: notify-pat\n", encoding="utf-8"
    )
    email_yaml = ["kind: email", "command: 'true'"]
    if email_env_var is not None:
        email_yaml.append(f"to: $env:{email_env_var}")
    else:
        email_yaml.append(f"to: {email_to}")
    if email_backup is not None:
        email_yaml.append(f"backup: {email_backup}")
    (channels / "email.yaml").write_text("\n".join(email_yaml) + "\n", encoding="utf-8")

    return fixtures


@pytest.fixture
def reference_lodging():
    """Factory: ``reference_lodging(root, **opts)`` writes the lodging and
    returns the per-source fixture paths. See ``_write_reference_lodging``."""
    return _write_reference_lodging


# --------------------------------------------------------------------------
# belfry_paths: the test_belfry.py state-dir / pid / heartbeat conventions.
# --------------------------------------------------------------------------


class _BelfryPaths:
    """The on-disk surface belfry reads under a sim root's ``state/`` dir, plus
    the two seeding helpers a scenario needs for the real-time checks.

    The sim daemon writes its sqlite at ``state/angelus.sqlite3`` but never the
    pid file (run() does, and the harness never calls run()), so a scenario
    writes the pid by hand -- exactly as test_belfry.py does."""

    def __init__(self, root: Path) -> None:
        self.root = Path(root)
        self.state_dir = self.root / "state"
        self.db_path = self.state_dir / "angelus.sqlite3"
        self.pid_file = self.state_dir / "angelus.pid"
        # Default belfry path (no env override in scenarios): the failcheck
        # watermark failure_surface bookmarks failed-dispatch ids against.
        self.failcheck = self.state_dir / "belfry-failcheck"

    def write_pid(self, pid: int) -> Path:
        """Write ``state/angelus.pid`` so pid_failure/drift/wedge/grace have a
        pid to read. Pass ``os.getpid()`` for an alive daemon (pid green)."""
        self.state_dir.mkdir(parents=True, exist_ok=True)
        self.pid_file.write_text(str(pid), encoding="utf-8")
        return self.pid_file

    def seed_wall_fresh_heartbeat(
        self, source_ref: str = "scheduled/heartbeat", when: datetime | None = None
    ) -> None:
        """Upsert a watch_state row whose last_checked_at is stamped relative to
        REAL now, so the wall-clock wedge_failure reads a fresh GLOBAL
        heartbeat. The test_belfry.py convention for the real-time checks: a sim
        fire pinned far from real-now stamps a heartbeat that wedge_failure
        (which reads datetime.now(UTC)) would see as stale. Most scenarios pin
        START at ~real-now so a real fire already does this; this helper is for
        a scenario that must seed a heartbeat without firing."""
        when = when or datetime.now(UTC)
        stamp = when.isoformat(timespec="milliseconds").replace("+00:00", "Z")
        self.state_dir.mkdir(parents=True, exist_ok=True)
        connection = sqlite3.connect(self.db_path)
        try:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS watch_state (
                    source_ref TEXT PRIMARY KEY,
                    last_checked_at TEXT NOT NULL,
                    last_state TEXT,
                    last_outcome TEXT,
                    last_changed_at TEXT,
                    last_observation_id INTEGER,
                    updated_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO watch_state
                    (source_ref, last_checked_at, last_state, last_outcome, updated_at)
                VALUES (?, ?, '200', 'ok', ?)
                ON CONFLICT(source_ref) DO UPDATE SET
                    last_checked_at = excluded.last_checked_at,
                    updated_at = excluded.updated_at
                """,
                (source_ref, stamp, stamp),
            )
            connection.commit()
        finally:
            connection.close()


@pytest.fixture
def belfry_paths():
    """Factory: ``belfry_paths(root)`` -> a ``_BelfryPaths`` for that sim root."""
    return _BelfryPaths
