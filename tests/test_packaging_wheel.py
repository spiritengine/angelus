"""The discriminating packaging test: a NON-EDITABLE install must be able to
locate and apply its bundled migrations.

This is the test that would have caught the 2026-06-13 outage
(finding-20260613-mbfc). The first real `make deploy` swapped the editable
install for a wheel install and the daemon crash-looped with
`FileNotFoundError: site-packages/migrations` -- migrations/ was a top-level
repo dir resolved via `__file__.parents[2]`, which only coincides with the repo
root under an editable install. The whole dev/editable suite stayed green
because every other test resolves migrations from the dev tree.

So this test must NOT exercise the editable/dev-tree copy. It builds a wheel
from the repo, installs it NON-EDITABLE into an isolated --prefix, and then in a
SUBPROCESS whose path points at that prefix (and whose cwd is neutral, so a
stray ./angelus can't shadow the install) runs the real init_db. It asserts the
genuinely-installed package both LOCATES its migrations and APPLIES all of them.

Mutation outcome (verified out-of-band, the way the other mutation checks in
this suite are): revert the resolution to `parents[2] / "migrations"`, OR drop
the `[tool.setuptools.package-data] angelus = ["migrations/*.sql"]` line from
pyproject, and this test goes RED -- the subprocess dies with the missing-
migrations FileNotFoundError instead of returning the 16 applied versions.

Marked `slow` because it shells out to a real wheel build + install (a few
seconds). It is NOT skipped: it runs in the default suite.
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[1]

# All migration filenames shipped in the package, so "applied all of them" is
# checked against the actual on-disk set rather than a hard-coded count.
ALL_MIGRATIONS = sorted(
    p.name for p in (REPO_ROOT / "angelus" / "migrations").glob("0*.sql")
)


# Runs inside the installed-prefix subprocess. Resolves migrations the way the
# daemon does (package-relative, via the installed angelus), applies them to a
# fresh db, and emits a JSON report. Any failure to locate the migrations raises
# here -- which is exactly the crash-loop class, surfaced as a nonzero exit.
_PROBE = r"""
import json, sys
from pathlib import Path
from angelus.storage.migrations import DEFAULT_MIGRATIONS_DIR as migdir
from angelus.storage import init_db

db = sys.argv[1]
conn = init_db(db)
try:
    versions = sorted(
        row[0] for row in conn.execute("SELECT version FROM schema_migrations")
    )
    obs = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='observations'"
    ).fetchone()
finally:
    conn.close()

print(json.dumps({
    "migdir": str(migdir),
    "versions": versions,
    "observations_sql": obs[0] if obs else None,
}))
"""


@pytest.fixture(scope="module")
def installed_prefix(tmp_path_factory) -> tuple[str, str]:
    """Build the wheel and install it NON-EDITABLE into an isolated --prefix.

    Returns (prefix, purelib). purelib is computed with the SAME interpreter that
    will run the probe (sys.executable), so it matches where pip placed the
    package under the prefix -- the deploy preflight resolves it identically.
    """
    prefix = tmp_path_factory.mktemp("install-prefix")
    wheelhouse = tmp_path_factory.mktemp("wheelhouse")

    # Build from a PRISTINE copy of the build inputs, not the working tree. The
    # real deploy installs from `pip install git+file://repo@sha`, i.e. a fresh
    # clone with no build/ or *.egg-info. Building in-place against this repo's
    # leftover build artifacts would let stale copies of the .sql leak into the
    # wheel and the test would pass even with the packaging reverted. Copying
    # only the genuine build inputs reproduces the clean-clone build the deploy
    # actually performs. (Verified: with package-data dropped, a clean build
    # omits the migrations and this test goes RED; an in-place build does not.)
    src = tmp_path_factory.mktemp("clean-src")
    for name in ("pyproject.toml", "README.md"):
        shutil.copy(REPO_ROOT / name, src / name)
    shutil.copytree(
        REPO_ROOT / "angelus", src / "angelus",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc"),
    )

    # --no-cache-dir so pip's local-dir wheel cache (~/.cache/pip/wheels) can't
    # serve a stale wheel built before a packaging change.
    build = subprocess.run(
        [sys.executable, "-m", "pip", "wheel", "--no-deps", "--no-cache-dir",
         "--no-build-isolation", "-w", str(wheelhouse), str(src)],
        check=False, capture_output=True, text=True,
    )
    assert build.returncode == 0, f"wheel build failed:\n{build.stderr}\n{build.stdout}"
    wheels = list(wheelhouse.glob("angelus-*.whl"))
    assert len(wheels) == 1, f"expected one angelus wheel, got {wheels}"

    install = subprocess.run(
        [sys.executable, "-m", "pip", "install", "--prefix", str(prefix),
         "--no-deps", "--no-build-isolation", "--ignore-installed", str(wheels[0])],
        check=False, capture_output=True, text=True,
    )
    assert install.returncode == 0, f"install failed:\n{install.stderr}\n{install.stdout}"

    purelib = sysconfig.get_path(
        "purelib", vars={"base": str(prefix), "platbase": str(prefix)}
    )
    return str(prefix), purelib


@pytest.mark.slow
def test_non_editable_install_locates_and_applies_migrations(
    installed_prefix, tmp_path
):
    prefix, purelib = installed_prefix

    # Sanity: the .sql really shipped inside the installed package tree.
    installed_pkg = Path(purelib) / "angelus"
    shipped = sorted(p.name for p in (installed_pkg / "migrations").glob("0*.sql"))
    assert shipped == ALL_MIGRATIONS, (
        "migrations did not ship in the wheel -- package-data is the only thing "
        "that carries them into a non-editable install"
    )

    # Run the real runner from a NEUTRAL cwd with ONLY the prefix on the path, so
    # the import resolves to the genuinely-installed package and no dev-tree
    # ./angelus can shadow it. This is the daemon's startup path, minus the daemon.
    env = dict(os.environ)
    env["PYTHONPATH"] = purelib
    db = tmp_path / "live.sqlite3"
    result = subprocess.run(
        [sys.executable, "-c", _PROBE, str(db)],
        cwd=str(tmp_path),  # neutral: no ./angelus here
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, (
        "installed package could not locate/apply its migrations -- this is the "
        f"crash-loop class:\nSTDERR:\n{result.stderr}\nSTDOUT:\n{result.stdout}"
    )
    report = json.loads(result.stdout)

    # It resolved migrations to the INSTALLED location, not the dev tree.
    assert report["migdir"].startswith(str(prefix)), report["migdir"]

    # Every migration applied.
    assert report["versions"] == ALL_MIGRATIONS
    assert len(report["versions"]) == len(ALL_MIGRATIONS)

    # A known late-migration effect took: 0015 rebuilt observations to widen the
    # status CHECK to include 'consumed'. Its presence proves the chain ran to
    # the end, not just the early files.
    assert report["observations_sql"] is not None
    assert "consumed" in report["observations_sql"]
