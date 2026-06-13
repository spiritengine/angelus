"""Post-install migration guard: it must import the INSTALLED package, never the
dev tree, regardless of the inherited environment.

deploy.py's verify_installed_migrations runs a probe subprocess that imports
angelus and checks the freshly-installed package can locate its bundled
migrations. The danger (codex fell-r1) is the NO-PREFIX mode -- the real prod
path, where pip installs into the env and there is no --prefix. There the probe
relied on pip_python's default path, but an inherited PYTHONPATH=<repo> (common
in a dev shell; `make deploy` inherits the operator's env) made the probe import
the DEV TREE, which always finds migrations package-relative -- so the guard
PASSED even when the installed wheel omitted migrations, and the daemon then
crash-looped on restart. The guard added to catch exactly that crash-loop class
could be bypassed in the mode prod uses.

These tests model an "installed env" with a real venv (its site-packages is on
the default path, so the no-prefix branch is exercised faithfully) carrying a
minimal angelus whose migrations are present or absent, and a separate "shadow"
tree that DOES carry migrations reachable only via PYTHONPATH. They assert the
guard's import is pinned to the installed package.

Marked `slow` (each builds a venv); they run in the default suite.
"""

from __future__ import annotations

import importlib.util
import subprocess
import sys
from pathlib import Path

import pytest


REPO_ROOT = Path(__file__).resolve().parents[1]
DEPLOY_PATH = REPO_ROOT / "deploy" / "deploy.py"


def _load_deploy():
    spec = importlib.util.spec_from_file_location("deploy_migguard_under_test", DEPLOY_PATH)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def deploy():
    return _load_deploy()


# ---------------------------------------------------------------------------
# A minimal angelus package that reproduces the runner's package-relative
# migrations resolution (files("angelus") / "migrations") and nothing else, so
# the guard's probe imports and resolves exactly as it does against the real
# wheel -- with full control over whether the migrations ship.
# ---------------------------------------------------------------------------

_MIGRATIONS_PY = (
    "from importlib.resources import files\n"
    "from pathlib import Path\n"
    "DEFAULT_MIGRATIONS_DIR = Path(str(files('angelus') / 'migrations'))\n"
)


def _write_min_angelus(pkg_parent: Path, *, with_migrations: bool) -> None:
    """Lay down `<pkg_parent>/angelus` -- importable, resolving its migrations dir
    package-relative. With with_migrations the dir ships a .sql (a correctly
    packaged install); without, the dir is absent (the wheel-omitted-migrations
    shape that crash-loops)."""
    pkg = pkg_parent / "angelus"
    (pkg / "storage").mkdir(parents=True)
    (pkg / "__init__.py").write_text("__version__ = '0.0.0-test'\n")
    (pkg / "storage" / "__init__.py").write_text("")
    (pkg / "storage" / "migrations.py").write_text(_MIGRATIONS_PY)
    if with_migrations:
        migdir = pkg / "migrations"
        migdir.mkdir()
        (migdir / "0001_test.sql").write_text("-- test migration\n")


def _make_installed_venv(venv_dir: Path, *, with_migrations: bool) -> tuple[Path, Path]:
    """Build a venv (no system site-packages, so the editable base angelus is NOT
    visible) and drop the minimal angelus into its site-packages -- an "installed
    env" reachable on the default path WITHOUT any PYTHONPATH. Returns
    (python, site_packages)."""
    subprocess.run(
        [sys.executable, "-m", "venv", "--without-pip", str(venv_dir)],
        check=True, capture_output=True,
    )
    py = venv_dir / "bin" / "python"
    site_packages = [p for p in venv_dir.glob("lib/python*/site-packages")]
    assert len(site_packages) == 1, site_packages
    _write_min_angelus(site_packages[0], with_migrations=with_migrations)
    return py, site_packages[0]


@pytest.fixture(scope="module")
def venv_no_migs(tmp_path_factory) -> tuple[Path, Path, Path]:
    venv_dir = tmp_path_factory.mktemp("venv-nomig") / "venv"
    py, sp = _make_installed_venv(venv_dir, with_migrations=False)
    return py, sp, venv_dir


@pytest.fixture(scope="module")
def venv_with_migs(tmp_path_factory) -> tuple[Path, Path, Path]:
    venv_dir = tmp_path_factory.mktemp("venv-mig") / "venv"
    py, sp = _make_installed_venv(venv_dir, with_migrations=True)
    return py, sp, venv_dir


@pytest.fixture(scope="module")
def shadow_tree(tmp_path_factory) -> Path:
    """A dev-tree stand-in carrying angelus + migrations, reachable only by
    putting it on PYTHONPATH. Importing it satisfies the migrations check, so a
    probe that lets it shadow the install false-passes."""
    root = tmp_path_factory.mktemp("shadow")
    _write_min_angelus(root, with_migrations=True)
    return root


def _cfg(deploy, monkeypatch, tmp_path, *, repo: Path, pip_python: Path,
         pip_prefix: Path | None):
    """A Config wired only at the touchpoints verify_installed_migrations reads
    (repo, pip_python, pip_prefix); everything else takes its default."""
    monkeypatch.setenv("ANGELUS_DEPLOY_REPO", str(repo))
    monkeypatch.setenv("ANGELUS_DEPLOY_LODGING", str(tmp_path / "lodging"))
    monkeypatch.setenv("ANGELUS_DEPLOY_PIP_PYTHON", str(pip_python))
    if pip_prefix is None:
        monkeypatch.delenv("ANGELUS_DEPLOY_PIP_PREFIX", raising=False)
    else:
        monkeypatch.setenv("ANGELUS_DEPLOY_PIP_PREFIX", str(pip_prefix))
    return deploy.build_config()


# ===========================================================================
# Codex fell-r1: no-prefix mode must not false-pass by importing the dev tree.
# ===========================================================================


@pytest.mark.slow
def test_no_prefix_inherited_pythonpath_does_not_false_pass(
    deploy, venv_no_migs, shadow_tree, tmp_path, monkeypatch
):
    """No-prefix mode, an installed package that LACKS migrations, and an inherited
    PYTHONPATH pointing at a tree that HAS them. The guard must FAIL: the probe
    must resolve the installed (migrations-less) package, not the shadow tree.

    The shadow tree is deliberately NOT under cfg.repo, so the snippet's
    dev-tree assertion cannot catch this case -- the PYTHONPATH sanitization is
    the sole defense. Mutation: drop the `env.pop('PYTHONPATH')` in the no-prefix
    branch and this goes RED (the probe imports the shadow tree, finds its
    migrations, and the guard false-passes)."""
    py, _sp, _venv = venv_no_migs
    repo = tmp_path / "unrelated-repo"  # not an ancestor of shadow_tree or venv
    repo.mkdir()
    monkeypatch.setenv("PYTHONPATH", str(shadow_tree))
    cfg = _cfg(deploy, monkeypatch, tmp_path, repo=repo, pip_python=py,
               pip_prefix=None)

    with pytest.raises(deploy.DeployError) as exc:
        deploy.verify_installed_migrations(cfg)
    assert "migrations" in str(exc.value)


@pytest.mark.slow
def test_no_prefix_correct_install_passes_despite_stray_pythonpath(
    deploy, venv_with_migs, shadow_tree, tmp_path, monkeypatch
):
    """No-prefix mode, a correctly-installed package (migrations present), and a
    stray PYTHONPATH still set. The guard must PASS -- the sanitization must not
    break the legitimate case."""
    py, _sp, _venv = venv_with_migs
    repo = tmp_path / "unrelated-repo"
    repo.mkdir()
    monkeypatch.setenv("PYTHONPATH", str(shadow_tree))
    cfg = _cfg(deploy, monkeypatch, tmp_path, repo=repo, pip_python=py,
               pip_prefix=None)

    # Must not raise.
    deploy.verify_installed_migrations(cfg)


@pytest.mark.slow
def test_dev_tree_resolution_assertion_fires(
    deploy, venv_with_migs, tmp_path, monkeypatch
):
    """The snippet's self-validation: when the imported angelus resolves UNDER
    cfg.repo, the guard fails closed (exit 8) even though migrations are present
    -- it positively proves it imported the installed copy, not the dev tree.

    Here cfg.repo is set to the venv root, so the installed package legitimately
    resolves under it. Mutation: remove the `is_relative_to(repo)` block from
    _INSTALLED_MIGRATIONS_SNIPPET and this goes RED (migrations are present, so
    the probe exits 0 and the guard passes)."""
    py, _sp, venv_dir = venv_with_migs
    cfg = _cfg(deploy, monkeypatch, tmp_path, repo=venv_dir, pip_python=py,
               pip_prefix=None)

    with pytest.raises(deploy.DeployError) as exc:
        deploy.verify_installed_migrations(cfg)
    assert "DEV TREE" in str(exc.value)
