"""Load non-secret runtime config from ``state/angelus.env`` (B16).

A single source of truth for non-secret config so the daemon and the belfry
can't drift apart. The 2026-05-29 incident was exactly that drift: the daemon
was relaunched outside systemd, lost ``ANGELUS_EMAIL_TO``, and silently stopped
delivering. With this file the same config is loaded no matter how the process
is started -- via systemd ``EnvironmentFile=``, a cron that sources it, or a
bare hand-launch that calls :func:`load_env_file` in code.

The file holds non-secrets only (recipients, healthcheck URLs, thresholds).
The real secret -- the SMTP password -- stays out of it (that is B20).

Precedence: an explicitly-set environment variable always wins over the file.
We never overwrite a name already present in the environment. This matches
systemd's ``EnvironmentFile=`` (which is overridden by ``Environment=`` and by
anything inherited) so the code path and the systemd path agree.
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
from pathlib import Path
from typing import MutableMapping

LOGGER = logging.getLogger(__name__)

DEFAULT_ENV_FILENAME = "angelus.env"

# Prefix marking a config value as a 1Password secret reference rather than a
# literal. Resolved by resolve_op_refs via `op read`, using the read-only
# `angelus-daemon` service-account token the systemd unit injects.
OP_REF_PREFIX = "op://"

# Bounds the `op read` subprocess so a hung op binary can't stall daemon
# startup. A service-account read is a fast network call; this is generous.
_OP_READ_TIMEOUT_SEC = 15.0


def env_file_path(root: Path) -> Path:
    """Return the canonical env-file path for a given angelus root."""
    return root / "state" / DEFAULT_ENV_FILENAME


def parse_env_file(text: str) -> dict[str, str]:
    """Parse ``KEY=value`` lines into a dict.

    Accepts the intersection of what systemd ``EnvironmentFile=`` and a shell
    ``set -a; . file`` both understand: ``KEY=value`` per line, ``#`` comments,
    blank lines, an optional leading ``export``, and one layer of matching
    single or double quotes around the value. Lines without ``=`` are ignored.
    """
    result: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export ") or line.startswith("export\t"):
            line = line[len("export"):].lstrip()
        if "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        if not key:
            continue
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]
        result[key] = value
    return result


def load_env_file(
    root: Path,
    *,
    environ: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    """Apply ``state/angelus.env`` into the environment, non-override.

    Names already present in the environment are left untouched (explicit env
    wins over the file). A missing file is a no-op. Returns the names actually
    applied (those that were absent and are now set), for logging.
    """
    target = os.environ if environ is None else environ
    try:
        text = env_file_path(root).read_text(encoding="utf-8")
    except FileNotFoundError:
        return {}
    applied: dict[str, str] = {}
    for key, value in parse_env_file(text).items():
        if key in target:
            continue
        target[key] = value
        applied[key] = value
    return applied


def resolve_op_refs(
    environ: MutableMapping[str, str] | None = None,
) -> dict[str, str]:
    """Resolve any env value that is a 1Password ``op://`` reference in place.

    A daemon-only hardening seam: the belfry has its own stdlib env loader and
    never calls this, so the belt layer stays literal and free of any 1Password
    dependency. For every name whose value starts with ``op://``, resolve it
    through ``op read`` using the read-only ``angelus-daemon`` service-account
    token the systemd unit injects (``OP_SERVICE_ACCOUNT_TOKEN``) -- a
    non-interactive path, unlike a biometric-gated desktop session. A ref set by
    systemd ``EnvironmentFile=`` or by :func:`load_env_file` is REPLACED with the
    resolved secret (a ref is a pointer, not a value, so this is not the
    explicit-env-wins override that load_env_file deliberately avoids).

    Fail SAFE. If the service-account token is absent (e.g. a hand-launch that
    didn't source the token file) or a read fails, the name is UNSET and a
    warning logged -- a consumer then sees "not configured" (the digest dead-man
    goes inert) rather than a bogus ``op://`` value or a daemon that won't start.
    Returns the names actually resolved, for logging.
    """
    target = os.environ if environ is None else environ
    refs = [(k, v) for k, v in target.items() if v.startswith(OP_REF_PREFIX)]
    if not refs:
        return {}
    if not target.get("OP_SERVICE_ACCOUNT_TOKEN"):
        for key, _ref in refs:
            LOGGER.warning(
                "%s is a 1Password ref but OP_SERVICE_ACCOUNT_TOKEN is unset; "
                "leaving it unset",
                key,
            )
            target.pop(key, None)
        return {}
    resolved: dict[str, str] = {}
    for key, ref in refs:
        try:
            secret = _op_read(ref)
        except Exception as exc:  # noqa: BLE001 - any failure -> fail-safe unset
            LOGGER.warning(
                "could not resolve secret ref for %s (%s); leaving it unset: %s",
                key,
                ref,
                exc,
            )
            target.pop(key, None)
            continue
        target[key] = secret
        resolved[key] = ref
    return resolved


def _op_read(ref: str) -> str:
    """Resolve a single ``op://`` reference via the ``op`` CLI. Raises on any
    failure (missing binary, non-zero exit, empty value, timeout)."""
    op = shutil.which("op")
    if not op:
        raise RuntimeError("op CLI not on PATH")
    result = subprocess.run(
        [op, "read", ref],
        capture_output=True,
        text=True,
        timeout=_OP_READ_TIMEOUT_SEC,
    )
    if result.returncode != 0:
        raise RuntimeError(result.stderr.strip() or f"op exited {result.returncode}")
    value = result.stdout.strip()
    if not value:
        raise RuntimeError("op returned an empty value")
    return value
