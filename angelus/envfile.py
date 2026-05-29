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

import os
from pathlib import Path
from typing import MutableMapping

DEFAULT_ENV_FILENAME = "angelus.env"


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
