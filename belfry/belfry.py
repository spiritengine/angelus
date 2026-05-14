#!/usr/bin/env python3
"""External reliability check for angelus.

Designed for raw cron: no angelus imports, no project dependencies.
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path


DEFAULT_WEDGE_THRESHOLD_SEC = 600


def main(argv: list[str] | None = None) -> int:
    argv = argv if argv is not None else sys.argv[1:]
    root = Path(argv[0] if argv else ".").resolve()
    state = root / "state"

    dead_reason = pid_failure(state / "angelus.pid")
    wedge_reason = None if dead_reason else wedge_failure(state / "angelus.sqlite3")

    if dead_reason or wedge_reason:
        reason = dead_reason or wedge_reason
        ok = ping_env("ANGELUS_BELFRY_DOWN_URL")
        ok = notify(reason) and ok
        print(f"angelus belfry: DOWN: {reason}", file=sys.stderr)
        return 1 if ok else 2

    ok = ping_env("ANGELUS_BELFRY_SUCCESS_URL")
    print("angelus belfry: ok")
    return 0 if ok else 2


def pid_failure(pid_file: Path) -> str | None:
    try:
        raw = pid_file.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return f"dead: missing PID file {pid_file}"
    try:
        pid = int(raw)
    except ValueError:
        return f"dead: invalid PID file contents {raw!r}"
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return f"dead: PID {pid} is not running"
    except PermissionError:
        # EPERM still implies the process exists even if cron cannot signal it.
        print(f"angelus belfry: cannot confirm PID {pid} via os.kill(0): permission denied", file=sys.stderr)
        return None
    return None


def wedge_failure(db_path: Path) -> str | None:
    threshold = wedge_threshold()
    try:
        fired_at = latest_fire(db_path)
    except sqlite3.Error as exc:
        return f"wedged: cannot read source_fires from {db_path}: {exc}"

    if fired_at is None:
        return "wedged: source_fires has no rows"
    age = datetime.now(UTC) - fired_at
    if age > threshold:
        return (
            f"wedged: last source fire at {fired_at.isoformat()} "
            f"({int(age.total_seconds())}s ago)"
        )
    return None


def wedge_threshold() -> timedelta:
    raw = os.environ.get("ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC")
    if raw is None:
        return timedelta(seconds=DEFAULT_WEDGE_THRESHOLD_SEC)
    try:
        seconds = int(raw)
    except ValueError:
        print(
            "angelus belfry: invalid ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC; using default",
            file=sys.stderr,
        )
        seconds = DEFAULT_WEDGE_THRESHOLD_SEC
    return timedelta(seconds=max(1, seconds))


def latest_fire(db_path: Path) -> datetime | None:
    quoted = urllib.parse.quote(str(db_path), safe="/:")
    uri = f"file:{quoted}?mode=ro"
    connection = sqlite3.connect(uri, uri=True)
    try:
        row = connection.execute("SELECT max(fired_at) FROM source_fires").fetchone()
    finally:
        connection.close()
    value = row[0] if row else None
    if value is None:
        return None
    return parse_utc(str(value))


def parse_utc(value: str) -> datetime:
    text = value.strip()
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def ping_env(name: str) -> bool:
    url = os.environ.get(name)
    if not url:
        print(f"angelus belfry: {name} is not set; skipping ping", file=sys.stderr)
        return False
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            status = getattr(response, "status", 200)
        print(f"angelus belfry: pinged {name} status={status}")
        return 200 <= int(status) < 300
    except Exception as exc:
        print(f"angelus belfry: failed to ping {name}: {exc}", file=sys.stderr)
        return False


def notify(reason: str) -> bool:
    message = f"angelus belfry alert: {reason}"
    try:
        result = subprocess.run(["notify-pat", message], check=False)
    except OSError as exc:
        print(f"angelus belfry: notify-pat failed to start: {exc}", file=sys.stderr)
        return False
    if result.returncode != 0:
        print(
            f"angelus belfry: notify-pat exited {result.returncode}",
            file=sys.stderr,
        )
        return False
    return True


if __name__ == "__main__":
    raise SystemExit(main())
