"""Canonical logging configuration (B21+B22).

One tail-able destination for the daemon's own logs: a rotating file at
``state/angelus.log``, written by the app itself via a
``RotatingFileHandler`` -- NOT by shell stdout redirection. That distinction
is the whole point. Before this, logs split by launch method: systemd
captured stdout to journald (and nothing landed in a file), while a manual
``angelus daemon > state/daemon.log`` wrote a file that journald never saw.
An operator tailing one destination was blind under the other launch path,
which is part of how the 2026-05-29 silent failure stayed invisible.

Routing the file through the app's own handler makes the destination
identical regardless of how the daemon was started. ``configure_logging``
is the single place this is wired; ``main()`` in :mod:`angelus.daemon` calls
it before constructing the daemon.

This is the logging framework's own timestamps (the ``Formatter``'s
``%(asctime)s``), distinct from the B24 domain clock -- log-line wall-time is
deliberately the real clock and is NOT routed through the injected ``Clock``.
"""

from __future__ import annotations

import logging
import logging.handlers
from pathlib import Path

LOG_FILENAME = "angelus.log"

# Rotation policy: 10 MiB per file, 5 rotations retained -> a ~60 MiB ceiling
# for the daemon's log footprint. The daemon emits thousands of INFO lines a
# day, so an unbounded file would grow without limit on a long-lived host;
# these bounds keep it tail-able and disk-safe without an external logrotate
# dependency. Documented in docs/logging.md.
MAX_BYTES = 10 * 1024 * 1024
BACKUP_COUNT = 5

LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"

# Marker attribute stamped on the handlers we install so configure_logging is
# idempotent: a second call (e.g. an in-process daemon restart inside a test)
# removes only our own handlers and re-adds them, rather than stacking
# duplicates or clobbering handlers a host/test harness installed.
_HANDLER_MARK = "_angelus_handler"


def log_path(root: Path) -> Path:
    """Resolve the canonical log file path under ``root``."""
    return root / "state" / LOG_FILENAME


def configure_logging(
    root: Path,
    *,
    level: int = logging.INFO,
    console: bool = True,
) -> Path:
    """Wire the root logger to write to ``state/angelus.log`` (rotating).

    Idempotent: calling it again swaps our previously-installed handlers for
    fresh ones rather than accumulating duplicates. Returns the resolved log
    path so callers/tests can read it back.

    ``console`` additionally mirrors records to ``stderr`` so an interactive
    ``angelus daemon`` is not silent and journald still gets a copy under
    systemd; the rotating file stays the canonical, identical destination
    either way.
    """
    path = log_path(root)
    path.parent.mkdir(parents=True, exist_ok=True)

    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Drop any handlers a prior configure_logging installed so repeated calls
    # don't stack. Handlers we did not install (a test harness's caplog, say)
    # are left untouched.
    for handler in list(root_logger.handlers):
        if getattr(handler, _HANDLER_MARK, False):
            root_logger.removeHandler(handler)
            handler.close()

    formatter = logging.Formatter(LOG_FORMAT)

    file_handler = logging.handlers.RotatingFileHandler(
        path,
        maxBytes=MAX_BYTES,
        backupCount=BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(formatter)
    setattr(file_handler, _HANDLER_MARK, True)
    root_logger.addHandler(file_handler)

    if console:
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        setattr(stream_handler, _HANDLER_MARK, True)
        root_logger.addHandler(stream_handler)

    return path
