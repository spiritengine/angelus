"""Fault-injection seam (B28).

A scoped, first-class way to force ONE channel's dispatch to fail on demand,
so the real detection/fixer/escalation machinery (per-channel health
escalation, B13 failover, B14 escalation ladder, B15 dead-letter) can be
exercised WITHOUT touching real channel config. An armed fault makes
``PipeDrain._send_channel`` raise a transport-shaped error instead of calling
the live sender; that error walks the rest of the pipeline exactly as a real
transport failure would, so an injected fault is indistinguishable downstream
from the genuine article -- which is the whole point.

The registry is in-memory ONLY. It is never persisted to sqlite or disk, so it
is cleared on process restart BY CONSTRUCTION -- there is no code path that
writes it anywhere durable. This mirrors the Clock seam (``angelus/clock.py``):
the daemon constructs one registry and threads it into every ``PipeDrain``, so
a live control op arms a fault across all pipes at once; tests construct their
own registry (or set the env flag and let the drain build one).

Two arming paths feed the same registry:
  - ``ANGELUS_FAULT_INJECT`` (comma-separated channel names) read at
    construction -- the no-daemon path B27 scenario fixtures and unit tests use;
  - the ``fault_inject`` control op on the running daemon -- live on-demand
    toggling, surfaced in ``angelus health`` so an armed fault is never silently
    forgotten.
"""

from __future__ import annotations

import os
from collections.abc import Iterable

# Env var holding the comma-separated channel names to arm at construction.
# Empty/unset means no faults. Whitespace around each name is trimmed and empty
# entries are ignored, so "email, , push," arms exactly {email, push}.
FAULT_INJECT_ENV = "ANGELUS_FAULT_INJECT"


class FaultRegistry:
    """In-memory set of channel names whose dispatch is currently forced to
    fail. Keyed by channel name (the same string as ``Channel.name``)."""

    def __init__(self, armed: Iterable[str] | None = None) -> None:
        self._armed: set[str] = set(armed or ())

    @classmethod
    def from_env(cls, environ: dict[str, str] | None = None) -> "FaultRegistry":
        """Build a registry from ``ANGELUS_FAULT_INJECT``.

        Read at call time (not import time) so a test can set the env var
        before constructing the drain and have the fault armed. ``environ``
        defaults to ``os.environ``; the parameter exists only to make the
        parsing unit-testable without mutating the process environment.
        """
        raw = (environ if environ is not None else os.environ).get(
            FAULT_INJECT_ENV, ""
        )
        names = (part.strip() for part in raw.split(","))
        return cls(name for name in names if name)

    def arm(self, channel_name: str) -> None:
        """Force ``channel_name``'s dispatch to fail until cleared."""
        self._armed.add(channel_name)

    def clear(self, channel_name: str) -> None:
        """Clear the fault on ``channel_name``. Idempotent -- clearing a
        channel that was never armed is a no-op, not an error."""
        self._armed.discard(channel_name)

    def clear_all(self) -> None:
        """Clear every armed fault."""
        self._armed.clear()

    def is_armed(self, channel_name: str) -> bool:
        return channel_name in self._armed

    def armed(self) -> list[str]:
        """The armed channel names, sorted -- a stable order for health output
        and test assertions."""
        return sorted(self._armed)

    def raise_if_armed(self, channel_name: str) -> None:
        """Raise a transport-shaped ``RuntimeError`` if ``channel_name`` is
        armed, otherwise return. The message is distinguishable (carries
        ``fault-injected``) so logs make clear a failure was injected, while
        the exception TYPE is the same ``RuntimeError`` a real sender raises --
        so the per-channel failure handling cannot tell the two apart."""
        if channel_name in self._armed:
            raise RuntimeError(
                f"fault-injected: {channel_name} forced failure"
            )
