"""Hot-reload of lodging YAML.

The watchdog Observer runs on its own thread; events are pushed onto a
thread-safe queue. An asyncio drain task pulls events, applies a per-file
debounce window, then re-parses the changed file, runs cross-reference
validation against the prospective lodging state, and either atomically
swaps the new dataclass into the daemon or emits an internal/lodging
finding describing why the file was rejected.

Tests bypass the observer thread by calling `process_pending_events()`
directly after dropping a file. The runtime path uses the observer.
"""

from __future__ import annotations

import asyncio
import logging
import queue
import time
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

from .config import (
    DISABLED_SUFFIX,
    Lodging,
    parse_channel,
    parse_dependency,
    parse_fixer,
    parse_pipe,
    parse_source,
    parse_triager,
    validate_cross_refs,
)

if TYPE_CHECKING:
    from angelus.daemon import AngelusDaemon

LOGGER = logging.getLogger(__name__)

WATCHED_DIRS = (
    "sources",
    "triagers",
    "pipes",
    "channels",
    "dependencies",
    "fixers",
)


@dataclass
class _Identified:
    """Result of mapping a filesystem path to a lodging entry."""

    kind: str  # "source" | "triager" | "pipe" | "channel" | "dependency" | "fixer"
    key: str  # canonical key in the matching Lodging dict
    yaml_path: Path  # path with .disabled stripped


def _identify(root: Path, path: Path) -> _Identified | None:
    """Map a watched path to (kind, key, yaml_path) or return None."""
    try:
        rel = path.relative_to(root)
    except ValueError:
        return None
    parts = rel.parts
    name = path.name
    if name.endswith(DISABLED_SUFFIX):
        name = name[: -len(DISABLED_SUFFIX)]
    if not name.endswith(".yaml"):
        return None
    stem = name[: -len(".yaml")]
    yaml_path = path.parent / name

    if len(parts) == 3 and parts[0] == "sources" and parts[1] == "scheduled":
        return _Identified("source", f"scheduled/{stem}", yaml_path)
    if len(parts) == 2 and parts[0] == "triagers":
        return _Identified("triager", stem, yaml_path)
    if len(parts) == 2 and parts[0] == "pipes":
        return _Identified("pipe", stem, yaml_path)
    if len(parts) == 2 and parts[0] == "channels":
        return _Identified("channel", stem, yaml_path)
    # dependencies/ is FLAT (dependencies/<name>.yaml, depth 2) like
    # channels/ -- NOT depth 3 like sources/scheduled/. The dependency
    # key is the filename stem (parse_dependency enforces name == stem).
    if len(parts) == 2 and parts[0] == "dependencies":
        return _Identified("dependency", stem, yaml_path)
    # fixers/ is FLAT for the bindings (fixers/<name>.yaml, depth 2). The
    # python handlers live one level down in fixers/handlers/<x>.py and are
    # never *.yaml, so the .yaml gate above already excludes them -- editing a
    # handler does not (and need not) trigger a reload, since each invocation
    # reads the handler file fresh as a subprocess.
    if len(parts) == 2 and parts[0] == "fixers":
        return _Identified("fixer", stem, yaml_path)
    return None


def _is_within(candidate: Path, base: Path) -> bool:
    try:
        candidate.relative_to(base)
        return True
    except ValueError:
        return False


def _parse(kind: str, root: Path, path: Path) -> Any:
    if kind == "source":
        return parse_source(path)
    if kind == "triager":
        return parse_triager(root, path)
    if kind == "pipe":
        return parse_pipe(path)
    if kind == "channel":
        return parse_channel(path)
    if kind == "dependency":
        return parse_dependency(path)
    if kind == "fixer":
        return parse_fixer(root, path)
    raise ValueError(f"unknown lodging kind {kind!r}")


def _swap(lodging: Lodging, kind: str, key: str, item: Any) -> Lodging:
    """Return a new Lodging with the given entry replaced."""
    if kind == "source":
        sources = dict(lodging.sources)
        sources[key] = item
        return replace(lodging, sources=sources)
    if kind == "triager":
        triagers = dict(lodging.triagers)
        triagers[key] = item
        return replace(lodging, triagers=triagers)
    if kind == "pipe":
        pipes = dict(lodging.pipes)
        pipes[key] = item
        return replace(lodging, pipes=pipes)
    if kind == "channel":
        channels = dict(lodging.channels)
        channels[key] = item
        return replace(lodging, channels=channels)
    if kind == "dependency":
        dependencies = dict(lodging.dependencies)
        dependencies[key] = item
        return replace(lodging, dependencies=dependencies)
    if kind == "fixer":
        fixers = dict(lodging.fixers)
        fixers[key] = item
        return replace(lodging, fixers=fixers)
    raise ValueError(f"unknown lodging kind {kind!r}")


def _without(lodging: Lodging, kind: str, key: str) -> Lodging:
    if kind == "source":
        sources = dict(lodging.sources)
        sources.pop(key, None)
        return replace(lodging, sources=sources)
    if kind == "triager":
        triagers = dict(lodging.triagers)
        triagers.pop(key, None)
        return replace(lodging, triagers=triagers)
    if kind == "pipe":
        pipes = dict(lodging.pipes)
        pipes.pop(key, None)
        return replace(lodging, pipes=pipes)
    if kind == "channel":
        channels = dict(lodging.channels)
        channels.pop(key, None)
        return replace(lodging, channels=channels)
    if kind == "dependency":
        dependencies = dict(lodging.dependencies)
        dependencies.pop(key, None)
        return replace(lodging, dependencies=dependencies)
    if kind == "fixer":
        fixers = dict(lodging.fixers)
        fixers.pop(key, None)
        return replace(lodging, fixers=fixers)
    raise ValueError(f"unknown lodging kind {kind!r}")


def _existing(lodging: Lodging, kind: str, key: str) -> Any:
    if kind == "source":
        return lodging.sources.get(key)
    if kind == "triager":
        return lodging.triagers.get(key)
    if kind == "pipe":
        return lodging.pipes.get(key)
    if kind == "channel":
        return lodging.channels.get(key)
    if kind == "dependency":
        return lodging.dependencies.get(key)
    if kind == "fixer":
        return lodging.fixers.get(key)
    raise ValueError(f"unknown lodging kind {kind!r}")


class _QueueingHandler(FileSystemEventHandler):
    """Pushes path strings into a thread-safe queue. No filtering here —
    the asyncio side decides what to do with each event."""

    def __init__(self, sink: queue.Queue[str]) -> None:
        self.sink = sink

    def on_any_event(self, event: FileSystemEvent) -> None:
        if event.is_directory:
            return
        self.sink.put(event.src_path)
        # On move events watchdog fires only one event with both src and dest;
        # enqueue the destination too so we don't miss the rename.
        dest = getattr(event, "dest_path", None)
        if dest:
            self.sink.put(dest)


class LodgingReloader:
    # Runtime lodging failures emit internal/lodging findings to the `now` pipe.
    # Startup failures crash the daemon — at startup the catalog and now-pipe
    # don't exist yet, so finding emission isn't possible. This asymmetry is
    # deliberate.
    def __init__(
        self,
        daemon: AngelusDaemon,
        root: Path,
        debounce_seconds: float = 1.0,
        poll_interval_seconds: float = 0.1,
    ) -> None:
        self.daemon = daemon
        self.root = root
        self.debounce_seconds = debounce_seconds
        self.poll_interval_seconds = poll_interval_seconds
        self.event_queue: queue.Queue[str] = queue.Queue()
        self._last_seen: dict[Path, float] = {}
        # Files that parsed but failed cross-ref, or that failed to parse.
        # Keyed by canonical yaml path; value is the rendered error message.
        self.rejected: dict[Path, str] = {}
        self.observer: Observer | None = None
        self._task: asyncio.Task[None] | None = None
        self._stopped = asyncio.Event()

    def start(self) -> None:
        if self.observer is not None:
            return
        handler = _QueueingHandler(self.event_queue)
        observer = Observer()
        # The observer schedules a dir once, here at startup; a dir created
        # afterwards is not watched until the next daemon restart (the 5a
        # scheduled-once limitation, unchanged for the other lodging dirs,
        # which exist in any real deployment). dependencies/ is the one dir
        # the spec expects to be absent initially, so rather than inherit
        # that limitation for it we ensure it exists before scheduling --
        # the dir is system-owned lodging, the same way sources/ etc. are,
        # and creating it empty is benign (_enabled_yaml_files yields []).
        # That makes the absent-at-startup and created-later cases both
        # behave: the observer watches a real (possibly empty) dir from
        # the start and sees the first dependencies/<name>.yaml dropped in.
        # fixers/ (B11) gets the same treatment for the same reason: a
        # deployment may not ship one yet, but dropping the first fixer file
        # should wire it without a restart.
        (self.root / "dependencies").mkdir(parents=True, exist_ok=True)
        (self.root / "fixers").mkdir(parents=True, exist_ok=True)
        for subdir in WATCHED_DIRS:
            target = self.root / subdir
            if target.exists():
                observer.schedule(handler, str(target), recursive=True)
        observer.start()
        self.observer = observer
        self._task = asyncio.create_task(self._poll_loop(), name="lodging-reload")

    async def stop(self) -> None:
        self._stopped.set()
        if self.observer is not None:
            self.observer.stop()
            self.observer.join(timeout=2)
            self.observer = None
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except (asyncio.CancelledError, Exception):
                pass
            self._task = None

    async def _poll_loop(self) -> None:
        try:
            while not self._stopped.is_set():
                await self.process_pending_events()
                try:
                    await asyncio.wait_for(
                        self._stopped.wait(),
                        timeout=self.poll_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    continue
        except asyncio.CancelledError:
            return

    async def process_pending_events(self, now: float | None = None) -> None:
        """Drain the queue, apply per-file debounce, and process ready files.

        Tests can call this directly after dropping a file (with a debounce
        of 0, every queued event becomes immediately ready)."""
        moment = time.monotonic() if now is None else now
        while True:
            try:
                raw = self.event_queue.get_nowait()
            except queue.Empty:
                break
            path = Path(raw)
            self._last_seen[path] = moment

        ready: list[Path] = []
        for path, last in list(self._last_seen.items()):
            if moment - last >= self.debounce_seconds:
                ready.append(path)
        for path in ready:
            self._last_seen.pop(path, None)
            await self._handle_path(path)

        if ready:
            await self._retry_rejected()

    async def _handle_path(self, path: Path) -> None:
        identified = _identify(self.root, path)
        if identified is None:
            return

        base = self.root / path.relative_to(self.root).parts[0]
        try:
            base_resolved = base.resolve(strict=False)
            resolved = path.resolve(strict=False)
        except OSError as exc:
            LOGGER.warning("lodging path resolve failed: %s (%s)", path, exc)
            return
        if not _is_within(resolved, base_resolved):
            LOGGER.warning(
                "rejecting lodging path outside base: %s -> %s",
                path,
                resolved,
            )
            return

        # Disabled-suffix file: treat as a removal of the corresponding entry.
        if path.name.endswith(DISABLED_SUFFIX):
            await self._apply_removal(identified, reason=f"{path.name} present")
            return

        # File deleted, or its sibling .disabled twin exists.
        disabled_twin = path.parent / (path.name + DISABLED_SUFFIX)
        if not path.exists() or disabled_twin.exists():
            await self._apply_removal(identified, reason="file removed")
            return

        try:
            item = _parse(identified.kind, self.root, identified.yaml_path)
        except Exception as exc:
            self._reject_load(identified.yaml_path, str(exc))
            return

        prospective = _swap(self.daemon.lodging, identified.kind, identified.key, item)
        errors = validate_cross_refs(prospective)
        if errors:
            self._reject_cross_ref(identified.yaml_path, "; ".join(errors))
            return

        existing = _existing(self.daemon.lodging, identified.kind, identified.key)
        if existing == item:
            # Content unchanged; nothing to swap. The file is valid, so this
            # is also a recovery edge for any prior load failure.
            self._clear_rejection(identified.yaml_path)
            return

        self._clear_rejection(identified.yaml_path)
        await self.daemon.apply_lodging(prospective)
        LOGGER.info(
            "lodging reload: %s %s -> applied",
            identified.kind,
            identified.key,
        )

    async def _apply_removal(self, identified: _Identified, reason: str) -> None:
        existing = _existing(self.daemon.lodging, identified.kind, identified.key)
        if existing is None:
            # The entry is already absent (a file that never loaded, e.g. one
            # that failed to parse). Removing it resolves any open load
            # failure, so clear.
            self._clear_rejection(identified.yaml_path)
            return
        prospective = _without(self.daemon.lodging, identified.kind, identified.key)
        errors = validate_cross_refs(prospective)
        if errors:
            self._reject_cross_ref(identified.yaml_path, "; ".join(errors))
            return
        self._clear_rejection(identified.yaml_path)
        await self.daemon.apply_lodging(prospective)
        LOGGER.info(
            "lodging reload: %s %s -> removed (%s)",
            identified.kind,
            identified.key,
            reason,
        )

    async def _retry_rejected(self) -> None:
        for path in list(self.rejected):
            await self._handle_path(path)

    def _clear_rejection(self, yaml_path: Path) -> None:
        """A lodging file resolved to a non-failing state -> drop its
        in-memory rejection and emit a clearance so the catalog closes any
        open internal/lodging incident for it (B30 recovery edge).

        Fired unconditionally on every success path, not only when the file
        was in self.rejected: self.rejected is in-process and empties on a
        daemon restart, but an incident opened before the restart is still
        open in the durable incidents table. Driving recovery off the
        catalog's gate (which reads that table) is what guarantees a file
        that failed before a restart and loads OK after it still clears --
        rather than going silent forever. When nothing is open the catalog's
        recovery gate drops the clearance to a no-op, so the unconditional
        call never floods."""
        rel = yaml_path.relative_to(self.root)
        was_rejected = self.rejected.pop(yaml_path, None) is not None
        self.daemon.catalog.write_internal_clearance(
            "internal/lodging",
            str(rel),
            f"{rel} loaded OK" if was_rejected else f"{rel} OK",
            set(self.daemon.lodging.pipes),
        )

    def _reject_load(self, yaml_path: Path, message: str) -> None:
        rel = yaml_path.relative_to(self.root)
        LOGGER.warning("lodging load_failed for %s: %s", rel, message)
        self.rejected[yaml_path] = message
        self.daemon.catalog.write_internal_finding(
            "internal/lodging",
            "load_failed",
            str(rel),
            _truncate(message),
            set(self.daemon.lodging.pipes),
        )

    def _reject_cross_ref(self, yaml_path: Path, message: str) -> None:
        rel = yaml_path.relative_to(self.root)
        LOGGER.warning("lodging cross_ref_broken for %s: %s", rel, message)
        self.rejected[yaml_path] = message
        self.daemon.catalog.write_internal_finding(
            "internal/lodging",
            "cross_ref_broken",
            str(rel),
            _truncate(message),
            set(self.daemon.lodging.pipes),
        )


_MAX_FINDING_BODY = 4000


def _truncate(text: str) -> str:
    if len(text) <= _MAX_FINDING_BODY:
        return text
    return text[:_MAX_FINDING_BODY] + "...[truncated]"


__all__ = ["LodgingReloader"]
