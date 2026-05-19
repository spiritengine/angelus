"""Lodging YAML loader.

Slice 5a exposes per-file parsers and a standalone cross-reference validator
so the hot-reload watcher can re-load a single file and validate it against
the rest of the live state without re-walking every directory.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

SUPPORTED_DIGEST_INPUTS = (
    "findings_since_last_drain",
    "open_incidents",
    "suppressed_findings",
    "recent_closures",
)

DISABLED_SUFFIX = ".disabled"


@dataclass(frozen=True)
class ScheduledSource:
    name: str
    source_ref: str
    cadence: str
    command: str
    timeout_seconds: float = 30.0


@dataclass(frozen=True)
class Triager:
    name: str
    source_ref: str
    handler_path: Path
    timeout_seconds: float = 60.0


@dataclass(frozen=True)
class Pipe:
    name: str
    cadence: str
    render_kind: str
    template: str | None
    channels: list[str]
    render: dict[str, Any] = field(default_factory=dict)
    rate_limit: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class Channel:
    name: str
    kind: str
    command: str
    to: str | None = None


@dataclass(frozen=True)
class Dependency:
    name: str
    check: str
    timeout_seconds: float = 30.0


@dataclass(frozen=True)
class Lodging:
    sources: dict[str, ScheduledSource]
    triagers: dict[str, Triager]
    pipes: dict[str, Pipe]
    channels: dict[str, Channel]
    dependencies: dict[str, Dependency]


def load_lodging(root: Path) -> Lodging:
    lodging = Lodging(
        sources=_load_sources(root),
        triagers=_load_triagers(root),
        pipes=_load_pipes(root),
        channels=_load_channels(root),
        dependencies=_load_dependencies(root),
    )
    errors = validate_cross_refs(lodging)
    if errors:
        raise ValueError("; ".join(errors))
    return lodging


def validate_cross_refs(lodging: Lodging) -> list[str]:
    """Return a list of cross-reference errors. Empty list means consistent."""
    errors: list[str] = []
    for triager in lodging.triagers.values():
        if triager.source_ref not in lodging.sources:
            errors.append(
                f"triager {triager.name} references missing source {triager.source_ref}"
            )
    for pipe in lodging.pipes.values():
        for channel in pipe.channels:
            if channel not in lodging.channels:
                errors.append(
                    f"pipe {pipe.name} references missing channel {channel}"
                )
        overflow = pipe.rate_limit.get("overflow")
        if overflow is not None and overflow not in lodging.pipes:
            errors.append(
                f"pipe {pipe.name} rate_limit.overflow references unknown pipe "
                f"{overflow!r}"
            )
    return errors


def parse_source(path: Path) -> ScheduledSource:
    data = _read_yaml(path)
    name = path.stem
    check = _required_dict(data, "check", path)
    if check.get("kind") != "shell":
        raise ValueError(f"{path}: only check.kind=shell is supported")
    return ScheduledSource(
        name=name,
        source_ref=f"scheduled/{name}",
        cadence=_required_str(data, "cadence", path),
        command=_required_str(check, "command", path),
        timeout_seconds=_optional_timeout(check, path, 30.0),
    )


def parse_triager(root: Path, path: Path) -> Triager:
    data = _read_yaml(path)
    name = path.stem
    inputs = _required_dict(data, "inputs", path)
    handler = _required_dict(data, "handler", path)
    if handler.get("kind") != "python":
        raise ValueError(f"{path}: only handler.kind=python is supported")
    handler_path = root / _required_str(handler, "path", path)
    if not handler_path.exists():
        raise ValueError(f"{path}: handler path does not exist: {handler_path}")
    return Triager(
        name=name,
        source_ref=_required_str(inputs, "source", path),
        handler_path=handler_path,
        timeout_seconds=_optional_timeout(handler, path, 60.0),
    )


def parse_pipe(path: Path) -> Pipe:
    data = _read_yaml(path)
    name = path.stem
    render = _required_dict(data, "render", path)
    if render.get("kind") == "dumb-alert":
        render_kind = "dumb-alert"
        template: str | None = _required_str(render, "template", path)
    elif isinstance(render.get("preamble"), list) and isinstance(render.get("body"), dict):
        for block in render["preamble"]:
            if not isinstance(block, dict):
                raise ValueError(f"{path}: expected preamble blocks to be mappings")
            if "source" in block:
                raise ValueError(f"{path}: preamble blocks do not accept source")
        _validate_digest_body(render["body"], path)
        render_kind = "digest"
        template = None
    else:
        raise ValueError(f"{path}: unsupported render shape")
    return Pipe(
        name=name,
        cadence=_required_str(data, "cadence", path),
        render_kind=render_kind,
        template=template,
        channels=list(data.get("channels") or []),
        render=render,
        rate_limit=dict(data.get("rate_limit") or {}),
    )


def parse_channel(path: Path) -> Channel:
    data = _read_yaml(path)
    name = path.stem
    kind = data.get("kind")
    if kind not in {"push", "email"}:
        raise ValueError(f"{path}: unsupported channel kind={kind!r}")
    if kind == "email":
        to = _required_str(data, "to", path)
    else:
        to = None
    return Channel(
        name=name,
        kind=str(kind),
        command=_required_str(data, "command", path),
        to=to,
    )


def parse_dependency(path: Path) -> Dependency:
    """Parse a dependencies/<name>.yaml lodging file.

    One check mechanism by design: `check` is a single shell command,
    exit 0 = healthy, non-zero = unhealthy. The spec mentions a
    "tripwire URL"; the deliberate simplification is that a tripwire-URL
    dependency is just a check command that curls the URL (e.g.
    `curl -fsS https://hc-ping.com/...`). No polymorphic probe-type
    framework -- the single check command, run via the same
    subprocess+kill-on-timeout pattern the sources already use, covers
    URL pings and local CLIs alike.
    """
    data = _read_yaml(path)
    name = _required_str(data, "name", path)
    if name != path.stem:
        raise ValueError(
            f"{path}: name {name!r} must match filename stem {path.stem!r}"
        )
    return Dependency(
        name=name,
        check=_required_str(data, "check", path),
        timeout_seconds=_optional_timeout(data, path, 30.0),
    )


def _enabled_yaml_files(directory: Path) -> list[Path]:
    """Return *.yaml files under directory, skipping any that have a sibling
    .disabled twin (or are themselves named *.yaml.disabled)."""
    if not directory.exists():
        return []
    files = sorted(directory.glob("*.yaml"))
    return [
        path
        for path in files
        if not (path.parent / (path.name + DISABLED_SUFFIX)).exists()
    ]


def _load_sources(root: Path) -> dict[str, ScheduledSource]:
    loaded: dict[str, ScheduledSource] = {}
    for path in _enabled_yaml_files(root / "sources" / "scheduled"):
        source = parse_source(path)
        loaded[source.source_ref] = source
    return loaded


def _load_triagers(root: Path) -> dict[str, Triager]:
    loaded: dict[str, Triager] = {}
    for path in _enabled_yaml_files(root / "triagers"):
        triager = parse_triager(root, path)
        loaded[triager.name] = triager
    return loaded


def _load_pipes(root: Path) -> dict[str, Pipe]:
    loaded: dict[str, Pipe] = {}
    for path in _enabled_yaml_files(root / "pipes"):
        pipe = parse_pipe(path)
        loaded[pipe.name] = pipe
    return loaded


def _load_channels(root: Path) -> dict[str, Channel]:
    loaded: dict[str, Channel] = {}
    for path in _enabled_yaml_files(root / "channels"):
        channel = parse_channel(path)
        loaded[channel.name] = channel
    return loaded


def _load_dependencies(root: Path) -> dict[str, Dependency]:
    loaded: dict[str, Dependency] = {}
    for path in _enabled_yaml_files(root / "dependencies"):
        dependency = parse_dependency(path)
        loaded[dependency.name] = dependency
    return loaded


def _read_yaml(path: Path) -> dict[str, Any]:
    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError(f"{path}: expected a YAML mapping")
    return data


def _required_dict(data: dict[str, Any], key: str, path: Path) -> dict[str, Any]:
    value = data.get(key)
    if not isinstance(value, dict):
        raise ValueError(f"{path}: expected {key} mapping")
    return value


def _required_str(data: dict[str, Any], key: str, path: Path) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}: expected non-empty string {key}")
    return value


def _validate_digest_body(body: dict[str, Any], path: Path) -> None:
    kind = body.get("kind")
    if kind != "llm":
        raise ValueError(
            f"{path}: body.kind must be 'llm' (got {kind!r})"
        )
    inputs = body.get("inputs")
    if not isinstance(inputs, list) or not inputs:
        raise ValueError(
            f"{path}: body.inputs must be a non-empty list of input names"
        )
    seen: set[str] = set()
    for name in inputs:
        if not isinstance(name, str):
            raise ValueError(
                f"{path}: body.inputs entries must be strings (got {name!r})"
            )
        if name not in SUPPORTED_DIGEST_INPUTS:
            raise ValueError(
                f"{path}: body.inputs has unknown name {name!r}; "
                f"supported: {', '.join(SUPPORTED_DIGEST_INPUTS)}"
            )
        if name in seen:
            raise ValueError(f"{path}: body.inputs has duplicate name {name!r}")
        seen.add(name)


def _optional_timeout(data: dict[str, Any], path: Path, default: float) -> float:
    value = data.get("timeout_seconds", data.get("timeout", default))
    if not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{path}: expected positive numeric timeout")
    return float(value)
