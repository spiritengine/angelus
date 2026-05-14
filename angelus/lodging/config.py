"""One-shot lodging loader for slice 1."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml


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
    template: str
    channels: list[str]


@dataclass(frozen=True)
class Channel:
    name: str
    kind: str
    command: str


@dataclass(frozen=True)
class Lodging:
    sources: dict[str, ScheduledSource]
    triagers: dict[str, Triager]
    pipes: dict[str, Pipe]
    channels: dict[str, Channel]


def load_lodging(root: Path) -> Lodging:
    sources = _load_sources(root)
    triagers = _load_triagers(root)
    pipes = _load_pipes(root)
    channels = _load_channels(root)

    for triager in triagers.values():
        if triager.source_ref not in sources:
            raise ValueError(
                f"triager {triager.name} references missing source {triager.source_ref}"
            )
    for pipe in pipes.values():
        for channel in pipe.channels:
            if channel not in channels:
                raise ValueError(f"pipe {pipe.name} references missing channel {channel}")

    return Lodging(sources=sources, triagers=triagers, pipes=pipes, channels=channels)


def _load_sources(root: Path) -> dict[str, ScheduledSource]:
    loaded: dict[str, ScheduledSource] = {}
    for path in sorted((root / "sources" / "scheduled").glob("*.yaml")):
        data = _read_yaml(path)
        name = path.stem
        source_ref = f"scheduled/{name}"
        check = _required_dict(data, "check", path)
        if check.get("kind") != "shell":
            raise ValueError(f"{path}: only check.kind=shell is supported")
        loaded[source_ref] = ScheduledSource(
            name=name,
            source_ref=source_ref,
            cadence=_required_str(data, "cadence", path),
            command=_required_str(check, "command", path),
            timeout_seconds=_optional_timeout(check, path, 30.0),
        )
    return loaded


def _load_triagers(root: Path) -> dict[str, Triager]:
    loaded: dict[str, Triager] = {}
    for path in sorted((root / "triagers").glob("*.yaml")):
        data = _read_yaml(path)
        name = path.stem
        inputs = _required_dict(data, "inputs", path)
        handler = _required_dict(data, "handler", path)
        if handler.get("kind") != "python":
            raise ValueError(f"{path}: only handler.kind=python is supported")
        handler_path = root / _required_str(handler, "path", path)
        if not handler_path.exists():
            raise ValueError(f"{path}: handler path does not exist: {handler_path}")
        loaded[name] = Triager(
            name=name,
            source_ref=_required_str(inputs, "source", path),
            handler_path=handler_path,
            timeout_seconds=_optional_timeout(handler, path, 60.0),
        )
    return loaded


def _load_pipes(root: Path) -> dict[str, Pipe]:
    loaded: dict[str, Pipe] = {}
    for path in sorted((root / "pipes").glob("*.yaml")):
        data = _read_yaml(path)
        name = path.stem
        render = _required_dict(data, "render", path)
        if render.get("kind") != "dumb-alert":
            raise ValueError(f"{path}: only render.kind=dumb-alert is supported")
        loaded[name] = Pipe(
            name=name,
            cadence=_required_str(data, "cadence", path),
            render_kind="dumb-alert",
            template=_required_str(render, "template", path),
            channels=list(data.get("channels") or []),
        )
    return loaded


def _load_channels(root: Path) -> dict[str, Channel]:
    loaded: dict[str, Channel] = {}
    for path in sorted((root / "channels").glob("*.yaml")):
        data = _read_yaml(path)
        name = path.stem
        if data.get("kind") != "push":
            raise ValueError(f"{path}: only channel kind=push is supported")
        loaded[name] = Channel(
            name=name,
            kind="push",
            command=str(data.get("command") or "notify-pat"),
        )
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


def _optional_timeout(data: dict[str, Any], path: Path, default: float) -> float:
    value = data.get("timeout_seconds", data.get("timeout", default))
    if not isinstance(value, int | float) or value <= 0:
        raise ValueError(f"{path}: expected positive numeric timeout")
    return float(value)
