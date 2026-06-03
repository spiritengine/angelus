"""Lodging YAML loader.

Slice 5a exposes per-file parsers and a standalone cross-reference validator
so the hot-reload watcher can re-load a single file and validate it against
the rest of the live state without re-walking every directory.
"""

from __future__ import annotations

import dataclasses
import logging
import os
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import yaml

LOGGER = logging.getLogger(__name__)

# Marker a channel config field uses to defer a value to an environment
# variable, resolved by the channel wrapper at send time (see
# channels/email.py `_resolve_to`). B18 reuses it to derive a channel's
# required config without hardcoding any channel kind or env name.
ENV_REF_PREFIX = "$env:"

SUPPORTED_DIGEST_INPUTS = (
    "findings_since_last_drain",
    "open_incidents",
    "suppressed_findings",
    "recent_closures",
    "fixer_actions",
)

DISABLED_SUFFIX = ".disabled"


@dataclass(frozen=True)
class ScheduledSource:
    name: str
    source_ref: str
    cadence: str
    command: str
    timeout_seconds: float = 30.0
    depends_on: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class Triager:
    name: str
    source_ref: str
    handler_path: Path
    timeout_seconds: float = 60.0
    metadata: dict[str, Any] = field(default_factory=dict)


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
    # Parse and cross-ref failures here raise ValueError, which crashes
    # daemon startup. The runtime reload path (LodgingReloader) instead
    # emits an internal/lodging finding to the `now` pipe and keeps the
    # prior state. The asymmetry is deliberate: at startup the catalog
    # and now-pipe haven't been constructed yet, so a finding has no
    # surface to land on -- raising is the only available signal.
    sources = _load_sources(root)
    triagers = _load_triagers(root)
    # Late import to avoid a config<->dynamic import cycle (dynamic.py
    # imports several helpers from this module).
    from angelus.lodging.dynamic import (
        expand as _expand_watches,
        load_entities,
        load_watches,
    )

    entities = load_entities(root)
    watches = load_watches(root)
    synth_sources, synth_triagers = _expand_watches(entities, watches)
    # Visibility for the missing-entities-dir failure mode: a deploy
    # without entities/ or watch/ produces zero synthesized sources and
    # the daemon goes dark on entity monitoring without otherwise
    # complaining. Logging the count makes the regression visible in
    # journal at every startup. See sonnet fell-r1 #3.
    LOGGER.info(
        "lodging entities=%d watches=%d -> synthesized sources=%d",
        len(entities),
        len(watches),
        len(synth_sources),
    )
    for ref, source in synth_sources.items():
        if ref in sources:
            raise ValueError(
                f"synthesized source {ref!r} collides with a hand-written "
                f"sources/scheduled/ file; rename one"
            )
        sources[ref] = source
    for name, triager in synth_triagers.items():
        if name in triagers:
            raise ValueError(
                f"synthesized triager {name!r} collides with a hand-written "
                f"triagers/ file; rename one"
            )
        triagers[name] = triager
    lodging = Lodging(
        sources=sources,
        triagers=triagers,
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
    for source in lodging.sources.values():
        for dependency_name in source.depends_on:
            if dependency_name not in lodging.dependencies:
                errors.append(
                    "source "
                    f"{source.source_ref} references missing dependency "
                    f"{dependency_name}"
                )
    for triager in lodging.triagers.values():
        if triager.source_ref not in lodging.sources:
            errors.append(
                f"triager {triager.name} references missing source {triager.source_ref}"
            )
        # Validate pipe refs in triager.metadata. Synthesized triagers
        # (entity+watch fan-out) carry target_pipe and clearance_pipe in
        # metadata; the handler emits findings routed to those names.
        # Without this check, a typo in watch/<x>.yaml (`target_pipe: nwo`)
        # silently drops every finding from that watch because
        # Catalog.write_finding skips unknown pipes. Same protection
        # extends to hand-written triagers that opt into the same shape.
        for key in ("target_pipe", "clearance_pipe"):
            ref = triager.metadata.get(key)
            if ref is None:
                continue
            if not isinstance(ref, str):
                errors.append(
                    f"triager {triager.name} metadata.{key} must be a string "
                    f"(got {type(ref).__name__})"
                )
                continue
            if ref not in lodging.pipes:
                errors.append(
                    f"triager {triager.name} metadata.{key} references "
                    f"missing pipe {ref!r}"
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


def channel_env_requirements(channel: Channel) -> list[str]:
    """Env var names a channel's config defers to via the `$env:NAME` marker.

    Domain-agnostic by construction: every string-valued config field is
    scanned for the marker the channel wrappers resolve at send time, so
    nothing is hardcoded. email's `to: $env:ANGELUS_EMAIL_TO` yields
    ANGELUS_EMAIL_TO; a future channel that adds a `$env:` field (or kind) is
    covered without touching this code. Order-preserving and de-duplicated.
    """
    names: list[str] = []
    for field_ in dataclasses.fields(channel):
        value = getattr(channel, field_.name)
        if isinstance(value, str) and value.startswith(ENV_REF_PREFIX):
            name = value[len(ENV_REF_PREFIX) :]
            if name and name not in names:
                names.append(name)
    return names


def missing_channel_config(
    lodging: Lodging, env: Mapping[str, str] | None = None
) -> dict[str, list[str]]:
    """Map each pipe-referenced channel to the env vars it requires but that
    are unset/empty in `env` (default: the process environment). An empty
    return means every referenced channel has its required config (B18).

    Only channels a pipe actually routes to are checked: an unreferenced
    channel file cannot dispatch, so its missing config is not a startup
    failure. A pipe referencing a channel that does not exist is left to
    `validate_cross_refs`, which already reports it.
    """
    environ: Mapping[str, str] = os.environ if env is None else env
    referenced: set[str] = set()
    for pipe in lodging.pipes.values():
        referenced.update(pipe.channels)
    missing: dict[str, list[str]] = {}
    for name in referenced:
        channel = lodging.channels.get(name)
        if channel is None:
            continue
        absent = [
            var for var in channel_env_requirements(channel) if not environ.get(var)
        ]
        if absent:
            missing[name] = absent
    return missing


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
        depends_on=_optional_str_list(data, "depends_on", path),
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
    metadata_raw = data.get("metadata")
    if metadata_raw is None:
        metadata: dict[str, Any] = {}
    elif isinstance(metadata_raw, dict):
        metadata = dict(metadata_raw)
    else:
        raise ValueError(f"{path}: metadata must be a mapping")
    return Triager(
        name=name,
        source_ref=_required_str(inputs, "source", path),
        handler_path=handler_path,
        timeout_seconds=_optional_timeout(handler, path, 60.0),
        metadata=metadata,
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
    channel = Channel(
        name=name,
        kind=str(kind),
        command=_required_str(data, "command", path),
        to=to,
    )
    _assert_well_formed_env_markers(channel, path)
    return channel


def _assert_well_formed_env_markers(channel: Channel, path: Path) -> None:
    """Reject a `$env:` marker with no variable name (e.g. `to: $env:`).

    A nameless marker is a config typo, not a runtime-absent value: it cannot
    name a var to check, so missing_channel_config would skip it and pass
    startup green while the channel wrapper raises at send time -- the same
    validate/send divergence B18 exists to close. Caught here at load, it joins
    every other structural channel-config error (a ValueError that fails the
    load) instead of becoming a silent-healthy daemon with a dead transport.
    """
    for field_ in dataclasses.fields(channel):
        value = getattr(channel, field_.name)
        if (
            isinstance(value, str)
            and value.startswith(ENV_REF_PREFIX)
            and not value[len(ENV_REF_PREFIX) :]
        ):
            raise ValueError(
                f"{path}: malformed env reference {value!r} in {field_.name} -- "
                f"missing variable name after {ENV_REF_PREFIX!r}"
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


def _optional_str_list(data: dict[str, Any], key: str, path: Path) -> list[str]:
    value = data.get(key)
    if value is None:
        return []
    if not isinstance(value, list):
        raise ValueError(f"{path}: expected {key} to be a list of strings")
    seen: set[str] = set()
    parsed: list[str] = []
    for entry in value:
        if not isinstance(entry, str) or not entry:
            raise ValueError(f"{path}: expected {key} entries to be non-empty strings")
        if entry in seen:
            raise ValueError(f"{path}: duplicate {key} entry {entry!r}")
        seen.add(entry)
        parsed.append(entry)
    return parsed


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
