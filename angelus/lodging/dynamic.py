"""Entity + watch expansion.

`entities/<name>.yaml` declares things in the world that angelus knows about.
`watch/<name>.yaml` declares behavior (check + triager + cadence + severity)
to apply to entities matching a selector. At load time we expand each
(watch, matching-entity) pair into a synthesized ScheduledSource + Triager
that rides the existing pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from string import Formatter
from typing import Any

from angelus.lodging.config import (
    DISABLED_SUFFIX,
    ScheduledSource,
    Triager,
    _enabled_yaml_files,
    _optional_str_list,
    _optional_timeout,
    _read_yaml,
    _required_dict,
    _required_str,
)


@dataclass(frozen=True)
class Entity:
    name: str
    kind: str
    labels: tuple[str, ...]
    attrs: dict[str, Any]


@dataclass(frozen=True)
class Watch:
    name: str
    selector: dict[str, Any]
    check_command: str
    check_timeout: float
    handler_path: Path
    handler_timeout: float
    cadence: str
    severity: str
    target_pipe: str
    clearance_pipe: str
    extra_metadata: dict[str, Any] = field(default_factory=dict)


def parse_entity(path: Path) -> Entity:
    data = _read_yaml(path)
    name = path.stem
    kind = _required_str(data, "kind", path)
    labels = tuple(_optional_str_list(data, "labels", path))
    # `entity`, `kind`, and `name` are computed at substitution time from
    # the entity itself; a stray YAML key with one of these names would
    # silently overwrite the computed value (opus fell-r1 finding #5),
    # producing a check command targeting the wrong host.
    reserved = {"kind", "labels", "entity", "name"}
    conflicts = [k for k in data if k in reserved - {"kind", "labels"}]
    if conflicts:
        raise ValueError(
            f"{path}: keys {sorted(conflicts)} are reserved (computed from "
            f"entity filename / kind)"
        )
    attrs = {k: v for k, v in data.items() if k not in reserved}
    return Entity(name=name, kind=kind, labels=labels, attrs=attrs)


def parse_watch(root: Path, path: Path) -> Watch:
    data = _read_yaml(path)
    name = path.stem
    selector_raw = data.get("selector")
    if selector_raw is None:
        selector: dict[str, Any] = {}
    elif isinstance(selector_raw, dict):
        selector = dict(selector_raw)
    else:
        raise ValueError(f"{path}: selector must be a mapping")
    check = _required_dict(data, "check", path)
    if check.get("kind") != "shell":
        raise ValueError(f"{path}: only check.kind=shell is supported")
    handler = _required_dict(data, "handler", path)
    if handler.get("kind") != "python":
        raise ValueError(f"{path}: only handler.kind=python is supported")
    handler_path = root / _required_str(handler, "path", path)
    if not handler_path.exists():
        raise ValueError(f"{path}: handler path does not exist: {handler_path}")
    cadence = _required_str(data, "cadence", path)
    severity = _required_str(data, "severity", path)
    target_pipe = _required_str(data, "target_pipe", path)
    # clearance_pipe is optional but must be a non-empty string when
    # supplied. The earlier coerce-via-isinstance shape silently fell
    # back to target_pipe on any non-string value (sonnet fell-r3),
    # which is the same silent-misroute category opus fell-r2 #4
    # flagged on KEY typos -- closing the value-type leg of it here.
    clearance_pipe_raw = data.get("clearance_pipe")
    if clearance_pipe_raw is None:
        clearance_pipe = target_pipe
    elif isinstance(clearance_pipe_raw, str) and clearance_pipe_raw:
        clearance_pipe = clearance_pipe_raw
    else:
        raise ValueError(
            f"{path}: clearance_pipe must be a non-empty string or omitted"
        )
    extra_raw = data.get("metadata")
    if extra_raw is None:
        extra: dict[str, Any] = {}
    elif isinstance(extra_raw, dict):
        extra = dict(extra_raw)
    else:
        raise ValueError(f"{path}: metadata must be a mapping")
    return Watch(
        name=name,
        selector=selector,
        check_command=_required_str(check, "command", path),
        check_timeout=_optional_timeout(check, path, 30.0),
        handler_path=handler_path,
        handler_timeout=_optional_timeout(handler, path, 60.0),
        cadence=cadence,
        severity=severity,
        target_pipe=target_pipe,
        clearance_pipe=clearance_pipe,
        extra_metadata=extra,
    )


def load_entities(root: Path) -> dict[str, Entity]:
    loaded: dict[str, Entity] = {}
    for path in _enabled_yaml_files(root / "entities"):
        entity = parse_entity(path)
        if entity.name in loaded:
            raise ValueError(f"{path}: duplicate entity name {entity.name!r}")
        loaded[entity.name] = entity
    return loaded


def load_watches(root: Path) -> dict[str, Watch]:
    loaded: dict[str, Watch] = {}
    for path in _enabled_yaml_files(root / "watch"):
        watch = parse_watch(root, path)
        if watch.name in loaded:
            raise ValueError(f"{path}: duplicate watch name {watch.name!r}")
        loaded[watch.name] = watch
    return loaded


def matches(selector: dict[str, Any], entity: Entity) -> bool:
    """Return True if entity matches the selector. AND across keys.

    Supported keys:
      - kind: exact string match against entity.kind
      - name: exact string match against entity.name
      - labels: list of labels that must ALL be present
      - labels_any: list of labels where at least one must be present
      - labels_none: list of labels where none may be present
    """
    if "kind" in selector and entity.kind != selector["kind"]:
        return False
    if "name" in selector and entity.name != selector["name"]:
        return False
    label_set = set(entity.labels)
    required = selector.get("labels")
    if isinstance(required, list):
        if not set(required).issubset(label_set):
            return False
    any_of = selector.get("labels_any")
    if isinstance(any_of, list) and any_of:
        if not set(any_of).intersection(label_set):
            return False
    none_of = selector.get("labels_none")
    if isinstance(none_of, list):
        if set(none_of).intersection(label_set):
            return False
    return True


def _substitute(
    template: str, entity: Entity, metadata: dict[str, Any] | None = None
) -> str:
    """Render a check command template with entity fields and watch metadata.

    Available placeholders: `entity` (the entity name), every key in
    entity.attrs, `kind`, and every key in the watch's `metadata:` block
    (e.g. `{stale_days}`, so the check jq can derive a threshold from the
    same single source of truth the handler reads). Raises if a placeholder
    has no value.

    Precedence: entity fields win over watch metadata. We layer metadata
    FIRST, then the computed entity identity (`entity`/`kind`) and entity
    attrs on top, so a metadata key that happens to collide with an entity
    field (e.g. a `metadata: {url: ...}` against a `url:` entity attr) can
    never silently retarget the check command -- the entity value always
    takes effect. This mirrors expand()'s metadata layering, where the
    canonical routing keys likewise clobber extra_metadata, not the reverse.
    """
    context: dict[str, Any] = dict(metadata or {})
    context["entity"] = entity.name
    context["kind"] = entity.kind
    context.update(entity.attrs)
    fmt = Formatter()
    referenced: set[str] = set()
    for _, field_name, _, _ in fmt.parse(template):
        if field_name is None:
            continue
        # Strip any attribute/index access, keep root identifier.
        root = field_name.split(".")[0].split("[")[0]
        if root:
            referenced.add(root)
    missing = [name for name in referenced if name not in context]
    if missing:
        raise ValueError(
            f"entity {entity.name!r} missing placeholders for watch command: "
            f"{sorted(missing)}"
        )
    return template.format(**context)


def expand(
    entities: dict[str, Entity],
    watches: dict[str, Watch],
) -> tuple[dict[str, ScheduledSource], dict[str, Triager]]:
    """Return synthesized (sources, triagers) for each (watch, matching-entity).

    Source naming: `scheduled/<watch>__<entity>`. The double underscore
    keeps the watch and entity names round-trippable when reading source
    refs in logs or angelus health output, even when entity names contain
    dots or dashes (e.g. sub.domain.example.com).
    """
    sources: dict[str, ScheduledSource] = {}
    triagers: dict[str, Triager] = {}
    for watch in watches.values():
        for entity in entities.values():
            if not matches(watch.selector, entity):
                continue
            synth_name = f"{watch.name}__{entity.name}"
            source_ref = f"scheduled/{synth_name}"
            if source_ref in sources:
                raise ValueError(
                    f"watch {watch.name!r} would produce a duplicate source "
                    f"{source_ref!r}; check for collisions with hand-written "
                    f"sources/scheduled/ files"
                )
            # Pass the watch's `metadata:` block so a check command can
            # derive thresholds from the same source of truth the handler
            # reads (e.g. stale-pr's jq computes its staleness cutoff from
            # {stale_days} rather than a duplicated literal). Entity fields
            # still take precedence inside _substitute.
            command = _substitute(watch.check_command, entity, watch.extra_metadata)
            sources[source_ref] = ScheduledSource(
                name=synth_name,
                source_ref=source_ref,
                cadence=watch.cadence,
                command=command,
                timeout_seconds=watch.check_timeout,
            )
            # Watch.extra_metadata is layered FIRST so the authoritative
            # routing/identity keys below can't be silently overwritten
            # by a `metadata:` block in the watch YAML (sonnet fell-r2 #3).
            # If an operator wants to add per-watch metadata for
            # downstream consumers they can put anything except these
            # protected keys in `metadata:`; conflicts get clobbered by
            # the canonical values, not the other way around.
            metadata: dict[str, Any] = dict(watch.extra_metadata)
            metadata.update(
                {
                    "entity": entity.name,
                    "entity_kind": entity.kind,
                    "entity_labels": list(entity.labels),
                    "severity": watch.severity,
                    "target_pipe": watch.target_pipe,
                    "clearance_pipe": watch.clearance_pipe,
                    "watch": watch.name,
                }
            )
            triagers[synth_name] = Triager(
                name=synth_name,
                source_ref=source_ref,
                handler_path=watch.handler_path,
                timeout_seconds=watch.handler_timeout,
                metadata=metadata,
            )
    return sources, triagers


__all__ = [
    "Entity",
    "Watch",
    "DISABLED_SUFFIX",
    "parse_entity",
    "parse_watch",
    "load_entities",
    "load_watches",
    "matches",
    "expand",
]
