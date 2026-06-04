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

# Condition kinds a fixer (B11) may bind to. Both are evaluated against live
# catalog state on each fixer-evaluation pass, so they are the conditions the
# in-daemon registry can actually observe about itself:
#   - open_internal_incident: an open incident (the daemon's own self-reported
#     failure, source like internal/dep) matching a declared source.
#   - channel_unhealthy: a channel marked unhealthy by real-traffic failures.
# "daemon-dead" is deliberately NOT here: the in-daemon loop cannot observe its
# own death, so that condition stays belfry's out-of-band domain (B12). Adding
# a kind is a code change (a new matcher in the daemon), exactly like adding a
# channel kind -- dropping a fixer YAML that binds an existing kind needs none.
SUPPORTED_FIXER_CONDITIONS = (
    "open_internal_incident",
    "channel_unhealthy",
)


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
    # B2 delivery SLA: the expected max interval (seconds) between successful
    # deliveries. None means the pipe opts out of the SLA check (e.g. the
    # immediate `now` pipe, which delivers on demand and has no cadence to lapse
    # against). Declared in YAML as `max_interval: 27h`.
    max_interval_seconds: int | None = None


@dataclass(frozen=True)
class Channel:
    name: str
    kind: str
    command: str
    to: str | None = None
    # B13 transport failover: the name of another channel this channel fails
    # over to when it is degraded on the immediate path (see
    # PipeDrain._drain_immediate). Optional and domain-agnostic -- ANY channel
    # may declare a backup; no email/push special-case lives in the loader.
    # validate_cross_refs enforces that the target exists, is not the channel
    # itself, and that the backup CHAIN does not cycle. `channel_env_requirements`
    # and `_assert_well_formed_env_markers` scan every str field for the
    # `$env:` marker; a backup holds a channel NAME, never an env ref, so it is
    # inert to both (it cannot start with `$env:`).
    backup: str | None = None


@dataclass(frozen=True)
class Dependency:
    name: str
    check: str
    timeout_seconds: float = 30.0


@dataclass(frozen=True)
class FixerCondition:
    """What a fixer binds to, matched against live catalog state (B11).

    kind is one of SUPPORTED_FIXER_CONDITIONS. The other fields are matchers
    interpreted per kind:
      - open_internal_incident: `source` (required) is matched exactly against
        an open incident's source; `incident_type` and `entity` (both optional)
        narrow it further when set.
      - channel_unhealthy: `channel` (optional) names a specific channel; when
        omitted the condition matches any unhealthy channel.
    """

    kind: str
    source: str | None = None
    incident_type: str | None = None
    entity: str | None = None
    channel: str | None = None


@dataclass(frozen=True)
class Fixer:
    """An autoremediation binding discovered under fixers/ (B11).

    A condition (above) bound to a python handler run as a subprocess -- the
    same isolation model as triagers/watches, so a fixer remediates by shelling
    out and can never corrupt live daemon state. The guardrails cap the blast
    radius: at most max_attempts within window_seconds for a given condition
    instance, and at least backoff_seconds between attempts. The dispatcher
    enforces them before ever invoking the handler.
    """

    name: str
    condition: FixerCondition
    handler_path: Path
    handler_timeout: float
    max_attempts: int
    window_seconds: int
    backoff_seconds: int


@dataclass(frozen=True)
class Lodging:
    sources: dict[str, ScheduledSource]
    triagers: dict[str, Triager]
    pipes: dict[str, Pipe]
    channels: dict[str, Channel]
    dependencies: dict[str, Dependency]
    fixers: dict[str, Fixer] = field(default_factory=dict)


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
        fixers=_load_fixers(root),
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
    for fixer in lodging.fixers.values():
        # A channel_unhealthy fixer naming a specific channel gets the same
        # typo protection a triager's target_pipe gets: a fixer bound to
        # `channel: psh` would silently never fire (no channel by that name can
        # be unhealthy), the same silent-misroute class validate_cross_refs
        # closes everywhere else. A nameless channel_unhealthy (any channel) and
        # the open_internal_incident source are not cross-refs -- an incident
        # source is dynamic, not a lodged entry -- so they are validated for
        # shape at parse time, not here.
        if (
            fixer.condition.kind == "channel_unhealthy"
            and fixer.condition.channel is not None
            and fixer.condition.channel not in lodging.channels
        ):
            errors.append(
                f"fixer {fixer.name} condition.channel references missing "
                f"channel {fixer.condition.channel!r}"
            )
    # B13 transport failover: a channel's `backup` must name a real OTHER
    # channel, and the backup CHAIN must not cycle. A cycle (A->B->A, or
    # longer) is a load-time error -- at runtime PipeDrain._failover_target
    # would otherwise have to defend against an infinite walk; catching it here
    # keeps the runtime walk a simple "follow links to the first healthy one".
    # Same silent-misroute class as every other cross-ref check: a `backup: psh`
    # typo would never deliver and never complain without this gate.
    for channel in lodging.channels.values():
        if channel.backup is None:
            continue
        if channel.backup == channel.name:
            errors.append(
                f"channel {channel.name} backup references itself"
            )
            continue
        if channel.backup not in lodging.channels:
            errors.append(
                f"channel {channel.name} backup references missing channel "
                f"{channel.backup!r}"
            )
            continue
        # Walk the chain from this channel; revisiting any channel already on
        # the path is a cycle. `seen` seeds with the owner so a chain that
        # loops back to its origin (A->B->A) is caught, not just a self-loop.
        # A dangling mid-chain link is left to the owning channel's own
        # missing-ref check above (it is reported once, when that channel is
        # the loop subject), so we stop without double-reporting it here.
        seen = {channel.name}
        cursor: str | None = channel.backup
        while cursor is not None:
            if cursor in seen:
                errors.append(
                    f"channel {channel.name} backup chain cycles "
                    f"(revisits {cursor!r})"
                )
                break
            seen.add(cursor)
            nxt = lodging.channels.get(cursor)
            if nxt is None:
                break
            cursor = nxt.backup
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
        max_interval_seconds=_optional_sla_interval(data, path),
    )


# Units for the delivery-SLA `max_interval` field. Deliberately its own
# grammar (with a 'd' for days) like the mute-duration parser: an SLA deadline
# is a distinct domain from scheduling cadence (no 'd') and mute silencing.
_SLA_INTERVAL_UNITS = {"s": 1, "m": 60, "h": 3600, "d": 86400}


def _optional_sla_interval(data: dict[str, Any], path: Path) -> int | None:
    """Parse the optional pipe `max_interval` (e.g. '27h') to seconds.

    Absent -> None (the pipe opts out of the B2 delivery-SLA check). A present
    value must be a positive integer magnitude with an s/m/h/d unit suffix; a
    bare number or bad unit is a load-time ValueError, so a typo fails loud
    rather than silently disabling the SLA.
    """
    raw = data.get("max_interval")
    if raw is None:
        return None
    if not isinstance(raw, str) or not raw.strip():
        raise ValueError(f"{path}: max_interval must be a non-empty string like '27h'")
    text = raw.strip().lower()
    suffix = text[-1]
    if suffix not in _SLA_INTERVAL_UNITS:
        raise ValueError(
            f"{path}: invalid max_interval {raw!r}: expected unit suffix "
            f"(s, m, h, d)"
        )
    magnitude_text = text[:-1].strip()
    try:
        magnitude = int(magnitude_text)
    except ValueError:
        raise ValueError(
            f"{path}: invalid max_interval {raw!r}: magnitude must be an integer"
        ) from None
    if magnitude <= 0:
        raise ValueError(f"{path}: invalid max_interval {raw!r}: must be positive")
    return magnitude * _SLA_INTERVAL_UNITS[suffix]


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
        # Optional B13 failover target. A present-but-empty/non-string value is
        # a typo, not an omission, so _optional_str fails it loud rather than
        # collapsing to None. The referential checks (exists / not-self / no
        # cycle) need the WHOLE channel set and so live in validate_cross_refs,
        # not here -- parse_channel sees one file at a time.
        backup=_optional_str(data, "backup", path),
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


def parse_fixer(root: Path, path: Path) -> "Fixer":
    """Parse a fixers/<name>.yaml lodging file (B11).

    Shape::

        condition:
          kind: open_internal_incident   # or channel_unhealthy
          source: internal/dep           # required for open_internal_incident
          incident_type: dependency_unhealthy   # optional narrower match
          entity: iotaschool.com         # optional narrower match
          # channel: email               # for channel_unhealthy (optional)
        handler:
          kind: python
          path: fixers/handlers/recheck.py
          timeout_seconds: 60
        guardrails:
          max_attempts: 3
          window_seconds: 3600
          backoff_seconds: 300

    The handler is a python file run as a subprocess (like triagers/watches),
    so it remediates by shelling out. `guardrails` is required and must cap the
    blast radius -- a fixer with no attempt ceiling is exactly the
    restart-a-misconfig-forever failure the master brief forbids.
    """
    data = _read_yaml(path)
    name = path.stem

    condition_raw = _required_dict(data, "condition", path)
    kind = _required_str(condition_raw, "kind", path)
    if kind not in SUPPORTED_FIXER_CONDITIONS:
        raise ValueError(
            f"{path}: unsupported condition.kind {kind!r}; "
            f"supported: {', '.join(SUPPORTED_FIXER_CONDITIONS)}"
        )
    source = _optional_str(condition_raw, "source", path)
    incident_type = _optional_str(condition_raw, "incident_type", path)
    entity = _optional_str(condition_raw, "entity", path)
    channel = _optional_str(condition_raw, "channel", path)
    if kind == "open_internal_incident":
        if source is None:
            raise ValueError(
                f"{path}: condition.kind=open_internal_incident requires "
                f"a non-empty 'source'"
            )
        if channel is not None:
            raise ValueError(
                f"{path}: condition.channel is only valid for "
                f"kind=channel_unhealthy"
            )
    elif kind == "channel_unhealthy":
        for stray, value in (
            ("source", source),
            ("incident_type", incident_type),
            ("entity", entity),
        ):
            if value is not None:
                raise ValueError(
                    f"{path}: condition.{stray} is only valid for "
                    f"kind=open_internal_incident"
                )
    condition = FixerCondition(
        kind=kind,
        source=source,
        incident_type=incident_type,
        entity=entity,
        channel=channel,
    )

    handler = _required_dict(data, "handler", path)
    if handler.get("kind") != "python":
        raise ValueError(f"{path}: only handler.kind=python is supported")
    handler_path = root / _required_str(handler, "path", path)
    if not handler_path.exists():
        raise ValueError(f"{path}: handler path does not exist: {handler_path}")
    handler_timeout = _optional_timeout(handler, path, 60.0)

    guardrails = _required_dict(data, "guardrails", path)
    max_attempts = _required_positive_int(guardrails, "max_attempts", path)
    window_seconds = _required_positive_int(guardrails, "window_seconds", path)
    backoff_seconds = _optional_nonneg_int(guardrails, "backoff_seconds", path, 0)

    return Fixer(
        name=name,
        condition=condition,
        handler_path=handler_path,
        handler_timeout=handler_timeout,
        max_attempts=max_attempts,
        window_seconds=window_seconds,
        backoff_seconds=backoff_seconds,
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


def _load_fixers(root: Path) -> dict[str, Fixer]:
    # fixers/ holds the YAML bindings; fixers/handlers/ holds the python
    # handler files. Only the flat *.yaml layer is a fixer (handlers are
    # *.py), so the same _enabled_yaml_files glob that scans every other
    # lodging dir picks up exactly the bindings and ignores the handlers.
    loaded: dict[str, Fixer] = {}
    for path in _enabled_yaml_files(root / "fixers"):
        fixer = parse_fixer(root, path)
        loaded[fixer.name] = fixer
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


def _optional_str(data: dict[str, Any], key: str, path: Path) -> str | None:
    """Return a non-empty string value for `key`, or None when absent.

    A present-but-empty or non-string value is a typo, not an omission, so it
    fails loud rather than silently collapsing to None (the same value-shape
    leg the watch clearance_pipe parser hardened)."""
    if key not in data:
        return None
    value = data[key]
    if not isinstance(value, str) or not value:
        raise ValueError(f"{path}: expected {key} to be a non-empty string")
    return value


def _required_positive_int(data: dict[str, Any], key: str, path: Path) -> int:
    value = data.get(key)
    # bool is an int subclass; `True`/`False` here is a YAML typo, not a count.
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{path}: expected positive integer {key}")
    return value


def _optional_nonneg_int(
    data: dict[str, Any], key: str, path: Path, default: int
) -> int:
    if key not in data:
        return default
    value = data[key]
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{path}: expected non-negative integer {key}")
    return value
