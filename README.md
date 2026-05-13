# Angelus

Scheduling and notification spine for autonomous agent work. Part of the spiritengine ecosystem (alongside spindle, mill, shuttle, skein, horizon, scry).

## Why this exists

Agent infrastructure today (spindle, mill, horizon) handles delegation and orchestration on demand. There's no reliable layer for **scheduled** autonomous work, **consolidated** routine reporting, or **escalated** urgent findings. Manual checks miss things; one-off scripts don't notice changes.

The activating example: iotaschool.com went down sometime before 2026-05-11 and was only caught by a manual run of a dead-link audit script. Angelus must catch the up→down transition within the cadence of whatever pipe alerts urgently.

Three independent reliability layers (belt, suspenders, boots) so the system itself can't silently stop watching.

## The frame

Pipeline shape:

    source → observation → triage → finding → pipe → channel → dispatch

- **Source** — anything that produces observations (cron, inbox, watch, webhook, manual).
- **Check** — the body a time-based source runs.
- **Observation** — a raw item in the pipeline, pre-classification.
- **Triage / Triager** — classification stage and the script/chain/mantle doing it.
- **Finding** — a triaged item with routing metadata.
- **Pipe** — a named aggregation tier with cadence + policy + channel(s) (e.g. `now`, `hourly`, `daily`, `weekly`).
- **Channel** — an output sink (push, email, skein, etc.).
- **Dispatch** — one send action.
- **Dependencies** — things angelus rests on; their failure is also angelus's failure.
- **Watchdog** — runs **health checks** to external sinks so angelus-dying is alerted independently.

## Design

Canonical spec: skein folio `brief-20260513-b1a3`.

Implementation roadmap (master + slice briefs): `brief-20260513-yctm`. Start here when picking up implementation.

M2 adversarial-test plan: `brief-20260513-cz9x`.

## Status

Slice 0 (project scaffolding) complete. Python package layout, Click CLI stub, SQLite migration scaffolding, systemd unit template, smoke test. Runtime features arrive in later slices.

## Install

```sh
pip install -e .
```

## Run

```sh
angelus --help
```

Slice 0 only guarantees the root CLI help surface. A placeholder `angelus daemon` command is present for the service template and future slices.

## Storage

SQLite is the authoritative lifecycle store. Migrations live in `migrations/` as ordered SQL files named `<NNNN>_<name>.sql`. The storage initializer enables WAL mode and applies pending migrations in order, with each migration and its `schema_migrations` bookkeeping recorded atomically.

The initial migration creates the v3.1 tables:

- `source_fires`
- `observations`
- `findings`
- `incidents`
- `triager_state`
- `pipe_queues`
- `dispatches`
- `dep_health`
- `schedule_registry`
- `backoff_store`

`observations` and `findings` both include a `status` column for the later `writing` to `ready` write-order protocol.

## Service template

`deploy/angelus.service` is checked in as a template only. Not installed or enabled by this repository.

## Development

```sh
pytest
```
