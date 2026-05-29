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
- **Belfry** — runs **health checks** to external sinks so angelus-dying is alerted independently.

## Design

Canonical spec: skein folio `brief-20260513-b1a3`.

Implementation roadmap (master + slice briefs): `brief-20260513-yctm`. Start here when picking up implementation.

M2 adversarial-test plan: `brief-20260513-cz9x`.

## Status

Slice 1 implements the first load-bearing vertical path: scheduled shell source,
observation storage, pull-based triage, finding and incident lifecycle, the `now`
pipe, and the `push` channel.

## Install

```sh
pip install -e .
```

## Run

```sh
angelus --help
angelus daemon
```

Set `ANGELUS_DRY_RUN=1` to write push payloads to `dispatches.log` instead of
calling `notify-pat`.

## Reliability

The belfry is the external reliability layer. It runs outside the daemon from
raw cron, checks `state/angelus.pid`, reads `source_fires` from
`state/angelus.sqlite3` in read-only mode, pings healthchecks.io, and calls
`notify-pat` directly when the daemon is dead or wedged.

Beyond liveness, the belfry also surfaces the daemon's own self-reported
failures, generically: on each tick it reads `dispatches` and `incidents`
(same read-only connection) and pings DOWN if any dispatch landed in
`status='failed'` since the last tick, or any `internal/*` incident is open.
Failed dispatches are edge-triggered off a last-seen-id bookmark
(`state/belfry-failcheck-at` by default, or `ANGELUS_BELFRY_FAILCHECK_PATH`
if overridden), so a transient failure pings once; open `internal/*`
incidents are level-triggered and keep belfry red until they close. The
check names no specific channel — it reads the schema the daemon already
writes, so a live-but-not-delivering daemon (the 2026-05-29 silent-email
failure mode) no longer reads as green.

Setup:

1. Register two healthchecks.io URLs as described in `belfry/healthchecks.example`.
2. Export `ANGELUS_BELFRY_SUCCESS_URL` and `ANGELUS_BELFRY_DOWN_URL` for cron.
3. Paste the entry from `belfry/crontab.example`.

The default wedge threshold is 10 minutes. Override it with
`ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC` if your source cadence needs more slack.

Optional boots layer: keep it advisory. The canonical watchdog lives in
`deploy/boots-watchdog.sh` and watches the same belfry sentinel file
(`state/belfry-pinged-at` by default, or `ANGELUS_BELFRY_SENTINEL_PATH` if
overridden). Install it from system cron or systemd, for example:

`*/10 * * * * ANGELUS_ROOT=/opt/angelus ANGELUS_BOOTS_NOTIFY='mail -s "angelus boots alert" you@example.com' /opt/angelus/deploy/boots-watchdog.sh`

The script alerts when the sentinel is missing or older than
`ANGELUS_BOOTS_STALE_MINUTES` (default `30`). It does not add a lodged
dependency check or any new angelus product behavior; the operator owns the
outermost watchdog wiring.

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

Subsequent migrations add:

- `observation_triage` — per-(observation, triager) lifecycle for the pull triage loop (status, last_error). Migration 0002; migration 0003 adds `attempt` and `next_attempt_at` for retry scheduling.
- `channel_health` — per-channel healthy/unhealthy status with the most recent failure; the threshold ladder in the pipe runner flips it. Migration 0003 (which also adds `next_attempt_at` to `pipe_queues` and the retry columns to `observation_triage`).
- `pipe_state` — last successful drain time per pipe, used by the daily/digest cadence. Migration 0004 (also adds a CHECKed `status` to `pipe_queues` and a `source` column to `dispatches`; `next_attempt_at` was added earlier by migration 0003).
- `mutes` — per-dedup_key suppression with an expiry and optional comment; consulted on dispatch. Migration 0005 (also adds `close_comment` to `incidents`).
- `dep_health` is dropped and recreated by migration 0006 with the slice-5c dependency-registry shape (`dependency_name` PK, status CHECK, `last_check_at` and `updated_at` NOT NULL, nullable `detail`).
- `digest_channel_attempts` — per-(pipe, channel) digest send-attempt counter so the digest path can consume the same channel_health threshold ladder the immediate path uses without inflating it by the per-cycle batch size. Daemon-restart-scoped (cleared at startup) to match `channel_health`. Migration 0007.

## Service template

`deploy/angelus.service` is checked in as a template only. Not installed or enabled by this repository.

## Development

```sh
pytest
```
