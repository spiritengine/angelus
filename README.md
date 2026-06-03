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

## Configuration

Non-secret runtime config lives in one place: `state/angelus.env`. This is the
single source of truth so the daemon and the belfry can't drift apart — the
2026-05-29 incident was exactly that drift, when the daemon was relaunched
outside systemd and silently lost `ANGELUS_EMAIL_TO`.

Copy the checked-in template and fill it in:

```sh
cp state/angelus.env.example state/angelus.env
```

`state/angelus.env` is gitignored; `state/angelus.env.example` is the tracked
template. It holds **non-secrets only** — recipient address, healthcheck URLs,
and the belfry thresholds:

- `ANGELUS_EMAIL_TO` — recipient for the email channel (daily digest). Not used by belfry.
- `ANGELUS_BELFRY_SUCCESS_URL`, `ANGELUS_BELFRY_DOWN_URL` — healthchecks.io pings.
- `ANGELUS_BELFRY_WEDGE_THRESHOLD_SEC` (default 600), `ANGELUS_BELFRY_STALE_AFTER_SEC`
  (default 1200), `ANGELUS_BELFRY_NOTIFY_COMMAND`, and the sentinel/failcheck
  path overrides — see the template for all knobs and defaults.

The SMTP password is a real secret and does **not** belong here (that move is
tracked separately as B20).

**Secret references.** A value in this file may be a 1Password secret reference
(`op://vault/item/field`) instead of a literal. The **daemon** resolves refs at
startup (`angelus.envfile.resolve_op_refs`) via `op read`, authenticated by the
read-only `angelus-daemon` service-account token the systemd unit injects (a
drop-in `EnvironmentFile` pointing at a mode-`0600` token file). This keeps the
real value out of the plaintext file while staying non-interactive — no
biometric, no session to lapse. Resolution is **fail-safe**: a ref that can't be
resolved (no token, `op` error) is left unset and the consumer degrades (e.g.
the digest dead-man goes inert), so startup never blocks on it. Belfry is
deliberately **excluded** — it is the pure-stdlib belt layer and must carry no
1Password dependency, so its `ANGELUS_BELFRY_*` URLs must remain literals. The
digest heartbeat URL is the first ref to use this; the SMTP password (B20) is
the natural next.

The file is loaded the same way no matter how a process starts:

- **systemd** — `deploy/angelus.service` carries `EnvironmentFile=-…/state/angelus.env`.
- **cron** — the entry in `belfry/crontab.example` sources it before running belfry.
- **in code** — the daemon (`angelus daemon`) and `belfry/belfry.py` both load it
  at startup, so a hand-launch outside systemd still inherits the config.

**Precedence: an explicitly-set environment variable always wins over the file.**
Nothing in the file overwrites a name already present in the environment (a
systemd `Environment=` line, a var exported in the shell or crontab, etc.). The
file only fills in names that are unset.

## Reliability

### Logging

The daemon writes one canonical, tail-able log at `state/angelus.log` (a
rotating file written by the app, not by stdout redirection), so systemd and
manual launches log identically — fixing the journald-vs-file split that
helped hide the 2026-05-29 incident. Every failure path logs at WARNING/ERROR
in addition to its database record, so `grep ERROR state/angelus.log` surfaces
real failures. Belfry keeps its own separate `state/belfry.log`. Full
details — rotation policy, severity levels, how to tail — in
[docs/logging.md](docs/logging.md).

### Transport separation

Urgent and routine alerts ride different transports, so a dead transport can't
swallow the alerts that matter most. The urgent/immediate pipe (`now`) and any
escalation path use push (`notify-pat`); email is the long-form transport. The
2026-05-29 incident was exactly this failure mode: email silently broke and the
alerts that would have surfaced it were also riding email — so urgent alerts
must never depend on email alone.

The routine digest pipe (`daily`, 07:00 local) is **additive across both
transports**: the email leg carries the full long-form digest (LLM synthesis
plus the structured preamble) and the push leg carries a compact summary
(`PipeDrain._render_compact` — a heartbeat header plus per-section counts and
capped headlines). They are rendered separately because telegram caps a message
at 4096 chars; the runner routes the full message to non-push channels and the
compact one to push. The point is that telegram **acks delivery** where SMTP
only hands off, so the push leg is the reliable receipt and email is the
nice-to-have full prose — if email keeps flaking it can be dropped without
losing the daily report. The cost of additive is that push now carries both the
urgent `now` alerts and the routine digest; the off-box dead-man below is the
backstop if push itself ever dies.

**Digest dead-man.** After a digest drain delivers on at least one channel, the
daemon pings `ANGELUS_DIGEST_HEARTBEAT_URL` (best-effort, last, never blocks or
fails the digest). Point this at a healthchecks.io check with a ~daily expected
cadence: if the digest ever silently stops firing, that off-box third party —
independent of the daemon *and* belfry — alerts directly. Unset, the ping is
skipped and the feature is inert, so it is safe to ship before the check is
provisioned. This closes the "the digest never ran and nobody noticed" gap;
it does not by itself prove inbox receipt (that residual is covered for push by
telegram's delivery ack, and is a known, accepted gap for the email leg).

The belfry is the external reliability layer. It runs outside the daemon from
raw cron, checks `state/angelus.pid`, reads `source_fires` from
`state/angelus.sqlite3` in read-only mode, pings healthchecks.io, and calls
`notify-pat` directly when the daemon is dead or wedged. Belfry must never
alert over email — email is the transport it exists to detect as broken.

Beyond liveness, the belfry also surfaces the daemon's own self-reported
failures, generically: on each tick it reads `dispatches` and `incidents`
(same read-only connection) and pings DOWN if any dispatch landed in
`status='failed'` since the last tick, or any `internal/*` incident is open.
Failed dispatches are edge-triggered off a last-seen-id bookmark
(`state/belfry-failcheck-at` by default, or `ANGELUS_BELFRY_FAILCHECK_PATH`
if overridden), so a transient failure pings once; open `internal/*`
incidents are level-triggered and keep belfry red until they close. The
check's detection logic names no specific channel — it reads the schema the
daemon already writes (a channel name appears only as diagnostic detail in the
alert text), so a live-but-not-delivering daemon (the 2026-05-29 silent-email
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
