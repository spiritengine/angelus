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

### Health surface (is it *working*?)

`angelus health` answers "is it working", not just "is it running". Alongside
sources, queues, deps, and channel health it carries a **delivery** section:
the last successful send per pipe (every configured pipe, `never` if it has
never delivered), the count of failed dispatches in the recent window (24h),
and the count of angelus's own open internal incidents. That last-successful-
send-per-pipe is the read the 2026-05-29 incident needed and lacked — the
daemon was alive and "healthy" while the daily pipe silently stopped
delivering. Output is plain text, one item per line (no tables/columns). The
same delivery surface renders on the daemon-down read-only fallback, since it
is built from the dispatch/incident schema rather than live daemon state.

### Logging

The daemon writes one canonical, tail-able log at `state/angelus.log` (a
rotating file written by the app, not by stdout redirection), so systemd and
manual launches log identically — fixing the journald-vs-file split that
helped hide the 2026-05-29 incident. Every failure path logs at WARNING/ERROR
in addition to its database record, so `grep ERROR state/angelus.log` surfaces
real failures. Belfry keeps its own separate `state/belfry.log`. Full
details — rotation policy, severity levels, how to tail — in
[docs/logging.md](docs/logging.md).

### Config integrity (fail-loud on bad env)

A misconfigured daemon must not come up silently healthy — the 2026-05-29
incident was a daemon that lost `ANGELUS_EMAIL_TO` and stayed green. At startup
the daemon validates that every channel a pipe routes to has its required env
config present. The requirement is derived domain-agnostically from each
channel's `$env:NAME` config markers (the same markers the channel wrappers
resolve at send time), so the check names no specific channel or variable:
email's `to: $env:ANGELUS_EMAIL_TO` yields the `ANGELUS_EMAIL_TO` requirement
for free, and a future channel that adds a `$env:` field is covered without
code changes. Only channels a pipe actually references are checked — an
unreferenced channel file can't dispatch.

On a missing variable the daemon starts in **degraded mode and alarms**, rather
than refusing to start. The systemd unit is `Restart=on-failure` /
`RestartSec=5`, so a nonzero exit on missing config would crash-loop every five
seconds and never reach a live transport — refuse-to-start fights the restart
loop. Instead the daemon comes up, logs an ERROR naming the channel and the
missing variable, and opens a high-severity `internal/config` incident routed
to `now` (push-only — deliberately off the very email transport a missing
`ANGELUS_EMAIL_TO` would break). The alarm therefore surfaces through push, the
health surface, and belfry's open-internal-incident check. The incident is
edge-triggered: every referenced channel whose config is present fires a paired
clearance, so a config fixed while the daemon was down closes the incident on
the next startup and the emission gate re-arms.

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

**Delivery SLA.** The failed-dispatch/incident surface above only fires when
something *errored*. The 2026-05-29 incident had no error — the daily pipe just
silently stopped delivering. So belfry also asserts each pipe delivers on its
**cadence**: a pipe declares an expected max interval between successful
deliveries (`max_interval: 27h` in `pipes/<name>.yaml`), the daemon persists it
to the `pipe_sla` table at startup (belfry is pure-stdlib and can't parse the
YAML itself), and on each tick belfry reads that table plus the last
`status='sent'` dispatch per pipe and pings DOWN if the window lapsed. It is
level-triggered (re-reports until a delivery resets the window) and alert-only
(never a restart — a stalled pipe is a product/logic failure, not absence, and
auto-restart would mask the cause). The overdue baseline for a never-delivered
pipe is `tracking_since`, set once when the SLA is first registered, so a fresh
deploy gets a full window of grace instead of alarming immediately. This is the
on-box, all-pipes generalization of the off-box digest dead-man — complementary,
not redundant: the dead-man covers only the daily pipe but survives the whole
box dying, while the SLA check covers every pipe but rides the same box.

The expected max interval is an **explicit per-pipe field**, not derived from
the pipe's cron `cadence`. Computing a guaranteed max gap from an arbitrary
cron expression is fragile (DST, irregular schedules, month boundaries); an
explicit `max_interval` is unambiguous, lets each pipe set its own grace (the
daily pipe uses 27h = 24h cadence + 3h, matching the dead-man's healthchecks
grace), and a malformed value fails the config load loudly rather than silently
disabling the check. Immediate pipes (`now`) declare no interval and opt out —
they deliver on demand with no cadence to lapse against.

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

### Escalation ladder

A delivery failure gets **louder** over time instead of looping quietly. The
immediate (`now`) path walks a three-rung ladder, each rung a strictly louder
statement than the last:

1. **Retry with backoff.** An undelivered finding advances a per-*finding*
   redelivery counter (`pipe_queues.attempts` + `next_attempt_at`) and is
   retried on a backoff schedule. A transient blip recovers here and never
   escalates.
2. **Fail over to an alternate channel.** Once a *channel* degrades (it crosses
   its per-channel failure threshold, or is already marked unhealthy), the
   runner delivers the finding over that channel's configured `backup`, so the
   content still gets out this drain while the degraded channel is alarmed
   separately (the `internal/dispatch` `channel_unhealthy` incident). This is
   the B13 transport failover; the backup chain is followed to the first healthy
   channel.
3. **Page out-of-band.** When a *product* finding exhausts its retry budget
   *without ever reaching any transport* — the primary and every backup failed —
   the daemon gives up on that content and makes the loss impossible to miss: it
   logs an ERROR and raises a **distinct, durable** internal finding
   (`internal/delivery` / `delivery_exhausted`, entity = the finding id, so each
   lost item is tracked and replayable individually). That opens an incident
   that stays open — keeping belfry red — until the content is actually
   delivered; it is **not** auto-cleared on a timer. Rung 3 fires only for
   product content: an `internal/*` finding (angelus's own distress signal) is
   excluded, exactly as it is from rung 2's failover — it already fans to every
   channel via B7 and belfry already carries its original incident off-box, so a
   second `internal/delivery` incident would be a false "content lost" premise
   with no redelivery path of its own to ever clear it.

Rung 3 is deliberately distinct from rung 2's `channel_unhealthy` alarm:
`channel_unhealthy` says "a transport is degraded" (transient, per-channel),
while `delivery_exhausted` says "we permanently gave up delivering this content
after the whole ladder" (durable, per-finding) — the louder statement, on its
own source so belfry, health, and the digest can tell them apart.

**Out-of-band model.** The daemon does **not** ping a healthcheck itself or
bypass the channel layer for rung 3. It emits the durable finding-level signal;
**belfry** — which already pings `ANGELUS_BELFRY_DOWN_URL` off-box on each tick
whenever it sees an open `internal/*` incident — is what carries it out-of-band.
This keeps the two-tier split intact (the in-daemon path handles live errors,
belfry owns the off-box page) and keeps any healthchecks dependency out of the
daemon. The rung-3 finding also fans to every channel via the internal-findings
fan, but its load-bearing delivery is belfry: by definition the channels just
failed.

**Configurable threshold.** The rung-3 give-up point is a per-pipe field,
`max_delivery_attempts` in `pipes/<name>.yaml`, so a pipe tunes how patient its
redelivery ladder is before it exhausts and pages out-of-band. Unset, it
defaults to the shared retry constant (5), so behaviour is unchanged. The field
tunes only the per-finding redelivery ladder — the per-channel health
thresholds that trigger rung 2 stay on the shared constant, since "how patient
am I about one finding's delivery" is a different question from "when is a
transport degraded".

**Recovery edge (live).** The `delivery_exhausted` incident closes when the
exhausted content is actually re-delivered: the `now`-path reconciliation fires a
paired `internal/delivery` clearance on every successful delivery (a no-op under
the B30 gate unless an incident is open), so the incident auto-closes the instant
the content gets out. The path that re-arms an exhausted (`failed`) queue row for
redelivery — `angelus replay <fid>` (`catalog.replay_finding` via the daemon's
`_op_replay` control op) — exists and is wired today, so this clear edge is
built, not deferred. B15's dead-letter handling remains a separate concern; the
clear edge does not wait on it.

### Autoremediation (fixers)

Detection makes a failure loud; a **fixer** lets the daemon *act* on one. A
fixer is a lodging entry under `fixers/` — discovered at load like
`triagers/`/`pipes/`, hot-reloadable, `.disabled`-honoring — that binds a
**condition** to a **handler** under **guardrails**:

```yaml
# fixers/<name>.yaml
condition:
  kind: open_internal_incident   # or channel_unhealthy
  source: internal/dep           # exact match; incident_type/entity narrow it
handler:
  kind: python                   # run as a subprocess, like a triager
  path: fixers/handlers/<x>.py
  timeout_seconds: 60
guardrails:
  max_attempts: 3                # within window_seconds, per condition instance
  window_seconds: 3600
  backoff_seconds: 300           # minimum spacing between attempts
```

The condition is matched against live catalog state on each evaluation pass —
`open_internal_incident` against the daemon's own open `internal/*` incidents,
`channel_unhealthy` against a channel marked unhealthy by real-traffic failures.
Daemon-death is deliberately **not** a fixer condition: the in-daemon loop can't
observe its own death, so that stays belfry's out-of-band job (it restarts on
*absence*; fixers handle *live* errors). The handler runs out-of-process and
remediates by shelling out, so a buggy fixer can't corrupt daemon state; it
reads the matched condition as JSON on stdin and reports `{"outcome": "...",
"note": "..."}` on stdout.

The guardrails are the contract that autoremediation never makes things worse:
at most `max_attempts` within `window_seconds` for a given condition instance,
spaced at least `backoff_seconds` apart, enforced before the handler ever runs —
this is what keeps a fixer from restart-looping a misconfiguration. When a fixer
exhausts its budget it simply stops firing (quietly): the underlying condition
stays loud through belfry and `angelus health`, and making the *giving-up*
itself escalate is the escalation ladder's job, not the registry's. Every
attempt is recorded and appended to the shared `state/fixers.log` audit trail —
the same file belfry's restart-fixer writes — so fixer actions flow into the
daily digest's `fixer_actions` section and any postmortem with no extra
plumbing.

`fixers/observe-internal-incident.yaml.disabled` ships as a documented, inert
template (and the `observe.py` handler it points at is the copy-me starting
point for a real fixer).

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
- `immediate_channel_attempts` — per-(pipe, channel) immediate-path send-attempt counter (B7 fell-r1 Finding 3). After B7 fans internal/* findings to every channel, the per-(finding, pipe) `pipe_queues.attempts` row can no longer carry per-channel escalation (it inflates +N per drain and goes terminal on the first co-fanned success). This table tracks each channel's consecutive failures across findings and drives channel_health independently, while `pipe_queues.attempts` stays as the per-finding redelivery ladder. Daemon-restart-scoped (cleared at startup) to match `channel_health` and `digest_channel_attempts`. Migration 0010.

## Service template

`deploy/angelus.service` is checked in as a template only. Not installed or enabled by this repository.

## Development

```sh
pytest
```
