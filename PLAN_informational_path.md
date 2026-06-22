# Plan: the informational path (a new delivery lane, separate from incidents)

Status: v2 for review. Author: Claude (Opus 4.8), 2026-06-21.
Engine repo: ~/projects/angelus. Live deployment + sqlite state: ~/projects/angelus-lodging.

Scope note: the incident path is correct and stays UNTOUCHED. This plan adds a
NEW, parallel informational path and consolidates delivery into the single
existing daily angelus email. Delivery-wedge bug fixes (read_body fail-soft,
declared-inputs, bounding clearance_findings_since) are out of scope here — minor
and tracked separately. This document is about how informational content enters
and leaves the system going forward.

## Goal

Patrick gets ONE angelus email a day (as today), but it must be more useful: it
must also carry informational content that today either never arrives or
masquerades as incident noise. "Many systems" must be able to deliver into it
cheaply, and an agent inspecting the system must NOT see informational items as
incidents.

## The two paths, after this change

- Incident path (unchanged). A watch observes a CONDITION it cannot assert is
  fine ("is parks.win up?"). The engine opens/closes an incident; belfry and
  fixers treat it as a problem; it can page; it renders in the email's "Open
  incidents" section.
- Informational path (new). A producer ASSERTS a fact it already knows ("here is
  a build seed", "BBQ today", "CI flaked twice overnight"). The engine delivers
  it once and forgets it: no incident is opened, so it is invisible by
  construction to belfry, fixers, health, and every "what is broken" read. It
  renders in the email's new "Updates" section.

Same delivery substrate underneath (findings → pipe_queues → digest drain);
opposite semantics on top. The only behavioral fork is "do not open an incident".

## Producer-facing model (how informational items are added going forward)

Today each producer is a bespoke triad: its own store file, its own drip source
that reads the store one item per tick, its own triager. Three lodged files per
producer. This plan collapses that to ONE shared intake so a new producer is a
record drop, not a config change.

Parts:
- One informational inbox under the lodging root (append-only feed with a lock,
  OR a drop directory). It is the durable buffer; it survives the daemon being
  down and replays on the next tick.
- One lodged drip source that reads the inbox each tick, one item per tick,
  collapsing on the item id (the same observation-collapse seeds/reminders use).
- One lodged triager that stamps lane=informational and routes the item to the
  digest it named (default: the daily email).
- A thin writer, `angelus inform ...` (the generalization of today's
  `reminder add`), so a producer never hand-rolls the record. A system that
  cannot call the CLI may append the JSON line directly.

Adding a producer going forward = call `angelus inform` (or append one line).
Dropping a producer = stop calling it; old items age out via the drip's prune
(reminders already prune fired rows >30d). No source/triager/handler per
producer.

### The record contract (one JSON object)

- `id` — stable, producer-chosen. This is the dedup key; dropping the same id
  twice is a no-op.
- `pipe` — which digest it lands in. Default: the daily email.
- `title` / `body` — the content to deliver.
- `source` — provenance label, so the email can say who said it.
- `severity` — optional, DISPLAY ONLY. It does not escalate, page, or open
  anything.
- optional `fire_date` / `expires` — for dated items (reminders).

That is the whole producer surface: no watch, no check, no condition, no
clearance.

## One email, two sections (delivery consolidation)

Today there are three digests: `daily` (07:00, incidents), `seeds` (08:00),
`reminders` (07:30). The seeds/reminders digests were split off ONLY because
routing them through `daily` re-showed them as never-closing open incidents — a
constraint the lane change dissolves (informational items open no incident, so
routing them through `daily` is safe).

Target: retire the separate `seeds` and `reminders` pipes; the single daily email
renders:
- Open incidents (incident path; unchanged).
- Updates (informational path; new) — informational findings since the last
  drain, grouped/labelled by `source`, rendered as content cards, never as
  incidents.

Seeds and reminders become the first two citizens of the shared intake, routed
into the daily email's Updates section. Any future producer joins by dropping
records into the same intake.

## Engine changes required

1. `lane` on findings — `condition` (default) | `informational`. A finding with
   lane=informational skips `_upsert_incident` and the open-incident emission
   gate; everything else (row, body, pipe enqueue, digest windowing) is reused
   unchanged. Migration: `ALTER TABLE findings ADD COLUMN lane TEXT NOT NULL
   DEFAULT 'condition'` (the DEFAULT is the backfill).
2. Lane decided PER FINDING TYPE, not per triager, and stamped in the ENGINE at
   the triager loop (`daemon.py:2046`), not in each handler. This matters because
   the seed/reminder triagers ALSO emit a `store_corrupt` finding — a genuine
   high-severity alert that MUST stay an incident. A blanket per-triager flag
   would make a torn store informational: belfry goes blind AND, with the
   incident gate gone, it re-pages every poll. So the engine maps lane from the
   finding TYPE (a small lodged type→lane map), keeping `store_corrupt`
   = condition.
3. Lane-level dedup (REQUIRED). The incident is today's dedup: a repeat finding
   while its incident is open is dropped. Informational skips that, and triage is
   at-least-once (a crash between `write_finding` and `mark_triage_success`
   re-emits), so we need a delivery-only dedup with no open/close semantics — a
   pre-write drop on `dedup_key WHERE lane='informational'` (partial unique
   index; `idx_findings_dedup_key` exists). This restores exactly the
   "deliver-once" the incident was providing.
4. The daily pipe gains an informational input + an Updates render block.
   `daily.yaml` body inputs add `informational_since_last_drain`; a new render
   template renders it as content cards. The "Open incidents" section stays
   incident-only (informational items are absent from `open_incidents()` by
   construction, so it cleans up by row removal, not template edits).
5. Config validation: reject unknown lane values at load; cross-ref the intake's
   default pipe the way `target_pipe`/`urgent_pipe` are already cross-ref'd.

## What an agent sees

- "What is broken" → reads incidents → sees zero informational items.
- "What is new" → reads the informational feed (`angelus inform ls`, or the
  lane='informational' filter on findings) → sees the updates.
- belfry / fixers / health → incident-derived → never touched by informational.

## Open decision for Patrick (not blocking the design)

Naming of the lane and the email section ("Updates" / "Briefing" / "Bulletin").
The Namer can settle this; it does not change the mechanism.

## RESOLUTIONS after v2 review (GPT-5.5 + Opus, 2026-06-21)

Both verdicts: sound with changes. Blockers to close before build (the first
three fail SILENTLY in production — treat as hard blockers):

1. One fixed informational finding type. An `angelus inform` record becomes a
   single fixed type (e.g. `info`); the type→lane map sends that one type to
   informational. This is what makes "drop a record, no config change" true — a
   new producer reuses the type, never edits the map. (Default lane stays
   `condition`, so a brand-new type would wrongly open an incident; the fixed
   type avoids that.)
2. `findings_for_pipe_since` must filter to `lane='condition'`. Otherwise
   informational items render TWICE in the email — once incident-framed via
   `findings_since_last_drain`, once in Updates. (catalog.py:961/982)
3. The `angelus inform` writer must resolve the inbox to the live lodging root by
   a non-cwd, non-engine-`__file__` mechanism (the 411e088 bug class). A
   lodging-bin script resolving from its own location, or an explicit pinned
   root that refuses the cwd default.
4. The generic triager validates `record['pipe']` against the lodged pipe set and
   fails LOUD on a miss (`write_finding` drops unknown pipes silently;
   per-record pipe is runtime data, so load-time cross-ref can't cover it).
5. dedup_key namespaced by source (`source:id`), and the informational dedup
   pre-check drops only against delivered/`ready` rows, never in-flight `writing`
   rows (a crashed half-write must not block the retry → silent loss).
6. Chronicler prompt is ops/incident-framed ("lead with the most severe item").
   Keep informational OUT of the chronicler synthesis input; render it only in a
   deterministic Updates card. (runner.py:1352-1374)
7. `severity` is display-only ⇒ informational never routes to `now`. Today high
   seeds jump to `now` and page — that behavior is incident-path and must not
   carry to the informational lane. (Decide: do urgent seeds stay an incident, or
   lose their page?)
8. has-clearance ⇒ must-be-condition. Types that pair with a `clearance`
   (stale_pr, watch-check-failed) cannot be informational, or their close half is
   silently dropped. Encode as a map invariant.
9. Throughput: one shared one-item-per-tick drip serializes all producers. Batch-
   drain per tick (or drop cadence) so a burst of N items doesn't take N×cadence.
10. `fire_date`/expiry enforced, not just displayed — a dated item emits only when
    due (reminders already do this; the generic intake must preserve it).

## Questions for reviewers

- Q1. Is the single shared intake (inbox + one drip + one triager + `angelus
  inform`) the right producer model for "many systems deliver cheaply", or is a
  direct catalog-write ingest (no drip/observation) cleaner? Argue the tradeoff
  against daemon-down durability and sqlite write concurrency.
- Q2. Is mapping lane from finding TYPE (vs. a per-finding flag the handler sets)
  robust for every current and plausible producer? Find a finding type that
  would be misclassified. Cite file:line.
- Q3. Folding seeds/reminders into the single daily email: any timing/framing
  reason the split must survive (cadence, render size, the chronicler prompt)?
- Q4. The lane-level dedup_key drop — does it fully replace the incident's dedup
  for the at-least-once triage retry, and can a dedup_key collide across lanes?
- Q5. Anything wrong, missing, or under-specified in the producer model or the
  one-email consolidation.
