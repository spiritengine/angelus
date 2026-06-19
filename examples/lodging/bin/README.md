# Pitch seed drip (lodging-side)

Wires **Pitch** — the daily LLM scan that finds build-worthy **seeds** — into
angelus as a scheduled source, without any engine change. This is option C
(the "drip") from SKEIN brief `brief-20260619-opi5`.

## Why a drip

A native source fire writes **at most one** observation, collapsed on a single
`state` change-signature. Pitch produces **N seeds per run**, each worth
deduping and tracking on its own. The drip reconciles the two shapes:

- Pitch runs on its **own** schedule (the `pitch` mill chain — minutes long,
  decoupled so it never runs inside a seconds-bounded source-fire) and **writes
  seeds to a store**.
- A fast lodging source (`sources/scheduled/pitch-seeds.yaml`, 10m) **drains the
  oldest unemitted seed per tick**, stamping `emitted_at`, and prints **one**
  observation whose `state` is the seed id.
- Distinct seeds are distinct states → one observation each. A fully-drained
  store prints a stable `state: "idle"`, which collapses to no observation.

So N seeds drain over N ticks. No engine patch.

## Pieces

- `seed_store.py` — the shared read/append/drain layer + the seed schema (below).
- `pitch_seed_drip.py` — the source command: `pitch_seed_drip.py [STORE_PATH]`.
- `../sources/scheduled/pitch-seeds.yaml` — the 10m drip source.
- `../triagers/pitch-seeds-watch.yaml` + `../triagers/handlers/pitch_seed.py` —
  turn a seed observation into a finding.
- `../pipes/seeds.yaml` + `../render-templates/seed-cards.j2` — the daily seed
  digest (batches every seed dripped since the last drain into one push/email).

## Store schema (`state/pitch-seeds.jsonl`)

One JSON object per line, under the lodging root. The daemon's working directory
is the lodging root, so the source resolves `state/pitch-seeds.jsonl` from cwd.

| field          | meaning                                                              |
| -------------- | ------------------------------------------------------------------- |
| `id`           | stable hash of (`cannon`, `event`); dedup key + observation `state`. |
| `discovered_at`| ISO-8601 UTC ms; drip order is oldest-first.                        |
| `cannon`       | the prepared capability aimed at the event.                         |
| `event`        | what changed in the world.                                          |
| `build_move`   | the buildable response (the thing to ship).                        |
| `severity`     | `"low"` default; `"high"` is reserved for the rare xz-scale seed.   |
| `emitted_at`   | `null` until drained, then the ISO-8601 UTC stamp of the tick.      |

### `discovered_at` ordering contract

Drip order is `next_unemitted`, which sorts by `discovered_at` **as a string**
(lexicographic), file order as the tiebreak. Lexicographic order is chronological
**only** when every writer uses one fixed timestamp shape. So the live `pitch`
chain MUST write `discovered_at` in the exact `make_seed` format — UTC, **millisecond**
precision, **trailing `Z`** (e.g. `2026-06-19T10:00:00.000Z`). Mixing offsets
(`+00:00`), dropping the `Z`, or varying sub-second width reorders the drain and
a newer seed can drain ahead of an older one. `make_seed` produces this shape;
hand-written rows must match it.

## Concurrency & crash safety

Two processes touch the store: the live `pitch` chain `append_seed`s while the
daemon's drip drains. Both safeguards live in `seed_store.py`:

- **One writer at a time.** `append_seed`, `mark_emitted`, and `drain_one` hold
  an advisory `flock` (`store_lock`, on a sibling `.lock` file) across the whole
  load→modify→write, and `write_seeds` uses a per-process-unique temp name. So a
  concurrent append is never lost and two writers never tear the file.
- **Emit then stamp.** The drip `peek`s the oldest unemitted seed, **prints and
  flushes** the observation, and only **then** stamps `emitted_at`
  (`mark_emitted`). A crash between the print and the stamp re-emits the seed
  next tick (at-least-once) rather than marking it emitted-but-unseen
  (at-most-once = a silent loss). The re-emit is harmless: same `id` → same
  `state` → the observation collapses, and the catalog gate drops the repeat.
  One residual loss window survives lodging-side and needs a daemon change to
  close: a SIGKILL **after** `mark_emitted`'s `os.replace` commits the stamp but
  **before** the process exits cleanly leaves the seed stamped while the daemon
  discards the non-clean exit's stdout — stamped-but-unseen. Sub-millisecond;
  see the comment in `pitch_seed_drip.py` and the engine follow-up in the tender.
- **Durable rename.** `write_seeds` fsyncs the temp file **and** the parent
  directory after `os.replace`, so the rename itself survives a power loss.

## Torn rows (corruption)

A malformed JSONL line is **never deleted**. `load_seeds` skips it so valid seeds
before and after still drain, and every `write_seeds` rewrite **carries the torn
bytes through verbatim** — a torn (possibly recoverable) seed is preserved, not
silently lost. Because a successful command's stderr is discarded by the daemon,
the torn condition is surfaced on the live path as an **observation**, not a log:
once no good seed remains to drain, the drip emits a `store_corrupt` observation
(fixed `state` so consecutive ticks collapse — it does not loop; fixed `entity`
so the catalog gate alerts **exactly once**). The triager routes it `high` →
`now` with `seeds` as the floor, carrying the malformed-line count. Net: a torn
store ⇒ one visible alert, no good seed blocked, torn bytes recoverable.

## Dedup (two layers)

1. `append_seed` skips a seed whose `id` is already in the store, and the drip
   stamps `emitted_at` so a seed is drained **once** (modulo a deliberate
   at-least-once re-emit on crash, collapsed downstream — see above).
2. The catalog emission gate keys on the seed `id` (the finding `entity`), so a
   repeat for the same seed opens no new incident and **never alerts twice**.

## Routing

Normal seeds batch into the daily `seeds` pipe. `severity == "high"` (rare,
xz-scale) **also** routes to the urgent `now` pipe via the triager's per-finding
`target_pipes`, jumping the daily queue — the same mechanism the canary/http
handlers use; no engine change. The daily `seeds` pipe stays a **never-drop
floor** beneath the urgent jump: `urgent_pipe` is not cross-ref validated at
load (only `target_pipe` is), so a typo in it must not be able to drop the seed.
See the handler's `_route`.

## Firing discipline

High recall, low precision (Patrick's spec): the triager does **not** filter for
quiet. A missed seed costs a talk and the territory; a spurious seed costs a
minute of triage. Do not tune this for silence.

## Out of scope here (private lodging repo, not this examples tree)

The live `pitch` schedule that **writes** the store, the real cannons, and the
live `state/pitch-seeds.jsonl`. This repo ships the contract and fixtures only —
never real cannons or seed data.
