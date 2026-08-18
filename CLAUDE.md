# Agent orientation — read before doing anything surprising

Angelus was split and published on 2026-06-12. Four repos now exist, and this
checkout is the one with sharp edges. The full migration plan is SKEIN brief
`brief-20260612-hwa0` (angelus site).

## The repo map

- **This checkout (~/projects/angelus)** — the engine code, on the ORIGINAL
  (pre-publication) history. `origin` is a **private off-disk backup** at
  `github.com/smythp/angelus` (this same pre-publication history; added
  2026-07-02 to get the felled work off a single disk). It is NOT the public
  repo — pushing to `origin` is safe; see Rule 1 about the public repo.
- **github.com/spiritengine/angelus (public)** — the canonical published
  engine. Same content, **different commit hashes**: history was rewritten
  with git-filter-repo to strip the personal lodging before publication.
- **~/projects/angelus-lodging → spiritengine/angelus-lodging (private)** —
  the live deployment root: entities/watches/channels/pipes YAML, state/,
  plus deploy/ backups (env, crontab block, systemd unit).
- **spiritengine/angelus-history (private)** — frozen archive of the old
  unfiltered lineage. Write nothing there, ever.

## Rules that will save you

1. **Do not add the _public_ `spiritengine/angelus` repo as a remote here and
   do not push to it.** (The private `origin` = `smythp/angelus` off-disk
   backup, added 2026-07-02, is fine and shares this checkout's real history —
   this rule is only about the public repo.) The
   histories are unrelated; a push will be rejected or, forced, would
   destroy the published history. To publish work from here during the gap:
   clone the public repo, `git cherry-pick` the new master commits onto it,
   push normally. Routine commits apply cleanly (the stripped paths no
   longer exist in the tree). Recipe + verification checklist: brief
   `brief-20260612-hwa0`.
2. **The daemon does not run from this directory.** Its working directory
   (and `state/`, logs, sqlite, control socket) is `~/projects/angelus-lodging`.
   Run `angelus health`, `angelus drain`, etc. from there. There is no
   `state/` here beyond the tracked `.example` template.
3. **Never commit real lodging here.** Entities, watches, channel configs,
   and anything naming Patrick's hosts/sites belong in the private lodging
   repo. This repo ships `examples/lodging/` only, and the tests run against
   it (full suite ~600 tests, `python -m pytest`).
4. **belfry and sre_runner run from cron with cwd at the lodging root.**
   Post-cutover `make deploy` copies both into `<lodging>/bin/` and cron runs
   them from there, so the old `__file__`-based `CODE_ROOT` (`parent.parent`)
   now resolves to the lodging root — a YAML-only repo with no engine code.
   That conflation has bitten repeatedly (the 2026-06-12 fell, commits
   5b0aa5b..772da21; then again at the cutover, issue-20260615-njaq). The
   fix: the engine repo must be resolved, not derived from file location.
   `sre_runner` now calls `resolve_engine_repo()` — it reads the installed
   `angelus` package's PEP 610 provenance (`direct_url.json`), falling back to
   the imported package's own location for an editable dev tree, and validates
   the result is a real engine checkout before spawning the SRE fixer agent
   there (fail-loud-and-page if it cannot). belfry's old `stale_deployment`
   commit-time-vs-process-start check (and `CODE_ROOT`) is GONE — it was
   doubly broken post-cutover (CODE_ROOT resolved to the codeless lodging
   root, and commit-time-vs-start false-pages every merge since master runs
   ahead of the pin by design). It is replaced by `code_drift_failure()`
   (piece 2 of brief-20260613-3spy): the daemon snapshots its own installed
   sha to `state/running-version` once at boot (PEP 610
   `direct_url.json` `vcs_info.commit_id`, or the literal `editable` for a dev
   tree), and belfry compares that PLAIN FILE against `state/installed-version`
   (the pin `make deploy` records). belfry does this with stdlib only — it
   reads two state files and does NOT import angelus and does NOT derive the
   engine repo (preserving its import-independence at `<lodging>/bin`). Drift
   is alert-only (never auto-restart — that is the 0015 loop) and the page is
   suppressed while a deploy hold is active (the deploy window is exactly when
   a transient mismatch is expected). The "installed N commits behind master"
   read lives in the daemon's daily digest only (never pages). Do NOT
   reintroduce a `__file__`/`CODE_ROOT` derivation for locating the engine
   repo, and do NOT make belfry import angelus or call git to determine drift.

## Pending: the lineage swap

At a quiet moment (no in-flight shards), this checkout gets replaced by a
clone of the public repo: publish any unpublished master commits first
(cherry-pick), then re-clone, carry over `.skein/` and untracked scratch,
prune worktrees. After that, origin = public, normal push flow, and this
file's "no remote" rule above is obsolete — update it. Details:
`brief-20260612-hwa0`.

## Backup map (what survives a dead disk)

- Engine code: public repo.
- Lodging YAML + angelus.env + crontab block + systemd unit: private
  lodging repo.
- Old unfiltered history: private archive repo.
- 1Password service-account token (`~/.config/angelus/op-service-account.env`):
  NOT in git anywhere by design — re-provision from 1Password on recovery.
- Runtime state (sqlite, logs): disk only, untracked by design.
