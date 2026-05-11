# Angelus

Scheduling and notification spine for autonomous agent work. Part of the spiritengine ecosystem (alongside spindle, mill, shuttle, skein, horizon, scry).

## Vocabulary

- **Angelus** — the system.
- **Peal** — a consolidated digest delivered on a fixed cadence (default daily). Many small findings → one bundle.
- **Strike** — a single immediate emergency dispatch that bypasses the peal.
- **Round** *(working term)* — one scheduled job that produces findings (peal-worthy or strike-worthy).
- **Ringer** *(working term)* — the trigger that fires a round (cron, email-to-patbot, file/state change).

## Why this exists

Agent infrastructure today (spindle, mill, horizon) handles delegation and orchestration on demand. There's no reliable layer for **scheduled** autonomous work, **consolidated** routine reporting, or **escalated** urgent findings. Manual checks miss things; one-off scripts don't notice changes.

The activating example: iotaschool.com went down sometime before 2026-05-11 and was only caught by a manual run of a dead-link audit script. A reliable Angelus round would have caught the up→down transition within an hour.

Belt and suspenders. Three independent reliability layers so the system itself can't silently stop watching.

## Design

Design lives in the SKEIN `angelus` site. Read in this order:

- `brief-20260511-unaw` — Architecture and vocabulary spec (north star).
- `brief-20260511-bkxr` — Trigger system (cron + email + state).
- `brief-20260511-3tb3` — Peal pipeline (digest consolidation + delivery).
- `brief-20260511-7nrm` — Strike pipeline (emergency dispatch, dedup, rate-limit).
- `brief-20260511-e701` — Reliability (three-layer watchdog).
- `brief-20260511-9gk5` — Kickoff rounds (six concrete first jobs).

Or `skein folios angelus` for the live list.

## Status

Design phase. No code yet. First concrete milestone (per the kickoff-rounds brief): a 3-URL liveness watch that proves trigger → check → finding → peal/strike → delivered, end-to-end.
