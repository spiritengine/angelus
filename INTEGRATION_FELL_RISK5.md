# Risk 5: A `dependency_unhealthy` finding is itself muteable

## Mechanism (with code refs)

Slice 5c surfaces a failing dependency by writing an internal finding from
the `dep_record` control op:

  `angelus/daemon.py` `_op_dep_record` (around line 278):

      self.catalog.write_internal_finding(
          "internal/dep",
          "dependency_unhealthy",
          name,                # e.g. "iotaschool"
          detail or "",
          set(self.lodging.pipes),
      )

`Catalog.write_internal_finding` calls `write_finding` with no explicit
`dedup_key`, so `write_finding` derives one
(`angelus/storage/catalog.py` `write_finding`, around line 250):

      dedup_key = str(
          finding.get("dedup_key")
          or f"{source}:{finding_type}:{entity}"
      )

so the dedup_key for the iotaschool example is exactly:

      internal/dep:dependency_unhealthy:iotaschool

Slice 5b-2 added the `mute` write op, which inserts a row keyed by
`dedup_key` and a future `expires_at` (`Catalog.add_mute`). The `now`
pipe drain consults it per finding
(`angelus/pipes/runner.py` `_drain_immediate`, around line 66):

      if self.catalog.is_muted(row["dedup_key"]):
          # records a 'muted' dispatch, marks pipe_queues 'dispatched',
          # and never sends.
          ...

So this single command, perfectly within the documented contract, silences
the README's activating-example alert:

    angelus mute add internal/dep:dependency_unhealthy:iotaschool 30d

Slice 5c also deliberately writes a fresh internal/dep finding on **every**
unhealthy probe — repeats are NOT deduped, so the operator keeps being told
until recovery (the slice-3 digest-failure precedent). The mute check sits
downstream of that decision and silently defeats it: each fresh
dependency_unhealthy finding is suppressed by the same active mute, with
no extra signal that this is happening.

## The two readings

**Reading A — legitimate operator control of a flapping dep.**
The mute grammar is the operator's coarse silencing tool for the same
class of alert routed through the same path. The slice-5b-2 contract is
that a mute by `dedup_key` silences the `now`-alert until it expires.
Treating dep_unhealthy alerts as a privileged exception would make mute
non-uniform — different alerts would obey different rules of the
operator's vocabulary, with no in-protocol way to know which. An operator
genuinely needs a way to silence a known-flapping dependency they're
already tracking (e.g. "iotaschool flaps for 20 minutes every morning,
mute for the morning") without disabling the dependency entry itself
(which would also stop dep-checking the dependency).

**Reading B — silently muting the exact failure angelus exists to catch.**
The README's activating example is iotaschool.com going down and being
caught only by a manual run, and the system's stated reason to exist is
to catch such transitions within an alerting cadence. Slice 5c chose
"emit a fresh finding every time" specifically to defeat the trap of an
operator never hearing about a still-down dep after the first alert.
Mute then re-opens that trap, AND it does so silently from the `now`-pipe
view: the operator who muted the alert may well have meant "snooze for an
hour"; if the dep is actually down for 30d, they will hear nothing about
it on `now` for 30d. Worse, this is the failure mode angelus *exists* to
prevent, not an incidental class of alert.

## What this integration fell verified — the saving rail

Risk 5 is **not** a code bug to silence by undoing the mute. The mute path
is correct by the slice-5b-2 contract; the dep_unhealthy finding is
correct by the slice-5c contract; the two meet without contradicting each
other in the code. What we DID verify (and locked down with a
discriminating regression test) is the rail that prevents Reading B from
becoming a total information loss:

* `Catalog.all_dep_health()` is the unfiltered reader for `dep_health`.
* `_op_health` surfaces it as `result["deps"]`.
* `_drain_immediate`'s mute check filters DISPATCH, not `dep_health`.
* The CLI `angelus health` renders the `deps` block over the control
  socket AND in the daemon-down sqlite-read fallback (`cli.py
  _render_health_fallback`).

So even with a 30d mute on `internal/dep:dependency_unhealthy:iotaschool`,
running `angelus health` continues to report:

      dependencies:
        iotaschool: unhealthy (last check ...)
          detail: exit 7: connection refused

The dep is silent on the urgent push channel, but it is loudly visible to
any operator who looks at health — which is the operational surface for
"is angelus catching things." The saving rail is exactly this: mute
silences the channel, the health view stays honest.

The regression test
`tests/test_m1_integration.py::test_muted_unhealthy_dep_is_silent_on_now_but_visible_in_health`
locks this in. Its discriminating inversion (recorded in FELL_NOTES.md)
mute-filters `all_dep_health` and confirms the test catches the loss.

## Recommendation

1. **Hold the current behaviour.** The mute contract and the dep_unhealthy
   "every fire" contract are each load-bearing on their own terms and
   were each shipped by their respective slices in isolation. Reading A
   is a real operator need; quietly carving out dep_unhealthy from
   mute would break the uniformity of the grammar and surprise the same
   operator the carve-out is meant to protect. The saving rail (health
   stays visible) covers Reading B without changing product behaviour.

2. **Make the saving rail durable.** This integration fell adds the
   regression test (above) that asserts a muted unhealthy dep is still
   listed unhealthy via the health op. Any future change that filters
   `all_dep_health` by mute (or otherwise hides muted unhealthy deps
   from the health view) trips that test.

3. **Future, NOT done in this fell — surface mutes alongside deps.**
   The natural follow-up is purely additive and lives in the health
   surface, not in the mute or dep paths: include the active mute (if
   any) for each unhealthy dep in `_op_health`'s `deps` block, so
   `angelus health` reads e.g.:

         iotaschool: unhealthy (last check ...)
           muted until 2026-06-18T... (flapping, acked)

   That would close the last gap in Reading B — an operator who muted
   "for the morning" and forgot would see, on every health check, that
   the mute is still active. This is a slice-of-its-own change (a
   contract addition to the health op's `deps` field, with a discriminating
   reader test and a CLI render update) and is filed here as the right
   next step, not implemented in this fell.

## Out of scope here

No product behaviour is changed for Risk 5. The five-risk fell is a fell,
not a re-spec; the mute and dep_unhealthy contracts are each what the
slices that shipped them said they were. This document surfaces the
collision the slices in isolation could not see, names the saving rail
that already exists, and proposes the next slice that closes the residual
gap — without touching what M1 already ships.
