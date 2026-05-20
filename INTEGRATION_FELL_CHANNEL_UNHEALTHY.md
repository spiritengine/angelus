# Channel unhealthy: muteability collision on the digest path

## Mechanism

The channel unhealthy alert is an internal finding on the `now` pipe:

* `angelus/pipes/runner.py` writes `internal/dispatch`,
  `channel_unhealthy`, entity `<channel>`
* the dedup key therefore resolves to
  `internal/dispatch:channel_unhealthy:<channel>`
* slice 5b-2's mute grammar applies uniformly by dedup key, so muting
  that key silences the urgent `now` dispatch

Independently, the underlying channel state is stored in
`channel_health`, and the digest path's in-flight retry ladder is
stored in `digest_channel_attempts`.

## Collision

The `now`-pipe alert is deliberately muteable, but the same broken
channel can still matter on the digest path:

* an operator may mute `internal/dispatch:channel_unhealthy:email`
  because they already know the urgent alert is noisy
* meanwhile the daily digest can still be at `email 3/5 attempts`
  or already unhealthy

If the health surface followed the mute and hid the corresponding
channel state, the operator would lose the only in-protocol view of the
underlying digest-path failure.

## Saving rail

The saving rail is the health surface, unfiltered by mute:

* `_op_health` surfaces `result["channels"]["health"]` from
  `Catalog.all_channel_health()`
* `_op_health` also surfaces `result["channels"]["attempts"]` from
  `Catalog.digest_channel_attempts()`
* `angelus health` renders both the live control-socket response and the
  daemon-down sqlite-read fallback

So muting the `now` finding silences the urgent dispatch but does not
hide either:

* the channel's unhealthy row
* the digest path's pre-threshold retry ladder

## Regression test

`tests/test_slice9_health_surface.py::test_muted_channel_unhealthy_is_silent_on_now_but_visible_in_health`
locks the rail:

* a `channel_unhealthy` finding for `email` is muted on the `now` pipe
* `now` drain records `dispatches.status == "muted"`
* `_op_health()["channels"]["health"]` still contains
  `{"channel": "email", "status": "unhealthy", ...}`

Its discriminating inversion is to mute-filter channel health out of the
health response; the `email` row assertion fires immediately.
