# Logging

Angelus writes its operational log to one canonical, tail-able file and uses
real severity levels so failures are visible without a database query.

## Where logs live

- **Daemon:** `state/angelus.log` — a rotating file written by the app itself
  (a Python `RotatingFileHandler`), not by shell stdout redirection. The path
  is identical whether the daemon is launched by systemd or by hand, because
  the destination is wired in the app's logging config
  (`angelus/logging_config.py`), not in the launch command.
- **Belfry:** `state/belfry.log` — the out-of-band watcher's own log (cron
  appends belfry's stdout/stderr there; see `belfry/crontab.example`), kept
  separate so a daemon problem can't take the watcher's log with it.

Both default under `state/`; override the daemon's root with `--root` (the log
follows `<root>/state/angelus.log`).

## Why a file, not journald

Before B21+B22, logs split by launch method: under systemd the daemon's stdout
went to journald and **no** file was written, while a manual
`angelus daemon > state/daemon.log` wrote a file journald never saw. An
operator (or agent) tailing one destination was blind under the other launch
path — part of how the 2026-05-29 silent failure stayed invisible for ~24h.

Routing the log through the app's own `RotatingFileHandler` makes the file the
same in both cases. The daemon also mirrors records to stderr, so an
interactive run isn't silent and journald still gets a copy under systemd — but
`state/angelus.log` is the canonical destination to read.

## Severity levels

The daemon used to emit INFO only — thousands of lines a day and not one
ERROR, ever. Every failure path now logs at WARNING or ERROR in addition to
the database record it already writes:

- **ERROR** — a delivery the system gave up on: a dispatch that exhausted its
  retry ladder, a failed daily-digest send, an LLM digest render that fell back
  to the structured body, a control-op handler that raised.
- **WARNING** — a transient failure that will retry, a channel transitioning to
  unhealthy, a source check that failed, a degraded-but-recoverable condition.
- **INFO** — normal lifecycle (sources fired, findings ready, scheduler
  started, shutdown).

So after a real delivery failure, `grep ERROR state/angelus.log` is non-empty
and the line names the pipe, channel, and error.

## Rotation policy

`RotatingFileHandler` with a 10 MiB cap per file and 5 rotations retained
(`angelus.log`, `angelus.log.1` … `angelus.log.5`) — roughly a 60 MiB ceiling.
This keeps the file tail-able and disk-safe without an external `logrotate`
dependency. The bounds live in `angelus/logging_config.py` (`MAX_BYTES`,
`BACKUP_COUNT`).

## Log-line timestamps vs. the domain clock

The `%(asctime)s` at the start of each line is the logging framework's own wall
time (real clock), deliberately distinct from the injectable B24 domain clock
that drives lifecycle ages and dispatch windows. Log-line time is **not** routed
through the seam — a test/sim that pins the domain clock still stamps log lines
with real time.

## Belfry log

`state/belfry.log` is belfry's own destination, written by cron appending the
script's stdout/stderr (`belfry/belfry.py >> state/belfry.log 2>&1`). Belfry
stays dependency-free and deliberately does not import the daemon's logging
stack, so this file is independent of `state/angelus.log` — a daemon-side log
problem can't take the watcher's record with it.

## How to tail

```sh
# Daemon, live
tail -f state/angelus.log

# Just the failures
grep -E ' (ERROR|WARNING) ' state/angelus.log

# Belfry
tail -f state/belfry.log
```
