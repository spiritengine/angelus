"""SQLite lifecycle operations.

Rate-limit accounting intentionally uses the dispatches table. Slice 3 stores
the finding source redundantly on each dispatch row so the per-source rolling
count is a simple indexed lookup instead of parsing finding_ids JSON.
"""

from __future__ import annotations

import json
import logging
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

from angelus.clock import Clock

LOGGER = logging.getLogger(__name__)

TRUST_RETRY_DELAYS = (
    timedelta(minutes=1),
    timedelta(minutes=10),
    timedelta(hours=1),
    timedelta(hours=8),
)
MAX_RETRY_ATTEMPTS = 5

# Sentinel finding_id returned when the B30 emission/recovery gate drops a
# write (a clearance with no open incident to close). sqlite rowids start at
# 1, so 0 can never collide with a real finding row and reads cleanly as
# "no finding was written".
_NO_FINDING = 0


def _format_time(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Catalog:
    def __init__(
        self,
        connection: sqlite3.Connection,
        root: Path,
        clock: Clock | None = None,
    ) -> None:
        self.connection = connection
        self.root = root
        # Injectable time seam (B24). Defaults to the real wall clock so
        # existing callers keep working; the daemon threads its shared clock
        # in, and tests pass a FakeClock to control every timestamp/window.
        self._clock = clock or Clock()

    def watch_state_for(self, source_ref: str) -> dict[str, Any] | None:
        """The source's single overwritten watch_state row, or None on the
        first-ever fire (no row yet). Read by _fire_source to decide whether a
        fire CHANGED -- a None return is a first sighting and always counts as
        a change so the first observation is never skipped."""
        row = self.connection.execute(
            "SELECT * FROM watch_state WHERE source_ref = ?",
            (source_ref,),
        ).fetchone()
        return dict(row) if row is not None else None

    def record_watch_check(
        self,
        source_ref: str,
        signature: str | None,
        outcome: str,
        observation_id: int | None,
    ) -> None:
        """Overwrite the source's watch_state row each fire -- the collapse
        bookkeeping that replaces the unbounded source_fires append.

        last_checked_at/last_outcome/updated_at are bumped on EVERY call: this
        is the "we checked at T" heartbeat belfry's wedge detection and
        health's per-source last-fire read, and it is the whole point of the
        collapse -- an unchanged tick still records that the daemon is alive
        and checking, it just writes no observation.

        observation_id distinguishes the two cases:
          * not None -> this fire was a CHANGE (or first sighting): an
            observation was just written. last_state advances to its signature,
            last_changed_at moves to now (the last REAL transition), and
            last_observation_id points at it.
          * None -> a collapsed (unchanged) tick: only the heartbeat columns
            move; last_state/last_changed_at/last_observation_id are left
            untouched so they keep describing the last real transition.

        signature is None only on the fail-safe path (signature computation
        raised, so the fire was forced to write). It is stored as NULL, which
        reads back as "unknown baseline" and makes the next fire compare unequal
        -- i.e. the next tick also writes. Erring toward an extra write is the
        correct direction: missing a transition is the one unacceptable failure.
        Self-committing, clock-stamped, mirroring the catalog's write idioms.
        """
        now = self._clock.now_iso()
        if observation_id is None:
            # Collapsed tick: bump the heartbeat only. The INSERT arm runs only
            # if no row exists yet, which cannot happen on a collapse (a first
            # sighting always writes an observation, taking the branch below) --
            # it is here so the upsert is total, leaving last_state NULL.
            self.connection.execute(
                """
                INSERT INTO watch_state
                    (source_ref, last_checked_at, last_outcome, updated_at)
                VALUES (?, ?, ?, ?)
                ON CONFLICT(source_ref) DO UPDATE SET
                    last_checked_at = excluded.last_checked_at,
                    last_outcome = excluded.last_outcome,
                    updated_at = excluded.updated_at
                """,
                (source_ref, now, outcome, now),
            )
        else:
            self.connection.execute(
                """
                INSERT INTO watch_state
                    (source_ref, last_checked_at, last_state, last_outcome,
                     last_changed_at, last_observation_id, updated_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source_ref) DO UPDATE SET
                    last_checked_at = excluded.last_checked_at,
                    last_state = excluded.last_state,
                    last_outcome = excluded.last_outcome,
                    last_changed_at = excluded.last_changed_at,
                    last_observation_id = excluded.last_observation_id,
                    updated_at = excluded.updated_at
                """,
                (source_ref, now, signature, outcome, now, observation_id, now),
            )
        self.connection.commit()

    def write_observation(
        self, source_ref: str, payload: dict[str, Any], provenance: dict[str, Any]
    ) -> int:
        now = self._clock.now_iso()
        cursor = self.connection.execute(
            """
            INSERT INTO observations
                (source, status, provenance, written_at, created_at)
            VALUES (?, 'writing', ?, ?, ?)
            """,
            # created_at stamped from the clock, not the wall-clock DEFAULT:
            # timeline_events windows observations.created_at against its
            # [since, until] bounds, and this row already clock-stamps
            # written_at/updated_at -- leaving created_at on the wall clock
            # would make a single row span two clocks under a FakeClock sim.
            (source_ref, json.dumps(provenance, sort_keys=True), now, now),
        )
        observation_id = int(cursor.lastrowid)
        self.connection.commit()
        body_ref = self._write_body("observations", observation_id, payload)
        self.connection.execute(
            """
            UPDATE observations
            SET status = 'ready', body_ref = ?, updated_at = ?
            WHERE id = ?
            """,
            (body_ref, self._clock.now_iso(), observation_id),
        )
        self.connection.commit()
        return observation_id

    def ready_observations_for(self, triager_name: str, source_ref: str) -> list[sqlite3.Row]:
        """Ready observations this triager can pick up: no triage row yet, or
        a 'failed' row whose scheduled retry is due. A 'failed' row with NULL
        next_attempt_at is exhausted (mark_triage_failed clears the schedule
        on its terminal branch and nothing else writes failed+NULL), so it is
        terminal for this triager and excluded -- the observation no longer
        flips to triage_failed the moment ONE triager exhausts, so this
        per-triager exclusion is what stops an exhausted triager from
        re-picking its own dead work while siblings finish theirs."""
        now = self._clock.now_iso()
        return list(
            self.connection.execute(
                """
                SELECT o.*
                FROM observations o
                LEFT JOIN observation_triage ot
                  ON ot.observation_id = o.id AND ot.triager_name = ?
                WHERE o.status = 'ready'
                  AND o.source = ?
                  AND (
                    ot.observation_id IS NULL
                    OR (
                        ot.status = 'failed'
                        AND ot.next_attempt_at IS NOT NULL
                        AND ot.next_attempt_at <= ?
                    )
                  )
                ORDER BY o.id
                LIMIT 20
                """,
                (triager_name, source_ref, now),
            )
        )

    def read_body(self, body_ref: str | None) -> dict[str, Any]:
        if not body_ref:
            return {}
        data = json.loads((self.root / body_ref).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"body is not a JSON object: {body_ref}")
        return data

    def mark_triage_processing(self, observation_id: int, triager_name: str) -> None:
        self.connection.execute(
            """
            INSERT INTO observation_triage (observation_id, triager_name, status)
            VALUES (?, ?, 'processing')
            ON CONFLICT (observation_id, triager_name) DO UPDATE SET
                status = 'processing',
                next_attempt_at = NULL,
                updated_at = excluded.updated_at
            """,
            (observation_id, triager_name),
        )
        self.connection.commit()

    def clear_triage_processing(
        self, observation_id: int, triager_name: str
    ) -> None:
        """Delete a 'processing' observation_triage row when its triager
        no longer needs the row to exist. Reached on two intents:

        - Hot-remove: a triager disappears from lodging after
          mark_triage_processing has written the row but before the
          triage runs. The observation must remain eligible for a later
          re-added triager; deleting the row lets ready_observations_for
          surface it again.
        - Shutdown-cancel: _triage_loop cancels its in-flight tasks on
          shutdown; the cancelled task never reaches mark_triage_success
          or mark_triage_failed and would otherwise leave a stuck
          'processing' row that recover_writing_rows does not heal.

        Bounded to status='processing' so a concurrent transition to
        'success'/'failed' that legitimately occurred BEFORE the
        cancellation arrived is not clobbered. Caller function names
        are deliberately not enumerated here -- they rot the moment
        new callers adopt the helper, and the intents above survive
        renames."""
        self.connection.execute(
            """
            DELETE FROM observation_triage
            WHERE observation_id = ? AND triager_name = ? AND status = 'processing'
            """,
            (observation_id, triager_name),
        )
        self.connection.commit()

    def mark_triage_success(self, observation_id: int, triager_name: str) -> None:
        self.connection.execute(
            """
            UPDATE observation_triage
            SET status = 'success',
                next_attempt_at = NULL,
                updated_at = ?
            WHERE observation_id = ? AND triager_name = ?
            """,
            (self._clock.now_iso(), observation_id, triager_name),
        )
        self.connection.commit()

    def mark_triage_failed(
        self, observation_id: int, triager_name: str, error: str
    ) -> bool:
        """Record one triage failure; returns True when this triager's
        retries are exhausted. Exhaustion is terminal for THIS triager only:
        the row keeps status='failed' with next_attempt_at NULL (the
        exhausted marker ready_observations_for excludes). The observation
        row is NOT flipped here -- flipping on the first exhausted triager
        used to drop the observation out of `ready` for every OTHER triager
        on the same source; the whole-row transition now happens in
        consume_observation_if_terminal once ALL expected triagers are
        terminal."""
        row = self.connection.execute(
            """
            SELECT attempt FROM observation_triage
            WHERE observation_id = ? AND triager_name = ?
            """,
            (observation_id, triager_name),
        ).fetchone()
        attempt = int(row["attempt"]) if row is not None else 1
        if attempt >= MAX_RETRY_ATTEMPTS:
            self.connection.execute(
                """
                UPDATE observation_triage
                SET status = 'failed',
                    last_error = ?,
                    next_attempt_at = NULL,
                    updated_at = ?
                WHERE observation_id = ? AND triager_name = ?
                """,
                (error, self._clock.now_iso(), observation_id, triager_name),
            )
            self.connection.commit()
            return True

        next_attempt = attempt + 1
        next_attempt_at = _format_time(
            self._clock.now() + TRUST_RETRY_DELAYS[attempt - 1]
        )
        self.connection.execute(
            """
            UPDATE observation_triage
            SET status = 'failed',
                attempt = ?,
                next_attempt_at = ?,
                last_error = ?,
                updated_at = ?
            WHERE observation_id = ? AND triager_name = ?
            """,
            (next_attempt, next_attempt_at, error, self._clock.now_iso(), observation_id, triager_name),
        )
        self.connection.commit()
        return False

    def consume_observation_if_terminal(
        self, observation_id: int, expected_triagers: set[str]
    ) -> str | None:
        """Flip a 'ready' observation to its terminal status once EVERY
        expected triager has a terminal triage row. Returns the new status
        ('consumed', or 'triage_failed' when at least one triager exhausted
        its retries) or None when the observation is not yet settled.

        Terminal per triager: 'success', or 'failed' with NULL
        next_attempt_at (the exhausted marker mark_triage_failed leaves; a
        retrying failure always carries a schedule). A missing row, a
        'processing' row, or a scheduled retry all mean that triager still
        owns work here, so nothing flips -- never lose an un-triaged or
        still-retrying observation.

        `expected_triagers` is the LODGED set matching this observation's
        source; lodging lives in the daemon (self.lodging.triagers), not the
        catalog, so the caller passes it in. Rows from triagers no longer in
        that set (hot-removed) neither block nor satisfy the transition --
        the lodged set is the authority on who must finish. An empty
        expected set never flips here: a source with no live triager is the
        grace-period sweep's job (consume_observations_without_triager), so
        a momentary lodging gap cannot consume fresh work instantly.

        The UPDATE is guarded on status='ready' so a repeat call, a
        concurrent flip, or a non-ready row ('writing', 'failed', already
        terminal) is a no-op returning None.
        """
        if not expected_triagers:
            return None
        placeholders = ", ".join("?" for _ in expected_triagers)
        names = sorted(expected_triagers)
        rows = self.connection.execute(
            f"""
            SELECT triager_name, status, next_attempt_at
            FROM observation_triage
            WHERE observation_id = ? AND triager_name IN ({placeholders})
            """,
            (observation_id, *names),
        ).fetchall()
        terminal: dict[str, str] = {}
        for row in rows:
            if row["status"] == "success":
                terminal[row["triager_name"]] = "success"
            elif row["status"] == "failed" and row["next_attempt_at"] is None:
                terminal[row["triager_name"]] = "exhausted"
        if set(terminal) != expected_triagers:
            return None
        new_status = (
            "triage_failed"
            if any(state == "exhausted" for state in terminal.values())
            else "consumed"
        )
        cursor = self.connection.execute(
            """
            UPDATE observations
            SET status = ?, updated_at = ?
            WHERE id = ? AND status = 'ready'
            """,
            (new_status, self._clock.now_iso(), observation_id),
        )
        self.connection.commit()
        return new_status if cursor.rowcount else None

    def consume_observations_without_triager(
        self, sources_with_triagers: set[str], grace_seconds: int
    ) -> int:
        """Flip 'ready' observations whose source has no live triager to
        'consumed' once they are older than the grace period. Returns the
        count flipped.

        This is the real pile-up case: a source with zero triagers never
        gets a triage row, so its observations would sit in `ready` forever.
        The grace (age measured against created_at on the injected clock)
        keeps a newly-lodged triager able to claim recent observations; past
        it, the row is consumed and reachable again only via
        reprocess_source. `sources_with_triagers` is the lodged truth the
        daemon passes in; observations from those sources are never touched
        here regardless of age -- their exit is consume_observation_if_terminal.
        """
        cutoff = _format_time(
            self._clock.now() - timedelta(seconds=grace_seconds)
        )
        sources = sorted(sources_with_triagers)
        placeholders = ", ".join("?" for _ in sources)
        not_in = f"AND source NOT IN ({placeholders})" if sources else ""
        cursor = self.connection.execute(
            f"""
            UPDATE observations
            SET status = 'consumed', updated_at = ?
            WHERE status = 'ready'
              AND created_at <= ?
              {not_in}
            """,
            (self._clock.now_iso(), cutoff, *sources),
        )
        self.connection.commit()
        return cursor.rowcount

    def ready_observations_for_sources(
        self, source_refs: set[str]
    ) -> list[sqlite3.Row]:
        """All 'ready' observations (id + source) for `source_refs`, oldest
        first. Feeds the daemon's shrunk-lodging reconciliation: when a
        source's lodged triager set may have shrunk (triager hot-removed, or
        a restart with a smaller lodging), each of these rows is re-run
        through consume_observation_if_terminal -- a triager that was the
        only non-terminal blocker can vanish from lodging, and without this
        re-evaluation nothing ever settles the observation (the terminal
        siblings never re-pick it, and consume_observations_without_triager
        skips sources that still have a lodged triager). Distinct from
        ready_observations_for, which answers what ONE triager can pick up;
        this answers what is still unsettled for whole sources."""
        if not source_refs:
            return []
        sources = sorted(source_refs)
        placeholders = ", ".join("?" for _ in sources)
        return list(
            self.connection.execute(
                f"""
                SELECT id, source FROM observations
                WHERE status = 'ready' AND source IN ({placeholders})
                ORDER BY id
                """,
                sources,
            )
        )

    def prior_state(self, triager_name: str, source_ref: str) -> dict[str, Any]:
        row = self.connection.execute(
            """
            SELECT state_blob FROM triager_state
            WHERE triager_name = ? AND source_name = ?
            """,
            (triager_name, source_ref),
        ).fetchone()
        if row is None:
            return {}
        data = json.loads(row["state_blob"])
        return data if isinstance(data, dict) else {}

    def update_triager_state(
        self, triager_name: str, source_ref: str, state: dict[str, Any]
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO triager_state (triager_name, source_name, state_blob, updated_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (triager_name, source_name) DO UPDATE SET
                state_blob = excluded.state_blob,
                updated_at = excluded.updated_at
            """,
            (triager_name, source_ref, json.dumps(state, sort_keys=True), self._clock.now_iso()),
        )
        self.connection.commit()

    def write_finding(
        self,
        observation_id: int | None,
        finding: dict[str, Any],
        known_pipes: set[str],
    ) -> int:
        """Write a finding, gated on incident transitions (B30).

        A finding registers (a row + pipe enqueue) only when it moves an
        incident across an edge:

        - A non-clearance finding emits only when it OPENS a NEW incident for
          its key (source, type, entity). A repeat while that incident is
          already open is dropped ENTIRELY -- no row, no body, no pipe
          enqueue, no occurrence count. Duration is recoverable from
          incidents.opened_at; a per-poll occurrence counter would itself be
          the write-per-poll amplification this gate exists to kill. The
          return is the existing open incident's latest_finding_id so callers
          that log/track a finding_id still reference the real opening row.
        - A clearance finding emits only when it CLOSES an open incident for
          (source, entity). A clearance with nothing open is a no-op and is
          dropped; it returns _NO_FINDING (0).

        The pre-check is a plain SELECT before any write, so the dropped
        (common, under-a-flood) case touches neither the findings table nor
        the body store. This whole class is synchronous and self-committing,
        so the check-then-write below cannot interleave with another finding.
        """
        source = str(finding["source"])
        finding_type = str(finding["type"])
        entity = str(finding["entity"])
        dedup_key = str(finding.get("dedup_key") or f"{source}:{finding_type}:{entity}")
        target_pipes = list(finding.get("target_pipes") or [])
        body = finding.get("body")
        body_obj = body if isinstance(body, dict) else {"text": body} if body else {}

        # --- EMISSION / RECOVERY GATE (B30) ---------------------------------
        if finding_type == "clearance":
            # Recovery edge. _close_incident closes by (source, entity)
            # regardless of type, so the gate mirrors that key: a clearance
            # fires only when some open incident exists for the entity.
            if not self._has_open_incident_for_entity(source, entity):
                return _NO_FINDING
        else:
            existing_open = self._open_incident_for_key(source, finding_type, entity)
            if existing_open is not None:
                # Repeat while the incident is open: drop entirely. Hand back
                # the opening finding's id so the contract (an int finding_id)
                # holds for callers that log it.
                latest = existing_open["latest_finding_id"]
                return int(latest) if latest is not None else _NO_FINDING

        now = self._clock.now_iso()
        cursor = self.connection.execute(
            """
            INSERT INTO findings (
                observation_id, source, type, entity, dedup_key, target_pipes,
                status, severity, occurred_at, created_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'writing', ?, ?, ?)
            """,
            (
                observation_id,
                source,
                finding_type,
                entity,
                dedup_key,
                json.dumps(target_pipes),
                finding.get("severity"),
                finding.get("timestamp") or now,
                # created_at is stamped from the injected clock, NOT left to the
                # schema's wall-clock DEFAULT. The digest's since-last-drain
                # windows (findings_for_pipe_since / clearance_findings_since)
                # compare f.created_at against last_drain_at, which is
                # clock-pinned (mark_pipe_drained writes self._clock.now_iso()).
                # A wall-clock created_at vs a clock-pinned last_drain_at is a
                # mixed-clock comparison: consistent in production (one real
                # clock) but silently wrong under a FakeClock sim, where it would
                # drop genuinely-new findings from the window. now_iso() is
                # byte-identical in shape to strftime('%Y-%m-%dT%H:%M:%fZ','now')
                # (millisecond ISO8601, trailing Z), so the catalog's
                # text-comparison ordering and any pre-existing rows stay
                # comparable; in production now_iso() equals the wall-clock
                # default at insert, so this is behaviour-preserving there.
                now,
            ),
        )
        finding_id = int(cursor.lastrowid)
        self.connection.commit()
        body_ref = self._write_body("findings", finding_id, body_obj)
        self.connection.execute(
            """
            UPDATE findings
            SET status = 'ready', body_ref = ?, updated_at = ?
            WHERE id = ?
            """,
            (body_ref, self._clock.now_iso(), finding_id),
        )
        if finding_type == "clearance":
            self._close_incident(source, entity, finding_id)
        else:
            opened_new = self._upsert_incident(
                source, finding_type, entity, dedup_key, finding_id
            )
            # The pre-check above already established no open incident exists
            # for this key, so the upsert must have opened a fresh one. If it
            # ever reported a refresh instead, the gate and the incident table
            # have drifted apart -- surface it rather than silently flood.
            if not opened_new:
                LOGGER.error(
                    "write_finding gate drift: %s/%s/%s passed the open-incident "
                    "pre-check but _upsert_incident refreshed an existing open "
                    "incident",
                    source,
                    finding_type,
                    entity,
                )
        for pipe in target_pipes:
            if pipe in known_pipes:
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO pipe_queues (
                        finding_id, pipe, status, created_at, updated_at
                    )
                    VALUES (?, ?, 'pending', ?, ?)
                    """,
                    # created_at clock-pinned for the same reason as the findings
                    # row above: suppressed_findings_since windows pq.created_at
                    # against last_drain_at (clock-pinned), and pending_pipe_items
                    # / dead_letter_items order by pq.created_at -- a wall-clock
                    # default would mix clocks under a sim. updated_at is pinned
                    # too so a never-dispatched pending row stays internally
                    # coherent with created_at under a FakeClock.
                    (finding_id, pipe, now, now),
                )
        self.connection.commit()
        return finding_id

    def pending_pipe_items(self, pipe: str, limit: int | None = 20) -> list[sqlite3.Row]:
        now = self._clock.now_iso()
        limit_sql = "" if limit is None else "LIMIT ?"
        params: tuple[Any, ...] = (pipe, now) if limit is None else (pipe, now, limit)
        return list(
            self.connection.execute(
                f"""
                SELECT pq.finding_id, pq.pipe, f.*
                FROM pipe_queues pq
                JOIN findings f ON f.id = pq.finding_id
                WHERE pq.pipe = ? AND pq.status = 'pending' AND f.status = 'ready'
                  AND (pq.next_attempt_at IS NULL OR pq.next_attempt_at <= ?)
                ORDER BY pq.created_at, pq.finding_id
                {limit_sql}
                """,
                params,
            )
        )

    def record_dispatch(
        self,
        pipe: str,
        channel: str,
        finding_ids: list[int],
        status: str,
        error: str | None = None,
        source: str | None = None,
        mark_queue: bool = True,
    ) -> None:
        # dispatched_at is the clock-pinned timestamp every windowed read uses
        # (sent_dispatch_count_for_*, failed_dispatch_count,
        # last_successful_dispatch_per_pipe, timeline_events all key off it and
        # it is always set here). dispatches.created_at is therefore left on the
        # schema's wall-clock DEFAULT: timeline_events reads
        # COALESCE(dispatched_at, created_at), and since dispatched_at is never
        # NULL on a row this method writes, the created_at fallback is
        # unreachable -- it is never compared against a clock-pinned value.
        self.connection.execute(
            """
            INSERT INTO dispatches (
                pipe, channel, finding_ids, status, attempts, last_error, dispatched_at, source
            )
            VALUES (?, ?, ?, ?, 1, ?, ?, ?)
            """,
            (pipe, channel, json.dumps(finding_ids), status, error, self._clock.now_iso(), source),
        )
        if status == "sent" and mark_queue:
            for finding_id in finding_ids:
                self.connection.execute(
                    """
                    UPDATE pipe_queues
                    SET status = 'dispatched', dispatched_at = ?, updated_at = ?
                    WHERE finding_id = ? AND pipe = ?
                    """,
                    (self._clock.now_iso(), self._clock.now_iso(), finding_id, pipe),
                )
        self.connection.commit()

    def mark_pipe_items_dispatched(self, pipe: str, finding_ids: list[int]) -> None:
        now = self._clock.now_iso()
        for finding_id in finding_ids:
            self.connection.execute(
                """
                UPDATE pipe_queues
                SET status = 'dispatched', dispatched_at = ?, updated_at = ?
                WHERE finding_id = ? AND pipe = ?
                """,
                (now, now, finding_id, pipe),
            )
        self.connection.commit()

    def sent_dispatch_count_for_channel(self, channel: str, since: str) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM dispatches
            WHERE channel = ? AND dispatched_at > ? AND status = 'sent'
            """,
            (channel, since),
        ).fetchone()
        return int(row["n"])

    def sent_dispatch_count_for_source(self, source: str, since: str) -> int:
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM dispatches
            WHERE source = ? AND dispatched_at > ? AND status = 'sent'
            """,
            (source, since),
        ).fetchone()
        return int(row["n"])

    def suppress_pipe_item_to(
        self, finding_id: int, source_pipe: str, target_pipe: str
    ) -> None:
        now = self._clock.now_iso()
        self.connection.execute(
            """
            UPDATE pipe_queues
            SET status = 'suppressed', updated_at = ?
            WHERE finding_id = ? AND pipe = ?
            """,
            (now, finding_id, source_pipe),
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO pipe_queues (
                finding_id, pipe, status, created_at, updated_at
            )
            VALUES (?, ?, 'pending', ?, ?)
            """,
            # Clock-pin created_at like write_finding's enqueue so every
            # pipe_queues row shares one clock -- pending_pipe_items orders by
            # pq.created_at, so a wall-clock default here would sort wrongly
            # against the clock-pinned rows under a sim.
            (finding_id, target_pipe, now, now),
        )
        self.connection.commit()

    def last_pipe_drain_at(self, pipe_name: str) -> str | None:
        row = self.connection.execute(
            "SELECT last_drain_at FROM pipe_state WHERE pipe_name = ?",
            (pipe_name,),
        ).fetchone()
        return None if row is None else row["last_drain_at"]

    def mark_pipe_drained(self, pipe_name: str, drained_at: str | None = None) -> str:
        drained_at = drained_at or self._clock.now_iso()
        self.connection.execute(
            """
            INSERT INTO pipe_state (pipe_name, last_drain_at)
            VALUES (?, ?)
            ON CONFLICT (pipe_name) DO UPDATE SET
                last_drain_at = excluded.last_drain_at
            """,
            (pipe_name, drained_at),
        )
        self.connection.commit()
        return drained_at

    def sync_pipe_sla(self, slas: dict[str, int]) -> None:
        """Reconcile the pipe_sla table to the pipes that currently declare a
        delivery SLA (B2). `slas` maps pipe_name -> max_interval_seconds.

        Belfry reads this table read-only to assert each pipe delivers on
        cadence -- belfry cannot parse YAML, so the daemon (the single sqlite
        writer) persists the contract here. Called at daemon startup from the
        loaded lodging.

        tracking_since is written ONCE per pipe and preserved by the upsert
        (ON CONFLICT updates only max_interval_seconds), so it is never moved on
        a later sync and stays the baseline for a never-delivered pipe across
        daemon restarts (a stall spanning restarts is still caught).
        max_interval_seconds is updated so a changed `max_interval` takes
        effect. Pipes no longer declaring an SLA are removed so a stale row
        can't keep belfry red after a pipe is reclassified or deleted.
        """
        now = self._clock.now_iso()
        for pipe_name, seconds in slas.items():
            self.connection.execute(
                """
                INSERT INTO pipe_sla (pipe_name, max_interval_seconds, tracking_since)
                VALUES (?, ?, ?)
                ON CONFLICT (pipe_name) DO UPDATE SET
                    max_interval_seconds = excluded.max_interval_seconds
                """,
                (pipe_name, seconds, now),
            )
        if slas:
            placeholders = ",".join("?" for _ in slas)
            self.connection.execute(
                f"DELETE FROM pipe_sla WHERE pipe_name NOT IN ({placeholders})",
                tuple(slas),
            )
        else:
            self.connection.execute("DELETE FROM pipe_sla")
        self.connection.commit()

    def sync_source_sla(self, slas: dict[str, int]) -> None:
        """Reconcile the source_sla table to the sources that currently declare
        a check SLA (0014). `slas` maps source_ref -> max_interval_seconds.

        The input-side mirror of sync_pipe_sla: belfry reads this table
        read-only to assert each source is still being CHECKED on cadence --
        belfry cannot parse the sources/*.yaml, so the daemon (the single sqlite
        writer) persists the contract here. Called at daemon startup and on hot
        reload from the loaded lodging.

        tracking_since is written ONCE per source and preserved by the upsert
        (ON CONFLICT updates only max_interval_seconds), so it is never moved on
        a later sync and stays the baseline for a never-checked source across
        daemon restarts (a stall spanning restarts is still caught).
        max_interval_seconds is updated so a changed cadence takes effect.
        Sources no longer tracked (removed, or a crontab source whose max-gap
        could not be bounded and so fell back to the global wedge backstop) are
        deleted so a stale row can't keep belfry red after the source is gone.
        """
        now = self._clock.now_iso()
        for source_ref, seconds in slas.items():
            self.connection.execute(
                """
                INSERT INTO source_sla
                    (source_ref, max_interval_seconds, tracking_since)
                VALUES (?, ?, ?)
                ON CONFLICT (source_ref) DO UPDATE SET
                    max_interval_seconds = excluded.max_interval_seconds
                """,
                (source_ref, seconds, now),
            )
        if slas:
            placeholders = ",".join("?" for _ in slas)
            self.connection.execute(
                f"DELETE FROM source_sla WHERE source_ref NOT IN ({placeholders})",
                tuple(slas),
            )
        else:
            self.connection.execute("DELETE FROM source_sla")
        self.connection.commit()

    def suppressed_findings_since(self, since: str | None) -> list[dict[str, Any]]:
        clause = "AND pq.created_at > ?" if since else ""
        params: tuple[Any, ...] = (since,) if since else ()
        rows = self.connection.execute(
            f"""
            SELECT f.*, pq.created_at AS queued_at
            FROM pipe_queues pq
            JOIN findings f ON f.id = pq.finding_id
            WHERE pq.status = 'suppressed' {clause}
            ORDER BY pq.created_at, f.id
            """,
            params,
        )
        return [self._finding_dict(row) for row in rows]

    def findings_for_pipe_since(
        self,
        pipe: str,
        since: str | None,
        exclude_types: tuple[str, ...] = (),
    ) -> list[dict[str, Any]]:
        clauses: list[str] = []
        params: list[Any] = [pipe]
        if since:
            clauses.append("AND f.created_at > ?")
            params.append(since)
        if exclude_types:
            placeholders = ",".join("?" for _ in exclude_types)
            clauses.append(f"AND f.type NOT IN ({placeholders})")
            params.extend(exclude_types)
        extra = " ".join(clauses)
        rows = self.connection.execute(
            f"""
            SELECT f.*, pq.created_at AS queued_at
            FROM pipe_queues pq
            JOIN findings f ON f.id = pq.finding_id
            WHERE pq.pipe = ? AND f.status = 'ready' {extra}
              AND NOT EXISTS (
                SELECT 1
                FROM pipe_queues suppressed
                WHERE suppressed.finding_id = f.id
                  AND suppressed.status = 'suppressed'
              )
            ORDER BY f.created_at, f.id
            """,
            tuple(params),
        )
        return [self._finding_dict(row) for row in rows]

    def open_incidents(self) -> list[dict[str, Any]]:
        rows = self.connection.execute(
            """
            SELECT i.*, f.severity
            FROM incidents i
            LEFT JOIN findings f ON f.id = i.latest_finding_id
            WHERE i.status = 'open'
            ORDER BY i.opened_at, i.id
            """
        )
        return [dict(row) for row in rows]

    def latest_source_fires(self) -> dict[str, str | None]:
        """Most recent check time per source, from the watch_state row's
        last_checked_at. Read-only; used by the health control op's per-source
        last-fire line. (watch_state holds one overwritten row per source, so
        no GROUP BY is needed -- last_checked_at IS the latest check.) The
        method name is kept for its callers; the underlying ledger
        (source_fires) was collapsed into watch_state."""
        rows = self.connection.execute(
            """
            SELECT source_ref, last_checked_at
            FROM watch_state
            """
        )
        return {row["source_ref"]: row["last_checked_at"] for row in rows}

    def observations_pending_triage_count(self) -> int:
        """Ready observations with no successful triage yet. Read-only.

        'Pending' counts a ready observation until some triager has a
        'success' row for it. Observations still retrying ('failed' with a
        future next_attempt_at) or with no matching triager remain counted --
        from an operator's view they are still waiting on triage. Terminal
        observations ('consumed', 'triage_failed') leave the count through
        the status='ready' filter the moment they settle, so this is
        O(genuinely-pending), not O(everything ever written).
        """
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM observations o
            WHERE o.status = 'ready'
              AND NOT EXISTS (
                SELECT 1 FROM observation_triage ot
                WHERE ot.observation_id = o.id AND ot.status = 'success'
              )
            """
        ).fetchone()
        return int(row["n"])

    def findings_pending_dispatch_by_pipe(self) -> dict[str, int]:
        """Pending pipe_queues rows grouped by pipe. Read-only."""
        rows = self.connection.execute(
            """
            SELECT pipe, COUNT(*) AS n
            FROM pipe_queues
            WHERE status = 'pending'
            GROUP BY pipe
            """
        )
        return {row["pipe"]: int(row["n"]) for row in rows}

    def last_successful_dispatch_per_pipe(self) -> dict[str, str]:
        """Most recent SUCCESSFUL (status='sent') dispatch timestamp per pipe.

        The delivery half of the health surface (B5): answers "is each pipe
        actually getting content out", not just "is the daemon running". A
        'muted' or 'failed' dispatch is not a delivery, so only 'sent' counts.
        Pipes with no successful send are simply absent from the map -- the
        caller (which knows the configured pipe set) renders those as 'never'.
        Read-only. Reused by B2's delivery-SLA check.
        """
        rows = self.connection.execute(
            """
            SELECT pipe, max(dispatched_at) AS last_at
            FROM dispatches
            WHERE status = 'sent' AND dispatched_at IS NOT NULL
            GROUP BY pipe
            """
        )
        return {row["pipe"]: row["last_at"] for row in rows}

    def failed_dispatch_count(self, window_hours: int = 24) -> int:
        """Count of failed dispatches in the last `window_hours` hours.

        A recent-window failure count for the health surface -- a nonzero
        value says "delivery is actively breaking now", distinct from a single
        open incident. Read-only. The window is measured off the injected
        clock so a test/sim observes it deterministically.
        """
        cutoff = _format_time(self._clock.now() - timedelta(hours=window_hours))
        # Inclusive `>=`, matching recently_closed_incidents (the other
        # windowed-from-now read); the rate-limit since-queries use exclusive
        # `>` against a caller-supplied instant. The boundary is a sub-ms edge
        # either way and not load-bearing.
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM dispatches
            WHERE status = 'failed' AND dispatched_at IS NOT NULL
              AND dispatched_at >= ?
            """,
            (cutoff,),
        ).fetchone()
        return int(row["n"])

    def open_internal_incident_count(self) -> int:
        """Count of open incidents whose source is one of angelus's own
        internal/* failure reports. The system's self-reported-failure tally
        for the health surface; mirrors belfry's open-internal read. Read-only.
        """
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM incidents
            WHERE status = 'open' AND source LIKE 'internal/%'
            """
        ).fetchone()
        return int(row["n"])

    def dead_letter_count(self) -> int:
        """Count of pipe_queues rows in the terminal 'dead_letter' state (B15).

        The system's "how much content have we permanently given up delivering"
        tally for the health surface. Distinct from failed_dispatch_count, which
        counts transient per-CHANNEL dispatches.status='failed' rows in a recent
        window: a dead_letter row is a per-FINDING give-up that persists until
        the finding is replayed and redelivered. Read-only.
        """
        row = self.connection.execute(
            "SELECT COUNT(*) AS n FROM pipe_queues WHERE status = 'dead_letter'"
        ).fetchone()
        return int(row["n"])

    def dead_letter_items(self, limit: int | None = None) -> list[dict[str, Any]]:
        """Findings whose pipe_queues row exhausted its redelivery ladder and
        landed in the terminal 'dead_letter' state (B15). Read-only.

        Each row carries enough to be ACTIONABLE without a second lookup: which
        finding (finding_id) and what it is (source/type/entity/severity), which
        pipe abandoned it, the last delivery error, how many attempts were burned,
        and WHEN it dead-lettered. dead_lettered_at is pq.updated_at -- the
        exhaustion edge in record_pipe_finding_undelivered sets
        status='dead_letter' and stamps updated_at in the same UPDATE, and no
        other transition writes a dead_letter row, so updated_at on a dead_letter
        row is exactly the moment it was abandoned.

        Ordered oldest-abandoned-first (by the queue row's created_at, then
        finding_id) so the longest-stuck item -- the one most overdue for a
        replay -- reads first. `limit` caps the rows for a bounded health render;
        None returns all (the count, surfaced separately, conveys the true total).
        """
        limit_sql = "" if limit is None else "LIMIT ?"
        params: tuple[Any, ...] = () if limit is None else (limit,)
        rows = self.connection.execute(
            f"""
            SELECT
                pq.finding_id AS finding_id,
                pq.pipe AS pipe,
                pq.last_error AS last_error,
                pq.attempts AS attempts,
                pq.updated_at AS dead_lettered_at,
                f.source AS source,
                f.type AS type,
                f.entity AS entity,
                f.severity AS severity
            FROM pipe_queues pq
            JOIN findings f ON f.id = pq.finding_id
            WHERE pq.status = 'dead_letter'
            ORDER BY pq.created_at, pq.finding_id
            {limit_sql}
            """,
            params,
        )
        return [dict(row) for row in rows]

    def recently_closed_incidents(self, days: int = 7) -> list[dict[str, Any]]:
        """Incidents closed within the last `days` days (default 7).
        Read-only; a plain SELECT, no write."""
        cutoff = _format_time(self._clock.now() - timedelta(days=days))
        rows = self.connection.execute(
            """
            SELECT i.*, f.severity
            FROM incidents i
            LEFT JOIN findings f ON f.id = i.latest_finding_id
            WHERE i.status = 'closed'
              AND i.closed_at IS NOT NULL
              AND i.closed_at >= ?
            ORDER BY i.closed_at DESC, i.id DESC
            """,
            (cutoff,),
        )
        return [dict(row) for row in rows]

    def clearance_findings_since(self, since: str | None) -> list[dict[str, Any]]:
        clause = "AND f.created_at > ?" if since else ""
        params: tuple[Any, ...] = (since,) if since else ()
        rows = self.connection.execute(
            f"""
            SELECT f.*
            FROM findings f
            WHERE f.type = 'clearance' AND f.status = 'ready' {clause}
            ORDER BY f.created_at, f.id
            """,
            params,
        )
        return [self._finding_dict(row) for row in rows]

    def timeline_events(self, since: str, until: str) -> list[dict[str, Any]]:
        """Reconstruct the ordered story for a [since, until] window.

        Interleaves observations, findings, and dispatches (including failures)
        by timestamp into a single chronological list. Read-only: three plain
        SELECTs unioned in Python so each event keeps its own shape. Bounds are
        inclusive and compared as ISO strings, which sort correctly because
        every timestamp column uses the same '...Z' millisecond format.

        There is no per-fire event any more: observation collapse means a fire
        writes an observation only on a state CHANGE, so the observations a
        source produces ARE its transitions -- the per-tick "we fired" noise
        (formerly read from source_fires) is exactly what collapse removed, and
        surfacing it here would re-introduce it. The "we are still checking"
        heartbeat now lives in watch_state.last_checked_at, which health and
        belfry read; it is not a story event.

        Same-instant ties break by kind (observation, finding, dispatch) then
        by id. This ordering is deterministic and stable, NOT causal: on an
        exact millisecond collision the originating event and the event it
        provoked cannot be told apart from the stored data alone. A failing
        dispatch and the channel_unhealthy finding it provokes can share a
        millisecond, and the static kind order would then render the finding
        above its dispatch -- the reverse of what happened. Sub-millisecond
        ordering therefore may not reflect causal order; only the millisecond
        timestamps disambiguate when they differ.
        """
        events: list[dict[str, Any]] = []
        for row in self.connection.execute(
            """
            SELECT id, source, status, created_at
            FROM observations
            WHERE created_at >= ? AND created_at <= ?
            """,
            (since, until),
        ):
            events.append(
                {
                    "ts": row["created_at"],
                    "kind": "observation",
                    "id": int(row["id"]),
                    "source": row["source"],
                    "status": row["status"],
                }
            )
        for row in self.connection.execute(
            """
            SELECT id, source, type, entity, severity, status, created_at
            FROM findings
            WHERE created_at >= ? AND created_at <= ?
            """,
            (since, until),
        ):
            events.append(
                {
                    "ts": row["created_at"],
                    "kind": "finding",
                    "id": int(row["id"]),
                    "source": row["source"],
                    "type": row["type"],
                    "entity": row["entity"],
                    "severity": row["severity"],
                    "status": row["status"],
                }
            )
        for row in self.connection.execute(
            """
            SELECT id, pipe, channel, status, last_error,
                   COALESCE(dispatched_at, created_at) AS ts
            FROM dispatches
            WHERE COALESCE(dispatched_at, created_at) >= ?
              AND COALESCE(dispatched_at, created_at) <= ?
            """,
            (since, until),
        ):
            events.append(
                {
                    "ts": row["ts"],
                    "kind": "dispatch",
                    "id": int(row["id"]),
                    "pipe": row["pipe"],
                    "channel": row["channel"],
                    "status": row["status"],
                    "error": row["last_error"],
                }
            )
        # Deterministic, stable tie-break -- not causal (see docstring).
        kind_order = {"observation": 0, "finding": 1, "dispatch": 2}
        events.sort(key=lambda e: (e["ts"], kind_order[e["kind"]], e["id"]))
        return events

    def record_pipe_finding_undelivered(
        self, pipe: str, finding_id: int, error: str, max_attempts: int | None = None
    ) -> bool:
        """Advance the per-finding redelivery ladder for the immediate path.

        This is rung 1 of the B14 escalation ladder (retry with backoff) AND the
        gate to rung 3: the True return -- a finding that has crossed the
        threshold to status='dead_letter' WITHOUT ever being delivered over any
        transport -- is the load-bearing hook the runner turns into the
        out-of-band page (PipeDrain._drain_immediate raises the durable
        internal/delivery `delivery_exhausted` incident on exactly this edge).

        ``max_attempts`` is the per-pipe rung-3 threshold (Pipe.max_delivery_
        attempts). None falls back to the module MAX_RETRY_ATTEMPTS so behaviour
        is unchanged for any caller that does not pass it -- a pipe tunes how
        patient its redelivery ladder is before it exhausts and pages out-of-
        band. The three OTHER MAX_RETRY_ATTEMPTS sites (the triage retry path,
        the per-channel digest counter, the per-channel immediate counter) stay
        on the shared constant deliberately: they answer the per-CHANNEL health
        question ("is this transport degraded?", rung 2's trigger), a different
        grain from this per-FINDING give-up decision, and a pipe's patience with
        a single finding's delivery should not silently re-tune when a channel
        is declared unhealthy. Only the rung the ladder actually walks to abandon
        content is configurable here.

        Called AT MOST ONCE per finding per drain, and only when ZERO channels
        delivered the finding this drain (see PipeDrain._drain_immediate). This
        answers the per-FINDING question -- "should this finding be retried
        later?" -- split cleanly from the per-CHANNEL question "is this channel
        unhealthy?", which now lives in immediate_channel_attempts via
        record_immediate_send_failure / record_immediate_send_success.

        The split is the extra reconciliation the digest path never faced.
        Before B7, the immediate path attempted one channel per finding, so the
        single pipe_queues.attempts row (keyed (finding_id, pipe)) doubled as
        BOTH the redelivery counter AND the channel-health counter, and the old
        record_pipe_send_failure advanced both at once. B7 fans internal/*
        findings to N channels through that one row; letting each channel
        advance it inflated it +N per drain and conflated the two concerns. This
        method keeps pipe_queues.attempts as ONLY the per-finding redelivery
        ladder: advanced once per undelivered drain, never per failed channel,
        and it no longer touches channel_health at all (mark_channel_unhealthy
        moved to the per-channel counter).

        Returns True when the finding has exhausted its redelivery attempts
        (status -> 'dead_letter' (B15), the explicit terminal state, so
        pending_pipe_items drops it); False when it stays retryable with
        next_attempt_at set to the backoff schedule.

        B15: the exhaustion-terminal is 'dead_letter', NOT the old 'failed'.
        'failed' on pipe_queues was indistinguishable from the dispatches
        table's transient per-channel 'failed'; 'dead_letter' names the give-up
        state so health/belfry can surface it and `angelus replay` can re-arm
        it. The state transition is the only change here -- the ladder
        arithmetic, the per-finding grain, and the rung-3 hook are unchanged.
        """
        row = self.connection.execute(
            """
            SELECT attempts FROM pipe_queues
            WHERE finding_id = ? AND pipe = ?
            """,
            (finding_id, pipe),
        ).fetchone()
        attempts = int(row["attempts"]) if row is not None else 0
        next_attempt = attempts + 1
        threshold = MAX_RETRY_ATTEMPTS if max_attempts is None else max_attempts
        if next_attempt >= threshold:
            now = self._clock.now_iso()
            self.connection.execute(
                """
                UPDATE pipe_queues
                SET attempts = ?,
                    last_error = ?,
                    next_attempt_at = NULL,
                    status = 'dead_letter',
                    updated_at = ?
                WHERE finding_id = ? AND pipe = ?
                """,
                (next_attempt, error, now, finding_id, pipe),
            )
            self.connection.commit()
            return True

        # TRUST_RETRY_DELAYS is a fixed 4-step schedule sized for the default
        # threshold (5 attempts -> indices 0..3). A pipe that configures a
        # HIGHER max_delivery_attempts walks past the end of the schedule, so
        # clamp to the last (longest) delay rather than IndexError: once a
        # finding has retried this many times the backoff is already at its
        # ceiling, and holding it there is the right behaviour for the extra
        # rungs.
        delay = TRUST_RETRY_DELAYS[min(next_attempt - 1, len(TRUST_RETRY_DELAYS) - 1)]
        next_attempt_at = _format_time(self._clock.now() + delay)
        self.connection.execute(
            """
            UPDATE pipe_queues
            SET attempts = ?,
                last_error = ?,
                next_attempt_at = ?,
                updated_at = ?
            WHERE finding_id = ? AND pipe = ?
            """,
            (next_attempt, error, next_attempt_at, self._clock.now_iso(), finding_id, pipe),
        )
        self.connection.commit()
        return False

    def mark_channel_unhealthy(self, channel: str, error: str) -> None:
        # Log only the healthy->unhealthy edge, not every re-affirmation of an
        # already-unhealthy channel, so the log carries the transition once
        # rather than once per failed drain (B22).
        already_unhealthy = self.is_channel_unhealthy(channel)
        self.connection.execute(
            """
            INSERT INTO channel_health (channel, status, last_error, updated_at)
            VALUES (?, 'unhealthy', ?, ?)
            ON CONFLICT (channel) DO UPDATE SET
                status = 'unhealthy',
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (channel, error, self._clock.now_iso()),
        )
        if not already_unhealthy:
            LOGGER.warning(
                "channel %s marked unhealthy: %s", channel, error
            )

    def is_channel_unhealthy(self, channel: str) -> bool:
        row = self.connection.execute(
            """
            SELECT 1
            FROM channel_health
            WHERE channel = ? AND status = 'unhealthy'
            """,
            (channel,),
        ).fetchone()
        return row is not None

    def clear_channel_health(self) -> None:
        self.connection.execute("DELETE FROM channel_health")
        self.connection.commit()

    def record_digest_send_failure(
        self, pipe: str, channel: str, error: str
    ) -> bool:
        """Increment the per-channel digest attempt counter.

        Returns True when this call crosses MAX_RETRY_ATTEMPTS and the
        channel is marked unhealthy in channel_health; False otherwise.

        Counter shape is (pipe, channel) -- intentionally NOT per-finding.
        A digest cycle attempts one channel carrying a batch of finding_ids;
        the immediate path's per-(pipe, finding_id) counter on pipe_queues
        would inflate the threshold N-per-cycle on the digest path. Reuses
        the same MAX_RETRY_ATTEMPTS threshold so the two paths' ladders
        match.
        """
        now = self._clock.now_iso()
        row = self.connection.execute(
            """
            SELECT attempts FROM digest_channel_attempts
            WHERE pipe = ? AND channel = ?
            """,
            (pipe, channel),
        ).fetchone()
        attempts = int(row["attempts"]) if row is not None else 0
        next_attempt = attempts + 1
        self.connection.execute(
            """
            INSERT INTO digest_channel_attempts
                (pipe, channel, attempts, last_error, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (pipe, channel) DO UPDATE SET
                attempts = excluded.attempts,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (pipe, channel, next_attempt, error, now),
        )
        crossed = next_attempt >= MAX_RETRY_ATTEMPTS
        if crossed:
            self.mark_channel_unhealthy(channel, error)
        self.connection.commit()
        return crossed

    def record_digest_send_success(self, pipe: str, channel: str) -> None:
        """Reset the per-channel digest attempt counter after a successful send.

        An intermittent channel must not gradually accumulate to threshold
        across a long stretch of (mostly-succeeding) cycles -- only N
        CONSECUTIVE failures should mark unhealthy, matching the immediate
        path's per-finding ladder semantics. channel_health itself is NOT
        cleared here: it is daemon-restart-scoped by slice-2 design, and
        re-clearing on a single success would let a flapping channel oscillate
        in and out of the immediate path's is_channel_unhealthy skip on
        every drain.
        """
        self.connection.execute(
            """
            DELETE FROM digest_channel_attempts
            WHERE pipe = ? AND channel = ?
            """,
            (pipe, channel),
        )
        self.connection.commit()

    def clear_digest_channel_attempts(self) -> None:
        """Wipe the per-channel digest attempt counter (daemon-restart scope).

        Mirrors clear_channel_health: the threshold ladder resets when the
        daemon restarts, so an operator-initiated restart is the supported
        path to re-enable a channel that has been marked unhealthy via the
        digest path.
        """
        self.connection.execute("DELETE FROM digest_channel_attempts")
        self.connection.commit()

    def record_immediate_send_failure(
        self, pipe: str, channel: str, finding_id: int, error: str
    ) -> bool:
        """Per-channel health escalation for the immediate (_drain_immediate) path.

        Mirrors record_digest_send_failure. Records the per-channel 'failed'
        dispatch row (so the belfry/B1 failed-dispatch surface still sees it,
        as the old record_pipe_send_failure did), then increments the
        per-(pipe, channel) counter in immediate_channel_attempts. Returns True
        when this call crosses MAX_RETRY_ATTEMPTS and the channel is marked
        unhealthy in channel_health; False otherwise.

        Counter shape is (pipe, channel) -- intentionally NOT per-finding.
        Channel health is a property of the CHANNEL, so the counter must
        accumulate a channel's failures ACROSS findings and reset on a success,
        exactly like digest_channel_attempts. A per-(pipe, channel, finding_id)
        key would reset on every finding and never escalate: the B7 fan retries
        a finding only until >=1 channel delivers it, so a persistently-down
        co-fanned channel is rarely re-attempted against the SAME finding -- its
        failures are spread one apiece across many DIFFERENT findings. So
        per-(pipe, channel) is the correct grain and matches the digest
        precedent (a round-1 review's offhand (pipe,channel,finding_id)
        suggestion would have made the counter unescalatable).

        Deliberately decoupled from the finding's pipe_queues row status: the
        escalation fires off THIS counter even when a co-fanned channel
        succeeded and marked the finding 'dispatched' -- the exact gap (defect
        b) the per-finding shared counter left open on the immediate path.
        """
        self.record_dispatch(
            pipe, channel, [finding_id], "failed", error, mark_queue=False
        )
        now = self._clock.now_iso()
        row = self.connection.execute(
            """
            SELECT attempts FROM immediate_channel_attempts
            WHERE pipe = ? AND channel = ?
            """,
            (pipe, channel),
        ).fetchone()
        attempts = int(row["attempts"]) if row is not None else 0
        next_attempt = attempts + 1
        self.connection.execute(
            """
            INSERT INTO immediate_channel_attempts
                (pipe, channel, attempts, last_error, updated_at)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (pipe, channel) DO UPDATE SET
                attempts = excluded.attempts,
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (pipe, channel, next_attempt, error, now),
        )
        crossed = next_attempt >= MAX_RETRY_ATTEMPTS
        if crossed:
            self.mark_channel_unhealthy(channel, error)
        self.connection.commit()
        return crossed

    def record_immediate_send_success(self, pipe: str, channel: str) -> None:
        """Reset the per-channel immediate attempt counter after a successful send.

        Mirrors record_digest_send_success: only N CONSECUTIVE channel failures
        mark a channel unhealthy, so a single success clears the ladder and an
        intermittent channel never gradually accumulates to threshold across
        many mostly-succeeding findings. channel_health itself is NOT cleared
        here -- it is daemon-restart-scoped, and re-clearing it on one success
        would let a flapping channel oscillate in and out of the immediate
        path's is_channel_unhealthy skip on every drain.
        """
        self.connection.execute(
            """
            DELETE FROM immediate_channel_attempts
            WHERE pipe = ? AND channel = ?
            """,
            (pipe, channel),
        )
        self.connection.commit()

    def clear_immediate_channel_attempts(self) -> None:
        """Wipe the per-channel immediate attempt counter (daemon-restart scope).

        Mirrors clear_digest_channel_attempts / clear_channel_health: the
        threshold ladder resets when the daemon restarts, so an operator-
        initiated restart is the supported path to re-enable a channel that has
        been marked unhealthy via the immediate path.
        """
        self.connection.execute("DELETE FROM immediate_channel_attempts")
        self.connection.commit()

    def write_internal_finding(
        self, source: str, finding_type: str, entity: str, body: str, known_pipes: set[str]
    ) -> int:
        return self.write_finding(
            None,
            {
                "source": source,
                "type": finding_type,
                "entity": entity,
                "severity": "high",
                "target_pipes": ["now"],
                "body": body,
            },
            known_pipes,
        )

    def write_internal_clearance(
        self, source: str, entity: str, body: str, known_pipes: set[str]
    ) -> int:
        """Emit a recovery clearance for an internal source (B30).

        Pairs with write_internal_finding: where that opens an incident on a
        failure edge, this closes it on the recovery edge so the emission gate
        re-arms and a genuine re-failure can alert again. The clearance is the
        REQUIRED counterpart to every internal failure finding -- a source
        that can open but never clear goes silent forever under the gate.

        Callers may fire this unconditionally on their success path (a
        healthy dep_record, a successful send/render, a file that loads OK):
        write_finding's recovery gate drops it to a no-op when no incident is
        open, so an unconditional call is edge-triggered for free and never
        floods. Returns the clearance finding_id, or _NO_FINDING when nothing
        was open to clear.

        target_pipes is empty by design: the clearance's load-bearing job is
        closing the incident (re-arming the gate), not paging. It still
        surfaces in the daily digest's recent_closures, which reads the
        findings table by type via clearance_findings_since regardless of
        pipe routing, so the recovery is reported without adding `now`-pipe
        noise -- preserving the long-standing "recovery is silent on now"
        contract for internal sources.
        """
        return self.write_finding(
            None,
            {
                "source": source,
                "type": "clearance",
                "entity": entity,
                "severity": "info",
                "target_pipes": [],
                "body": body,
            },
            known_pipes,
        )

    def recover_triage_processing_rows(self) -> int:
        """Delete observation_triage rows left at status='processing' from
        a prior daemon's hard exit (SIGKILL, OS kill, host crash). Returns
        the row count cleared.

        Two intents this recovers from, both of which can leave the row
        orphaned at 'processing' across a daemon restart:
        - hard kill: SIGKILL/SIGSEGV/host loss bypasses Python's
          shutdown handlers, so the in-process graceful-cancel arm
          (which clears the same row via a triage task's cancellation
          handler) never fires; this is the hard-exit companion to
          that arm.
        - host crash mid-triage: same shape, same orphan.

        Bounded to status='processing' so any transition that
        legitimately completed before the crash (status now 'success' or
        'failed') is untouched. Runs once at daemon startup before the
        triage loop spins up, so there is no concurrency to race
        against."""
        cursor = self.connection.execute(
            "DELETE FROM observation_triage WHERE status = 'processing'"
        )
        self.connection.commit()
        return cursor.rowcount

    def recover_writing_rows(self) -> tuple[int, int]:
        recovered = 0
        failed = 0
        for table in ("observations", "findings"):
            rows = list(
                self.connection.execute(
                    f"SELECT id, body_ref FROM {table} WHERE status = 'writing'"
                )
            )
            for row in rows:
                body_ref = row["body_ref"]
                status = (
                    "ready"
                    if body_ref and (self.root / body_ref).exists()
                    else "failed"
                )
                if status == "ready":
                    recovered += 1
                else:
                    failed += 1
                self.connection.execute(
                    f"UPDATE {table} SET status = ?, updated_at = ? WHERE id = ?",
                    (status, self._clock.now_iso(), row["id"]),
                )
        self.connection.commit()
        return recovered, failed

    def _write_body(self, kind: str, row_id: int, body: dict[str, Any]) -> str:
        date = self._clock.now_iso()[:10]
        directory = self.root / kind / date / str(row_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "body.json"
        path.write_text(json.dumps(body or {}, sort_keys=True) + "\n", encoding="utf-8")
        return str(path.relative_to(self.root))

    def _finding_dict(self, row) -> dict[str, Any]:
        item = dict(row)
        body = self.read_body(item.get("body_ref"))
        item["body"] = body
        item["body_text"] = str(body.get("text") or "")
        try:
            item["target_pipes"] = json.loads(item.get("target_pipes") or "[]")
        except json.JSONDecodeError:
            item["target_pipes"] = []
        return item

    def _open_incident_for_key(
        self, source: str, finding_type: str, entity: str
    ) -> sqlite3.Row | None:
        """The open incident for the exact key (source, type, entity), or None.

        This is the emission-gate authority for a non-clearance finding: the
        key matches the one-open-per-entity unique index, so a hit means the
        condition is already being tracked and a repeat must be dropped.
        """
        return self.connection.execute(
            """
            SELECT id, latest_finding_id
            FROM incidents
            WHERE source = ? AND type = ? AND entity = ? AND status = 'open'
            LIMIT 1
            """,
            (source, finding_type, entity),
        ).fetchone()

    def _has_open_incident_for_entity(self, source: str, entity: str) -> bool:
        """Whether any open incident exists for (source, entity), any type.

        Mirrors _close_incident's (source, entity) close key: a clearance
        clears whatever is open for the entity regardless of which failure
        type opened it, so the recovery gate asks the same question.
        """
        row = self.connection.execute(
            """
            SELECT 1 FROM incidents
            WHERE source = ? AND entity = ? AND status = 'open'
            LIMIT 1
            """,
            (source, entity),
        ).fetchone()
        return row is not None

    def _upsert_incident(
        self, source: str, finding_type: str, entity: str, dedup_key: str, finding_id: int
    ) -> bool:
        """Open or refresh the incident for (source, type, entity).

        Returns True when this call OPENED a new incident (no open row existed
        for the key), False when it refreshed an already-open one. The caller
        (write_finding's gate) relies on this transition signal: under the
        B30 gate every non-clearance finding that reaches here has passed the
        open-incident pre-check, so a True is expected; a False means the gate
        and the table have drifted.
        """
        now = self._clock.now_iso()
        opened_new = self._open_incident_for_key(source, finding_type, entity) is None
        # opened_at/closed_at are the clock-pinned incident timestamps every
        # windowed read uses (recently_closed_incidents windows closed_at;
        # open_incidents/recently_closed order by opened_at/closed_at).
        # incidents.created_at is never compared in a since/window query, so it
        # is left on the schema's wall-clock DEFAULT.
        self.connection.execute(
            """
            INSERT INTO incidents (
                source, type, entity, dedup_key, opened_at, status, latest_finding_id
            )
            VALUES (?, ?, ?, ?, ?, 'open', ?)
            ON CONFLICT (source, type, entity) WHERE status = 'open'
            DO UPDATE SET
                latest_finding_id = excluded.latest_finding_id,
                updated_at = excluded.opened_at
            """,
            (source, finding_type, entity, dedup_key, now, finding_id),
        )
        return opened_new

    def _close_incident(self, source: str, entity: str, finding_id: int) -> None:
        now = self._clock.now_iso()
        self.connection.execute(
            """
            UPDATE incidents
            SET status = 'closed',
                closed_at = ?,
                latest_finding_id = ?,
                updated_at = ?
            WHERE source = ? AND entity = ? AND status = 'open'
            """,
            (now, finding_id, now, source, entity),
        )

    # --- slice 5b-2 control-socket write ops -----------------------------
    #
    # Every method below is synchronous and self-committing, like the rest
    # of this class. The control-socket handlers that call them must not
    # await between the db write and its commit (cancel-safety contract):
    # keeping these synchronous is what guarantees that by construction.
    # Each is idempotent under at-least-once delivery on natural state --
    # there is no request-id/dedup cache anywhere.

    def add_mute(
        self, dedup_key: str, duration_seconds: int, comment: str | None
    ) -> str:
        """Insert a mute row and return its resolved expires_at.

        A mute is a row, never an upsert. Re-applying the same mute op
        (at-least-once retry) inserts another overlapping row; "is X
        muted now?" is EXISTS a row with expires_at > now, so overlapping
        rows are harmless and a retry leaves the same effective state.
        The same shape also makes "extend the mute" correct: a later,
        longer mute is just another row with a farther expires_at.
        """
        now_dt = self._clock.now()
        expires_at = _format_time(now_dt + timedelta(seconds=duration_seconds))
        self.connection.execute(
            """
            INSERT INTO mutes (dedup_key, expires_at, created_at, comment)
            VALUES (?, ?, ?, ?)
            """,
            (dedup_key, expires_at, _format_time(now_dt), comment),
        )
        self.connection.commit()
        return expires_at

    def is_muted(self, dedup_key: str) -> bool:
        """True if an unexpired mute exists for dedup_key. Read-only.

        expires_at and the clock's now string are the same fixed-width ISO8601
        UTC format (millisecond precision, Z suffix), so the lexicographic
        `expires_at > ?` comparison is a correct time comparison and an
        expired mute does not match -- no GC sweeper is needed.
        """
        row = self.connection.execute(
            "SELECT 1 FROM mutes WHERE dedup_key = ? AND expires_at > ? LIMIT 1",
            (dedup_key, self._clock.now_iso()),
        ).fetchone()
        return row is not None

    def active_mutes(self) -> list[dict[str, Any]]:
        """Active mutes -- rows whose expires_at is still in the future.
        Read-only; a plain SELECT, no write.

        Symmetric with is_muted: the same `expires_at > now`
        lexicographic predicate is the only mechanism, so an expired
        mute simply does not appear and no GC sweeper is needed. The
        operator listing mutes wants the ones in effect, so this is
        active-only by design. Ordered by expires_at ascending --
        soonest-to-lift first -- with id as a stable tiebreaker for
        overlapping rows that share an expires_at.
        """
        rows = self.connection.execute(
            """
            SELECT dedup_key, expires_at, created_at, comment
            FROM mutes
            WHERE expires_at > ?
            ORDER BY expires_at ASC, id ASC
            """,
            (self._clock.now_iso(),),
        )
        return [dict(row) for row in rows]

    def active_mute_for(self, dedup_key: str) -> dict[str, Any] | None:
        """The effective active mute for one dedup key, if any. Read-only.

        Overlapping mute rows are legal. For the operator-facing health
        surface the load-bearing question is "until when is this muted
        right now?", so we surface the farthest-future active row.
        """
        row = self.connection.execute(
            """
            SELECT expires_at, created_at, comment
            FROM mutes
            WHERE dedup_key = ? AND expires_at > ?
            ORDER BY expires_at DESC, id DESC
            LIMIT 1
            """,
            (dedup_key, self._clock.now_iso()),
        ).fetchone()
        return dict(row) if row is not None else None

    def all_channel_health(self) -> list[dict[str, Any]]:
        """Every channel_health row, channel-ordered. Read-only.

        This is the unfiltered operator rail for a channel marked
        unhealthy, regardless of whether the corresponding
        internal/dispatch finding is muted on the now pipe.
        """
        rows = self.connection.execute(
            """
            SELECT channel, status, last_error, updated_at
            FROM channel_health
            ORDER BY channel
            """
        )
        return [dict(row) for row in rows]

    def digest_channel_attempts(self) -> list[dict[str, Any]]:
        """Digest channel attempt ladder rows with attempts > 0. Read-only.

        The digest path's in-flight retry state is its own operator
        surface: attempts accumulate before channel_health flips, so this
        reader makes the ladder visible before threshold.
        """
        rows = self.connection.execute(
            """
            SELECT pipe, channel, attempts, last_error, updated_at
            FROM digest_channel_attempts
            WHERE attempts > 0
            ORDER BY pipe, channel
            """
        )
        return [dict(row) for row in rows]

    def immediate_channel_attempts(self) -> list[dict[str, Any]]:
        """Immediate-path per-channel attempt ladder rows with attempts > 0.

        Read-only operator surface mirroring digest_channel_attempts: the
        immediate path's in-flight per-channel retry state accumulates before
        channel_health flips, so this reader makes the ladder visible before
        the unhealthy threshold is crossed.
        """
        rows = self.connection.execute(
            """
            SELECT pipe, channel, attempts, last_error, updated_at
            FROM immediate_channel_attempts
            WHERE attempts > 0
            ORDER BY pipe, channel
            """
        )
        return [dict(row) for row in rows]

    def close_incident(
        self, incident_id: int, comment: str | None
    ) -> str:
        """Close an open incident. Returns 'closed', 'already_closed', or
        'not_found'.

        Idempotent: the UPDATE is bounded to status='open', so a second
        apply matches 0 rows and reports 'already_closed' with no
        double-effect (the original closed_at/close_comment are untouched).
        """
        now = self._clock.now_iso()
        cursor = self.connection.execute(
            """
            UPDATE incidents
            SET status = 'closed',
                closed_at = ?,
                close_comment = ?,
                updated_at = ?
            WHERE id = ? AND status = 'open'
            """,
            (now, comment, now, incident_id),
        )
        closed = cursor.rowcount == 1
        self.connection.commit()
        if closed:
            return "closed"
        row = self.connection.execute(
            "SELECT 1 FROM incidents WHERE id = ?", (incident_id,)
        ).fetchone()
        return "already_closed" if row is not None else "not_found"

    def replay_finding(
        self, finding_id: int, known_pipes: set[str]
    ) -> dict[str, Any]:
        """Re-queue a finding to its still-known target pipes so the next
        drain dispatches it again.

        Returns {"outcome": "requeued"|"already_queued"|"not_found",
        "finding_id": id, "pipes": [...]}.

        Idempotency guard (mandatory): a (finding, pipe) that is already
        'pending' is left untouched -- a double replay before any drain
        therefore does not double-queue. A row in any other state
        (dispatched/dead_letter/suppressed) is reset to 'pending' so the
        finding is genuinely re-dispatched; a missing row is inserted. The
        reset is generic ("status != 'pending'"), so a B15 dead-lettered row
        re-arms here exactly as the old 'failed' terminal did -- replay is the
        one path that pulls content back out of dead-letter.
        """
        frow = self.connection.execute(
            "SELECT target_pipes FROM findings WHERE id = ?", (finding_id,)
        ).fetchone()
        if frow is None:
            return {"outcome": "not_found", "finding_id": finding_id, "pipes": []}
        try:
            target_pipes = json.loads(frow["target_pipes"] or "[]")
        except json.JSONDecodeError:
            target_pipes = []
        requeued: list[str] = []
        for pipe in target_pipes:
            if pipe not in known_pipes:
                continue
            existing = self.connection.execute(
                "SELECT status FROM pipe_queues WHERE finding_id = ? AND pipe = ?",
                (finding_id, pipe),
            ).fetchone()
            if existing is None:
                now = self._clock.now_iso()
                self.connection.execute(
                    """
                    INSERT INTO pipe_queues (
                        finding_id, pipe, status, created_at, updated_at
                    )
                    VALUES (?, ?, 'pending', ?, ?)
                    """,
                    # Clock-pin created_at like every other pipe_queues enqueue
                    # so the column is single-clock for pending_pipe_items'
                    # ORDER BY pq.created_at under a sim.
                    (finding_id, pipe, now, now),
                )
                requeued.append(pipe)
            elif existing["status"] != "pending":
                self.connection.execute(
                    """
                    UPDATE pipe_queues
                    SET status = 'pending',
                        dispatched_at = NULL,
                        next_attempt_at = NULL,
                        attempts = 0,
                        updated_at = ?
                    WHERE finding_id = ? AND pipe = ? AND status != 'pending'
                    """,
                    (self._clock.now_iso(), finding_id, pipe),
                )
                requeued.append(pipe)
            # else: already 'pending' -> the guard. Skip so a double
            # replay does not double-queue.
        self.connection.commit()
        if requeued:
            return {
                "outcome": "requeued",
                "finding_id": finding_id,
                "pipes": requeued,
            }
        return {
            "outcome": "already_queued",
            "finding_id": finding_id,
            "pipes": [],
        }

    def reprocess_source(self, source: str) -> int:
        """Delete observation_triage rows for observations from `source`
        so the triage loop re-picks those observations
        (ready_observations_for excludes observations that already have a
        triage row), and flip the source's terminal observations
        ('consumed', 'triage_failed') back to 'ready'. Returns the number
        of distinct observations that will be re-triaged.

        The flip is deliberate: ready_observations_for filters
        status='ready', so without it a consumed observation would keep its
        body on disk yet be unreachable forever -- reprocess is the one
        documented way back from terminal. Both halves are bounded to the
        given source and committed together. Idempotent: a second apply
        finds the rows already 'ready' with no triage rows, so it flips
        nothing, deletes nothing, and returns 0.
        """
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM observations o
            WHERE o.source = ?
              AND (
                o.status IN ('consumed', 'triage_failed')
                OR EXISTS (
                    SELECT 1 FROM observation_triage ot
                    WHERE ot.observation_id = o.id
                )
              )
            """,
            (source,),
        ).fetchone()
        count = int(row["n"])
        self.connection.execute(
            """
            DELETE FROM observation_triage
            WHERE observation_id IN (
                SELECT id FROM observations WHERE source = ?
            )
            """,
            (source,),
        )
        self.connection.execute(
            """
            UPDATE observations
            SET status = 'ready', updated_at = ?
            WHERE source = ? AND status IN ('consumed', 'triage_failed')
            """,
            (self._clock.now_iso(), source),
        )
        self.connection.commit()
        return count

    # --- slice 5c dependency registry ------------------------------------
    #
    # Same construction as the 5b-2 write ops above: synchronous and
    # self-committing, no request-id/dedup cache. Idempotency is the
    # dep_health primary key + ON CONFLICT upsert -- re-applying the same
    # dep_record yields the same end state. The _op_dep_record handler must
    # not await between this write and its commit; keeping this synchronous
    # is what guarantees that by construction.

    def record_dep_health(
        self,
        dependency_name: str,
        status: str,
        last_check_at: str,
        detail: str | None,
    ) -> None:
        """Upsert one dep_health row. The dependency_name PK + ON CONFLICT
        is the entire idempotency mechanism: a retried dep_record overwrites
        the row with identical values and leaves the same end state."""
        self.connection.execute(
            """
            INSERT INTO dep_health (
                dependency_name, status, last_check_at, detail, updated_at
            )
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT (dependency_name) DO UPDATE SET
                status = excluded.status,
                last_check_at = excluded.last_check_at,
                detail = excluded.detail,
                updated_at = excluded.updated_at
            """,
            (dependency_name, status, last_check_at, detail, self._clock.now_iso()),
        )
        self.connection.commit()

    def delete_dep_health(self, dependency_name: str) -> None:
        """Drop a dependency's dep_health row.

        Called by apply_lodging when a dependency is hot-removed from
        lodging. Without this the row would orphan: nothing else ever
        deletes dep_health, and a removed dependency can never receive
        another dep_record (the dep-check probe exits non-zero for an
        unlodged name), so all_dep_health() -- the health op's reader --
        would surface a frozen, unrecoverable status forever. Synchronous
        and self-committing like the rest of this class; the caller must
        not await between this and its commit. Idempotent: a second call
        deletes nothing."""
        self.connection.execute(
            "DELETE FROM dep_health WHERE dependency_name = ?",
            (dependency_name,),
        )
        self.connection.commit()

    def all_dep_health(self) -> list[dict[str, Any]]:
        """Every dep_health row, name-ordered. Read-only; a plain SELECT.

        This is the mandatory reader for dep_health -- the health op
        surfaces it so a written dep status is never dead config."""
        rows = self.connection.execute(
            """
            SELECT dependency_name, status, last_check_at, detail, updated_at
            FROM dep_health
            ORDER BY dependency_name
            """
        )
        return [dict(row) for row in rows]

    def record_fixer_attempt(
        self, fixer_name: str, condition_key: str, outcome: str
    ) -> str:
        """Append one fixer attempt to the ledger and return its timestamp (B11).

        Written for every attempt the dispatcher actually makes (the guardrails
        having allowed it), so the rolling-window count and the backoff spacing
        both read off real attempts. Append-only, self-committing like the rest
        of this class."""
        attempted_at = self._clock.now_iso()
        self.connection.execute(
            """
            INSERT INTO fixer_attempts (
                fixer_name, condition_key, attempted_at, outcome
            )
            VALUES (?, ?, ?, ?)
            """,
            (fixer_name, condition_key, attempted_at, outcome),
        )
        self.connection.commit()
        return attempted_at

    def fixer_attempt_count_in_window(
        self, fixer_name: str, condition_key: str, window_seconds: int
    ) -> int:
        """Count attempts for (fixer, condition) within the trailing window.

        The guardrail's max_attempts is compared against this. The window is
        measured from the catalog clock's now, so a FakeClock test controls it.
        Read-only."""
        cutoff = _format_time(self._clock.now() - timedelta(seconds=window_seconds))
        row = self.connection.execute(
            """
            SELECT COUNT(*) AS n
            FROM fixer_attempts
            WHERE fixer_name = ? AND condition_key = ? AND attempted_at >= ?
            """,
            (fixer_name, condition_key, cutoff),
        ).fetchone()
        return int(row["n"])

    def last_fixer_attempt_at(
        self, fixer_name: str, condition_key: str
    ) -> str | None:
        """Most recent attempt timestamp for (fixer, condition), or None.

        Backoff spacing is measured against this. Deliberately unwindowed: the
        last attempt governs spacing even if it predates the count window, so
        a long backoff is honored regardless of window length. Read-only."""
        row = self.connection.execute(
            """
            SELECT MAX(attempted_at) AS last_at
            FROM fixer_attempts
            WHERE fixer_name = ? AND condition_key = ?
            """,
            (fixer_name, condition_key),
        ).fetchone()
        return None if row is None else row["last_at"]
