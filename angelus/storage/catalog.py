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

    def record_source_fire(
        self, source_name: str, scheduled_at: str | None, outcome: str
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO source_fires (source_name, scheduled_at, fired_at, outcome)
            VALUES (?, ?, ?, ?)
            """,
            (source_name, scheduled_at, self._clock.now_iso(), outcome),
        )
        self.connection.commit()

    def write_observation(
        self, source_ref: str, payload: dict[str, Any], provenance: dict[str, Any]
    ) -> int:
        now = self._clock.now_iso()
        cursor = self.connection.execute(
            """
            INSERT INTO observations (source, status, provenance, written_at)
            VALUES (?, 'writing', ?, ?)
            """,
            (source_ref, json.dumps(provenance, sort_keys=True), now),
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
                        AND (ot.next_attempt_at IS NULL OR ot.next_attempt_at <= ?)
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
        row = self.connection.execute(
            """
            SELECT attempt FROM observation_triage
            WHERE observation_id = ? AND triager_name = ?
            """,
            (observation_id, triager_name),
        ).fetchone()
        attempt = int(row["attempt"]) if row is not None else 1
        if attempt >= MAX_RETRY_ATTEMPTS:
            now = self._clock.now_iso()
            self.connection.execute(
                """
                UPDATE observation_triage
                SET status = 'failed',
                    last_error = ?,
                    next_attempt_at = NULL,
                    updated_at = ?
                WHERE observation_id = ? AND triager_name = ?
                """,
                (error, now, observation_id, triager_name),
            )
            self.connection.execute(
                """
                UPDATE observations
                SET status = 'triage_failed', updated_at = ?
                WHERE id = ?
                """,
                (now, observation_id),
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
                status, severity, occurred_at
            )
            VALUES (?, ?, ?, ?, ?, ?, 'writing', ?, ?)
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
                    INSERT OR IGNORE INTO pipe_queues (finding_id, pipe, status)
                    VALUES (?, ?, 'pending')
                    """,
                    (finding_id, pipe),
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
            INSERT OR IGNORE INTO pipe_queues (finding_id, pipe, status)
            VALUES (?, ?, 'pending')
            """,
            (finding_id, target_pipe),
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
        """Most recent fired_at per source, from the source_fires ledger.
        Read-only; used by the health control op."""
        rows = self.connection.execute(
            """
            SELECT source_name, max(fired_at) AS last_fire_at
            FROM source_fires
            GROUP BY source_name
            """
        )
        return {row["source_name"]: row["last_fire_at"] for row in rows}

    def observations_pending_triage_count(self) -> int:
        """Ready observations with no successful triage yet. Read-only.

        'Pending' counts a ready observation until some triager has a
        'success' row for it. Observations still retrying ('failed' with a
        future next_attempt_at) or with no matching triager remain counted --
        from an operator's view they are still waiting on triage.
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

        Interleaves source fires, observations, findings, and dispatches
        (including failures) by timestamp into a single chronological list.
        Read-only: four plain SELECTs unioned in Python so each event keeps
        its own shape. Bounds are inclusive and compared as ISO strings,
        which sort correctly because every timestamp column uses the same
        '...Z' millisecond format.

        Same-instant ties break by kind (fire, observation, finding,
        dispatch) then by id. This ordering is deterministic and stable,
        NOT causal: on an exact millisecond collision the originating event
        and the event it provoked cannot be told apart from the stored data
        alone. A failing dispatch and the channel_unhealthy finding it
        provokes can share a millisecond, and the static kind order would
        then render the finding above its dispatch -- the reverse of what
        happened. Sub-millisecond ordering therefore may not reflect causal
        order; only the millisecond timestamps disambiguate when they differ.
        """
        events: list[dict[str, Any]] = []
        for row in self.connection.execute(
            """
            SELECT id, source_name, outcome, fired_at
            FROM source_fires
            WHERE fired_at >= ? AND fired_at <= ?
            """,
            (since, until),
        ):
            events.append(
                {
                    "ts": row["fired_at"],
                    "kind": "fire",
                    "id": int(row["id"]),
                    "source": row["source_name"],
                    "outcome": row["outcome"],
                }
            )
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
        kind_order = {"fire": 0, "observation": 1, "finding": 2, "dispatch": 3}
        events.sort(key=lambda e: (e["ts"], kind_order[e["kind"]], e["id"]))
        return events

    def record_pipe_send_failure(
        self, pipe: str, channel: str, finding_id: int, error: str
    ) -> bool:
        self.record_dispatch(pipe, channel, [finding_id], "failed", error)
        row = self.connection.execute(
            """
            SELECT attempts FROM pipe_queues
            WHERE finding_id = ? AND pipe = ?
            """,
            (finding_id, pipe),
        ).fetchone()
        attempts = int(row["attempts"]) if row is not None else 0
        next_attempt = attempts + 1
        if next_attempt >= MAX_RETRY_ATTEMPTS:
            now = self._clock.now_iso()
            self.connection.execute(
                """
                UPDATE pipe_queues
                SET attempts = ?,
                    last_error = ?,
                    next_attempt_at = NULL,
                    status = 'failed',
                    updated_at = ?
                WHERE finding_id = ? AND pipe = ?
                """,
                (next_attempt, error, now, finding_id, pipe),
            )
            self.mark_channel_unhealthy(channel, error)
            self.connection.commit()
            return True

        next_attempt_at = _format_time(
            self._clock.now() + TRUST_RETRY_DELAYS[next_attempt - 1]
        )
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
        (dispatched/failed/suppressed) is reset to 'pending' so the
        finding is genuinely re-dispatched; a missing row is inserted.
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
                self.connection.execute(
                    """
                    INSERT INTO pipe_queues (finding_id, pipe, status)
                    VALUES (?, ?, 'pending')
                    """,
                    (finding_id, pipe),
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
        triage row). Returns the number of distinct observations that
        will be re-triaged.

        Idempotent: a second apply finds the rows already gone and
        deletes nothing (0 observations). The DELETE is strictly bounded
        to the given source via the observations subquery, the same
        bounded-delete shape clear_triage_processing uses.
        """
        row = self.connection.execute(
            """
            SELECT COUNT(DISTINCT ot.observation_id) AS n
            FROM observation_triage ot
            JOIN observations o ON o.id = ot.observation_id
            WHERE o.source = ?
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
