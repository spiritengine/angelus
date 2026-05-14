"""SQLite lifecycle operations.

Rate-limit accounting intentionally uses the dispatches table. Slice 3 stores
the finding source redundantly on each dispatch row so the per-source rolling
count is a simple indexed lookup instead of parsing finding_ids JSON.
"""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

TRUST_RETRY_DELAYS = (
    timedelta(minutes=1),
    timedelta(minutes=10),
    timedelta(hours=1),
    timedelta(hours=8),
)
MAX_RETRY_ATTEMPTS = 5


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _utcnow_dt() -> datetime:
    return datetime.now(UTC)


def _format_time(value: datetime) -> str:
    return value.isoformat(timespec="milliseconds").replace("+00:00", "Z")


class Catalog:
    def __init__(self, connection: sqlite3.Connection, root: Path) -> None:
        self.connection = connection
        self.root = root

    def record_source_fire(
        self, source_name: str, scheduled_at: str | None, outcome: str
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO source_fires (source_name, scheduled_at, fired_at, outcome)
            VALUES (?, ?, ?, ?)
            """,
            (source_name, scheduled_at, utcnow(), outcome),
        )
        self.connection.commit()

    def write_observation(
        self, source_ref: str, payload: dict[str, Any], provenance: dict[str, Any]
    ) -> int:
        now = utcnow()
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
            (body_ref, utcnow(), observation_id),
        )
        self.connection.commit()
        return observation_id

    def ready_observations_for(self, triager_name: str, source_ref: str) -> list[sqlite3.Row]:
        now = utcnow()
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

    def mark_triage_success(self, observation_id: int, triager_name: str) -> None:
        self.connection.execute(
            """
            UPDATE observation_triage
            SET status = 'success',
                next_attempt_at = NULL,
                updated_at = ?
            WHERE observation_id = ? AND triager_name = ?
            """,
            (utcnow(), observation_id, triager_name),
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
            now = utcnow()
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
            _utcnow_dt() + TRUST_RETRY_DELAYS[attempt - 1]
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
            (next_attempt, next_attempt_at, error, utcnow(), observation_id, triager_name),
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
            (triager_name, source_ref, json.dumps(state, sort_keys=True), utcnow()),
        )
        self.connection.commit()

    def write_finding(
        self,
        observation_id: int | None,
        finding: dict[str, Any],
        known_pipes: set[str],
    ) -> int:
        source = str(finding["source"])
        finding_type = str(finding["type"])
        entity = str(finding["entity"])
        dedup_key = str(finding.get("dedup_key") or f"{source}:{finding_type}:{entity}")
        target_pipes = list(finding.get("target_pipes") or [])
        body = finding.get("body")
        body_obj = body if isinstance(body, dict) else {"text": body} if body else {}
        now = utcnow()
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
            (body_ref, utcnow(), finding_id),
        )
        if finding_type == "clearance":
            self._close_incident(source, entity, finding_id)
        else:
            self._upsert_incident(source, finding_type, entity, dedup_key, finding_id)
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

    def pending_pipe_items(self, pipe: str) -> list[sqlite3.Row]:
        now = utcnow()
        return list(
            self.connection.execute(
                """
                SELECT pq.finding_id, pq.pipe, f.*
                FROM pipe_queues pq
                JOIN findings f ON f.id = pq.finding_id
                WHERE pq.pipe = ? AND pq.status = 'pending' AND f.status = 'ready'
                  AND (pq.next_attempt_at IS NULL OR pq.next_attempt_at <= ?)
                ORDER BY pq.created_at, pq.finding_id
                LIMIT 20
                """,
                (pipe, now),
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
            (pipe, channel, json.dumps(finding_ids), status, error, utcnow(), source),
        )
        if status == "sent" and mark_queue:
            for finding_id in finding_ids:
                self.connection.execute(
                    """
                    UPDATE pipe_queues
                    SET status = 'dispatched', dispatched_at = ?, updated_at = ?
                    WHERE finding_id = ? AND pipe = ?
                    """,
                    (utcnow(), utcnow(), finding_id, pipe),
                )
        self.connection.commit()

    def mark_pipe_items_dispatched(self, pipe: str, finding_ids: list[int]) -> None:
        now = utcnow()
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

    def suppress_pipe_item_to_daily(self, finding_id: int, pipe: str) -> None:
        now = utcnow()
        self.connection.execute(
            """
            UPDATE pipe_queues
            SET status = 'suppressed', updated_at = ?
            WHERE finding_id = ? AND pipe = ?
            """,
            (now, finding_id, pipe),
        )
        self.connection.execute(
            """
            INSERT OR IGNORE INTO pipe_queues (finding_id, pipe, status)
            VALUES (?, 'daily', 'pending')
            """,
            (finding_id,),
        )
        self.connection.commit()

    def last_pipe_drain_at(self, pipe_name: str) -> str | None:
        row = self.connection.execute(
            "SELECT last_drain_at FROM pipe_state WHERE pipe_name = ?",
            (pipe_name,),
        ).fetchone()
        return None if row is None else row["last_drain_at"]

    def mark_pipe_drained(self, pipe_name: str, drained_at: str | None = None) -> str:
        drained_at = drained_at or utcnow()
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

    def findings_for_pipe_since(self, pipe: str, since: str | None) -> list[dict[str, Any]]:
        clause = "AND f.created_at > ?" if since else ""
        params: tuple[Any, ...] = (pipe, since) if since else (pipe,)
        rows = self.connection.execute(
            f"""
            SELECT f.*, pq.created_at AS queued_at
            FROM pipe_queues pq
            JOIN findings f ON f.id = pq.finding_id
            WHERE pq.pipe = ? AND f.status = 'ready' {clause}
              AND NOT EXISTS (
                SELECT 1
                FROM pipe_queues suppressed
                WHERE suppressed.finding_id = f.id
                  AND suppressed.status = 'suppressed'
              )
            ORDER BY f.created_at, f.id
            """,
            params,
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
            now = utcnow()
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
            _utcnow_dt() + TRUST_RETRY_DELAYS[next_attempt - 1]
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
            (next_attempt, error, next_attempt_at, utcnow(), finding_id, pipe),
        )
        self.connection.commit()
        return False

    def mark_channel_unhealthy(self, channel: str, error: str) -> None:
        self.connection.execute(
            """
            INSERT INTO channel_health (channel, status, last_error, updated_at)
            VALUES (?, 'unhealthy', ?, ?)
            ON CONFLICT (channel) DO UPDATE SET
                status = 'unhealthy',
                last_error = excluded.last_error,
                updated_at = excluded.updated_at
            """,
            (channel, error, utcnow()),
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
                    (status, utcnow(), row["id"]),
                )
        self.connection.commit()
        return recovered, failed

    def _write_body(self, kind: str, row_id: int, body: dict[str, Any]) -> str:
        date = utcnow()[:10]
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

    def _upsert_incident(
        self, source: str, finding_type: str, entity: str, dedup_key: str, finding_id: int
    ) -> None:
        now = utcnow()
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

    def _close_incident(self, source: str, entity: str, finding_id: int) -> None:
        now = utcnow()
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
