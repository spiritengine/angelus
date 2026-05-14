"""SQLite lifecycle operations for slice 1."""

from __future__ import annotations

import json
import sqlite3
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


def utcnow() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


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
        return list(
            self.connection.execute(
                """
                SELECT o.*
                FROM observations o
                LEFT JOIN observation_triage ot
                  ON ot.observation_id = o.id AND ot.triager_name = ?
                WHERE o.status = 'ready'
                  AND o.source = ?
                  AND ot.observation_id IS NULL
                ORDER BY o.id
                LIMIT 20
                """,
                (triager_name, source_ref),
            )
        )

    def read_body(self, body_ref: str | None) -> dict[str, Any]:
        if not body_ref:
            return {}
        data = json.loads((self.root / body_ref).read_text(encoding="utf-8"))
        if not isinstance(data, dict):
            raise ValueError(f"body is not a JSON object: {body_ref}")
        return data

    def mark_triage_processing(self, observation_id: int, triager_name: str) -> bool:
        try:
            self.connection.execute(
                """
                INSERT INTO observation_triage (observation_id, triager_name, status)
                VALUES (?, ?, 'processing')
                """,
                (observation_id, triager_name),
            )
        except sqlite3.IntegrityError:
            return False
        self.connection.commit()
        return True

    def mark_triage_success(self, observation_id: int, triager_name: str) -> None:
        self.connection.execute(
            """
            UPDATE observation_triage
            SET status = 'success', updated_at = ?
            WHERE observation_id = ? AND triager_name = ?
            """,
            (utcnow(), observation_id, triager_name),
        )
        self.connection.commit()

    def mark_triage_failed(
        self, observation_id: int, triager_name: str, error: str
    ) -> None:
        self.connection.execute(
            """
            UPDATE observation_triage
            SET status = 'failed', last_error = ?, updated_at = ?
            WHERE observation_id = ? AND triager_name = ?
            """,
            (error, utcnow(), observation_id, triager_name),
        )
        self.connection.commit()

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
        observation_id: int,
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
        return list(
            self.connection.execute(
                """
                SELECT pq.finding_id, pq.pipe, f.*
                FROM pipe_queues pq
                JOIN findings f ON f.id = pq.finding_id
                WHERE pq.pipe = ? AND pq.status = 'pending' AND f.status = 'ready'
                ORDER BY pq.created_at, pq.finding_id
                LIMIT 20
                """,
                (pipe,),
            )
        )

    def record_dispatch(
        self,
        pipe: str,
        channel: str,
        finding_ids: list[int],
        status: str,
        error: str | None = None,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO dispatches (
                pipe, channel, finding_ids, status, attempts, last_error, dispatched_at
            )
            VALUES (?, ?, ?, ?, 1, ?, ?)
            """,
            (pipe, channel, json.dumps(finding_ids), status, error, utcnow()),
        )
        if status == "sent":
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

    def _write_body(self, kind: str, row_id: int, body: dict[str, Any]) -> str:
        date = utcnow()[:10]
        directory = self.root / kind / date / str(row_id)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / "body.json"
        path.write_text(json.dumps(body or {}, sort_keys=True) + "\n", encoding="utf-8")
        return str(path.relative_to(self.root))

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
