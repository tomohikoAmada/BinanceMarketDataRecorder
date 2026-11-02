"""Capacity space sampling, forecasts, metric batches, and deployment sessions."""

from __future__ import annotations

import json
import sqlite3
import time
from collections.abc import Iterator, Mapping, Sequence
from contextlib import suppress

from .base import CatalogBase
from .types import DEPLOYMENT_TRANSITIONS, CatalogStateError, DeploymentState


class CatalogReportsMixin(CatalogBase):
    """Space forecasting, daily metric batches, and deployment audit tracking."""

    def record_space_sample(
        self,
        *,
        sample_id: str,
        scope_id: str,
        storage_id: str | None,
        observed_at_utc_ns: int,
        total_bytes: int,
        free_bytes: int,
        archive_backlog_bytes: int,
        oldest_unarchived_at_utc_ns: int | None,
        severity: str,
    ) -> bool:
        if (
            not sample_id
            or not scope_id
            or not severity
            or observed_at_utc_ns < 0
            or total_bytes <= 0
            or free_bytes < 0
            or free_bytes > total_bytes
            or archive_backlog_bytes < 0
            or (oldest_unarchived_at_utc_ns is not None and oldest_unarchived_at_utc_ns < 0)
        ):
            raise ValueError("invalid storage space sample")
        with self._transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM storage_space_samples WHERE sample_id = ?",
                (sample_id,),
            ).fetchone():
                return False
            connection.execute(
                """
                INSERT INTO storage_space_samples(
                    sample_id, scope_id, storage_id, observed_at_utc_ns,
                    total_bytes, free_bytes, archive_backlog_bytes,
                    oldest_unarchived_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    sample_id,
                    scope_id,
                    storage_id,
                    observed_at_utc_ns,
                    total_bytes,
                    free_bytes,
                    archive_backlog_bytes,
                    oldest_unarchived_at_utc_ns,
                ),
            )
            current = connection.execute(
                "SELECT severity FROM storage_alert_state WHERE scope_id = ?",
                (scope_id,),
            ).fetchone()
            previous = str(current["severity"]) if current is not None else None
            if previous != severity:
                evidence = json.dumps(
                    {
                        "free_bytes": free_bytes,
                        "total_bytes": total_bytes,
                        "sample_id": sample_id,
                    },
                    sort_keys=True,
                    separators=(",", ":"),
                )
                connection.execute(
                    """
                    INSERT INTO storage_alert_events(
                        scope_id, from_severity, to_severity,
                        occurred_at_utc_ns, evidence_json, idempotency_key
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        scope_id,
                        previous,
                        severity,
                        observed_at_utc_ns,
                        evidence,
                        f"space-severity:{sample_id}",
                    ),
                )
                connection.execute(
                    """
                    INSERT INTO storage_alert_state(
                        scope_id, severity, updated_at_utc_ns
                    ) VALUES (?, ?, ?)
                    ON CONFLICT(scope_id) DO UPDATE SET
                        severity = excluded.severity,
                        updated_at_utc_ns = excluded.updated_at_utc_ns
                    """,
                    (scope_id, severity, observed_at_utc_ns),
                )
        return True

    def space_samples(self, scope_id: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM storage_space_samples
                WHERE scope_id = ?
                ORDER BY observed_at_utc_ns, sample_id
                """,
                (scope_id,),
            ).fetchall()
        return [dict(row) for row in rows]

    def space_samples_between(
        self,
        scope_id: str,
        *,
        start_exclusive_utc_ns: int,
        end_inclusive_utc_ns: int,
    ) -> list[dict[str, object]]:
        """Return ordered space samples in one explicit bounded time range."""

        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM storage_space_samples
                WHERE scope_id = ?
                  AND observed_at_utc_ns > ?
                  AND observed_at_utc_ns <= ?
                ORDER BY observed_at_utc_ns, sample_id
                """,
                (scope_id, start_exclusive_utc_ns, end_inclusive_utc_ns),
            ).fetchall()
        return [dict(row) for row in rows]

    def latest_space_sample_at_or_before(
        self, scope_id: str, observed_at_utc_ns: int
    ) -> dict[str, object] | None:
        """Return the current-sample winner at or before a timestamp."""

        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM storage_space_samples
                WHERE scope_id = ? AND observed_at_utc_ns <= ?
                ORDER BY observed_at_utc_ns DESC, sample_id ASC
                LIMIT 1
                """,
                (scope_id, observed_at_utc_ns),
            ).fetchone()
        return dict(row) if row is not None else None

    def latest_predecessor_space_sample_at_or_before(
        self, scope_id: str, observed_at_utc_ns: int
    ) -> dict[str, object] | None:
        """Return the old window-anchor winner at or before a timestamp."""

        with self._lock:
            row = self._connection.execute(
                """
                SELECT * FROM storage_space_samples
                WHERE scope_id = ? AND observed_at_utc_ns <= ?
                ORDER BY observed_at_utc_ns DESC, sample_id DESC
                LIMIT 1
                """,
                (scope_id, observed_at_utc_ns),
            ).fetchone()
        return dict(row) if row is not None else None

    def space_sample_count(self, scope_id: str) -> int:
        """Return the exact historical sample count for one scope."""

        with self._lock:
            row = self._connection.execute(
                """
                SELECT COUNT(*) AS sample_count
                FROM storage_space_samples
                WHERE scope_id = ?
                """,
                (scope_id,),
            ).fetchone()
        if row is None:
            raise CatalogStateError("storage space sample count is unavailable")
        return int(row["sample_count"])

    def space_forecast_read_set(
        self,
        scope_id: str,
        *,
        start_exclusive_utc_ns: int,
        end_inclusive_utc_ns: int,
    ) -> tuple[
        list[dict[str, object]],
        dict[str, object] | None,
        dict[str, object] | None,
        int,
    ]:
        """Read all forecast inputs from one deferred SQLite read snapshot.

        The bounded range is deliberately the first read after ``BEGIN`` so
        SQLite pins one WAL snapshot before the remaining forecast inputs are
        read.  Python forecast calculations happen after this method commits.
        """

        with self._lock:
            self._connection.execute("BEGIN")
            try:
                sample_rows = self._connection.execute(
                    """
                    SELECT * FROM storage_space_samples
                    WHERE scope_id = ?
                      AND observed_at_utc_ns > ?
                      AND observed_at_utc_ns <= ?
                    ORDER BY observed_at_utc_ns, sample_id
                    """,
                    (scope_id, start_exclusive_utc_ns, end_inclusive_utc_ns),
                ).fetchall()
                predecessor_row = self._connection.execute(
                    """
                    SELECT * FROM storage_space_samples
                    WHERE scope_id = ? AND observed_at_utc_ns <= ?
                    ORDER BY observed_at_utc_ns DESC, sample_id DESC
                    LIMIT 1
                    """,
                    (scope_id, start_exclusive_utc_ns),
                ).fetchone()
                current_row = self._connection.execute(
                    """
                    SELECT * FROM storage_space_samples
                    WHERE scope_id = ? AND observed_at_utc_ns <= ?
                    ORDER BY observed_at_utc_ns DESC, sample_id ASC
                    LIMIT 1
                    """,
                    (scope_id, end_inclusive_utc_ns),
                ).fetchone()
                count_row = self._connection.execute(
                    """
                    SELECT COUNT(*) AS sample_count
                    FROM storage_space_samples
                    WHERE scope_id = ?
                    """,
                    (scope_id,),
                ).fetchone()
                if count_row is None:
                    raise CatalogStateError("storage space sample count is unavailable")
                read_set = (
                    [dict(row) for row in sample_rows],
                    dict(predecessor_row) if predecessor_row is not None else None,
                    dict(current_row) if current_row is not None else None,
                    int(count_row["sample_count"]),
                )
            except BaseException:
                if self._connection.in_transaction:
                    with suppress(sqlite3.Error):
                        self._connection.execute("ROLLBACK")
                raise
            else:
                self._connection.execute("COMMIT")
        return read_set

    def storage_alert_events(self, *, scope_id: str | None = None) -> list[dict[str, object]]:
        query = "SELECT * FROM storage_alert_events"
        parameters: tuple[object, ...] = ()
        if scope_id is not None:
            query += " WHERE scope_id = ?"
            parameters = (scope_id,)
        query += " ORDER BY occurred_at_utc_ns, event_id"
        with self._lock:
            rows = self._connection.execute(query, parameters).fetchall()
        output: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            evidence_json = document.pop("evidence_json")
            document["evidence"] = json.loads(str(evidence_json))
            output.append(document)
        return output

    def create_deployment(
        self,
        *,
        deployment_id: str,
        reason: str,
        market: str,
        symbol: str,
        active_instance_id: str,
        active_version: str,
        candidate_instance_id: str,
        candidate_version: str,
        occurred_at_utc_ns: int,
        evidence: Mapping[str, object],
    ) -> None:
        identity = (
            deployment_id,
            reason,
            market,
            symbol,
            active_instance_id,
            active_version,
            candidate_instance_id,
            candidate_version,
        )
        if not all(identity) or occurred_at_utc_ns < 0:
            raise ValueError("invalid deployment identity")
        evidence_json = json.dumps(dict(evidence), sort_keys=True, separators=(",", ":"))
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT * FROM deployment_sessions WHERE deployment_id = ?",
                (deployment_id,),
            ).fetchone()
            if existing is not None:
                expected = {
                    "reason": reason,
                    "market": market,
                    "symbol": symbol,
                    "active_instance_id": active_instance_id,
                    "active_version": active_version,
                    "candidate_instance_id": candidate_instance_id,
                    "candidate_version": candidate_version,
                }
                if any(existing[key] != value for key, value in expected.items()):
                    raise CatalogStateError("deployment identity changed")
                return
            connection.execute(
                """
                INSERT INTO deployment_sessions(
                    deployment_id, reason, market, symbol,
                    active_instance_id, active_version,
                    candidate_instance_id, candidate_version,
                    state, created_at_utc_ns, updated_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    *identity,
                    DeploymentState.CANDIDATE_STARTING,
                    occurred_at_utc_ns,
                    occurred_at_utc_ns,
                ),
            )
            connection.execute(
                """
                INSERT INTO deployment_events(
                    deployment_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, NULL, ?, ?, ?, ?)
                """,
                (
                    deployment_id,
                    DeploymentState.CANDIDATE_STARTING,
                    occurred_at_utc_ns,
                    evidence_json,
                    f"deployment-start:{deployment_id}",
                ),
            )

    def transition_deployment(
        self,
        deployment_id: str,
        to_state: DeploymentState,
        *,
        idempotency_key: str,
        evidence: Mapping[str, object],
        occurred_at_utc_ns: int,
        error: str | None = None,
    ) -> None:
        if not deployment_id or not idempotency_key or occurred_at_utc_ns < 0:
            raise ValueError("invalid deployment transition")
        evidence_json = json.dumps(dict(evidence), sort_keys=True, separators=(",", ":"))
        with self._transaction() as connection:
            if connection.execute(
                "SELECT 1 FROM deployment_events WHERE idempotency_key = ?",
                (idempotency_key,),
            ).fetchone():
                return
            row = connection.execute(
                "SELECT state FROM deployment_sessions WHERE deployment_id = ?",
                (deployment_id,),
            ).fetchone()
            if row is None:
                raise CatalogStateError(f"unknown deployment {deployment_id}")
            from_state = DeploymentState(row["state"])
            if to_state not in DEPLOYMENT_TRANSITIONS[from_state]:
                raise CatalogStateError(f"invalid deployment transition {from_state} -> {to_state}")
            overlap_started = (
                occurred_at_utc_ns if to_state is DeploymentState.OVERLAP_CONFIRMED else None
            )
            cutover = occurred_at_utc_ns if to_state is DeploymentState.CUTOVER_COMPLETE else None
            connection.execute(
                """
                UPDATE deployment_sessions
                SET state = ?, updated_at_utc_ns = ?,
                    overlap_started_at_utc_ns =
                        COALESCE(?, overlap_started_at_utc_ns),
                    cutover_at_utc_ns = COALESCE(?, cutover_at_utc_ns),
                    last_error = ?
                WHERE deployment_id = ?
                """,
                (
                    to_state,
                    occurred_at_utc_ns,
                    overlap_started,
                    cutover,
                    error,
                    deployment_id,
                ),
            )
            connection.execute(
                """
                INSERT INTO deployment_events(
                    deployment_id, from_state, to_state, occurred_at_utc_ns,
                    evidence_json, idempotency_key
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    deployment_id,
                    from_state,
                    to_state,
                    occurred_at_utc_ns,
                    evidence_json,
                    idempotency_key,
                ),
            )

    def deployment(self, deployment_id: str) -> dict[str, object] | None:
        with self._lock:
            row = self._connection.execute(
                "SELECT * FROM deployment_sessions WHERE deployment_id = ?",
                (deployment_id,),
            ).fetchone()
        return dict(row) if row else None

    def deployments(self) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM deployment_sessions
                ORDER BY created_at_utc_ns, deployment_id
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def deployment_events(self, deployment_id: str) -> list[dict[str, object]]:
        with self._lock:
            rows = self._connection.execute(
                """
                SELECT * FROM deployment_events
                WHERE deployment_id = ?
                ORDER BY event_id
                """,
                (deployment_id,),
            ).fetchall()
        result: list[dict[str, object]] = []
        for row in rows:
            document = dict(row)
            document["evidence"] = json.loads(str(document.pop("evidence_json")))
            result.append(document)
        return result

    def record_metric_batch(
        self,
        *,
        batch_id: str,
        rows: Sequence[tuple[str, str, str, Mapping[str, object]]],
        committed_at_utc_ns: int | None = None,
    ) -> bool:
        """Atomically persist aggregate rows; retrying a batch never double-counts."""

        if not batch_id or not rows:
            raise ValueError("metric batch requires an identity and at least one row")
        committed_at = time.time_ns() if committed_at_utc_ns is None else committed_at_utc_ns
        if committed_at < 0:
            raise ValueError("metric batch commit time must be non-negative")
        serialized = [
            (
                batch_id,
                utc_date,
                market,
                stream,
                json.dumps(dict(aggregate), sort_keys=True, separators=(",", ":")),
                committed_at,
            )
            for utc_date, market, stream, aggregate in rows
        ]
        if any(not utc_date or not market or not stream for utc_date, market, stream, _ in rows):
            raise ValueError("metric batch row identity must be non-empty")
        with self._transaction() as connection:
            existing = connection.execute(
                "SELECT COUNT(*) AS count FROM metric_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
            if existing is not None and int(existing["count"]) > 0:
                return False
            connection.executemany(
                """
                INSERT INTO metric_batches(
                    batch_id, utc_date, market, stream, aggregate_json,
                    committed_at_utc_ns
                ) VALUES (?, ?, ?, ?, ?, ?)
                """,
                serialized,
            )
        return True

    def metric_batches(self, utc_date: str) -> list[dict[str, object]]:
        return self._metric_batch_rows("utc_date = ?", (utc_date,))

    def metric_batches_through(self, utc_date: str) -> list[dict[str, object]]:
        return self._metric_batch_rows("utc_date <= ?", (utc_date,))

    def iter_metric_batches_through(
        self,
        utc_date: str,
        *,
        start_date: str = "",
    ) -> Iterator[dict[str, object]]:
        """Page historical report inputs without pinning a long WAL read transaction."""
        after = (start_date, "", "", "")
        while True:
            with self._lock:
                cursor = self._connection.execute(
                    "SELECT batch_id, utc_date, market, stream, aggregate_json, "
                    "committed_at_utc_ns FROM metric_batches WHERE utc_date <= ? "
                    "AND (utc_date, market, stream, batch_id) > (?, ?, ?, ?) "
                    "ORDER BY utc_date, market, stream, batch_id LIMIT 512",
                    (utc_date, *after),
                )
                rows = cursor.fetchall()
                cursor.close()
            if not rows:
                return
            for row in rows:
                yield {
                    "batch_id": str(row["batch_id"]),
                    "utc_date": str(row["utc_date"]),
                    "market": str(row["market"]),
                    "stream": str(row["stream"]),
                    "aggregate": json.loads(row["aggregate_json"]),
                    "committed_at_utc_ns": int(row["committed_at_utc_ns"]),
                }
            last = rows[-1]
            after = (
                str(last["utc_date"]),
                str(last["market"]),
                str(last["stream"]),
                str(last["batch_id"]),
            )

    def report_dates_before(self, utc_date: str) -> list[str]:
        """Committed metric/remote-authority dates for the separate report process."""
        with self._lock:
            rows = self._connection.execute(
                "SELECT DISTINCT utc_date FROM metric_batches WHERE utc_date < ? "
                "ORDER BY utc_date DESC",
                (utc_date,),
            ).fetchall()
            days = {str(row[0]) for row in rows}
            if self._remote_schema_present:
                rows = self._connection.execute(
                    "SELECT DISTINCT date(created_at_utc_ns / 1000000000, 'unixepoch') AS day "
                    "FROM remote_archive_transactions WHERE day < ?",
                    (utc_date,),
                ).fetchall()
                days.update(str(row[0]) for row in rows)
        return sorted(days, reverse=True)

    def daily_report_input_token(self, utc_date: str, end_utc_ns: int) -> dict[str, list[int]]:
        """Scheduling revision only: report building still validates authority."""
        with self._lock:
            metrics = self._connection.execute(
                "SELECT COUNT(*), COALESCE(MAX(rowid), 0) FROM metric_batches WHERE utc_date <= ?",
                (utc_date,),
            ).fetchone()
            remote = (0, 0)
            remote_events = (0, 0)
            if self._remote_schema_present:
                remote = self._connection.execute(
                    "SELECT COUNT(*), COALESCE(MAX(rowid), 0) "
                    "FROM remote_archive_transactions WHERE created_at_utc_ns < ?",
                    (end_utc_ns,),
                ).fetchone()
                remote_events = self._connection.execute(
                    "SELECT COUNT(*), COALESCE(MAX(e.event_id), 0) "
                    "FROM remote_archive_events e JOIN remote_archive_transactions t "
                    "ON t.receipt_id = e.receipt_id WHERE t.created_at_utc_ns < ?",
                    (end_utc_ns,),
                ).fetchone()
        return {
            "metrics": [int(v) for v in metrics],
            "remote": [int(v) for v in remote],
            "remote_events": [int(v) for v in remote_events],
        }

    def _metric_batch_rows(
        self, where_clause: str, parameters: tuple[object, ...]
    ) -> list[dict[str, object]]:
        if where_clause not in {"utc_date = ?", "utc_date <= ?"}:
            raise ValueError("unsupported metric batch query")
        with self._lock:
            rows = self._connection.execute(
                f"""
                SELECT batch_id, utc_date, market, stream, aggregate_json,
                       committed_at_utc_ns
                FROM metric_batches
                WHERE {where_clause}
                ORDER BY utc_date, market, stream, batch_id
                """,
                parameters,
            ).fetchall()
        return [
            {
                "batch_id": str(row["batch_id"]),
                "utc_date": str(row["utc_date"]),
                "market": str(row["market"]),
                "stream": str(row["stream"]),
                "aggregate": json.loads(row["aggregate_json"]),
                "committed_at_utc_ns": int(row["committed_at_utc_ns"]),
            }
            for row in rows
        ]

    def metric_batch_count(self, batch_id: str) -> int:
        with self._lock:
            row = self._connection.execute(
                "SELECT COUNT(*) AS count FROM metric_batches WHERE batch_id = ?",
                (batch_id,),
            ).fetchone()
        return int(row["count"])
