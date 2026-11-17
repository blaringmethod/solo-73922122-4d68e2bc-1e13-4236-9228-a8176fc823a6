"""SQLite storage.

Consistency units
-----------------
*The active dataset* is the union of samples and batches ingested so far. Each
batch number identifies one physical shipment and carries a monotonically
increasing ``version``. Batches are never mutated in place: corrections insert
a new version and mark the old row ``superseded``.

*A trace* is a query snapshot. On acceptance the current intake id and every
live batch version are recorded. The result rows and the ``complete`` flag are
written in one transaction, so a reader never observes a partially published
result; a failure leaves zero result rows.

*Invalidation*: a correction marks every completed trace that pinned the
superseded version. Reading such a trace returns 409 instead of stale data.

*Scan receipts* are append-only facts keyed by a client-supplied ``scan_id``.
Re-submitting identical content is a no-op; conflicting content is rejected.
Receipts are never updated or deleted, so the set of receipts for a batch is
exactly identified by the maximum row id (the *highwater mark*).

*A reconciliation* pins one batch version and that batch's receipt highwater
mark at acceptance. Findings and the ``complete`` flag are written in one
transaction after re-checking both pins, so a report is never half-published
and never published over inputs that moved during computation. A published
report is marked stale (``reconciliation_invalidations``) when a new receipt
for its batch is recorded or its batch is corrected; identical re-reports and
unrelated batches do not affect it.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

UTC = timezone.utc

SCHEMA = """
PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS intakes (
    id         INTEGER PRIMARY KEY AUTOINCREMENT,
    status     TEXT NOT NULL CHECK (status IN ('active', 'archived')),
    created_at TEXT NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS intakes_one_active
    ON intakes(status) WHERE status = 'active';

CREATE TABLE IF NOT EXISTS samples (
    intake_id    INTEGER NOT NULL REFERENCES intakes(id) ON DELETE CASCADE,
    sample_id    TEXT NOT NULL,
    contaminated INTEGER NOT NULL DEFAULT 0 CHECK (contaminated IN (0, 1)),
    PRIMARY KEY (intake_id, sample_id)
);

CREATE TABLE IF NOT EXISTS batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    intake_id   INTEGER NOT NULL REFERENCES intakes(id) ON DELETE CASCADE,
    batch_no    TEXT NOT NULL,
    version     INTEGER NOT NULL CHECK (version >= 1),
    origin      TEXT NOT NULL,
    destination TEXT NOT NULL,
    departed_at TEXT NOT NULL,
    arrived_at  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    superseded  INTEGER NOT NULL DEFAULT 0 CHECK (superseded IN (0, 1)),
    UNIQUE (intake_id, batch_no, version)
);

CREATE UNIQUE INDEX IF NOT EXISTS batches_one_live
    ON batches(intake_id, batch_no) WHERE superseded = 0;

CREATE TABLE IF NOT EXISTS sample_batches (
    intake_id INTEGER NOT NULL REFERENCES intakes(id) ON DELETE CASCADE,
    sample_id TEXT NOT NULL,
    batch_no  TEXT NOT NULL,
    PRIMARY KEY (intake_id, sample_id, batch_no)
);

CREATE TABLE IF NOT EXISTS traces (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    intake_id    INTEGER NOT NULL REFERENCES intakes(id),
    status       TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'failed')),
    accepted_at  TEXT NOT NULL,
    completed_at TEXT,
    error        TEXT,
    sources_json TEXT NOT NULL,
    pinned_json  TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS trace_results (
    trace_id         INTEGER NOT NULL REFERENCES traces(id) ON DELETE CASCADE,
    rank             INTEGER NOT NULL,
    sample_id        TEXT NOT NULL,
    first_arrival    TEXT NOT NULL,
    quarantine_until TEXT NOT NULL,
    path_json        TEXT NOT NULL,
    PRIMARY KEY (trace_id, sample_id)
);

CREATE TABLE IF NOT EXISTS trace_invalidations (
    trace_id       INTEGER NOT NULL REFERENCES traces(id) ON DELETE CASCADE,
    batch_no       TEXT NOT NULL,
    version        INTEGER NOT NULL,
    invalidated_at TEXT NOT NULL,
    reason         TEXT NOT NULL,
    PRIMARY KEY (trace_id, batch_no, version)
);

CREATE TABLE IF NOT EXISTS scans (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    intake_id   INTEGER NOT NULL REFERENCES intakes(id) ON DELETE CASCADE,
    scan_id     TEXT NOT NULL,
    batch_no    TEXT NOT NULL,
    direction   TEXT NOT NULL CHECK (direction IN ('departure', 'arrival')),
    site        TEXT NOT NULL,
    scanned_at  TEXT NOT NULL,
    created_at  TEXT NOT NULL,
    UNIQUE (intake_id, scan_id)
);

CREATE INDEX IF NOT EXISTS scans_by_batch ON scans(intake_id, batch_no, id);

CREATE TABLE IF NOT EXISTS reconciliations (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    intake_id      INTEGER NOT NULL REFERENCES intakes(id),
    batch_no       TEXT NOT NULL,
    status         TEXT NOT NULL CHECK (status IN ('pending', 'complete', 'failed')),
    batch_version  INTEGER NOT NULL,
    scan_highwater INTEGER NOT NULL,
    scan_count     INTEGER NOT NULL,
    accepted_at    TEXT NOT NULL,
    completed_at   TEXT,
    error          TEXT
);

CREATE TABLE IF NOT EXISTS reconciliation_findings (
    reconciliation_id INTEGER NOT NULL REFERENCES reconciliations(id) ON DELETE CASCADE,
    rank         INTEGER NOT NULL,
    kind         TEXT NOT NULL,
    direction    TEXT NOT NULL,
    detail_json  TEXT NOT NULL,
    PRIMARY KEY (reconciliation_id, rank)
);

CREATE TABLE IF NOT EXISTS reconciliation_invalidations (
    reconciliation_id INTEGER NOT NULL REFERENCES reconciliations(id) ON DELETE CASCADE,
    reason         TEXT NOT NULL,
    invalidated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS request_log (
    id      INTEGER PRIMARY KEY AUTOINCREMENT,
    kind    TEXT NOT NULL,
    at      TEXT NOT NULL,
    payload TEXT NOT NULL
);
"""

_SCHEMA_LOCK = threading.Lock()


def utcnow() -> datetime:
    return datetime.now(UTC)


def iso(dt: datetime) -> str:
    """Fixed-width UTC string; lexicographic order equals chronological order."""
    return dt.astimezone(UTC).isoformat(timespec="microseconds")


def parse_iso(value: str) -> datetime:
    return datetime.fromisoformat(value)


class StoreError(Exception):
    """Base class for expected storage failures."""


class ConflictError(StoreError):
    """The request conflicts with current state (HTTP 409)."""


class NotFoundError(StoreError):
    """Unknown record (HTTP 404)."""


class InvalidReferenceError(StoreError):
    """Payload refers to something that is not in the dataset (HTTP 422)."""


class Store:
    def __init__(self, path: str | Path) -> None:
        self.path = str(path)
        self._lock = threading.RLock()
        self._ready = False
        self._memory_conn: sqlite3.Connection | None = None
        if self.path != ":memory:":
            Path(self.path).expanduser().resolve().parent.mkdir(
                parents=True, exist_ok=True
            )

    def _ensure_schema(self) -> None:
        if self._ready:
            return
        with _SCHEMA_LOCK:
            if self._ready:
                return
            conn = self._new_connection()
            try:
                conn.executescript(SCHEMA)
            finally:
                if self.path == ":memory:":
                    self._memory_conn = conn
                else:
                    conn.close()
            self._ready = True

    def _new_connection(self) -> sqlite3.Connection:
        conn = sqlite3.connect(
            self.path,
            timeout=30,
            isolation_level=None,
            check_same_thread=self.path == ":memory:",
        )
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=30000")
        return conn

    @contextmanager
    def connection(self) -> Iterator[sqlite3.Connection]:
        self._ensure_schema()
        if self._memory_conn is not None:
            # One connection backs an in-memory database; serialise access.
            with self._lock:
                yield self._memory_conn
            return
        conn = self._new_connection()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """Serialised write transaction (BEGIN IMMEDIATE)."""
        self._ensure_schema()
        if self._memory_conn is not None:
            with self._lock:
                self._memory_conn.execute("BEGIN IMMEDIATE")
                try:
                    yield self._memory_conn
                except BaseException:
                    self._memory_conn.execute("ROLLBACK")
                    raise
                self._memory_conn.execute("COMMIT")
            return

        conn = self._new_connection()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            conn.execute("COMMIT")
        finally:
            conn.close()

    def mark_interrupted(self, *, older_than: timedelta = timedelta(minutes=10)) -> int:
        """Fail stale pending traces/reconciliations left by a crashed process.

        Only rows accepted more than ``older_than`` ago are touched. With
        several API workers sharing one database, another worker may legitimately
        be computing right now; the grace period keeps startup recovery from
        failing live work. A pending row has no published results, so marking
        it failed never exposes partial data.
        """
        cutoff = iso(utcnow() - older_than)
        now = iso(utcnow())
        with self.transaction() as conn:
            cur = conn.execute(
                "UPDATE traces SET status='failed', completed_at=?,"
                " error=? WHERE status='pending' AND accepted_at<?",
                (now, "interrupted before completion", cutoff),
            )
            cur2 = conn.execute(
                "UPDATE reconciliations SET status='failed', completed_at=?,"
                " error=? WHERE status='pending' AND accepted_at<?",
                (now, "interrupted before completion", cutoff),
            )
            return cur.rowcount + cur2.rowcount

    # ----------------------------------------------------------------- intake

    @staticmethod
    def _active_intake_id(conn: sqlite3.Connection, *, create: bool) -> int | None:
        row = conn.execute(
            "SELECT id FROM intakes WHERE status='active'"
        ).fetchone()
        if row is not None:
            return int(row["id"])
        if not create:
            return None
        cur = conn.execute(
            "INSERT INTO intakes(status, created_at) VALUES ('active', ?)",
            (iso(utcnow()),),
        )
        return int(cur.lastrowid)

    def ingest(
        self,
        *,
        samples: list[dict[str, Any]],
        batches: list[dict[str, Any]],
        source_sample_ids: list[str],
    ) -> dict[str, Any]:
        """Add samples/batches to the active dataset atomically.

        Identical re-submissions are idempotent. A batch number whose live
        values differ from the submitted values is rejected; it must go through
        :meth:`correct_batch` with an explicit ``seen_version``.

        Raises:
            ConflictError: conflicting batch definition, or unknown source id.
        """
        now = iso(utcnow())
        with self.transaction() as conn:
            intake_id = self._active_intake_id(conn, create=True)
            assert intake_id is not None

            inserted_batches = 0
            for batch in batches:
                live = conn.execute(
                    "SELECT * FROM batches WHERE intake_id=? AND batch_no=?"
                    " AND superseded=0",
                    (intake_id, batch["batch_no"]),
                ).fetchone()
                if live is not None:
                    same = (
                        live["origin"] == batch["origin"]
                        and live["destination"] == batch["destination"]
                        and live["departed_at"] == iso(batch["departed_at"])
                        and live["arrived_at"] == iso(batch["arrived_at"])
                    )
                    if same:
                        continue
                    raise ConflictError(
                        f"batch {batch['batch_no']!r} already exists at version"
                        f" {live['version']} with different values; submit a"
                        " correction with seen_version instead"
                    )
                conn.execute(
                    "INSERT INTO batches(intake_id, batch_no, version, origin,"
                    " destination, departed_at, arrived_at, created_at)"
                    " VALUES (?, ?, 1, ?, ?, ?, ?, ?)",
                    (
                        intake_id,
                        batch["batch_no"],
                        batch["origin"],
                        batch["destination"],
                        iso(batch["departed_at"]),
                        iso(batch["arrived_at"]),
                        now,
                    ),
                )
                inserted_batches += 1

            inserted_samples = 0
            for sample in samples:
                missing = [
                    batch_no
                    for batch_no in sample["batch_nos"]
                    if conn.execute(
                        "SELECT 1 FROM batches WHERE intake_id=? AND batch_no=?"
                        " AND superseded=0",
                        (intake_id, batch_no),
                    ).fetchone()
                    is None
                ]
                if missing:
                    raise InvalidReferenceError(
                        f"sample {sample['sample_id']!r} references unknown"
                        f" batch(es): {', '.join(sorted(missing))}"
                    )
                existing = conn.execute(
                    "SELECT contaminated FROM samples WHERE intake_id=?"
                    " AND sample_id=?",
                    (intake_id, sample["sample_id"]),
                ).fetchone()
                if existing is None:
                    conn.execute(
                        "INSERT INTO samples(intake_id, sample_id, contaminated)"
                        " VALUES (?, ?, ?)",
                        (
                            intake_id,
                            sample["sample_id"],
                            int(sample["contaminated"]),
                        ),
                    )
                    inserted_samples += 1
                elif sample["contaminated"] and not existing["contaminated"]:
                    conn.execute(
                        "UPDATE samples SET contaminated=1 WHERE intake_id=?"
                        " AND sample_id=?",
                        (intake_id, sample["sample_id"]),
                    )
                for batch_no in sample["batch_nos"]:
                    conn.execute(
                        "INSERT OR IGNORE INTO sample_batches(intake_id,"
                        " sample_id, batch_no) VALUES (?, ?, ?)",
                        (intake_id, sample["sample_id"], batch_no),
                    )
            for sample_id in source_sample_ids:
                updated = conn.execute(
                    "UPDATE samples SET contaminated=1 WHERE intake_id=?"
                    " AND sample_id=?",
                    (intake_id, sample_id),
                ).rowcount
                if not updated:
                    raise InvalidReferenceError(
                        f"source sample {sample_id!r} not found in the dataset"
                    )

            dataset = self._dataset(conn, intake_id)
            return {
                "intake_id": intake_id,
                "created_at": dataset["created_at"],
                "sample_count": len(dataset["samples"]),
                "batch_count": len(dataset["batches"]),
                "source_sample_ids": dataset["source_sample_ids"],
                "inserted_batches": inserted_batches,
                "inserted_samples": inserted_samples,
            }

    # ------------------------------------------------------------------ reads

    @staticmethod
    def _dataset(
        conn: sqlite3.Connection, intake_id: int
    ) -> dict[str, Any]:
        intake = conn.execute(
            "SELECT created_at FROM intakes WHERE id=?", (intake_id,)
        ).fetchone()
        samples = conn.execute(
            "SELECT sample_id, contaminated FROM samples"
            " WHERE intake_id=? ORDER BY sample_id",
            (intake_id,),
        ).fetchall()
        links = conn.execute(
            "SELECT sample_id, batch_no FROM sample_batches"
            " WHERE intake_id=? ORDER BY sample_id, batch_no",
            (intake_id,),
        ).fetchall()
        batches = conn.execute(
            "SELECT batch_no, version, origin, destination, departed_at,"
            " arrived_at FROM batches WHERE intake_id=? AND superseded=0"
            " ORDER BY batch_no",
            (intake_id,),
        ).fetchall()
        by_sample: dict[str, list[str]] = {}
        for link in links:
            by_sample.setdefault(link["sample_id"], []).append(link["batch_no"])
        return {
            "intake_id": intake_id,
            "created_at": parse_iso(intake["created_at"]),
            "samples": [
                {
                    "sample_id": row["sample_id"],
                    "contaminated": bool(row["contaminated"]),
                    "batch_nos": by_sample.get(row["sample_id"], []),
                }
                for row in samples
            ],
            "batches": [dict(row) for row in batches],
            "source_sample_ids": [
                row["sample_id"] for row in samples if row["contaminated"]
            ],
        }

    def dataset(self) -> dict[str, Any] | None:
        with self.connection() as conn:
            intake = conn.execute(
                "SELECT id FROM intakes WHERE status='active'"
            ).fetchone()
            if intake is None:
                return None
            return self._dataset(conn, int(intake["id"]))

    def snapshot(self) -> dict[str, Any] | None:
        """One consistent read transaction over the active dataset.

        Returns the batches with the samples riding on them, plus the pinned
        version of every batch. ``None`` when no dataset exists yet. Used for
        read-only inspection; ``accept_trace`` takes its own snapshot inside
        the write transaction that pins it.
        """
        with self.connection() as conn:
            conn.execute("BEGIN")
            try:
                return self._snapshot_in(conn)
            finally:
                conn.execute("COMMIT")

    # ----------------------------------------------------------- corrections

    def correct_batch(
        self,
        *,
        batch_no: str,
        seen_version: int,
        origin: str,
        destination: str,
        departed_at: datetime,
        arrived_at: datetime,
    ) -> dict[str, Any]:
        """Insert a replacement version and invalidate dependent results.

        Raises:
            NotFoundError: batch unknown in the active dataset.
            ConflictError: stale ``seen_version`` or a no-op correction.
        """
        now = iso(utcnow())
        with self.transaction() as conn:
            intake_id = self._active_intake_id(conn, create=False)
            if intake_id is None:
                raise NotFoundError("no active dataset")
            current = conn.execute(
                "SELECT * FROM batches WHERE intake_id=? AND batch_no=?"
                " AND superseded=0",
                (intake_id, batch_no),
            ).fetchone()
            if current is None:
                raise NotFoundError(f"batch {batch_no!r} not found")
            version = int(current["version"])
            if version != seen_version:
                raise ConflictError(
                    f"stale version: batch {batch_no!r} is at version {version},"
                    f" client saw {seen_version}"
                )
            if (
                current["origin"] == origin
                and current["destination"] == destination
                and current["departed_at"] == iso(departed_at)
                and current["arrived_at"] == iso(arrived_at)
            ):
                raise ConflictError(
                    "correction does not change the batch; nothing was replaced"
                )

            conn.execute(
                "UPDATE batches SET superseded=1 WHERE id=?",
                (int(current["id"]),),
            )
            new_version = version + 1
            conn.execute(
                "INSERT INTO batches(intake_id, batch_no, version, origin,"
                " destination, departed_at, arrived_at, created_at, superseded)"
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?, 0)",
                (
                    intake_id,
                    batch_no,
                    new_version,
                    origin,
                    destination,
                    iso(departed_at),
                    iso(arrived_at),
                    now,
                ),
            )

            rows = conn.execute(
                "SELECT t.id FROM traces t, json_each(t.pinned_json) e"
                " WHERE t.intake_id=? AND t.status='complete'"
                " AND e.key=? AND CAST(e.value AS INTEGER)=?",
                (intake_id, batch_no, version),
            ).fetchall()
            invalidated = sorted(int(row["id"]) for row in rows)
            reason = (
                f"batch {batch_no!r} version {version} was superseded by"
                f" version {new_version}"
            )
            for trace_id in invalidated:
                conn.execute(
                    "INSERT OR IGNORE INTO trace_invalidations(trace_id,"
                    " batch_no, version, invalidated_at, reason)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (trace_id, batch_no, version, now, reason),
                )
            invalidated_reconciliations = self._invalidate_reconciliations(
                conn,
                intake_id,
                batch_no,
                reason=(
                    f"batch {batch_no!r} corrected from version {version} to"
                    f" version {new_version}"
                ),
            )
            return {
                "batch_no": batch_no,
                "previous_version": version,
                "version": new_version,
                "origin": origin,
                "destination": destination,
                "departed_at": departed_at,
                "arrived_at": arrived_at,
                "invalidated_trace_ids": invalidated,
                "invalidated_reconciliation_ids": invalidated_reconciliations,
            }

    # ---------------------------------------------------------------- traces

    def accept_trace(
        self,
        *,
        sources: list[str] | None,
        intake_id: int | None = None,
    ) -> tuple[int, dict[str, Any], list[str]]:
        """Atomically snapshot the dataset and pin the trace to it.

        Reading the snapshot and inserting the ``pending`` trace happen in one
        ``BEGIN IMMEDIATE`` transaction, so a concurrent intake or correction
        can never land between "what we saw" and "what we pinned".

        Args:
            sources: explicit source sample ids, or ``None`` to use every
                sample flagged contaminated in the snapshot.
            intake_id: retained for callers that already hold a snapshot; when
                given and the active intake differs, the request is rejected.

        Returns:
            ``(trace_id, snapshot, sources)`` where snapshot is the read-only
            data the computation must use.

        Raises:
            NotFoundError: no active dataset.
            InvalidReferenceError: an explicit source sample is unknown.
            ConflictError: the intake changed since the caller read it.
        """
        with self.transaction() as conn:
            snapshot = self._snapshot_in(conn)
            if snapshot is None:
                raise NotFoundError("no dataset")
            if intake_id is not None and intake_id != snapshot["intake_id"]:
                raise ConflictError("the dataset was replaced; re-run the trace")

            if sources is None:
                resolved = snapshot["source_sample_ids"]
            else:
                unknown = sorted(set(sources) - set(snapshot["all_sample_ids"]))
                if unknown:
                    raise InvalidReferenceError(
                        f"unknown source sample(s): {', '.join(unknown)}"
                    )
                resolved = list(sources)

            cur = conn.execute(
                "INSERT INTO traces(intake_id, status, accepted_at,"
                " sources_json, pinned_json) VALUES (?, 'pending', ?, ?, ?)",
                (
                    snapshot["intake_id"],
                    iso(utcnow()),
                    json.dumps(resolved),
                    json.dumps(snapshot["pinned"], sort_keys=True),
                ),
            )
            return int(cur.lastrowid), snapshot, resolved

    @staticmethod
    def _snapshot_in(conn: sqlite3.Connection) -> dict[str, Any] | None:
        intake = conn.execute(
            "SELECT id, created_at FROM intakes WHERE status='active'"
        ).fetchone()
        if intake is None:
            return None
        intake_id = int(intake["id"])
        samples = conn.execute(
            "SELECT sample_id, contaminated FROM samples"
            " WHERE intake_id=? ORDER BY sample_id",
            (intake_id,),
        ).fetchall()
        links = conn.execute(
            "SELECT sample_id, batch_no FROM sample_batches"
            " WHERE intake_id=? ORDER BY sample_id, batch_no",
            (intake_id,),
        ).fetchall()
        batches = conn.execute(
            "SELECT batch_no, version, origin, destination,"
            " departed_at, arrived_at FROM batches"
            " WHERE intake_id=? AND superseded=0 ORDER BY batch_no",
            (intake_id,),
        ).fetchall()
        riders: dict[str, list[str]] = {}
        for link in links:
            riders.setdefault(link["batch_no"], []).append(link["sample_id"])
        return {
            "intake_id": intake_id,
            "created_at": parse_iso(intake["created_at"]),
            "samples": [dict(row) for row in samples],
            "batches": [
                {**dict(row), "samples": riders.get(row["batch_no"], [])}
                for row in batches
            ],
            "pinned": {row["batch_no"]: int(row["version"]) for row in batches},
            "source_sample_ids": [
                row["sample_id"] for row in samples if row["contaminated"]
            ],
            "all_sample_ids": [row["sample_id"] for row in samples],
        }

    def publish_results(
        self,
        trace_id: int,
        results: list[dict[str, Any]],
        *,
        pinned: dict[str, int],
    ) -> None:
        """Publish every result row and the complete flag in one transaction.

        Refuses to publish if any pinned batch version has since changed.
        """
        now = iso(utcnow())
        with self.transaction() as conn:
            trace = conn.execute(
                "SELECT status FROM traces WHERE id=?", (trace_id,)
            ).fetchone()
            if trace is None:
                raise NotFoundError("trace not found")
            if trace["status"] != "pending":
                raise ConflictError(
                    f"trace {trace_id} is {trace['status']}, not pending"
                )
            live = {
                row["batch_no"]: int(row["version"])
                for row in conn.execute(
                    "SELECT batch_no, version FROM batches"
                    " WHERE intake_id=(SELECT intake_id FROM traces WHERE id=?)"
                    " AND superseded=0",
                    (trace_id,),
                ).fetchall()
            }
            changed = sorted(
                batch_no
                for batch_no, version in pinned.items()
                if live.get(batch_no) != version
            )
            if changed:
                raise ConflictError(
                    "snapshot went stale during computation:"
                    f" {', '.join(changed)}"
                )
            for rank, result in enumerate(results):
                conn.execute(
                    "INSERT INTO trace_results(trace_id, rank, sample_id,"
                    " first_arrival, quarantine_until, path_json)"
                    " VALUES (?, ?, ?, ?, ?, ?)",
                    (
                        trace_id,
                        rank,
                        result["sample_id"],
                        iso(result["first_arrival_at"]),
                        iso(result["quarantine_until"]),
                        json.dumps(
                            [
                                {
                                    "batch_no": leg["batch_no"],
                                    "origin": leg["origin"],
                                    "destination": leg["destination"],
                                    "departed_at": iso(leg["departed_at"]),
                                    "arrived_at": iso(leg["arrived_at"]),
                                    "version": int(leg["version"]),
                                }
                                for leg in result["path"]
                            ]
                        ),
                    ),
                )
            conn.execute(
                "UPDATE traces SET status='complete', completed_at=?, error=NULL"
                " WHERE id=?",
                (now, trace_id),
            )

    def fail_trace(self, trace_id: int, error: str) -> None:
        """Delete any partial rows and record the failure."""
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM trace_results WHERE trace_id=?", (trace_id,)
            )
            conn.execute(
                "UPDATE traces SET status='failed', completed_at=?, error=?"
                " WHERE id=? AND status='pending'",
                (iso(utcnow()), error, trace_id),
            )

    def read_trace(self, trace_id: int) -> dict[str, Any] | None:
        """Read a trace and everything needed to render it, atomically.

        A single read transaction ensures the snapshot header, its result rows
        and its invalidation markers are mutually consistent, so a correction
        committed mid-read cannot produce a half-stale response.

        Returns ``None`` for an unknown trace.
        """
        with self.connection() as conn:
            conn.execute("BEGIN")
            try:
                trace = conn.execute(
                    "SELECT * FROM traces WHERE id=?", (trace_id,)
                ).fetchone()
                if trace is None:
                    return None
                results = conn.execute(
                    "SELECT * FROM trace_results WHERE trace_id=? ORDER BY rank",
                    (trace_id,),
                ).fetchall()
                invalidations = conn.execute(
                    "SELECT * FROM trace_invalidations WHERE trace_id=?"
                    " ORDER BY invalidated_at, batch_no",
                    (trace_id,),
                ).fetchall()
                pinned = json.loads(trace["pinned_json"])
                pinned_rows = conn.execute(
                    "SELECT batch_no, version, origin, destination, departed_at,"
                    " arrived_at FROM batches WHERE intake_id=?"
                    " ORDER BY batch_no",
                    (int(trace["intake_id"]),),
                ).fetchall()
                return {
                    "trace": trace,
                    "results": results,
                    "invalidations": invalidations,
                    "pinned": pinned,
                    "batches": [
                        {
                            "batch_no": row["batch_no"],
                            "version": int(row["version"]),
                            "origin": row["origin"],
                            "destination": row["destination"],
                            "departed_at": parse_iso(row["departed_at"]),
                            "arrived_at": parse_iso(row["arrived_at"]),
                        }
                        for row in pinned_rows
                        if pinned.get(row["batch_no"]) == int(row["version"])
                    ],
                }
            finally:
                conn.execute("COMMIT")

    def trace(self, trace_id: int) -> sqlite3.Row | None:
        with self.connection() as conn:
            return conn.execute(
                "SELECT * FROM traces WHERE id=?", (trace_id,)
            ).fetchone()

    def trace_results(self, trace_id: int) -> list[sqlite3.Row]:
        with self.connection() as conn:
            return conn.execute(
                "SELECT * FROM trace_results WHERE trace_id=? ORDER BY rank",
                (trace_id,),
            ).fetchall()

    def invalidations(self, trace_id: int) -> list[sqlite3.Row]:
        with self.connection() as conn:
            return conn.execute(
                "SELECT * FROM trace_invalidations WHERE trace_id=?"
                " ORDER BY invalidated_at, batch_no",
                (trace_id,),
            ).fetchall()

    def trace_history(self) -> list[sqlite3.Row]:
        with self.connection() as conn:
            return conn.execute(
                "SELECT t.*,"
                " (SELECT COUNT(*) FROM trace_results r"
                "   WHERE r.trace_id=t.id) AS result_count,"
                " (SELECT COUNT(*) FROM trace_invalidations i"
                "   WHERE i.trace_id=t.id) AS invalidation_count"
                " FROM traces t ORDER BY t.id DESC"
            ).fetchall()

    # ------------------------------------------------------- scan receipts

    def record_scans(self, *, scans: list[dict[str, Any]]) -> dict[str, Any]:
        """Record scan receipts atomically.

        Each receipt carries a client-supplied ``scan_id``. Re-submitting a
        ``scan_id`` with identical content is an idempotent no-op; the same
        id with different content rejects the whole request. Receipts for
        unknown batches are rejected. Every newly recorded receipt marks the
        completed reconciliations of its batch stale, in the same transaction.

        Raises:
            NotFoundError: no active dataset.
            InvalidReferenceError: a receipt names an unknown batch.
            ConflictError: a ``scan_id`` was already recorded with different
                content.
        """
        now = iso(utcnow())
        with self.transaction() as conn:
            intake_id = self._active_intake_id(conn, create=False)
            if intake_id is None:
                raise NotFoundError("no active dataset")

            receipts: list[dict[str, Any]] = []
            inserted_by_batch: dict[str, list[str]] = {}
            for scan in scans:
                existing = conn.execute(
                    "SELECT * FROM scans WHERE intake_id=? AND scan_id=?",
                    (intake_id, scan["scan_id"]),
                ).fetchone()
                if existing is not None:
                    same = (
                        existing["batch_no"] == scan["batch_no"]
                        and existing["direction"] == scan["direction"]
                        and existing["site"] == scan["site"]
                        and existing["scanned_at"] == iso(scan["scanned_at"])
                    )
                    if same:
                        receipts.append({**scan, "outcome": "duplicate"})
                        continue
                    raise ConflictError(
                        f"scan {scan['scan_id']!r} was already recorded with"
                        " different content; refusing to record a second"
                        " version of the same scan id"
                    )
                live = conn.execute(
                    "SELECT 1 FROM batches WHERE intake_id=? AND batch_no=?"
                    " AND superseded=0",
                    (intake_id, scan["batch_no"]),
                ).fetchone()
                if live is None:
                    raise InvalidReferenceError(
                        f"scan {scan['scan_id']!r} references unknown batch"
                        f" {scan['batch_no']!r}"
                    )
                conn.execute(
                    "INSERT INTO scans(intake_id, scan_id, batch_no,"
                    " direction, site, scanned_at, created_at)"
                    " VALUES (?, ?, ?, ?, ?, ?, ?)",
                    (
                        intake_id,
                        scan["scan_id"],
                        scan["batch_no"],
                        scan["direction"],
                        scan["site"],
                        iso(scan["scanned_at"]),
                        now,
                    ),
                )
                receipts.append({**scan, "outcome": "recorded"})
                inserted_by_batch.setdefault(scan["batch_no"], []).append(
                    scan["scan_id"]
                )

            invalidated: list[int] = []
            for batch_no, scan_ids in inserted_by_batch.items():
                invalidated.extend(
                    self._invalidate_reconciliations(
                        conn,
                        intake_id,
                        batch_no,
                        reason=(
                            f"{len(scan_ids)} new scan receipt(s) recorded for"
                            f" batch {batch_no!r}: {', '.join(sorted(scan_ids))}"
                        ),
                    )
                )
            return {
                "recorded": sum(1 for r in receipts if r["outcome"] == "recorded"),
                "duplicates": sum(
                    1 for r in receipts if r["outcome"] == "duplicate"
                ),
                "scans": receipts,
                "invalidated_reconciliation_ids": sorted(set(invalidated)),
            }

    def scans_for_batch(self, batch_no: str) -> list[dict[str, Any]] | None:
        """All receipts for one batch, in recording order.

        Returns ``None`` when the batch is unknown in the active dataset.
        """
        with self.connection() as conn:
            intake_id = self._active_intake_id(conn, create=False)
            if intake_id is None:
                return None
            live = conn.execute(
                "SELECT 1 FROM batches WHERE intake_id=? AND batch_no=?"
                " AND superseded=0",
                (intake_id, batch_no),
            ).fetchone()
            if live is None:
                return None
            rows = conn.execute(
                "SELECT * FROM scans WHERE intake_id=? AND batch_no=?"
                " ORDER BY id",
                (intake_id, batch_no),
            ).fetchall()
            return [self._scan_out(row) for row in rows]

    @staticmethod
    def _scan_out(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "scan_id": row["scan_id"],
            "batch_no": row["batch_no"],
            "direction": row["direction"],
            "site": row["site"],
            "scanned_at": parse_iso(row["scanned_at"]),
            "recorded_at": parse_iso(row["created_at"]),
        }

    # ------------------------------------------------------ reconciliations

    @staticmethod
    def _invalidate_reconciliations(
        conn: sqlite3.Connection, intake_id: int, batch_no: str, *, reason: str
    ) -> list[int]:
        """Mark every completed reconciliation of ``batch_no`` stale."""
        rows = conn.execute(
            "SELECT id FROM reconciliations WHERE intake_id=? AND batch_no=?"
            " AND status='complete'",
            (intake_id, batch_no),
        ).fetchall()
        now = iso(utcnow())
        ids = sorted(int(row["id"]) for row in rows)
        for reconciliation_id in ids:
            conn.execute(
                "INSERT INTO reconciliation_invalidations(reconciliation_id,"
                " reason, invalidated_at) VALUES (?, ?, ?)",
                (reconciliation_id, reason, now),
            )
        return ids

    def accept_reconciliation(
        self, *, batch_no: str
    ) -> tuple[int, dict[str, Any], list[dict[str, Any]]]:
        """Pin the batch version and its receipt set, atomically.

        Reading the ledger record and the receipts and inserting the
        ``pending`` reconciliation happen in one ``BEGIN IMMEDIATE``
        transaction, so a concurrent scan or correction can never land
        between "what we saw" and "what we pinned".

        Returns:
            ``(reconciliation_id, batch, scans)``: the pinned ledger record
            and the pinned receipt set, which the computation must use.

        Raises:
            NotFoundError: no active dataset, or unknown batch.
        """
        with self.transaction() as conn:
            intake_id = self._active_intake_id(conn, create=False)
            if intake_id is None:
                raise NotFoundError("no active dataset")
            batch = conn.execute(
                "SELECT * FROM batches WHERE intake_id=? AND batch_no=?"
                " AND superseded=0",
                (intake_id, batch_no),
            ).fetchone()
            if batch is None:
                raise NotFoundError(f"batch {batch_no!r} not found")
            scans = conn.execute(
                "SELECT * FROM scans WHERE intake_id=? AND batch_no=?"
                " ORDER BY id",
                (intake_id, batch_no),
            ).fetchall()
            highwater = int(scans[-1]["id"]) if scans else 0
            cur = conn.execute(
                "INSERT INTO reconciliations(intake_id, batch_no, status,"
                " batch_version, scan_highwater, scan_count, accepted_at)"
                " VALUES (?, ?, 'pending', ?, ?, ?, ?)",
                (
                    intake_id,
                    batch_no,
                    int(batch["version"]),
                    highwater,
                    len(scans),
                    iso(utcnow()),
                ),
            )
            pinned_batch = {
                "batch_no": batch["batch_no"],
                "version": int(batch["version"]),
                "origin": batch["origin"],
                "destination": batch["destination"],
                "departed_at": parse_iso(batch["departed_at"]),
                "arrived_at": parse_iso(batch["arrived_at"]),
            }
            pinned_scans = [self._scan_out(row) for row in scans]
            return int(cur.lastrowid), pinned_batch, pinned_scans

    def publish_reconciliation(
        self, reconciliation_id: int, findings: list[dict[str, Any]]
    ) -> None:
        """Publish every finding and the complete flag in one transaction.

        Refuses to publish when the pinned batch version or the pinned
        receipt set changed while the report was being computed.
        """
        now = iso(utcnow())
        with self.transaction() as conn:
            recon = conn.execute(
                "SELECT * FROM reconciliations WHERE id=?",
                (reconciliation_id,),
            ).fetchone()
            if recon is None:
                raise NotFoundError("reconciliation not found")
            if recon["status"] != "pending":
                raise ConflictError(
                    f"reconciliation {reconciliation_id} is"
                    f" {recon['status']}, not pending"
                )
            intake_id = int(recon["intake_id"])
            batch_no = recon["batch_no"]
            live = conn.execute(
                "SELECT version FROM batches WHERE intake_id=? AND batch_no=?"
                " AND superseded=0",
                (intake_id, batch_no),
            ).fetchone()
            live_version = int(live["version"]) if live is not None else None
            if live_version != int(recon["batch_version"]):
                raise ConflictError(
                    f"batch {batch_no!r} moved to version {live_version}"
                    " during reconciliation; not publishing a stale report"
                )
            row = conn.execute(
                "SELECT COALESCE(MAX(id), 0) AS highwater, COUNT(*) AS n"
                " FROM scans WHERE intake_id=? AND batch_no=?",
                (intake_id, batch_no),
            ).fetchone()
            if int(row["highwater"]) != int(recon["scan_highwater"]) or int(
                row["n"]
            ) != int(recon["scan_count"]):
                raise ConflictError(
                    f"new scan receipts arrived for batch {batch_no!r} during"
                    " reconciliation; not publishing a stale report"
                )
            for rank, finding in enumerate(findings):
                detail = dict(finding)
                for key in ("expected_at", "actual_at"):
                    if detail.get(key) is not None:
                        detail[key] = iso(detail[key])
                conn.execute(
                    "INSERT INTO reconciliation_findings(reconciliation_id,"
                    " rank, kind, direction, detail_json)"
                    " VALUES (?, ?, ?, ?, ?)",
                    (
                        reconciliation_id,
                        rank,
                        finding["kind"],
                        finding["direction"],
                        json.dumps(detail, sort_keys=True),
                    ),
                )
            conn.execute(
                "UPDATE reconciliations SET status='complete', completed_at=?,"
                " error=NULL WHERE id=?",
                (now, reconciliation_id),
            )

    def fail_reconciliation(self, reconciliation_id: int, error: str) -> None:
        """Delete any partial findings and record the failure."""
        with self.transaction() as conn:
            conn.execute(
                "DELETE FROM reconciliation_findings WHERE reconciliation_id=?",
                (reconciliation_id,),
            )
            conn.execute(
                "UPDATE reconciliations SET status='failed', completed_at=?,"
                " error=? WHERE id=? AND status='pending'",
                (iso(utcnow()), error, reconciliation_id),
            )

    def read_reconciliation(
        self, reconciliation_id: int
    ) -> dict[str, Any] | None:
        """Read a reconciliation and everything needed to render it.

        A single read transaction keeps the header, its findings, its
        invalidation markers and its pinned inputs mutually consistent.

        Returns ``None`` for an unknown reconciliation.
        """
        with self.connection() as conn:
            conn.execute("BEGIN")
            try:
                recon = conn.execute(
                    "SELECT * FROM reconciliations WHERE id=?",
                    (reconciliation_id,),
                ).fetchone()
                if recon is None:
                    return None
                findings = conn.execute(
                    "SELECT * FROM reconciliation_findings"
                    " WHERE reconciliation_id=? ORDER BY rank",
                    (reconciliation_id,),
                ).fetchall()
                invalidations = conn.execute(
                    "SELECT * FROM reconciliation_invalidations"
                    " WHERE reconciliation_id=? ORDER BY rowid",
                    (reconciliation_id,),
                ).fetchall()
                intake_id = int(recon["intake_id"])
                batch = conn.execute(
                    "SELECT * FROM batches WHERE intake_id=? AND batch_no=?"
                    " AND version=?",
                    (intake_id, recon["batch_no"], int(recon["batch_version"])),
                ).fetchone()
                scans = conn.execute(
                    "SELECT * FROM scans WHERE intake_id=? AND batch_no=?"
                    " AND id<=? ORDER BY id",
                    (
                        intake_id,
                        recon["batch_no"],
                        int(recon["scan_highwater"]),
                    ),
                ).fetchall()
                return {
                    "reconciliation": recon,
                    "findings": findings,
                    "invalidations": invalidations,
                    "batch": batch,
                    "scans": [self._scan_out(row) for row in scans],
                }
            finally:
                conn.execute("COMMIT")

    def reconciliation(self, reconciliation_id: int) -> sqlite3.Row | None:
        with self.connection() as conn:
            return conn.execute(
                "SELECT * FROM reconciliations WHERE id=?",
                (reconciliation_id,),
            ).fetchone()

    def reconciliation_history(self) -> list[sqlite3.Row]:
        with self.connection() as conn:
            return conn.execute(
                "SELECT r.*,"
                " (SELECT COUNT(*) FROM reconciliation_findings f"
                "   WHERE f.reconciliation_id=r.id) AS finding_count,"
                " (SELECT COUNT(*) FROM reconciliation_invalidations i"
                "   WHERE i.reconciliation_id=r.id) AS invalidation_count"
                " FROM reconciliations r ORDER BY r.id DESC"
            ).fetchall()

    # ----------------------------------------------------------------- audit

    def log(self, kind: str, payload: Any) -> None:
        with self.transaction() as conn:
            conn.execute(
                "INSERT INTO request_log(kind, at, payload) VALUES (?, ?, ?)",
                (kind, iso(utcnow()), json.dumps(payload, sort_keys=True)),
            )
