"""Streaming exact terminal/baseline qualification over private frozen controls.

Temporary SQLite indexes provide disk joins/sorts. RAM is one shard, one Raw
frame, and configured live continuation; there is no N-sized Python inventory.
"""

from __future__ import annotations

import hashlib
import json
import sqlite3
import tempfile
from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import Any

from ..storage.acceptance_delta import DeltaSnapshot, ordered_rows
from ..storage.catalog import ALLOWED_TRANSITIONS, ARCHIVE_CHUNK_STATES
from .acceptance import AcceptanceError, canonical_json
from .acceptance_v5_corpus import read_catalog
from .acceptance_v5_delta import ARCHIVE_STEPS, cursor_id, row_digest, verify_archive_lifecycle
from .acceptance_v5_io import AUDIT_DOMAIN, frozen_manifests, read_document, shard_records
from .acceptance_v5_online import OnlineReplay, replay_online
from .acceptance_v5_raw import AuditInterrupted, cancellable_unit
from .acceptance_v5_reconnect import advance_reconnect, stream_key
from .deployment_identity import DeploymentIdentity

AUDIT_FAMILIES = (
    "manifest",
    "catalog_chunk",
    "chunk_transition",
    "archive_transaction",
    "archive_event",
    "operational_event",
    "raw_location",
)
SEALED_STATES = {
    "SEALED",
    "ARCHIVE_COPYING",
    "ARCHIVE_VERIFYING",
    "ARCHIVED_VERIFIED",
    "LOCAL_DELETE_PENDING",
    "LOCAL_DELETED",
}


def order_key(key: str | int) -> str:
    return f"{key:020d}" if isinstance(key, int) else key


def record(
    family: str,
    key: str | int,
    authority: dict[str, Any],
    *,
    findings: list[str] | None = None,
) -> dict[str, Any]:
    return {
        "family": family,
        "key": key,
        "authority": authority,
        "authority_sha256": row_digest(authority),
        "comparison_authority_sha256": None,
        "findings": sorted(findings or []),
    }


@contextmanager
def audit_index(root: Path) -> Iterator[sqlite3.Connection]:
    with tempfile.TemporaryDirectory(prefix=".audit-index-", dir=root) as directory:
        index = sqlite3.connect(Path(directory) / "index.sqlite")
        index.row_factory = sqlite3.Row
        index.execute("PRAGMA cache_size=-1024")
        index.execute("""CREATE TABLE manifests(
            path TEXT PRIMARY KEY,chunk_id TEXT NOT NULL UNIQUE,
            mapping TEXT NOT NULL,manifest TEXT NOT NULL,proof TEXT,
            market TEXT NOT NULL,symbol TEXT NOT NULL,stream TEXT NOT NULL,created INTEGER NOT NULL
        ) WITHOUT ROWID""")
        index.execute("""CREATE TABLE previous(
            family TEXT NOT NULL,key_order TEXT NOT NULL,body TEXT NOT NULL,
            PRIMARY KEY(family,key_order)) WITHOUT ROWID""")
        index.execute("""CREATE TABLE online(
            family TEXT NOT NULL,numeric_id INTEGER NOT NULL,body TEXT NOT NULL,
            PRIMARY KEY(family,numeric_id)) WITHOUT ROWID""")
        index.execute("""CREATE TABLE chunk_state(
            chunk_id TEXT PRIMARY KEY,state TEXT NOT NULL,transition_id INTEGER NOT NULL
        ) WITHOUT ROWID""")
        index.execute("""CREATE TABLE cached(
            family TEXT NOT NULL,key_order TEXT NOT NULL,body TEXT NOT NULL,
            PRIMARY KEY(family,key_order)) WITHOUT ROWID""")
        index.execute("""CREATE TABLE gaps(
            identity TEXT PRIMARY KEY,market TEXT,symbol TEXT,stream TEXT,
            old_connection TEXT,new_connection TEXT,start INTEGER,end INTEGER,pair TEXT
        ) WITHOUT ROWID""")
        index.execute(
            "CREATE INDEX gaps_exact_pair ON gaps"
            "(market,symbol,stream,old_connection,new_connection)"
        )
        index.execute("""CREATE TABLE chunk_markers(
            chunk_id TEXT PRIMARY KEY,last_id INTEGER NOT NULL,sealed_id INTEGER
        ) WITHOUT ROWID""")
        index.execute(
            "CREATE TABLE boundaries(path TEXT PRIMARY KEY,body TEXT NOT NULL) WITHOUT ROWID"
        )
        index.execute(
            "CREATE TABLE sample_chunk_state(chunk_id TEXT PRIMARY KEY,state TEXT) WITHOUT ROWID"
        )
        index.execute("CREATE INDEX sample_chunk_state_open ON sample_chunk_state(state,chunk_id)")
        index.execute(
            "CREATE TABLE sample_archive_state"
            "(transaction_id TEXT PRIMARY KEY,state TEXT) WITHOUT ROWID"
        )
        index.execute("CREATE INDEX sample_archive_state_open ON sample_archive_state(state)")
        try:
            yield index
        finally:
            index.close()


def _index_previous(
    index: sqlite3.Connection,
    predecessor: Mapping[str, Any] | None,
    identity: DeploymentIdentity,
    archive_roots: Mapping[str, Path],
) -> None:
    if predecessor is None:
        return
    path = Path(predecessor["audit_root_path"])
    root, digest = read_document(path)
    if digest != predecessor["audit_root_sha256"]:
        raise AcceptanceError("prior audit root changed")
    # Historical control/chain replay uses its pinned Raw proofs. The current
    # audit independently rereads all qualified Raw and compares those proofs;
    # legitimately retired old local paths need not remain available.
    from .acceptance_v5_finalize import verify_audit

    verify_audit(
        root=path.parent,
        identity=identity,
        archive_roots=archive_roots,
        stage_root=path.parent.parent if root["evidence_kind"] == "terminal-audit-root" else None,
        historical_control_only=True,
    )
    last_family, last_key = -1, ""
    for item in shard_records(
        path.parent / "shards",
        domain=AUDIT_DOMAIN,
        anchor=root["anchor_sha256"],
        summary=root["shards"],
        kind="terminal-audit-shard",
    ):
        family = AUDIT_FAMILIES.index(item["family"])
        key = order_key(item["key"])
        if family < last_family or (family == last_family and key <= last_key):
            raise AcceptanceError("prior audit record order/identity differs")
        if item["authority_sha256"] != row_digest(item["authority"]) or item["findings"]:
            raise AcceptanceError("prior audit record is not eligible/valid")
        index.execute(
            "INSERT INTO previous VALUES(?,?,?)", (item["family"], key, canonical_json(item))
        )
        last_family, last_key = family, key


def _index_online(
    index: sqlite3.Connection,
    stage_root: Path | None,
    identity: DeploymentIdentity,
    snapshot: DeltaSnapshot,
) -> OnlineReplay | None:
    if stage_root is None:
        return None
    transitions = iter(ordered_rows(snapshot.connection, "chunk_transitions", "transition_id"))
    archive_events = iter(
        ordered_rows(snapshot.connection, "archive_transaction_events", "event_id")
    )
    next_chunk, next_archive = next(transitions, None), next(archive_events, None)

    def projection(sample: Mapping[str, Any]) -> None:
        nonlocal next_chunk, next_archive
        high = sample["high_water"]
        if any(high[family] > snapshot.high_water[family] for family in high):
            raise AcceptanceError("online high-water exceeds frozen historical authority")
        while next_chunk and next_chunk["transition_id"] <= high["chunk"]:
            index.execute(
                "INSERT OR REPLACE INTO sample_chunk_state VALUES(?,?)",
                (next_chunk["chunk_id"], next_chunk["to_state"]),
            )
            if next_chunk["to_state"] in set(ARCHIVE_CHUNK_STATES.values()):
                key = str(next_chunk["idempotency_key"])
                event_key = (
                    "reserve:" + key[len("archive-reserve:") :]
                    if key.startswith("archive-reserve:")
                    else key[6:]
                )
                event = snapshot.exact("archive_transaction_events", "idempotency_key", event_key)
                if event is None or event["event_id"] > high["archive"]:
                    raise AcceptanceError("online high-waters split an atomic archive transaction")
            next_chunk = next(transitions, None)
        while next_archive and next_archive["event_id"] <= high["archive"]:
            prefix = (
                "archive-reserve:"
                if next_archive["idempotency_key"].startswith("reserve:")
                else "chunk:"
            )
            key = prefix + (
                next_archive["transaction_id"]
                if prefix == "archive-reserve:"
                else next_archive["idempotency_key"]
            )
            transition = snapshot.exact("chunk_transitions", "idempotency_key", key)
            if transition is None or transition["transition_id"] > high["chunk"]:
                raise AcceptanceError("online high-waters split an atomic archive transaction")
            index.execute(
                "INSERT OR REPLACE INTO sample_archive_state VALUES(?,?)",
                (next_archive["transaction_id"], next_archive["to_state"]),
            )
            next_archive = next(archive_events, None)
        if sample["snapshot_status"] != "complete":
            return
        expected = {
            row["chunk_id"]: row["state"]
            for row in index.execute(
                "SELECT chunk_id,state FROM sample_chunk_state "
                "WHERE state IN ('ACTIVE','RECOVERED','SEALING') LIMIT ?",
                (
                    sample.get("open_chunk_bound", 0) + 1
                    if sample["evidence_kind"] == "stage-start"
                    else open_bound + 1,
                ),
            )
        }
        observed = {row["chunk_id"]: row["state"] for row in sample["open_chunks"]}
        if len(observed) != len(sample["open_chunks"]) or observed != expected:
            raise AcceptanceError("online open lifecycle projection differs from frozen ledger")
        backlog = {
            state: int(
                index.execute(
                    "SELECT COUNT(*) FROM sample_archive_state WHERE state=?", (state,)
                ).fetchone()[0]
            )
            for state in ("COPYING", "VERIFYING", "VERIFIED", "LOCAL_DELETE_PENDING")
        }
        if sample["archive_backlog"] != backlog:
            raise AcceptanceError("online archive backlog projection differs from frozen ledger")

    start, _sha = read_document(stage_root / "stage-start.json", limit=8 * 1024 * 1024)
    open_bound = start["open_chunk_bound"]

    def observe(family: str, entry: Mapping[str, Any]) -> None:
        companions = entry["companions"]
        archive = companions.get("archive")
        if archive:
            for table, key, rows in (
                ("archive_transaction_events", "event_id", archive["events"]),
                ("chunk_transitions", "transition_id", archive["transitions"]),
            ):
                for row in rows:
                    if snapshot.exact(table, key, row[key]) != row:
                        raise AcceptanceError("online causal companion differs from frozen ledger")
        for row in companions.get("discontinuity", []):
            event = snapshot.exact("operational_events", "event_id", row["event_id"])
            sequence = snapshot.exact("operational_event_sequence", "event_seq", row["event_seq"])
            if (
                event is None
                or sequence is None
                or {**event, "event_seq": sequence["event_seq"]} != row
            ):
                raise AcceptanceError("online discontinuity companion differs from frozen ledger")
        try:
            index.execute(
                "INSERT INTO online VALUES(?,?,?)",
                (
                    family,
                    cursor_id(family, entry["row"]),
                    canonical_json(dict(entry)),
                ),
            )
        except sqlite3.IntegrityError as exc:
            raise AcceptanceError("online delta consumed more than once") from exc

    return replay_online(
        stage_root, identity, require_target=True, row_observer=observe, sample_observer=projection
    )


def _compare(old: dict[str, Any], current: dict[str, Any]) -> list[str]:
    family = current["family"]
    before, after = old["authority"], current["authority"]
    if family == "manifest":
        if before["mapping"] != after["mapping"]:
            return ["historical_manifest_mutation_or_replacement"]
    elif family == "catalog_chunk":
        immutable: tuple[str, ...] = ("chunk_id", "created_at_utc_ns")
        if before["state"] in SEALED_STATES:
            immutable += (
                "sealed_path",
                "manifest_path",
                "record_count",
                "stored_bytes",
                "stored_sha256",
                "uncompressed_bytes",
                "uncompressed_sha256",
            )
        if any(before.get(key) != after.get(key) for key in immutable):
            return ["historical_catalog_identity_changed"]
    elif family == "archive_transaction":
        mutable = {
            "state",
            "updated_at_utc_ns",
            "verified_at_utc_ns",
            "local_deleted_at_utc_ns",
            "attempt_count",
            "last_error",
        }
        if {key: value for key, value in before.items() if key not in mutable} != {
            key: value for key, value in after.items() if key not in mutable
        }:
            return ["historical_archive_authority_changed"]
    elif family == "raw_location":
        if before["manifest_sha256"] != after["manifest_sha256"]:
            return ["historical_raw_manifest_authority_changed"]
        before_raw = before.get("proof", {}).get("local") or before.get("proof", {}).get("archive")
        after_raw = after.get("proof", {}).get("local") or after.get("proof", {}).get("archive")
        if before_raw != after_raw:
            return ["historical_raw_bytes_or_scan_authority_changed"]
    elif before != after:
        return ["historical_catalog_ledger_changed"]
    return []


def merge_family(
    index: sqlite3.Connection,
    family: str,
    current: Iterator[dict[str, Any]],
) -> Iterator[dict[str, Any]]:
    previous = iter(
        index.execute(
            "SELECT key_order,body FROM previous WHERE family=? ORDER BY key_order", (family,)
        )
    )
    old_row = next(previous, None)
    last_key = ""
    for item in current:
        key = order_key(item["key"])
        if key <= last_key:
            raise AcceptanceError("current audit records are not unique/canonical")
        last_key = key
        while old_row is not None and old_row["key_order"] < key:
            old = json.loads(old_row["body"])
            missing = record(
                family, old["key"], {"missing": True}, findings=["historical_authority_deleted"]
            )
            missing["comparison_authority_sha256"] = old["authority_sha256"]
            yield missing
            old_row = next(previous, None)
        if old_row is not None and old_row["key_order"] == key:
            old = json.loads(old_row["body"])
            item["comparison_authority_sha256"] = old["authority_sha256"]
            item["findings"] = sorted(set(item["findings"]) | set(_compare(old, item)))
            old_row = next(previous, None)
        yield item
    while old_row is not None:
        old = json.loads(old_row["body"])
        missing = record(
            family, old["key"], {"missing": True}, findings=["historical_authority_deleted"]
        )
        missing["comparison_authority_sha256"] = old["authority_sha256"]
        yield missing
        old_row = next(previous, None)


class AuditRecords:
    def __init__(
        self,
        *,
        root: Path,
        corpus: dict[str, Any],
        identity: DeploymentIdentity,
        predecessor: Mapping[str, Any] | None,
        stage_root: Path | None,
        archive_roots: Mapping[str, Path],
        products: list[list[str]],
        raw_unit: Callable[..., dict[str, Any] | None] = cancellable_unit,
        progress: Callable[[int], None] | None = None,
        cached_records: Callable[[], Iterator[dict[str, Any]]] | None = None,
    ) -> None:
        self.root, self.corpus, self.identity = root, corpus, identity
        self.predecessor, self.stage_root = predecessor, stage_root
        self.archive_roots, self.products = archive_roots, products
        self.raw_unit, self.progress = raw_unit, progress
        self.cached_records = cached_records
        self.counts = {family: 0 for family in AUDIT_FAMILIES}
        self.findings: set[str] = set()
        self.continuation_seed: dict[str, Any] = {"reconnect": {}, "open_discontinuities": {}}
        self.aggregate = hashlib.sha256()
        self.bytes_verified = 0
        self.archive_bytes_verified = 0
        self.replay: OnlineReplay | None = None

    def __iter__(self) -> Iterator[dict[str, Any]]:
        with (
            audit_index(self.root) as index,
            read_catalog(self.root / "catalog.sqlite", immutable=True) as catalog,
        ):
            if self.cached_records:
                for cached in self.cached_records():
                    index.execute(
                        "INSERT INTO cached VALUES(?,?,?)",
                        (
                            cached["family"],
                            order_key(cached["key"]),
                            canonical_json(cached),
                        ),
                    )
            _index_previous(index, self.predecessor, self.identity, self.archive_roots)
            snapshot = DeltaSnapshot(catalog)
            replay = _index_online(index, self.stage_root, self.identity, snapshot)
            self.replay = replay
            self.continuation_seed["archive_counts"] = snapshot.archive_backlog()
            for transition in ordered_rows(catalog, "chunk_transitions", "transition_id"):
                index.execute(
                    "INSERT INTO chunk_markers VALUES(?,?,?) ON CONFLICT(chunk_id) DO UPDATE "
                    "SET last_id=excluded.last_id,"
                    "sealed_id=COALESCE(chunk_markers.sealed_id,excluded.sealed_id)",
                    (
                        transition["chunk_id"],
                        transition["transition_id"],
                        transition["transition_id"] if transition["to_state"] == "SEALED" else None,
                    ),
                )
            for mapping, manifest in frozen_manifests(self.root, self.corpus):
                try:
                    index.execute(
                        "INSERT INTO manifests VALUES(?,?,?,?,?,?,?,?,?)",
                        (
                            mapping["path"],
                            mapping["chunk_id"],
                            canonical_json(mapping),
                            canonical_json(manifest),
                            None,
                            manifest["market"],
                            manifest["symbol"],
                            manifest["stream"],
                            manifest["created_at_utc_ns"],
                        ),
                    )
                except sqlite3.IntegrityError as exc:
                    raise AcceptanceError("duplicate frozen manifest/chunk identity") from exc
            generators = {
                "manifest": self._manifests(index, snapshot),
                "catalog_chunk": self._chunks(index, catalog),
                "chunk_transition": self._transitions(index, catalog, replay),
                "archive_transaction": self._archive_transactions(snapshot, catalog),
                "archive_event": self._archive_events(index, snapshot, catalog, replay),
                "operational_event": self._operational_events(index, snapshot, catalog, replay),
                "raw_location": self._raw_locations(index),
            }
            for family in AUDIT_FAMILIES:
                for item in merge_family(index, family, generators[family]):
                    item["timing"] = self._timing(index, snapshot, item)
                    self.counts[family] += 1
                    self.findings.update(item["findings"])
                    self.aggregate.update(canonical_json(item))
                    yield item

    def _timing(
        self,
        index: sqlite3.Connection,
        snapshot: DeltaSnapshot,
        item: dict[str, Any],
    ) -> str:
        if item["authority"].get("missing") is True:
            return "FROZEN_CORPUS"
        replay = self.replay
        if replay is None or replay.target is None:
            return "BASELINE_CORPUS"
        family = item["family"]
        cursor_family = {
            "chunk_transition": "chunk",
            "archive_event": "archive",
            "operational_event": "operational",
        }.get(family)
        if cursor_family:
            identifier = int(item["key"])
        elif family in {"manifest", "raw_location"}:
            row = index.execute(
                "SELECT chunk_id FROM manifests WHERE path=?", (item["key"],)
            ).fetchone()
            marker = (
                index.execute(
                    "SELECT sealed_id FROM chunk_markers WHERE chunk_id=?", (row["chunk_id"],)
                ).fetchone()
                if row
                else None
            )
            if not marker or marker["sealed_id"] is None:
                return "FROZEN_CORPUS"
            identifier, cursor_family = marker["sealed_id"], "chunk"
        elif family == "catalog_chunk":
            marker = index.execute(
                "SELECT last_id FROM chunk_markers WHERE chunk_id=?", (item["key"],)
            ).fetchone()
            if marker is None:
                return "FROZEN_CORPUS"
            identifier, cursor_family = marker["last_id"], "chunk"
        else:
            events = snapshot.archive_lifecycle(str(item["key"]))["events"]
            identifier, cursor_family = (
                max((event["event_id"] for event in events), default=0),
                "archive",
            )
        if identifier > replay.target["target_high_water"][cursor_family]:
            return "POST_TARGET_HANDOFF"
        if identifier <= replay.start["predecessor"]["cursor_tuple"][cursor_family]:
            return "HISTORICAL_AUTHORITY"
        return "ONLINE_WINDOW_AUTHORITY"

    def _manifests(
        self, index: sqlite3.Connection, snapshot: DeltaSnapshot
    ) -> Iterator[dict[str, Any]]:
        for indexed in index.execute("SELECT * FROM manifests ORDER BY path"):
            mapping, manifest = json.loads(indexed["mapping"]), json.loads(indexed["manifest"])
            chunk = snapshot.exact("chunks", "chunk_id", manifest["chunk_id"])
            findings: list[str] = []
            proof = None
            if chunk is None or chunk.get("manifest_path") != mapping["path"]:
                findings.append("catalog_manifest_disagreement")
            else:
                try:
                    tx = snapshot.exact("archive_transactions", "chunk_id", manifest["chunk_id"])
                    archive = snapshot.archive_lifecycle(tx["transaction_id"]) if tx else None
                    if archive:
                        verify_archive_lifecycle(archive, snapshot.high_water)
                    archive_root = self.archive_roots.get(tx["storage_id"]) if tx else None
                    cached = index.execute(
                        "SELECT body FROM cached WHERE family='manifest' AND key_order=?",
                        (mapping["path"],),
                    ).fetchone()
                    if cached:
                        authority = json.loads(cached["body"])["authority"]
                        if authority["mapping"] != mapping or authority["manifest"] != manifest:
                            raise AcceptanceError("resume manifest/control corpus changed")
                        proof = authority["proof"]
                    else:
                        proof = self.raw_unit(
                            {
                                "data_root": self.corpus["data_root"],
                                "manifest": manifest,
                                "manifest_sha256": mapping["sha256"],
                                "manifest_path": mapping["path"],
                                "chunk": chunk,
                                "archive": archive,
                                "archive_root": str(archive_root) if archive_root else None,
                            },
                            remaining_ns=None,
                            progress=self.progress,
                        )
                    if proof and "error" in proof:
                        findings.append("raw_or_archive_integrity_failure")
                    else:
                        if proof is None:
                            raise AcceptanceError("terminal Raw qualification did not complete")
                        self.bytes_verified += (
                            proof["local"]["stored_bytes"] if proof["local"] else 0
                        )
                        self.archive_bytes_verified += (
                            proof["archive"]["stored_bytes"] if proof["archive"] else 0
                        )
                        index.execute(
                            "UPDATE manifests SET proof=? WHERE path=?",
                            (canonical_json(proof), mapping["path"]),
                        )
                except AuditInterrupted:
                    raise
                except (AcceptanceError, OSError, ValueError, RuntimeError) as exc:
                    findings.append("raw_or_archive_integrity_failure")
                    proof = {"error": f"{type(exc).__name__}: {exc}"}
            yield record(
                "manifest",
                mapping["path"],
                {
                    "mapping": mapping,
                    "manifest": manifest,
                    "proof": proof,
                },
                findings=findings,
            )

    def _chunks(
        self, index: sqlite3.Connection, catalog: sqlite3.Connection
    ) -> Iterator[dict[str, Any]]:
        for row in ordered_rows(catalog, "chunks", "chunk_id"):
            findings = []
            manifest = index.execute(
                "SELECT path FROM manifests WHERE chunk_id=?", (row["chunk_id"],)
            ).fetchone()
            if row["state"] in SEALED_STATES and (
                manifest is None or manifest["path"] != row["manifest_path"]
            ):
                findings.append("catalog_manifest_disagreement")
            if row["state"] in {"ACTIVE", "RECOVERED", "SEALING"}:
                findings.append("unfinished_chunk_at_quiescence")
            yield record("catalog_chunk", row["chunk_id"], row, findings=findings)

    def _check_online_row(
        self,
        index: sqlite3.Connection,
        family: str,
        row: dict[str, Any],
        replay: OnlineReplay | None,
    ) -> list[str]:
        if replay is None:
            return []
        numeric_id = cursor_id(family, row)
        baseline = replay.start["predecessor"]["cursor_tuple"][family]
        target = replay.target
        if target is None:
            raise AcceptanceError("terminal target is missing")
        if not baseline < numeric_id <= target["cursor_tuple"][family]:
            return []
        entry_row = index.execute(
            "SELECT body FROM online WHERE family=? AND numeric_id=?", (family, numeric_id)
        ).fetchone()
        if entry_row is None:
            return ["online_delta_omission"]
        entry = json.loads(entry_row["body"])
        if entry["row"] != row:
            return ["online_delta_row_mismatch"]
        if family == "chunk" and row["to_state"] == "SEALED":
            frozen = index.execute(
                "SELECT mapping,manifest,proof FROM manifests WHERE chunk_id=?", (row["chunk_id"],)
            ).fetchone()
            unit = entry.get("unit")
            if frozen is None or not isinstance(unit, dict) or frozen["proof"] is None:
                return ["online_raw_authority_missing"]
            mapping, manifest, proof = (
                json.loads(frozen["mapping"]),
                json.loads(frozen["manifest"]),
                json.loads(frozen["proof"]),
            )
            online_raw = unit["raw"].get("local") or unit["raw"].get("archive")
            exact_raw = proof.get("local") or proof.get("archive")
            if (
                unit["manifest_sha256"] != mapping["sha256"]
                or unit["manifest"] != manifest
                or unit["manifest_path"] != mapping["path"]
                or online_raw != exact_raw
            ):
                return ["online_raw_authority_mismatch"]
        return []

    def _transitions(
        self,
        index: sqlite3.Connection,
        catalog: sqlite3.Connection,
        replay: OnlineReplay | None,
    ) -> Iterator[dict[str, Any]]:
        for row in ordered_rows(catalog, "chunk_transitions", "transition_id"):
            findings = self._check_online_row(index, "chunk", row, replay)
            prior = index.execute(
                "SELECT state FROM chunk_state WHERE chunk_id=?", (row["chunk_id"],)
            ).fetchone()
            before, after = row["from_state"], row["to_state"]
            if (
                (prior is None and (before is not None or after not in {"ACTIVE", "SEALING"}))
                or (prior is not None and prior["state"] != before)
                or (
                    before is not None
                    and before != after
                    and after not in ALLOWED_TRANSITIONS.get(before, set())
                )
            ):
                findings.append("illegal_chunk_ledger")
            index.execute(
                "INSERT OR REPLACE INTO chunk_state VALUES(?,?,?)",
                (row["chunk_id"], after, row["transition_id"]),
            )
            yield record("chunk_transition", row["transition_id"], row, findings=findings)
        # Validation findings attach to raw_location records (the final family).
        for row in catalog.execute("SELECT chunk_id,state FROM chunks ORDER BY chunk_id"):
            state = index.execute(
                "SELECT state FROM chunk_state WHERE chunk_id=?", (row["chunk_id"],)
            ).fetchone()
            if state is None or state["state"] != row["state"]:
                self.findings.add("catalog_chunk_ledger_disagreement")

    def _archive_transactions(
        self,
        snapshot: DeltaSnapshot,
        catalog: sqlite3.Connection,
    ) -> Iterator[dict[str, Any]]:
        for row in ordered_rows(catalog, "archive_transactions", "transaction_id"):
            findings = []
            try:
                verify_archive_lifecycle(
                    snapshot.archive_lifecycle(row["transaction_id"]), snapshot.high_water
                )
            except AcceptanceError:
                findings.append("archive_lifecycle_disagreement")
            yield record("archive_transaction", row["transaction_id"], row, findings=findings)

    def _archive_events(
        self,
        index: sqlite3.Connection,
        snapshot: DeltaSnapshot,
        catalog: sqlite3.Connection,
        replay: OnlineReplay | None,
    ) -> Iterator[dict[str, Any]]:
        for row in ordered_rows(catalog, "archive_transaction_events", "event_id"):
            findings = self._check_online_row(index, "archive", row, replay)
            keys = {f"{step[0]}:{row['transaction_id']}" for step in ARCHIVE_STEPS}
            if row["idempotency_key"] not in keys:
                findings.append("unrecognized_archive_lifecycle_event")
            yield record("archive_event", row["event_id"], row, findings=findings)

    def _operational_events(
        self,
        index: sqlite3.Connection,
        snapshot: DeltaSnapshot,
        catalog: sqlite3.Connection,
        replay: OnlineReplay | None,
    ) -> Iterator[dict[str, Any]]:
        from .acceptance_v5_delta import _companions

        for row in ordered_rows(catalog, "operational_event_sequence", "event_seq"):
            findings = self._check_online_row(index, "operational", row, replay)
            try:
                companions = snapshot.companions("operational", row)
                _companions("operational", row, companions, snapshot.high_water)
                pair = companions.get("discontinuity")
                if pair:
                    start = next(
                        item
                        for item in pair
                        if item["event_type"] == "STREAM_DISCONTINUITY_STARTED"
                    )
                    complete = next(
                        (
                            item
                            for item in pair
                            if item["event_type"] == "STREAM_DISCONTINUITY_COMPLETED"
                        ),
                        None,
                    )
                    start_body = json.loads(start["evidence_json"])
                    complete_body = json.loads(complete["evidence_json"]) if complete else {}
                    identity = f"{stream_key(row)}:{row['gap_id']}"
                    index.execute(
                        "INSERT OR REPLACE INTO gaps VALUES(?,?,?,?,?,?,?,?,?)",
                        (
                            identity,
                            row["market"],
                            row["symbol"],
                            row["stream"],
                            start_body.get("original_connection_id"),
                            complete_body.get("new_connection_id"),
                            start_body["gap_started_at_utc_ns"],
                            complete_body.get("gap_ended_at_utc_ns"),
                            canonical_json(pair),
                        ),
                    )
            except AcceptanceError:
                findings.append("operational_discontinuity_authority_malformed")
            yield record("operational_event", row["event_seq"], row, findings=findings)

    def _raw_locations(self, index: sqlite3.Connection) -> Iterator[dict[str, Any]]:
        # Chronological disk sort, one stream context at a time. No N-sized list.
        context: dict[str, Any] = {}
        last_stream = None
        configured = {tuple(product) for product in self.products}
        t0 = self.replay.start["t0_utc_ns"] if self.replay else None
        for row in index.execute(
            "SELECT * FROM manifests ORDER BY market,symbol,stream,created,chunk_id"
        ):
            key = (row["market"], row["symbol"], row["stream"])
            if key != last_stream:
                context = {}
                last_stream = key
            if row["proof"] is None:
                continue
            manifest, proof = json.loads(row["manifest"]), json.loads(row["proof"])
            raw = proof.get("local") or proof.get("archive")
            pairs = []
            previous = context.get(stream_key(manifest))
            if previous and raw and raw["first_frame"]:
                for gap in index.execute(
                    "SELECT pair FROM gaps WHERE market=? AND symbol=? AND stream=? "
                    "AND old_connection=? AND new_connection=? LIMIT 3",
                    (
                        *key,
                        previous["last_frame"]["connection_id"],
                        raw["first_frame"]["connection_id"],
                    ),
                ):
                    pairs.extend(json.loads(gap["pair"]))
            findings, boundary = advance_reconnect(
                context,
                manifest,
                proof,
                discontinuity_rows=pairs,
                t0_utc_ns=t0,
                context_bound=1,
            )
            index.execute(
                "INSERT INTO boundaries VALUES(?,?)",
                (
                    row["path"],
                    canonical_json(
                        {
                            "boundary": boundary,
                            "findings": findings,
                        }
                    ),
                ),
            )
            if key[:2] in configured:
                self.continuation_seed["reconnect"].update(context)
        for gap in index.execute("SELECT * FROM gaps WHERE end IS NULL ORDER BY identity"):
            if (gap["market"], gap["symbol"]) in configured:
                gap_key = f"{gap['market']}:{gap['symbol']}:{gap['stream']}"
                if gap_key in self.continuation_seed["open_discontinuities"]:
                    self.findings.add("overlapping_open_discontinuities")
                self.continuation_seed["open_discontinuities"][gap_key] = json.loads(gap["pair"])[0]
        if len(self.continuation_seed["reconnect"]) > len(self.products) * 5:
            raise AcceptanceError("terminal continuation exceeds configured topology")
        for row in index.execute("SELECT path,mapping,proof FROM manifests ORDER BY path"):
            mapping = json.loads(row["mapping"])
            proof = json.loads(row["proof"]) if row["proof"] else {}
            boundary = index.execute(
                "SELECT body FROM boundaries WHERE path=?", (row["path"],)
            ).fetchone()
            projection = (
                json.loads(boundary["body"]) if boundary else {"boundary": None, "findings": []}
            )
            findings = projection["findings"] if proof else ["raw_or_archive_integrity_failure"]
            yield record(
                "raw_location",
                row["path"],
                {
                    "manifest_sha256": mapping["sha256"],
                    "proof": proof,
                    "reconnect": projection["boundary"],
                },
                findings=findings,
            )
