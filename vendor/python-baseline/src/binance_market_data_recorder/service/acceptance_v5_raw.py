"""Streaming Raw qualification with a process cancellation boundary.

There is no full-frame or transition list. The parent can kill a unit blocked
in filesystem I/O; a killed unit returns no acknowledgement authority.
"""

from __future__ import annotations

import hashlib
import json
import multiprocessing
import os
import signal
import time
from collections.abc import Callable, Mapping
from contextlib import suppress
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any, BinaryIO

import google_crc32c
import zstandard

from ..archive.manager import ARCHIVE_MANIFEST_SCHEMA, _validate_external_manifest
from ..audit.reconnect_boundaries import (
    BLUE_GREEN_OVERLAP,
    EXPLICIT_SEQUENCE_GAP,
    UNKNOWN,
    UNMARKED_RECONNECT,
    WEBSOCKET_STREAMS,
    _frame_document,
    _frame_has_gap,
    _frame_identity,
    _overlap_pair,
)
from ..domain.event import EventEnvelope
from ..spool.format import (
    FRAME_PREFIX,
    FRAME_PREFIX_WITHOUT_CRC,
    ChunkStatistics,
    decode_chunk_header,
    decode_envelope,
)
from ..spool.seal import parse_strict_manifest_bytes
from ..storage.macos import validate_registered_root
from .acceptance import AcceptanceError, canonical_json, sha256_bytes
from .acceptance_v5_io import (
    MAX_CANONICAL_BYTES_PER_SHARD,
    _file_identity,
    canonical_path,
    hash_stream,
    open_exact,
)

AUDIT_NO_PROGRESS_TIMEOUT_NS = 900_000_000_000
AUDIT_PROGRESS_INTERVAL_NS = 60_000_000_000


class RawAuthorityPending(AcceptanceError):
    """No acknowledgement until a later snapshot proves exact retirement authority."""


class RawRetiredDuringQualification(AcceptanceError):
    """Validated bytes lost their local pathname; snapshot authority still applies."""


class AuditInterrupted(AcceptanceError):
    """A liveness/interruption failure is not evidence of data corruption."""


def _read_exact(source: BinaryIO, size: int) -> bytes:
    block = source.read(size)
    if len(block) == size:
        return block
    result = bytearray(block)
    while len(result) < size:
        block = source.read(size - len(result))
        if not block:
            break
        result.extend(block)
    return bytes(result)


def scan_raw(
    root: Path,
    relative: str,
    manifest: Mapping[str, Any],
    *,
    progress: Callable[[int], None] | None = None,
) -> dict[str, Any]:
    """Exact stored/decompressed hashes, frame CRC/identity/statistics, O(one frame)."""
    canonical_path(relative)
    with open_exact(root, relative) as source:
        before = _file_identity(os.fstat(source.fileno()))
        stored_size, stored_sha = hash_stream(source, progress)
        after_hash = _file_identity(os.fstat(source.fileno()))
    if (stored_size, stored_sha) != (manifest["stored_bytes"], manifest["stored_sha256"]):
        raise AcceptanceError("Raw stored bytes/SHA disagreement")
    stats = ChunkStatistics()
    digest = hashlib.sha256()
    size = 0
    first = None
    previous: tuple[int, EventEnvelope] | None = None
    previous_previous: tuple[int, EventEnvelope] | None = None
    counts = {
        kind: 0 for kind in (EXPLICIT_SEQUENCE_GAP, BLUE_GREEN_OVERLAP, UNMARKED_RECONNECT, UNKNOWN)
    }
    transition_times: dict[str, dict[str, int | None]] = {
        kind: {"min": None, "max": None} for kind in counts
    }
    transition_digest = "0" * 64
    connections = set(manifest["connection_ids"])
    collectors = set(manifest["collector_instance_ids"])
    capture_flags = set(manifest["capture_flags"])
    sequence_keys = set(manifest["sequence_ranges"])
    with open_exact(root, relative) as source:
        before_scan_stat = os.fstat(source.fileno())
        before_scan = _file_identity(before_scan_stat)
        with zstandard.ZstdDecompressor().stream_reader(source, closefd=False) as decoded:
            header, header_bytes = decode_chunk_header(decoded)
            chunk_id = str(header.chunk_id)
            expected_header = {
                "chunk_id": str(header.chunk_id),
                "created_at_utc_ns": header.created_at_utc_ns,
                "market": header.market,
                "symbol": header.symbol,
                "stream": header.stream,
                "collector_version": header.collector_version,
                "chunk_schema_version": header.chunk_schema_version,
                "envelope_schema_version": header.envelope_schema_version,
            }
            if any(manifest.get(key) != value for key, value in expected_header.items()):
                raise AcceptanceError("Raw header/manifest disagreement")
            digest.update(header_bytes)
            size += len(header_bytes)
            websocket = header.stream in WEBSOCKET_STREAMS.get(header.market, frozenset())
            while prefix := _read_exact(decoded, FRAME_PREFIX.size):
                if len(prefix) != FRAME_PREFIX.size:
                    raise AcceptanceError("truncated Raw prefix")
                length, flags, reserved, crc = FRAME_PREFIX.unpack(prefix)
                if length > header.max_frame_bytes or flags or reserved:
                    raise AcceptanceError("invalid Raw frame prefix")
                body = _read_exact(decoded, length)
                if (
                    len(body) != length
                    or google_crc32c.value(prefix[: FRAME_PREFIX_WITHOUT_CRC.size] + body) != crc
                ):
                    raise AcceptanceError("Raw frame truncation/CRC disagreement")
                envelope = decode_envelope(body)
                if (envelope.market, envelope.symbol, envelope.stream) != (
                    header.market,
                    header.symbol,
                    header.stream,
                ):
                    raise AcceptanceError("Raw frame identity disagreement")
                # The frozen control bounds every aggregate key/set, including hostile bytes.
                if (
                    envelope.connection_id not in connections
                    or envelope.collector_instance_id not in collectors
                    or not set(envelope.capture_flags) <= capture_flags
                    or not set(envelope.source_sequence) <= sequence_keys
                ):
                    raise AcceptanceError("Raw statistics outside manifest authority")
                index = stats.record_count
                if first is None:
                    first = _frame_identity(chunk_id, index, envelope)
                # Full validation/statistics still cover every frame. Boundary hashes and
                # FrameIdentity objects are needed only at transitions and the retained tail.
                if websocket and previous and previous[1].connection_id != envelope.connection_id:
                    frame = _frame_identity(chunk_id, index, envelope)
                    old_frame = _frame_identity(chunk_id, *previous)
                    if _frame_has_gap(frame) or (
                        _frame_has_gap(old_frame)
                        and previous_previous is not None
                        and previous_previous[1].connection_id == previous[1].connection_id
                    ):
                        kind = EXPLICIT_SEQUENCE_GAP
                    elif _frame_has_gap(old_frame):
                        kind = UNKNOWN
                    elif _overlap_pair(old_frame, frame):
                        kind = BLUE_GREEN_OVERLAP
                    else:
                        kind = UNMARKED_RECONNECT
                    counts[kind] += 1
                    interval = transition_times[kind]
                    instant = frame.receive_time_utc_ns
                    interval["min"] = (
                        min(interval["min"], instant) if interval["min"] is not None else instant
                    )
                    interval["max"] = (
                        max(interval["max"], instant) if interval["max"] is not None else instant
                    )
                    transition_digest = sha256_bytes(
                        bytes.fromhex(transition_digest)
                        + canonical_json(
                            {
                                "old": _frame_document(old_frame),
                                "new": _frame_document(frame),
                                "kind": kind,
                            }
                        )
                    )
                previous_previous, previous = previous, (index, envelope)
                stats.add(envelope)
                digest.update(prefix)
                digest.update(body)
                size += len(prefix) + len(body)
                if progress:
                    progress(len(prefix) + len(body))
        after_scan_stat = os.fstat(source.fileno())
        after_scan = _file_identity(after_scan_stat)
    expected_stats = {
        "record_count": stats.record_count,
        "receive_time_utc_range_ns": {
            "min": stats.receive_time_utc_min_ns,
            "max": stats.receive_time_utc_max_ns,
        },
        "receive_monotonic_range_ns": {
            "min": stats.receive_monotonic_min_ns,
            "max": stats.receive_monotonic_max_ns,
        },
        "exchange_time_ranges": {
            key: {"min": values[0], "max": values[1]}
            for key, values in stats.exchange_time_ranges.items()
        },
        "sequence_ranges": stats.sequence_ranges(),
        "connection_ids": sorted(stats.connection_ids),
        "collector_instance_ids": sorted(stats.collector_instance_ids)
        or [header.collector_instance_id],
        "uncompressed_bytes": size,
        "uncompressed_sha256": digest.hexdigest(),
    }
    if any(manifest.get(key) != value for key, value in expected_stats.items()):
        raise AcceptanceError("Raw statistics/manifest disagreement")
    if (
        not before == after_hash == before_scan == after_scan
        or before_scan_stat.st_nlink != after_scan_stat.st_nlink
    ):
        # POSIX unlink changes ctime on the still-open inode. This is neither a
        # content proof failure nor authorization to acknowledge a retired source.
        # Only a fully validated, otherwise unchanged, newly unlinked inode may
        # take the local-absence path; replacement and metadata/content mutation
        # keep the strict failure path.
        if (
            before == after_hash == before_scan
            and before_scan[:4] == after_scan[:4]
            and before_scan_stat.st_nlink == 1
            and after_scan_stat.st_nlink == 0
        ):
            try:
                with open_exact(root, relative):
                    pass
            except FileNotFoundError as exc:
                raise RawRetiredDuringQualification("Raw unlinked during qualification") from exc
        raise AcceptanceError("Raw changed during qualification")
    return {
        "stored_bytes": stored_size,
        "stored_sha256": stored_sha,
        "uncompressed_bytes": size,
        "uncompressed_sha256": digest.hexdigest(),
        "first_frame": _frame_document(first) if first else None,
        "last_frame": _frame_document(_frame_identity(chunk_id, *previous)) if previous else None,
        "penultimate_frame": (
            _frame_document(_frame_identity(chunk_id, *previous_previous))
            if previous_previous
            else None
        ),
        "intra_transition_counts": counts,
        "intra_transition_digest": transition_digest,
        "intra_transition_time_ranges": transition_times,
    }


def qualify_unit(
    task: Mapping[str, Any], progress: Callable[[int], None] | None = None
) -> dict[str, Any]:
    """Use exact snapshot rows, never requery the live Catalog after its boundary."""
    manifest = task["manifest"]
    manifest_sha = task["manifest_sha256"]
    chunk = task["chunk"]
    for manifest_key, chunk_key in (
        ("chunk_id", "chunk_id"),
        ("relative_path", "sealed_path"),
        ("record_count", "record_count"),
        ("stored_bytes", "stored_bytes"),
        ("stored_sha256", "stored_sha256"),
        ("uncompressed_bytes", "uncompressed_bytes"),
        ("uncompressed_sha256", "uncompressed_sha256"),
    ):
        if manifest.get(manifest_key) != chunk.get(chunk_key):
            raise AcceptanceError("Catalog/manifest disagreement")
    local_root = Path(task["data_root"])
    result: dict[str, Any] = {"local": None, "archive": None}
    with suppress(FileNotFoundError, RawRetiredDuringQualification):
        result["local"] = scan_raw(
            local_root, manifest["relative_path"], manifest, progress=progress
        )
    archive = task.get("archive")
    if archive and archive["transaction"]["state"] in {
        "VERIFIED",
        "LOCAL_DELETE_PENDING",
        "LOCAL_DELETED",
    }:
        transaction, target = archive["transaction"], archive["target"]
        archive_root = task.get("archive_root")
        if not archive_root or not isinstance(target, dict):
            raise AcceptanceError("registered READY/LOW_SPACE archive location unavailable")
        root = Path(archive_root)
        validate_registered_root(
            root,
            volume_uuid=target["volume_uuid"],
            relative_path=target["relative_path"],
            storage_id=target["storage_id"],
            marker_nonce=target["marker_nonce"],
        )
        expected = {
            "archive_manifest_schema_version": ARCHIVE_MANIFEST_SCHEMA,
            "transaction_id": transaction["transaction_id"],
            "chunk_id": manifest["chunk_id"],
            "storage_id": transaction["storage_id"],
            "volume_uuid": target["volume_uuid"],
            "registered_relative_path": target["relative_path"],
            "artifact_relative_path": transaction["target_relative_path"],
            "stored_bytes": manifest["stored_bytes"],
            "stored_sha256": manifest["stored_sha256"],
            "source_manifest_sha256": manifest_sha,
        }
        if (
            transaction["source_relative_path"] != manifest["relative_path"]
            or transaction["source_manifest_relative_path"] != task["manifest_path"]
            or transaction["source_manifest_sha256"] != manifest_sha
        ):
            raise AcceptanceError("archive source authority disagreement")
        with open_exact(root, transaction["external_manifest_relative_path"]) as source:
            body = source.read(4 * MAX_CANONICAL_BYTES_PER_SHARD + 1)
        if len(body) > 4 * MAX_CANONICAL_BYTES_PER_SHARD:
            raise AcceptanceError("external manifest exceeds control bound")
        _validate_external_manifest(json.loads(body), expected)
        result["archive"] = scan_raw(
            root, transaction["target_relative_path"], manifest, progress=progress
        )
        result["archive_manifest_sha256"] = sha256_bytes(body)
    if result["local"] is None and not (
        archive
        and archive["transaction"]["state"] in {"LOCAL_DELETE_PENDING", "LOCAL_DELETED"}
        and chunk["state"] in {"LOCAL_DELETE_PENDING", "LOCAL_DELETED"}
        and result["archive"] is not None
    ):
        raise RawAuthorityPending("unauthorized Raw absence in frozen snapshot")
    result["chunk_id"] = manifest["chunk_id"]
    result["manifest_sha256"] = manifest_sha
    return result


def qualify_task(
    task: Mapping[str, Any], progress: Callable[[int], None] | None = None
) -> dict[str, Any]:
    if task.get("operation") == "run-audit":
        from .acceptance_v5_finalize import run_audit
        from .deployment_identity import DeploymentIdentity

        path, digest, document = run_audit(
            root=Path(task["root"]),
            corpus=task["corpus"],
            corpus_sha=task["corpus_sha"],
            identity=DeploymentIdentity.from_document(task["identity"]),
            anchor_sha=task["anchor"],
            predecessor=task["predecessor"],
            stage_root=Path(task["stage_root"]) if task["stage_root"] else None,
            products=task["products"],
            archive_roots={key: Path(value) for key, value in task["archive_roots"].items()},
            resume=task["resume"],
            progress_callback=progress,
            _controlled=True,
        )
        return {"path": str(path), "sha256": digest, "document": document}
    if task.get("operation") == "freeze-corpus":
        from .acceptance_v5_corpus import freeze_corpus
        from .deployment_identity import DeploymentIdentity

        path, digest, document = freeze_corpus(
            data_root=Path(task["data_root"]),
            root=Path(task["root"]),
            identity=DeploymentIdentity.from_document(task["identity"]),
            anchor_sha256=task["anchor"],
            boot_id=task["boot_id"],
            progress=progress,
        )
        return {"path": str(path), "sha256": digest, "document": document}
    if task.get("operation") == "catalog-snapshot":
        return capture_snapshot(task)
    if task.get("operation") != "new-manifest":
        return qualify_unit(task, progress)
    root, relative = Path(task["data_root"]), task["manifest_path"]
    with open_exact(root, relative) as source:
        before = _file_identity(os.fstat(source.fileno()))
        body = source.read(MAX_CANONICAL_BYTES_PER_SHARD + 1)
        after = _file_identity(os.fstat(source.fileno()))
    with open_exact(root, relative) as source:
        path_identity = _file_identity(os.fstat(source.fileno()))
    if before != after or before != path_identity:
        raise AcceptanceError("new manifest descriptor/path changed")
    if len(body) > MAX_CANONICAL_BYTES_PER_SHARD:
        raise AcceptanceError("new manifest exceeds control byte bound")
    manifest = parse_strict_manifest_bytes(body, path=root / relative)
    manifest_sha = sha256_bytes(body)
    proof = qualify_unit({**task, "manifest": manifest, "manifest_sha256": manifest_sha}, progress)
    return {
        "manifest": manifest,
        "manifest_sha256": manifest_sha,
        "manifest_path": relative,
        "raw": proof,
    }


def capture_snapshot(task: Mapping[str, Any]) -> dict[str, Any]:
    """Fixed pages and indexed companions in one read snapshot, no filesystem work."""
    from ..storage.acceptance_delta import CURSORS, SQL_PAGE_CAP, DeltaSnapshot
    from .acceptance_v5_corpus import read_catalog
    from .acceptance_v5_delta import FAMILIES

    candidates: dict[str, list[dict[str, Any]]] = {family: [] for family in FAMILIES}
    targets: dict[str, dict[str, Any]] = {}
    max_pages = task.get("max_pages_per_family", 1)
    if type(max_pages) is not int or max_pages not in {1, 4}:
        raise AcceptanceError("unsupported delta snapshot batch bound")
    with read_catalog(Path(task["catalog_path"])) as connection:
        connection.execute("BEGIN")
        snapshot = DeltaSnapshot(connection)
        for family in FAMILIES:
            cursor = task["processed"][family]
            for _ in range(max_pages):
                page = snapshot.page(family, cursor)
                for row in page:
                    companions = snapshot.companions(family, row)
                    candidates[family].append({"row": row, "companions": companions})
                    archive = companions.get("archive")
                    if archive and archive.get("target"):
                        target = archive["target"]
                        targets[target["storage_id"]] = {
                            "target": target,
                            "control": snapshot.exact(
                                "storage_control", "storage_id", target["storage_id"]
                            )
                            or {"state": "ACTIVE"},
                        }
                if len(page) < SQL_PAGE_CAP:
                    break
                cursor = page[-1][CURSORS[family][1]]
        return {
            "high_water": snapshot.high_water,
            "candidates": candidates,
            "targets": targets,
            "open_chunks": snapshot.bounded_open_chunks(task["open_chunk_bound"]),
            "archive_backlog": snapshot.archive_backlog(),
        }


def _server(pipe: Connection) -> None:
    """Stateless reusable process; each task remains independently killable."""
    try:
        while True:
            task = pipe.recv()
            if task is None:
                return
            pending_bytes = 0
            reported_at = time.monotonic_ns()

            def report(size: int) -> None:
                nonlocal pending_bytes, reported_at
                pending_bytes += size
                now = time.monotonic_ns()
                if pending_bytes >= 1024 * 1024 or now - reported_at >= AUDIT_PROGRESS_INTERVAL_NS:
                    pipe.send(("progress", pending_bytes))
                    pending_bytes, reported_at = 0, now

            try:
                result = qualify_task(task, report)
                if pending_bytes:
                    pipe.send(("progress", pending_bytes))
                pipe.send(("complete", result))
            except RawAuthorityPending as exc:
                pipe.send(("pending", str(exc)))
            except Exception as exc:
                pipe.send(("error", f"{type(exc).__name__}: {exc}"))
    except (EOFError, BrokenPipeError):
        return
    finally:
        pipe.close()


class CancellableExecutor:
    """Amortize spawn cost without giving an unfinished task any acknowledgement."""

    def __init__(self) -> None:
        self.pipe: Connection | None = None
        self.process: Any = None

    def close(self) -> None:
        if self.pipe:
            self.pipe.close()
            self.pipe = None
        if self.process:
            if self.process.is_alive():
                self.process.terminate()
                self.process.join(0.5)
            if self.process.is_alive():
                self.process.kill()
            self.process.join()
            self.process = None

    def __call__(
        self,
        task: dict[str, Any],
        *,
        remaining_ns: int | None,
        progress: Callable[[int], None] | None = None,
        time_ns: Callable[[], int] = time.monotonic_ns,
    ) -> dict[str, Any] | None:
        if remaining_ns is not None and remaining_ns <= 0:
            return None
        if self.pipe is None:
            context = multiprocessing.get_context("spawn")
            self.pipe, child = context.Pipe()
            self.process = context.Process(target=_server, args=(child,), daemon=True)
            self.process.start()
            child.close()
        started = last_progress = time_ns()
        self.pipe.send(task)
        try:
            while True:
                now = time_ns()
                if remaining_ns is not None and now - started >= remaining_ns:
                    self.close()
                    return None
                if now - last_progress > AUDIT_NO_PROGRESS_TIMEOUT_NS:
                    raise AuditInterrupted("terminal audit watchdog: no forward progress")
                if self.pipe.poll(0.02):
                    kind, value = self.pipe.recv()
                    if kind == "complete" and isinstance(value, dict):
                        return value
                    if kind == "error":
                        raise AcceptanceError(str(value))
                    if kind == "pending":
                        raise RawAuthorityPending(str(value))
                    if kind != "progress" or not isinstance(value, int) or value <= 0:
                        raise AcceptanceError("invalid qualification worker message")
                    last_progress = now
                    if progress:
                        progress(value)
                elif not self.process.is_alive():
                    raise AuditInterrupted(
                        "qualification worker exited without completed authority"
                    )
        except (EOFError, BrokenPipeError) as exc:
            self.close()
            raise AuditInterrupted("qualification worker interrupted") from exc
        except BaseException:
            self.close()
            raise


def _worker(pipe: Connection, task: dict[str, Any]) -> None:
    if task.get("operation") == "run-audit":
        os.setsid()
        pipe.send(("group-ready", os.getpid()))
    pending_bytes = 0
    reported_at = time.monotonic_ns()

    def report(size: int) -> None:
        nonlocal pending_bytes, reported_at
        pending_bytes += size
        now = time.monotonic_ns()
        if pending_bytes >= 1024 * 1024 or now - reported_at >= AUDIT_PROGRESS_INTERVAL_NS:
            pipe.send(("progress", pending_bytes))
            pending_bytes, reported_at = 0, now

    try:
        result = qualify_task(task, report)
        if pending_bytes:
            pipe.send(("progress", pending_bytes))
        pipe.send(("complete", result))
    except RawAuthorityPending as exc:
        pipe.send(("pending", str(exc)))
    except Exception as exc:
        pipe.send(("error", f"{type(exc).__name__}: {exc}"))
    finally:
        pipe.close()


def cancellable_unit(
    task: dict[str, Any],
    *,
    remaining_ns: int | None,
    progress: Callable[[int], None] | None = None,
    time_ns: Callable[[], int] = time.monotonic_ns,
    worker: Callable[[Connection, dict[str, Any]], None] = _worker,
) -> dict[str, Any] | None:
    """None means budget exhaustion, not a completed or acknowledged Raw unit."""
    if remaining_ns is not None and remaining_ns <= 0:
        return None
    context = multiprocessing.get_context("spawn")
    receiver, sender = context.Pipe(duplex=False)
    process = context.Process(
        target=worker, args=(sender, task), daemon=task.get("operation") != "run-audit"
    )
    started = last_progress = time_ns()
    process.start()
    sender.close()
    group_ready = False
    try:
        while True:
            now = time_ns()
            if remaining_ns is not None and now - started >= remaining_ns:
                return None
            if now - last_progress > AUDIT_NO_PROGRESS_TIMEOUT_NS:
                raise AuditInterrupted("terminal audit watchdog: no forward progress")
            if receiver.poll(0.02):
                kind, value = receiver.recv()
                if (
                    kind == "group-ready"
                    and value == process.pid
                    and task.get("operation") == "run-audit"
                ):
                    group_ready = True
                    continue
                if kind == "complete":
                    if not isinstance(value, dict):
                        raise AcceptanceError("invalid Raw worker result")
                    return value
                if kind == "error":
                    raise AcceptanceError(str(value))
                if kind == "pending":
                    raise RawAuthorityPending(str(value))
                if kind != "progress" or not isinstance(value, int) or value <= 0:
                    raise AcceptanceError("invalid Raw worker progress")
                last_progress = now
                if progress:
                    progress(value)
            elif not process.is_alive():
                raise AuditInterrupted(f"Raw qualification worker exited {process.exitcode}")
    finally:
        receiver.close()
        if task.get("operation") == "run-audit" and process.pid is not None:
            with suppress(ProcessLookupError):
                group_ready = group_ready or os.getpgid(process.pid) == process.pid
        if group_ready:
            assert process.pid is not None
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGTERM)
        if process.is_alive():
            process.terminate()
            process.join(0.5)
        if process.is_alive():
            process.kill()
        if group_ready:
            assert process.pid is not None
            with suppress(ProcessLookupError):
                os.killpg(process.pid, signal.SIGKILL)
        process.join()
