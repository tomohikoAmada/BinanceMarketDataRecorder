"""崩溃可协调的归档事务和独立提交的本地删除。

ArchiveManager 实现 ADR-0015:每笔事务保留最旧的 SEALED chunk,将其流式
复制到注册外部卷上的 .copying 临时文件,通过大小和 SHA-256 验证完整回读,
原子重命名为最终不可变名称,提交外部 manifest,在 Catalog 中提交已验证位置,
然后单独授权内部源删除。

事务步骤与崩溃边界:
1. _reserve:验证源 sealed artifact 和 manifest,以 COPYING 状态创建幂等
   Catalog archive_transaction。chunk 从 SEALED 迁移到 ARCHIVE_COPYING。
2. _copy:将源字节流式传输到目标 .copying 文件,fsync,目录 fsync。
   若目标已存在,验证其匹配(幂等;不覆盖)。
3. VERIFYING 转换:提交到 Catalog。
4. _verify_and_commit_external:验证目标大小/哈希,从 .copying 原子重命名为
   最终名称,fsync 目录。写入嵌入 Raw manifest 字节(base64)的外部 manifest,
   实现自包含验证。
5. VERIFIED 转换:提交到 Catalog。chunk 迁移到 ARCHIVED_VERIFIED。
6. LOCAL_DELETE_PENDING 转换:重新验证外部提交,提交到 Catalog。
7. _local_delete:验证内部源与存储哈希匹配,unlink 源文件,fsync sealed 目录。
   LOCAL_DELETED 转换提交删除。

每一步均可重启协调。若外部卷在复制期间消失(EIO、ENXIO、ENODEV),错误记录为
DISAPPEARED_DURING_COPY,内部源保留。卷重新出现后事务幂等重试。

内部源删除仅在第 5 步 VERIFIED 已提交、进入第 6 步 LOCAL_DELETE_PENDING
并再次验证外部提交后授权。第 1-5 步永不删除内部数据。
进入 LOCAL_DELETED 后,外部 artifact 可能是唯一副本;这不是备份策略。
"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import time
import uuid
from collections.abc import Callable
from contextlib import suppress
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from ..metrics.model import MetricAggregate
from ..metrics.recorder import utc_date_from_ns
from ..spool.seal import SealError, validate_sealed_artifact
from ..storage.catalog import ArchiveState, Catalog, ChunkState
from ..storage.layout import StorageLayout, fsync_directory
from ..storage.macos import StorageRegistrationError, validate_registered_root

ARCHIVE_MANIFEST_SCHEMA = "external-archive-manifest.v1"
COPY_BUFFER_BYTES = 1024 * 1024
FaultHook = Callable[[str, Path | None], None]


class ArchiveError(RuntimeError):
    """The archive transaction cannot safely advance."""


@dataclass(frozen=True, slots=True)
class ArchiveTarget:
    storage_id: str
    volume_uuid: str
    registered_relative_path: str
    marker_nonce: str
    root: Path


@dataclass(frozen=True, slots=True)
class ArchiveResult:
    transaction_id: str | None
    chunk_id: str | None
    state: str
    archived_bytes: int
    deleted_local_bytes: int
    warning: str | None = None


class ArchiveManager:
    def __init__(
        self,
        *,
        layout: StorageLayout,
        catalog: Catalog,
        target: ArchiveTarget,
        fault_hook: FaultHook | None = None,
        utc_clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.layout = layout
        self.catalog = catalog
        self.target = target
        self.fault_hook = fault_hook
        self.utc_clock_ns = utc_clock_ns

    def run_once(self) -> ArchiveResult:
        transaction = self._next_transaction()
        if transaction is None:
            return ArchiveResult(None, None, "NO_ELIGIBLE_CHUNKS", 0, 0)
        return self._run_transaction(transaction)

    def release_verified_once(self) -> ArchiveResult:
        """Release one locally retained source whose external commit is verified."""

        transaction = next(
            (
                row
                for row in self.catalog.archive_transactions(
                    storage_id=self.target.storage_id
                )
                if ArchiveState(str(row["state"]))
                in {ArchiveState.VERIFIED, ArchiveState.LOCAL_DELETE_PENDING}
            ),
            None,
        )
        if transaction is None:
            return ArchiveResult(None, None, "NO_VERIFIED_RELEASE", 0, 0)
        return self._run_transaction(transaction)

    def _run_transaction(
        self, transaction: dict[str, object]
    ) -> ArchiveResult:
        transaction_id = str(transaction["transaction_id"])
        self.catalog.begin_archive_attempt(transaction_id)
        self._hit("attempt_started")
        try:
            return self._advance(transaction_id)
        except Exception as exc:
            error: ArchiveError
            if isinstance(exc, ArchiveError):
                error = exc
            elif isinstance(exc, OSError) and exc.errno in {5, 6, 19}:
                error = ArchiveError(f"DISAPPEARED_DURING_COPY: {exc}")
            else:
                error = ArchiveError(str(exc))
            with suppress(Exception):
                self.catalog.record_archive_error(
                    transaction_id, f"{type(error).__name__}: {error}"
                )
            with suppress(Exception):
                self._record_attempt_failure(transaction_id, error)
            if error is exc:
                raise
            raise error from exc

    def status(self) -> dict[str, object]:
        transactions = self.catalog.archive_transactions(storage_id=self.target.storage_id)
        backlog = self.catalog.chunks_in_states(
            ChunkState.SEALED,
            ChunkState.ARCHIVE_COPYING,
            ChunkState.ARCHIVE_VERIFYING,
            ChunkState.ARCHIVED_VERIFIED,
            ChunkState.LOCAL_DELETE_PENDING,
        )
        return {
            "storage_id": self.target.storage_id,
            "status": (
                "DISAPPEARED_DURING_COPY"
                if any(
                    "DISAPPEARED_DURING_COPY" in str(row.get("last_error"))
                    for row in transactions
                )
                else "OK"
            ),
            "transactions": transactions,
            "transaction_count": len(transactions),
            "backlog_files": len(backlog),
            "backlog_bytes": sum(_row_int(row, "stored_bytes", default=0) for row in backlog),
            "unique_copy_warning": (
                "After LOCAL_DELETED, the registered external artifact may be the only copy."
            ),
        }

    def verify_all(self) -> dict[str, object]:
        self._validate_target_identity()
        results: list[dict[str, object]] = []
        for transaction in self.catalog.archive_transactions(
            storage_id=self.target.storage_id
        ):
            state = ArchiveState(str(transaction["state"]))
            if state not in {
                ArchiveState.VERIFIED,
                ArchiveState.LOCAL_DELETE_PENDING,
                ArchiveState.LOCAL_DELETED,
            }:
                continue
            try:
                self._validate_external_commit(transaction)
            except ArchiveError as exc:
                results.append(
                    {
                        "chunk_id": transaction["chunk_id"],
                        "status": "FAILED",
                        "reason": str(exc),
                    }
                )
            else:
                results.append(
                    {"chunk_id": transaction["chunk_id"], "status": "VERIFIED"}
                )
        failures = sum(item["status"] == "FAILED" for item in results)
        return {
            "storage_id": self.target.storage_id,
            "status": (
                "NO_VERIFIED_FILES"
                if not results
                else ("FAILED" if failures else "VERIFIED")
            ),
            "verified_files": len(results) - failures,
            "failed_files": failures,
            "pending_files": len(
                self.catalog.archive_transactions(storage_id=self.target.storage_id)
            )
            - len(results),
            "files": results,
        }

    def _next_transaction(self) -> dict[str, object] | None:
        transaction = self.catalog.oldest_incomplete_archive_transaction(
            self.target.storage_id
        )
        if transaction is not None:
            return transaction
        chunk = self.catalog.oldest_chunk_in_states(ChunkState.SEALED)
        if chunk is None:
            return None
        return self._reserve(chunk)

    def _reserve(self, chunk: dict[str, object]) -> dict[str, object]:
        chunk_id = str(chunk["chunk_id"])
        source = self._internal_path(chunk, "sealed_path", self.layout.sealed)
        manifest_path = self._internal_path(
            chunk, "manifest_path", self.layout.manifests
        )
        manifest_bytes, manifest = _load_json_bytes(manifest_path)
        _validate_raw_manifest(manifest, chunk_id=chunk_id, chunk=chunk)
        try:
            validate_sealed_artifact(source, manifest)
        except (OSError, SealError) as exc:
            raise ArchiveError(f"source Raw validation failed: {exc}") from exc
        transaction_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"bmdr-archive-v1:{self.target.storage_id}:{chunk_id}",
            )
        )
        target_relative = f"raw/{source.name}"
        target_temp_relative = f"raw/.{source.name}.{transaction_id}.copying"
        external_manifest_relative = f"manifests/{chunk_id}.archive-manifest.json"
        transaction = self.catalog.reserve_archive_transaction(
            transaction_id=transaction_id,
            chunk_id=chunk_id,
            storage_id=self.target.storage_id,
            market=_required_text(manifest, "market"),
            stream=_required_text(manifest, "stream"),
            source_relative_path=self.layout.relative(source),
            source_manifest_relative_path=self.layout.relative(manifest_path),
            source_manifest_sha256=hashlib.sha256(manifest_bytes).hexdigest(),
            target_relative_path=target_relative,
            target_temp_relative_path=target_temp_relative,
            external_manifest_relative_path=external_manifest_relative,
            stored_bytes=_required_int(manifest, "stored_bytes"),
            stored_sha256=_required_text(manifest, "stored_sha256"),
        )
        self._hit("after_reserve")
        return transaction

    def _advance(self, transaction_id: str) -> ArchiveResult:
        transaction = self._required_transaction(transaction_id)
        state = ArchiveState(str(transaction["state"]))
        if state is ArchiveState.COPYING:
            self._copy(transaction)
            self.catalog.transition_archive(
                transaction_id,
                ArchiveState.VERIFYING,
                idempotency_key=f"archive-verifying:{transaction_id}",
                evidence={"copy_fsync": True},
            )
            self._hit("after_copy_catalog_transition")
            transaction = self._required_transaction(transaction_id)
            state = ArchiveState.VERIFYING
        if state is ArchiveState.VERIFYING:
            external_manifest = self._verify_and_commit_external(transaction)
            verified_at = _required_int(external_manifest, "verified_at_utc_ns")
            self._hit("before_catalog_commit")
            self.catalog.transition_archive(
                transaction_id,
                ArchiveState.VERIFIED,
                idempotency_key=f"archive-verified:{transaction_id}",
                evidence={
                    "external_manifest_schema": ARCHIVE_MANIFEST_SCHEMA,
                    "stored_sha256": transaction["stored_sha256"],
                },
                verified_at_utc_ns=verified_at,
            )
            self._hit("after_catalog_commit")
            transaction = self._required_transaction(transaction_id)
            state = ArchiveState.VERIFIED
        if state is ArchiveState.VERIFIED:
            self._record_archived_metric(transaction)
            self.catalog.transition_archive(
                transaction_id,
                ArchiveState.LOCAL_DELETE_PENDING,
                idempotency_key=f"local-delete-pending:{transaction_id}",
                evidence={"external_commit_revalidated_before_delete": True},
            )
            transaction = self._required_transaction(transaction_id)
            state = ArchiveState.LOCAL_DELETE_PENDING
        deleted_bytes = 0
        if state is ArchiveState.LOCAL_DELETE_PENDING:
            self._validate_external_commit(transaction)
            self._hit("before_local_delete")
            source = self._source_path(transaction)
            if source.exists():
                self._validate_source_stored_identity(source, transaction)
                source.unlink()
                deleted_bytes = _row_int(transaction, "stored_bytes")
                self._hit("after_local_unlink", source)
                fsync_directory(self.layout.sealed)
            self._hit("before_local_delete_catalog_commit")
            deleted_at = self.utc_clock_ns()
            self._record_deleted_metric(transaction, deleted_at_utc_ns=deleted_at)
            self.catalog.transition_archive(
                transaction_id,
                ArchiveState.LOCAL_DELETED,
                idempotency_key=f"local-deleted:{transaction_id}",
                evidence={"source_absent": True},
                local_deleted_at_utc_ns=deleted_at,
            )
            transaction = self._required_transaction(transaction_id)
            self._hit("after_local_delete_catalog_commit")
        final = self._required_transaction(transaction_id)
        return ArchiveResult(
            transaction_id=transaction_id,
            chunk_id=str(final["chunk_id"]),
            state=str(final["state"]),
            archived_bytes=_row_int(final, "stored_bytes"),
            deleted_local_bytes=deleted_bytes,
            warning=(
                "The registered external artifact may now be the only Raw data copy."
            ),
        )

    def _copy(self, transaction: dict[str, object]) -> None:
        self._validate_target_identity()
        self._ensure_external_directories()
        source = self._source_path(transaction)
        self._validate_source_bundle(source, transaction)
        target = self._external_path(str(transaction["target_relative_path"]))
        temporary = self._external_path(str(transaction["target_temp_relative_path"]))
        if target.exists():
            self._validate_external_artifact(target, transaction)
            if temporary.exists():
                temporary.unlink()
                fsync_directory(temporary.parent)
            return
        if temporary.exists():
            temporary.unlink()
            fsync_directory(temporary.parent)
        descriptor = os.open(
            temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600
        )
        self._hit("before_copy", temporary)
        try:
            with source.open("rb", buffering=0) as source_handle:
                while block := source_handle.read(COPY_BUFFER_BYTES):
                    _write_all(descriptor, block)
                    self._hit("copy_progress", temporary)
            self._hit("before_copy_fsync", temporary)
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        fsync_directory(temporary.parent)
        self._hit("after_copy_fsync", temporary)

    def _verify_and_commit_external(
        self, transaction: dict[str, object]
    ) -> dict[str, object]:
        self._validate_target_identity()
        self._ensure_external_directories()
        source = self._source_path(transaction)
        self._validate_source_bundle(source, transaction)
        target = self._external_path(str(transaction["target_relative_path"]))
        temporary = self._external_path(str(transaction["target_temp_relative_path"]))
        if target.exists():
            self._validate_external_artifact(target, transaction)
            if temporary.exists():
                temporary.unlink()
                fsync_directory(temporary.parent)
        else:
            if not temporary.exists():
                self._copy(transaction)
            self._hit("before_verify", temporary)
            try:
                self._validate_external_artifact(temporary, transaction)
            except ArchiveError:
                temporary.unlink(missing_ok=True)
                fsync_directory(temporary.parent)
                raise
            self._hit("after_verify", temporary)
            os.replace(temporary, target)
            fsync_directory(target.parent)
            self._hit("after_final_rename", target)
        raw_manifest_path = self._source_manifest_path(transaction)
        raw_manifest_bytes, raw_manifest = _load_json_bytes(raw_manifest_path)
        if hashlib.sha256(raw_manifest_bytes).hexdigest() != transaction[
            "source_manifest_sha256"
        ]:
            raise ArchiveError("source manifest changed after reservation")
        verified_at = self.utc_clock_ns()
        document: dict[str, object] = {
            "archive_manifest_schema_version": ARCHIVE_MANIFEST_SCHEMA,
            "transaction_id": transaction["transaction_id"],
            "chunk_id": transaction["chunk_id"],
            "storage_id": self.target.storage_id,
            "volume_uuid": self.target.volume_uuid,
            "registered_relative_path": self.target.registered_relative_path,
            "artifact_relative_path": transaction["target_relative_path"],
            "stored_bytes": transaction["stored_bytes"],
            "stored_sha256": transaction["stored_sha256"],
            "source_manifest_sha256": transaction["source_manifest_sha256"],
            "raw_manifest": raw_manifest,
            "raw_manifest_bytes_base64": base64.b64encode(raw_manifest_bytes).decode(
                "ascii"
            ),
            "verification": {
                "full_readback": True,
                "size_match": True,
                "sha256_match": True,
            },
            "verified_at_utc_ns": verified_at,
        }
        external_manifest = self._external_path(
            str(transaction["external_manifest_relative_path"])
        )
        if external_manifest.exists():
            existing_bytes, existing = _load_json_bytes(external_manifest)
            del existing_bytes
            _validate_external_manifest(existing, document)
            document = existing
        else:
            partial = external_manifest.with_name(
                f".{external_manifest.name}.{transaction['transaction_id']}.partial"
            )
            if partial.exists():
                partial.unlink()
                fsync_directory(partial.parent)
            _atomic_json(external_manifest, partial, document)
        self._hit("after_external_manifest", external_manifest)
        return document

    def _validate_external_commit(self, transaction: dict[str, object]) -> None:
        self._validate_target_identity()
        artifact = self._external_path(str(transaction["target_relative_path"]))
        self._validate_external_artifact(artifact, transaction)
        manifest_path = self._external_path(
            str(transaction["external_manifest_relative_path"])
        )
        _, manifest = _load_json_bytes(manifest_path)
        expected = {
            "archive_manifest_schema_version": ARCHIVE_MANIFEST_SCHEMA,
            "transaction_id": transaction["transaction_id"],
            "chunk_id": transaction["chunk_id"],
            "storage_id": self.target.storage_id,
            "volume_uuid": self.target.volume_uuid,
            "registered_relative_path": self.target.registered_relative_path,
            "artifact_relative_path": transaction["target_relative_path"],
            "stored_bytes": transaction["stored_bytes"],
            "stored_sha256": transaction["stored_sha256"],
            "source_manifest_sha256": transaction["source_manifest_sha256"],
        }
        _validate_external_manifest(manifest, expected)

    def _validate_external_artifact(
        self, path: Path, transaction: dict[str, object]
    ) -> None:
        if not path.is_file():
            raise ArchiveError(f"external artifact is missing: {path.name}")
        expected_size = _row_int(transaction, "stored_bytes")
        if path.stat().st_size != expected_size:
            raise ArchiveError("external artifact size mismatch")
        digest = hashlib.sha256()
        with path.open("rb", buffering=0) as handle:
            while block := handle.read(COPY_BUFFER_BYTES):
                digest.update(block)
                self._hit("verify_progress", path)
        if digest.hexdigest() != transaction["stored_sha256"]:
            raise ArchiveError("external artifact SHA-256 mismatch")

    def _validate_source_bundle(
        self, source: Path, transaction: dict[str, object]
    ) -> None:
        manifest_bytes, manifest = _load_json_bytes(
            self._source_manifest_path(transaction)
        )
        if hashlib.sha256(manifest_bytes).hexdigest() != transaction[
            "source_manifest_sha256"
        ]:
            raise ArchiveError("source manifest SHA-256 mismatch")
        _validate_raw_manifest(
            manifest, chunk_id=str(transaction["chunk_id"]), chunk=transaction
        )
        try:
            validate_sealed_artifact(source, manifest)
        except (OSError, SealError) as exc:
            raise ArchiveError(f"source Raw validation failed: {exc}") from exc

    def _validate_source_stored_identity(
        self, source: Path, transaction: dict[str, object]
    ) -> None:
        if not source.is_file():
            raise ArchiveError("internal source is missing before authorized deletion")
        if source.stat().st_size != _row_int(transaction, "stored_bytes"):
            raise ArchiveError("internal source size changed before deletion")
        if _sha256_file(source) != transaction["stored_sha256"]:
            raise ArchiveError("internal source hash changed before deletion")

    def _validate_target_identity(self) -> None:
        try:
            validate_registered_root(
                self.target.root,
                volume_uuid=self.target.volume_uuid,
                relative_path=self.target.registered_relative_path,
                storage_id=self.target.storage_id,
                marker_nonce=self.target.marker_nonce,
            )
        except StorageRegistrationError as exc:
            raise ArchiveError(f"registered target identity unavailable: {exc}") from exc

    def _ensure_external_directories(self) -> None:
        for name in ("raw", "manifests"):
            path = self.target.root / name
            if path.is_symlink():
                raise ArchiveError(f"archive subdirectory is a symbolic link: {name}")
            path.mkdir(mode=0o700, exist_ok=True)
            if path.is_symlink():
                raise ArchiveError(f"archive subdirectory became a symbolic link: {name}")
            resolved = path.resolve()
            if resolved.parent != self.target.root.resolve() or resolved.name != name:
                raise ArchiveError("archive subdirectory resolves outside registered root")
            if not resolved.is_dir():
                raise ArchiveError(f"archive subdirectory is unavailable: {name}")
            fsync_directory(resolved)
        fsync_directory(self.target.root)

    def _external_path(self, relative: str) -> Path:
        unresolved = self.target.root
        for part in Path(relative).parts:
            unresolved /= part
            if unresolved.is_symlink():
                raise ArchiveError("external archive path is a symbolic link")
        candidate = unresolved.resolve()
        try:
            resolved_relative = candidate.relative_to(self.target.root.resolve()).as_posix()
        except ValueError as exc:
            raise ArchiveError("external path escapes registered root") from exc
        if resolved_relative != relative:
            raise ArchiveError("external path resolves through an unexpected alias")
        return candidate

    def _internal_path(
        self, row: dict[str, object], key: str, required_parent: Path
    ) -> Path:
        relative = row.get(key)
        if not isinstance(relative, str) or not relative:
            raise ArchiveError(f"Catalog chunk lacks {key}")
        candidate = (self.layout.root / relative).resolve()
        if candidate.parent != required_parent.resolve():
            raise ArchiveError(f"Catalog {key} escapes its internal directory")
        return candidate

    def _source_path(self, transaction: dict[str, object]) -> Path:
        return self._internal_path(
            {"path": transaction["source_relative_path"]}, "path", self.layout.sealed
        )

    def _source_manifest_path(self, transaction: dict[str, object]) -> Path:
        return self._internal_path(
            {"path": transaction["source_manifest_relative_path"]},
            "path",
            self.layout.manifests,
        )

    def _required_transaction(self, transaction_id: str) -> dict[str, object]:
        transaction = self.catalog.archive_transaction(transaction_id)
        if transaction is None:
            raise ArchiveError(f"archive transaction disappeared: {transaction_id}")
        return transaction

    def _record_archived_metric(self, transaction: dict[str, object]) -> None:
        verified_at = _row_int(transaction, "verified_at_utc_ns")
        aggregate = MetricAggregate()
        aggregate.increment("archived_files")
        aggregate.increment("archived_bytes", _row_int(transaction, "stored_bytes"))
        self.catalog.record_metric_batch(
            batch_id=f"archive-verified:{transaction['transaction_id']}",
            rows=[
                (
                    utc_date_from_ns(verified_at),
                    str(transaction["market"]),
                    str(transaction["stream"]),
                    aggregate.document(),
                )
            ],
            committed_at_utc_ns=verified_at,
        )

    def _record_deleted_metric(
        self,
        transaction: dict[str, object],
        *,
        deleted_at_utc_ns: int,
    ) -> None:
        aggregate = MetricAggregate()
        aggregate.increment(
            "deleted_local_bytes", _row_int(transaction, "stored_bytes")
        )
        self.catalog.record_metric_batch(
            batch_id=f"archive-local-delete:{transaction['transaction_id']}",
            rows=[
                (
                    utc_date_from_ns(deleted_at_utc_ns),
                    str(transaction["market"]),
                    str(transaction["stream"]),
                    aggregate.document(),
                )
            ],
            committed_at_utc_ns=deleted_at_utc_ns,
        )

    def _record_attempt_failure(
        self,
        transaction_id: str,
        error: ArchiveError,
    ) -> None:
        transaction = self._required_transaction(transaction_id)
        attempt_count = _row_int(transaction, "attempt_count")
        source = self._source_path(transaction)
        failure_kind = (
            "DISAPPEARED_DURING_COPY"
            if "DISAPPEARED_DURING_COPY" in str(error)
            else "ARCHIVE_ERROR"
        )
        self.catalog.record_operational_event(
            event_id=f"archive-attempt-failed:{transaction_id}:{attempt_count}",
            event_type="ARCHIVE_ATTEMPT_FAILED",
            occurred_at_utc_ns=self.utc_clock_ns(),
            evidence={
                "transaction_id": transaction_id,
                "chunk_id": transaction["chunk_id"],
                "storage_id": transaction["storage_id"],
                "attempt_count": attempt_count,
                "catalog_state": transaction["state"],
                "failure_kind": failure_kind,
                "error": f"{type(error).__name__}: {error}",
                "source_exists": source.is_file(),
                "source_preserved": source.is_file(),
            },
        )

    def _hit(self, point: str, path: Path | None = None) -> None:
        if self.fault_hook is not None:
            self.fault_hook(point, path)


def _load_json_bytes(path: Path) -> tuple[bytes, dict[str, object]]:
    try:
        body = path.read_bytes()
        decoded: Any = json.loads(body)
    except (OSError, json.JSONDecodeError) as exc:
        raise ArchiveError(f"cannot read manifest {path}: {exc}") from exc
    if not isinstance(decoded, dict):
        raise ArchiveError(f"manifest is not an object: {path}")
    return body, cast(dict[str, object], decoded)


def _validate_raw_manifest(
    manifest: dict[str, object], *, chunk_id: str, chunk: dict[str, object]
) -> None:
    expected = {
        "manifest_schema_version": "raw-chunk-manifest.v1",
        "chunk_id": chunk_id,
        "stored_bytes": chunk["stored_bytes"],
        "stored_sha256": chunk["stored_sha256"],
    }
    mismatches = [key for key, value in expected.items() if manifest.get(key) != value]
    if mismatches:
        raise ArchiveError(
            f"source Raw manifest/Catalog mismatch: {', '.join(sorted(mismatches))}"
        )


def _validate_external_manifest(
    document: dict[str, object], expected: dict[str, object]
) -> None:
    stable_fields = (
        "archive_manifest_schema_version",
        "transaction_id",
        "chunk_id",
        "storage_id",
        "volume_uuid",
        "registered_relative_path",
        "artifact_relative_path",
        "stored_bytes",
        "stored_sha256",
        "source_manifest_sha256",
    )
    mismatches = [
        field for field in stable_fields if document.get(field) != expected.get(field)
    ]
    if mismatches:
        raise ArchiveError(
            f"external manifest identity mismatch: {', '.join(sorted(mismatches))}"
        )
    encoded = document.get("raw_manifest_bytes_base64")
    embedded = document.get("raw_manifest")
    if not isinstance(encoded, str) or not isinstance(embedded, dict):
        raise ArchiveError("external manifest lacks embedded Raw manifest evidence")
    try:
        raw_bytes = base64.b64decode(encoded, validate=True)
        decoded = json.loads(raw_bytes)
    except (ValueError, json.JSONDecodeError) as exc:
        raise ArchiveError("external manifest Raw evidence is invalid") from exc
    if decoded != embedded:
        raise ArchiveError("external manifest Raw document/bytes mismatch")
    if hashlib.sha256(raw_bytes).hexdigest() != document.get(
        "source_manifest_sha256"
    ):
        raise ArchiveError("external manifest Raw SHA-256 mismatch")
    if document.get("verification") != {
        "full_readback": True,
        "size_match": True,
        "sha256_match": True,
    }:
        raise ArchiveError("external manifest lacks complete verification evidence")
    verified_at = document.get("verified_at_utc_ns")
    if (
        not isinstance(verified_at, int)
        or isinstance(verified_at, bool)
        or verified_at < 0
    ):
        raise ArchiveError("external manifest lacks a valid verification time")


def _atomic_json(path: Path, partial: Path, document: dict[str, object]) -> None:
    body = (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    descriptor = os.open(partial, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        _write_all(descriptor, body)
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(partial, path)
    fsync_directory(path.parent)


def _write_all(descriptor: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        written = os.write(descriptor, view)
        if written <= 0:
            raise OSError("write returned no progress")
        view = view[written:]


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb", buffering=0) as handle:
        while block := handle.read(COPY_BUFFER_BYTES):
            digest.update(block)
    return digest.hexdigest()


def _required_text(document: dict[str, object], key: str) -> str:
    value = document.get(key)
    if not isinstance(value, str) or not value:
        raise ArchiveError(f"manifest lacks {key}")
    return value


def _required_int(document: dict[str, object], key: str) -> int:
    value = document.get(key)
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ArchiveError(f"manifest lacks valid {key}")
    return value


def _row_int(row: dict[str, object], key: str, *, default: int | None = None) -> int:
    value = row.get(key)
    if value is None and default is not None:
        return default
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise ArchiveError(f"Catalog archive row lacks valid {key}")
    return value
