"""Durable local receive and receipt authority for M22.3.

The source selection is supplied by the read-only M22.1 kernel.  This module
does not query or mutate the live Catalog and never deletes a source.  It
orders immutable local facts as Raw, external archive manifest, Archive Set
entry, then receipt; no receipt is returned until its parent directory has
been fsynced and the complete chain has been independently revalidated.
"""

from __future__ import annotations

import base64
import errno
import hashlib
import json
import os
import stat
import sys
import time
import uuid
from collections.abc import Callable, Mapping
from contextlib import closing, suppress
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import BinaryIO, Protocol, cast

from ..spool.seal import MANIFEST_SCHEMA_VERSION
from ..storage.layout import fsync_directory
from .archive_set import (
    ArchiveMediumIdentity,
    ArchiveSetEntry,
    ArchiveSetError,
    ArchiveSetStore,
    _publish_no_clobber,
)
from .manager import ARCHIVE_MANIFEST_SCHEMA
from .remote_source import (
    REMOTE_SOURCE_DESCRIPTOR_SCHEMA,
    RemoteSourceSelection,
    canonical_descriptor_bytes,
    descriptor_sha256,
)

REMOTE_ARCHIVE_RECEIPT_SCHEMA = "remote-archive-receipt.v1"
REMOTE_RECEIVE_VERIFICATION_VERSION = "m22.3-receive-verification.v1"
REMOTE_RECEIVE_VERIFICATION_OUTCOME = "VERIFIED"
REMOTE_RECEIVE_BUFFER_BYTES = 1024 * 1024
REMOTE_RECEIPTS_DIRECTORY_NAME = "receipts"

FaultHook = Callable[[str, Path | None], None]

_RECEIPT_FIELDS = (
    "receipt_schema_version",
    "receipt_id",
    "session_id",
    "verification_version",
    "verification_outcome",
    "source_descriptor_schema_version",
    "source_descriptor_sha256",
    "chunk_id",
    "source_relative_path",
    "source_manifest_relative_path",
    "source_manifest_sha256",
    "stored_bytes",
    "stored_sha256",
    "archive_set_id",
    "storage_id",
    "artifact_relative_path",
    "archive_set_entry_sha256",
)
_ARCHIVE_MANIFEST_FIELDS = (
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
    "raw_manifest",
    "raw_manifest_bytes_base64",
    "verification",
    "verified_at_utc_ns",
)


class RemoteReceiveError(RuntimeError):
    """A local receive cannot establish the complete durable authority chain."""


class StoredByteProvider(Protocol):
    """Minimal M22.3 source of stored Raw bytes; it has no deletion semantics."""

    def open_stored_bytes(self, selection: RemoteSourceSelection) -> BinaryIO:
        """Open the stored byte stream associated with ``selection``."""


@dataclass(frozen=True, slots=True)
class RemoteReceiveTarget:
    """Caller-selected logical set and registered physical destination."""

    archive_set_id: str
    storage_id: str
    volume_uuid: str
    registered_relative_path: str
    marker_nonce: str
    root: Path

    def __post_init__(self) -> None:
        for field in (
            "archive_set_id",
            "storage_id",
            "volume_uuid",
            "marker_nonce",
        ):
            _require_text(getattr(self, field), field)
        if self.archive_set_id == self.storage_id:
            raise RemoteReceiveError("archive_set_id and storage_id must differ")
        _require_relative_path(
            self.registered_relative_path,
            "registered_relative_path",
            allow_dot=True,
        )
        object.__setattr__(self, "root", Path(self.root).expanduser())


@dataclass(frozen=True, slots=True)
class RemoteArchiveReceipt:
    """Canonical immutable receipt for one session and one durable local chain."""

    receipt_schema_version: str
    receipt_id: str
    session_id: str
    verification_version: str
    verification_outcome: str
    source_descriptor_schema_version: str
    source_descriptor_sha256: str
    chunk_id: str
    source_relative_path: str
    source_manifest_relative_path: str
    source_manifest_sha256: str
    stored_bytes: int
    stored_sha256: str
    archive_set_id: str
    storage_id: str
    artifact_relative_path: str
    archive_set_entry_sha256: str

    def document(self) -> dict[str, object]:
        return {
            "receipt_schema_version": self.receipt_schema_version,
            "receipt_id": self.receipt_id,
            "session_id": self.session_id,
            "verification_version": self.verification_version,
            "verification_outcome": self.verification_outcome,
            "source_descriptor_schema_version": self.source_descriptor_schema_version,
            "source_descriptor_sha256": self.source_descriptor_sha256,
            "chunk_id": self.chunk_id,
            "source_relative_path": self.source_relative_path,
            "source_manifest_relative_path": self.source_manifest_relative_path,
            "source_manifest_sha256": self.source_manifest_sha256,
            "stored_bytes": self.stored_bytes,
            "stored_sha256": self.stored_sha256,
            "archive_set_id": self.archive_set_id,
            "storage_id": self.storage_id,
            "artifact_relative_path": self.artifact_relative_path,
            "archive_set_entry_sha256": self.archive_set_entry_sha256,
        }

    def identity_document(self) -> dict[str, object]:
        document = self.document()
        del document["receipt_id"]
        return document

    def canonical_bytes(self) -> bytes:
        self.validate()
        return _canonical_json(self.document())

    def validate(self) -> None:
        if self.receipt_schema_version != REMOTE_ARCHIVE_RECEIPT_SCHEMA:
            raise RemoteReceiveError("unsupported remote archive receipt schema")
        if self.verification_version != REMOTE_RECEIVE_VERIFICATION_VERSION:
            raise RemoteReceiveError("unsupported receive verification version")
        if self.verification_outcome != REMOTE_RECEIVE_VERIFICATION_OUTCOME:
            raise RemoteReceiveError("receipt verification outcome is not VERIFIED")
        if self.source_descriptor_schema_version != REMOTE_SOURCE_DESCRIPTOR_SCHEMA:
            raise RemoteReceiveError("unsupported source descriptor schema")
        _require_uuid4(self.session_id, "session_id")
        _require_safe_segment(self.chunk_id, "chunk_id")
        _require_relative_path(self.source_relative_path, "source_relative_path")
        _require_relative_path(
            self.source_manifest_relative_path, "source_manifest_relative_path"
        )
        _require_relative_path(self.artifact_relative_path, "artifact_relative_path")
        for field in (
            "receipt_id",
            "source_descriptor_sha256",
            "source_manifest_sha256",
            "stored_sha256",
            "archive_set_entry_sha256",
        ):
            _require_sha256(getattr(self, field), field)
        for field in ("archive_set_id", "storage_id"):
            _require_text(getattr(self, field), field)
        _require_nonnegative_int(self.stored_bytes, "stored_bytes")
        expected = hashlib.sha256(_canonical_json(self.identity_document())).hexdigest()
        if self.receipt_id != expected:
            raise RemoteReceiveError("receipt_id does not match receipt identity")

    @classmethod
    def build(
        cls,
        *,
        selection: RemoteSourceSelection,
        target: RemoteReceiveTarget,
        session_id: str,
        artifact_relative_path: str,
        archive_set_entry_sha256: str,
    ) -> RemoteArchiveReceipt:
        descriptor = selection.descriptor
        values: dict[str, object] = {
            "receipt_schema_version": REMOTE_ARCHIVE_RECEIPT_SCHEMA,
            "session_id": session_id,
            "verification_version": REMOTE_RECEIVE_VERIFICATION_VERSION,
            "verification_outcome": REMOTE_RECEIVE_VERIFICATION_OUTCOME,
            "source_descriptor_schema_version": descriptor.descriptor_schema_version,
            "source_descriptor_sha256": selection.descriptor_sha256,
            "chunk_id": descriptor.chunk_id,
            "source_relative_path": descriptor.source_relative_path,
            "source_manifest_relative_path": descriptor.source_manifest_relative_path,
            "source_manifest_sha256": descriptor.source_manifest_sha256,
            "stored_bytes": descriptor.stored_bytes,
            "stored_sha256": descriptor.stored_sha256,
            "archive_set_id": target.archive_set_id,
            "storage_id": target.storage_id,
            "artifact_relative_path": artifact_relative_path,
            "archive_set_entry_sha256": archive_set_entry_sha256,
        }
        receipt_id = hashlib.sha256(_canonical_json(values)).hexdigest()
        receipt = cls(receipt_id=receipt_id, **values)  # type: ignore[arg-type]
        receipt.validate()
        return receipt

    @classmethod
    def from_bytes(cls, body: bytes) -> RemoteArchiveReceipt:
        document = _decode_json_object(body, "remote archive receipt")
        _require_exact_fields(document, _RECEIPT_FIELDS, "remote archive receipt")
        try:
            receipt = cls(**document)  # type: ignore[arg-type]
        except TypeError as exc:
            raise RemoteReceiveError("remote archive receipt fields are invalid") from exc
        receipt.validate()
        if receipt.canonical_bytes() != body:
            raise RemoteReceiveError("remote archive receipt is not canonical")
        return receipt


def generate_archive_session_id() -> str:
    """Generate the canonical UUID4 identity for one receipt session."""

    return str(uuid.uuid4())


def receive_transaction_id(
    selection: RemoteSourceSelection,
    target: RemoteReceiveTarget,
    *,
    artifact_relative_path: str | None = None,
) -> str:
    """Derive stable external-manifest identity for one source/destination."""

    _validate_selection(selection)
    artifact = artifact_relative_path or _artifact_relative_path(selection)
    _require_relative_path(artifact, "artifact_relative_path")
    material = (
        "bmdr-remote-receive-v1:"
        f"{selection.descriptor_sha256}:"
        f"{target.archive_set_id}:"
        f"{target.storage_id}:"
        f"{artifact}"
    )
    return str(uuid.uuid5(uuid.NAMESPACE_URL, material))


class RemoteReceiver:
    """Receive one validated source into an immutable registered medium."""

    def __init__(
        self,
        *,
        provider: StoredByteProvider,
        target: RemoteReceiveTarget,
        fault_hook: FaultHook | None = None,
        utc_clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self.provider = provider
        self.target = target
        self.fault_hook = fault_hook
        self.utc_clock_ns = utc_clock_ns

    def receive(
        self,
        selection: RemoteSourceSelection,
        *,
        session_id: str,
    ) -> RemoteArchiveReceipt:
        """Commit and independently revalidate the complete local receipt chain."""

        try:
            return self._receive(selection, session_id=session_id)
        except RemoteReceiveError:
            raise
        except Exception as exc:
            raise RemoteReceiveError(f"remote receive failed closed: {exc}") from exc

    def _receive(
        self,
        selection: RemoteSourceSelection,
        *,
        session_id: str,
    ) -> RemoteArchiveReceipt:
        _require_supported_platform()
        source_manifest = _validate_selection(selection)
        del source_manifest
        _require_uuid4(session_id, "session_id")
        store = self._bind_target()
        raw_directory, manifests_directory, receipts_directory = (
            self._ensure_receive_directories(store)
        )
        self._revalidate_target(store)

        artifact_relative = _artifact_relative_path(selection)
        artifact_path = self.target.root.resolve() / artifact_relative
        self._commit_raw(selection, store, raw_directory, artifact_path)

        transaction_id = receive_transaction_id(
            selection,
            self.target,
            artifact_relative_path=artifact_relative,
        )
        manifest_relative = (
            f"manifests/{selection.descriptor.chunk_id}.archive-manifest.json"
        )
        manifest_path = self.target.root.resolve() / manifest_relative
        manifest_bytes = self._commit_archive_manifest(
            selection,
            store,
            manifests_directory,
            manifest_path,
            transaction_id=transaction_id,
            artifact_relative_path=artifact_relative,
        )
        archive_manifest_sha256 = hashlib.sha256(manifest_bytes).hexdigest()

        self._revalidate_target(store)
        self._hit("k8_before_archive_set_entry_commit")
        entry = ArchiveSetEntry(
            archive_set_id=self.target.archive_set_id,
            storage_id=self.target.storage_id,
            chunk_id=selection.descriptor.chunk_id,
            artifact_relative_path=artifact_relative,
            archive_manifest_relative_path=manifest_relative,
            archive_manifest_sha256=archive_manifest_sha256,
            stored_bytes=selection.descriptor.stored_bytes,
            stored_sha256=selection.descriptor.stored_sha256,
            source_manifest_sha256=selection.descriptor.source_manifest_sha256,
        )
        committed = store.commit_entry(entry)
        reread = store.read_entry(selection.descriptor.chunk_id)
        if committed != entry or reread != entry:
            raise RemoteReceiveError("Archive Set entry readback mismatch")
        entry_sha256 = hashlib.sha256(reread.canonical_bytes()).hexdigest()
        self._hit("k9_after_archive_set_entry_durable", store.entries_directory)

        receipt = RemoteArchiveReceipt.build(
            selection=selection,
            target=self.target,
            session_id=session_id,
            artifact_relative_path=artifact_relative,
            archive_set_entry_sha256=entry_sha256,
        )
        self._revalidate_target(store)
        receipt_path = receipts_directory / f"{receipt.receipt_id}.json"
        self._commit_receipt(
            selection, receipt, receipt_path, receipts_directory
        )
        return revalidate_remote_archive_receipt(
            selection=selection,
            target=self.target,
            receipt_id=receipt.receipt_id,
        )

    def _bind_target(self) -> ArchiveSetStore:
        return ArchiveSetStore.bind(
            self.target.root,
            archive_set_id=self.target.archive_set_id,
            storage_id=self.target.storage_id,
            volume_uuid=self.target.volume_uuid,
            registered_relative_path=self.target.registered_relative_path,
            marker_nonce=self.target.marker_nonce,
        )

    def _revalidate_target(self, store: ArchiveSetStore) -> None:
        if store.read_identity() != store.identity:
            raise RemoteReceiveError("Archive Set medium identity changed")

    def _ensure_receive_directories(
        self, store: ArchiveSetStore
    ) -> tuple[Path, Path, Path]:
        root = self.target.root.resolve()
        raw = _ensure_direct_directory(root, "raw")
        manifests = _ensure_direct_directory(root, "manifests")
        archive_set = store.archive_set_directory
        _validate_direct_directory(archive_set, root, "archive-set")
        receipts = _ensure_direct_directory(archive_set, REMOTE_RECEIPTS_DIRECTORY_NAME)
        fsync_directory(archive_set)
        fsync_directory(root)
        return raw, manifests, receipts

    def _commit_raw(
        self,
        selection: RemoteSourceSelection,
        store: ArchiveSetStore,
        raw_directory: Path,
        final: Path,
    ) -> None:
        expected_bytes = selection.descriptor.stored_bytes
        expected_sha256 = selection.descriptor.stored_sha256
        try:
            _verify_regular_file(final, expected_bytes, expected_sha256)
        except FileNotFoundError:
            pass
        else:
            fsync_directory(raw_directory)
            _verify_regular_file(final, expected_bytes, expected_sha256)
            self._revalidate_target(store)
            self._hit("k7_after_raw_durable", final)
            return

        self._hit("k0_before_artifact_temp", raw_directory)
        temporary = raw_directory / (
            f".{final.name}.{uuid.uuid4().hex}.receiving"
        )
        owned = False
        descriptor = -1
        try:
            descriptor = _exclusive_create(temporary)
            owned = True
            try:
                self._stream_exact(selection, descriptor, temporary)
                self._hit("k2_after_artifact_writes_before_fsync", temporary)
                os.fsync(descriptor)
                self._hit("k3_after_artifact_fsync_before_close", temporary)
            finally:
                os.close(descriptor)
                descriptor = -1
            _verify_regular_file(
                temporary,
                expected_bytes,
                expected_sha256,
                progress=lambda: self._hit("k4_during_artifact_temp_readback", temporary),
            )
            self._hit("k5_after_artifact_verification_before_publish", temporary)
            self._revalidate_target(store)
            published = _publish_no_clobber(temporary, final)
            if published:
                owned = False
                self._hit("k6_after_raw_rename_before_parent_fsync", final)
            else:
                _verify_regular_file(final, expected_bytes, expected_sha256)
            fsync_directory(raw_directory)
            _verify_regular_file(final, expected_bytes, expected_sha256)
            self._revalidate_target(store)
            self._hit("k7_after_raw_durable", final)
        finally:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            if owned:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def _stream_exact(
        self,
        selection: RemoteSourceSelection,
        destination_descriptor: int,
        temporary: Path,
    ) -> None:
        remaining = selection.descriptor.stored_bytes
        try:
            stream = self.provider.open_stored_bytes(selection)
            with closing(stream):
                while remaining:
                    block = stream.read(min(REMOTE_RECEIVE_BUFFER_BYTES, remaining))
                    if not isinstance(block, bytes):
                        raise RemoteReceiveError("stored byte provider returned non-bytes")
                    if not block:
                        raise RemoteReceiveError("stored byte provider ended before expected size")
                    if len(block) > remaining:
                        raise RemoteReceiveError("stored byte provider exceeded expected size")
                    _write_all(destination_descriptor, block)
                    remaining -= len(block)
                    self._hit("k1_during_artifact_transfer", temporary)
                extra = stream.read(1)
                if not isinstance(extra, bytes):
                    raise RemoteReceiveError("stored byte provider returned non-bytes")
                if extra:
                    raise RemoteReceiveError("stored byte provider returned extra bytes")
        except RemoteReceiveError:
            raise
        except Exception as exc:
            raise RemoteReceiveError(f"stored byte provider failure: {exc}") from exc

    def _commit_archive_manifest(
        self,
        selection: RemoteSourceSelection,
        store: ArchiveSetStore,
        manifests_directory: Path,
        final: Path,
        *,
        transaction_id: str,
        artifact_relative_path: str,
    ) -> bytes:
        self._revalidate_target(store)
        source_manifest = _decode_json_object(
            selection.manifest_bytes, "source Raw manifest"
        )
        expected = _archive_manifest_expected(
            selection,
            self.target,
            transaction_id=transaction_id,
            artifact_relative_path=artifact_relative_path,
        )
        document = {
            **expected,
            "raw_manifest": source_manifest,
            "raw_manifest_bytes_base64": base64.b64encode(
                selection.manifest_bytes
            ).decode("ascii"),
            "verification": {
                "full_readback": True,
                "size_match": True,
                "sha256_match": True,
            },
            "verified_at_utc_ns": self.utc_clock_ns(),
        }
        body = _canonical_json(document)
        try:
            existing = _safe_read_regular_file(final)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            _validate_archive_manifest_bytes(
                existing,
                expected=expected,
                source_manifest_bytes=selection.manifest_bytes,
            )
            fsync_directory(manifests_directory)
            winner = _safe_read_regular_file(final)
            _validate_archive_manifest_bytes(
                winner,
                expected=expected,
                source_manifest_bytes=selection.manifest_bytes,
            )
            self._hit("k7d_after_archive_manifest_durable", final)
            return winner

        temporary = manifests_directory / (
            f".{final.name}.{uuid.uuid4().hex}.partial"
        )
        owned = False
        descriptor = -1
        try:
            descriptor = _exclusive_create(temporary)
            owned = True
            try:
                _write_all(descriptor, body)
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
                descriptor = -1
            temp_body = _safe_read_regular_file(temporary)
            if temp_body != body:
                raise RemoteReceiveError("archive manifest temp readback mismatch")
            _validate_archive_manifest_bytes(
                temp_body,
                expected=expected,
                source_manifest_bytes=selection.manifest_bytes,
            )
            self._revalidate_target(store)
            published = _publish_no_clobber(temporary, final)
            if published:
                owned = False
                self._hit("k7m_after_archive_manifest_rename_before_parent_fsync", final)
            winner = _safe_read_regular_file(final)
            _validate_archive_manifest_bytes(
                winner,
                expected=expected,
                source_manifest_bytes=selection.manifest_bytes,
            )
            fsync_directory(manifests_directory)
            winner = _safe_read_regular_file(final)
            _validate_archive_manifest_bytes(
                winner,
                expected=expected,
                source_manifest_bytes=selection.manifest_bytes,
            )
            self._revalidate_target(store)
            self._hit("k7d_after_archive_manifest_durable", final)
            return winner
        finally:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            if owned:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def _commit_receipt(
        self,
        selection: RemoteSourceSelection,
        receipt: RemoteArchiveReceipt,
        final: Path,
        receipts_directory: Path,
    ) -> None:
        body = receipt.canonical_bytes()
        try:
            existing = _safe_read_regular_file(final)
        except FileNotFoundError:
            existing = None
        if existing is not None:
            observed = RemoteArchiveReceipt.from_bytes(existing)
            if observed != receipt:
                raise RemoteReceiveError("existing remote archive receipt conflicts")
            revalidate_remote_archive_receipt(
                selection=selection,
                target=self.target,
                receipt_id=receipt.receipt_id,
            )
            fsync_directory(receipts_directory)
            self._hit("k12_after_receipt_parent_durable", final)
            return

        temporary = receipts_directory / (
            f".{final.name}.{uuid.uuid4().hex}.partial"
        )
        owned = False
        descriptor = -1
        try:
            descriptor = _exclusive_create(temporary)
            owned = True
            try:
                _write_all(descriptor, body)
                os.fsync(descriptor)
                self._hit("k10_after_receipt_file_fsync_before_publish", temporary)
            finally:
                os.close(descriptor)
                descriptor = -1
            if RemoteArchiveReceipt.from_bytes(_safe_read_regular_file(temporary)) != receipt:
                raise RemoteReceiveError("receipt temp readback mismatch")
            published = _publish_no_clobber(temporary, final)
            if published:
                owned = False
                self._hit("k11_after_receipt_rename_before_parent_fsync", final)
            winner = RemoteArchiveReceipt.from_bytes(_safe_read_regular_file(final))
            if winner != receipt:
                raise RemoteReceiveError("existing remote archive receipt conflicts")
            fsync_directory(receipts_directory)
            self._hit("k12_after_receipt_parent_durable", final)
        finally:
            if descriptor >= 0:
                with suppress(OSError):
                    os.close(descriptor)
            if owned:
                with suppress(OSError):
                    temporary.unlink(missing_ok=True)

    def _hit(self, point: str, path: Path | None = None) -> None:
        if self.fault_hook is not None:
            self.fault_hook(point, path)


def revalidate_remote_archive_receipt(
    *,
    selection: RemoteSourceSelection,
    target: RemoteReceiveTarget,
    receipt_id: str,
) -> RemoteArchiveReceipt:
    """Independently revalidate a durable receipt and every authority it binds."""

    try:
        _require_supported_platform()
        _require_sha256(receipt_id, "receipt_id")
        _validate_selection(selection)
        store = ArchiveSetStore.open(target.root)
        if store.identity != _target_identity(target):
            raise RemoteReceiveError("Archive Set medium identity does not match target")
        root = target.root.resolve()
        receipts_directory = root / "archive-set" / REMOTE_RECEIPTS_DIRECTORY_NAME
        _validate_direct_directory(root / "archive-set", root, "archive-set")
        _validate_direct_directory(
            receipts_directory, root / "archive-set", REMOTE_RECEIPTS_DIRECTORY_NAME
        )
        receipt_path = receipts_directory / f"{receipt_id}.json"
        receipt = RemoteArchiveReceipt.from_bytes(
            _safe_read_regular_file(receipt_path)
        )
        if receipt.receipt_id != receipt_id:
            raise RemoteReceiveError("receipt filename identity mismatch")
        expected_artifact = _artifact_relative_path(selection)
        expected_receipt = {
            "source_descriptor_schema_version": (
                selection.descriptor.descriptor_schema_version
            ),
            "source_descriptor_sha256": selection.descriptor_sha256,
            "chunk_id": selection.descriptor.chunk_id,
            "source_relative_path": selection.descriptor.source_relative_path,
            "source_manifest_relative_path": (
                selection.descriptor.source_manifest_relative_path
            ),
            "source_manifest_sha256": selection.descriptor.source_manifest_sha256,
            "stored_bytes": selection.descriptor.stored_bytes,
            "stored_sha256": selection.descriptor.stored_sha256,
            "archive_set_id": target.archive_set_id,
            "storage_id": target.storage_id,
            "artifact_relative_path": expected_artifact,
        }
        mismatches = [
            field
            for field, value in expected_receipt.items()
            if getattr(receipt, field) != value
        ]
        if mismatches:
            raise RemoteReceiveError(
                "receipt/source/target identity mismatch: "
                + ", ".join(sorted(mismatches))
            )

        entry = store.read_entry(receipt.chunk_id)
        entry_sha256 = hashlib.sha256(entry.canonical_bytes()).hexdigest()
        if entry_sha256 != receipt.archive_set_entry_sha256:
            raise RemoteReceiveError("Archive Set entry digest does not match receipt")
        expected_entry = {
            "archive_set_id": target.archive_set_id,
            "storage_id": target.storage_id,
            "chunk_id": selection.descriptor.chunk_id,
            "artifact_relative_path": expected_artifact,
            "stored_bytes": selection.descriptor.stored_bytes,
            "stored_sha256": selection.descriptor.stored_sha256,
            "source_manifest_sha256": selection.descriptor.source_manifest_sha256,
        }
        entry_mismatches = [
            field
            for field, value in expected_entry.items()
            if getattr(entry, field) != value
        ]
        if entry_mismatches:
            raise RemoteReceiveError(
                "Archive Set entry identity mismatch: "
                + ", ".join(sorted(entry_mismatches))
            )
        _require_relative_path(
            entry.archive_manifest_relative_path,
            "archive_manifest_relative_path",
        )
        manifest_path = _resolve_exact_relative_file(
            root,
            entry.archive_manifest_relative_path,
            expected_parent="manifests",
        )
        manifest_body = _safe_read_regular_file(manifest_path)
        if hashlib.sha256(manifest_body).hexdigest() != entry.archive_manifest_sha256:
            raise RemoteReceiveError("archive manifest digest does not match entry")
        transaction_id = receive_transaction_id(
            selection,
            target,
            artifact_relative_path=expected_artifact,
        )
        _validate_archive_manifest_bytes(
            manifest_body,
            expected=_archive_manifest_expected(
                selection,
                target,
                transaction_id=transaction_id,
                artifact_relative_path=expected_artifact,
            ),
            source_manifest_bytes=selection.manifest_bytes,
        )
        artifact_path = _resolve_exact_relative_file(
            root,
            entry.artifact_relative_path,
            expected_parent="raw",
        )
        _verify_regular_file(
            artifact_path,
            selection.descriptor.stored_bytes,
            selection.descriptor.stored_sha256,
        )
        store.read_identity()
        fsync_directory(receipts_directory)
        final_receipt = RemoteArchiveReceipt.from_bytes(
            _safe_read_regular_file(receipt_path)
        )
        if final_receipt != receipt:
            raise RemoteReceiveError("receipt changed during independent revalidation")
        store.read_identity()
        return receipt
    except RemoteReceiveError:
        raise
    except (ArchiveSetError, OSError, ValueError) as exc:
        raise RemoteReceiveError(f"receipt revalidation failed closed: {exc}") from exc


def _target_identity(target: RemoteReceiveTarget) -> ArchiveMediumIdentity:
    return ArchiveMediumIdentity(
        archive_set_id=target.archive_set_id,
        storage_id=target.storage_id,
        volume_uuid=target.volume_uuid,
        registered_relative_path=target.registered_relative_path,
        marker_nonce=target.marker_nonce,
    )


def _validate_selection(selection: RemoteSourceSelection) -> dict[str, object]:
    descriptor = selection.descriptor
    canonical = canonical_descriptor_bytes(descriptor)
    if canonical != selection.descriptor_bytes:
        raise RemoteReceiveError("source descriptor bytes are not canonical or consistent")
    digest = descriptor_sha256(selection.descriptor_bytes)
    if digest != selection.descriptor_sha256:
        raise RemoteReceiveError("source descriptor digest mismatch")
    if descriptor.descriptor_schema_version != REMOTE_SOURCE_DESCRIPTOR_SCHEMA:
        raise RemoteReceiveError("unsupported source descriptor schema")
    if descriptor.manifest_schema_version != MANIFEST_SCHEMA_VERSION:
        raise RemoteReceiveError("unsupported source Raw manifest schema")
    _require_safe_segment(descriptor.chunk_id, "chunk_id")
    for field in ("market", "stream", "chunk_schema_version", "envelope_schema_version"):
        _require_text(getattr(descriptor, field), field)
    _require_relative_path(descriptor.source_relative_path, "source_relative_path")
    _require_relative_path(
        descriptor.source_manifest_relative_path, "source_manifest_relative_path"
    )
    _require_nonnegative_int(descriptor.stored_bytes, "stored_bytes")
    for field in ("stored_sha256", "source_manifest_sha256"):
        _require_sha256(getattr(descriptor, field), field)
    if hashlib.sha256(selection.manifest_bytes).hexdigest() != (
        descriptor.source_manifest_sha256
    ):
        raise RemoteReceiveError("source manifest digest mismatch")
    manifest = _decode_json_object(selection.manifest_bytes, "source Raw manifest")
    expected = {
        "manifest_schema_version": descriptor.manifest_schema_version,
        "chunk_id": descriptor.chunk_id,
        "market": descriptor.market,
        "stream": descriptor.stream,
        "relative_path": descriptor.source_relative_path,
        "stored_bytes": descriptor.stored_bytes,
        "stored_sha256": descriptor.stored_sha256,
        "chunk_schema_version": descriptor.chunk_schema_version,
        "envelope_schema_version": descriptor.envelope_schema_version,
    }
    mismatches = [
        field for field, value in expected.items() if manifest.get(field) != value
    ]
    if mismatches:
        raise RemoteReceiveError(
            "source descriptor/manifest identity mismatch: "
            + ", ".join(sorted(mismatches))
        )
    return manifest


def _artifact_relative_path(selection: RemoteSourceSelection) -> str:
    source = PurePosixPath(selection.descriptor.source_relative_path)
    name = source.name
    _require_safe_segment(name, "source sealed basename")
    value = f"raw/{name}"
    _require_relative_path(value, "artifact_relative_path")
    return value


def _archive_manifest_expected(
    selection: RemoteSourceSelection,
    target: RemoteReceiveTarget,
    *,
    transaction_id: str,
    artifact_relative_path: str,
) -> dict[str, object]:
    descriptor = selection.descriptor
    return {
        "archive_manifest_schema_version": ARCHIVE_MANIFEST_SCHEMA,
        "transaction_id": transaction_id,
        "chunk_id": descriptor.chunk_id,
        "storage_id": target.storage_id,
        "volume_uuid": target.volume_uuid,
        "registered_relative_path": target.registered_relative_path,
        "artifact_relative_path": artifact_relative_path,
        "stored_bytes": descriptor.stored_bytes,
        "stored_sha256": descriptor.stored_sha256,
        "source_manifest_sha256": descriptor.source_manifest_sha256,
    }


def _validate_archive_manifest_bytes(
    body: bytes,
    *,
    expected: Mapping[str, object],
    source_manifest_bytes: bytes,
) -> dict[str, object]:
    document = _decode_json_object(body, "external archive manifest")
    _require_exact_fields(document, _ARCHIVE_MANIFEST_FIELDS, "external archive manifest")
    if _canonical_json(document) != body:
        raise RemoteReceiveError("external archive manifest is not canonical")
    mismatches = [
        field for field, value in expected.items() if document.get(field) != value
    ]
    if mismatches:
        raise RemoteReceiveError(
            "external archive manifest identity mismatch: "
            + ", ".join(sorted(mismatches))
        )
    encoded = document.get("raw_manifest_bytes_base64")
    embedded = document.get("raw_manifest")
    if not isinstance(encoded, str) or not isinstance(embedded, dict):
        raise RemoteReceiveError("external archive manifest lacks Raw evidence")
    try:
        raw_bytes = base64.b64decode(encoded, validate=True)
    except ValueError as exc:
        raise RemoteReceiveError("external archive manifest Raw bytes are invalid") from exc
    if raw_bytes != source_manifest_bytes:
        raise RemoteReceiveError("external archive manifest Raw bytes changed")
    if _decode_json_object(raw_bytes, "embedded source Raw manifest") != embedded:
        raise RemoteReceiveError("external archive manifest Raw document/bytes mismatch")
    if hashlib.sha256(raw_bytes).hexdigest() != document.get(
        "source_manifest_sha256"
    ):
        raise RemoteReceiveError("external archive manifest Raw digest mismatch")
    if document.get("verification") != {
        "full_readback": True,
        "size_match": True,
        "sha256_match": True,
    }:
        raise RemoteReceiveError("external archive manifest verification is incomplete")
    _require_nonnegative_int(document.get("verified_at_utc_ns"), "verified_at_utc_ns")
    return document


def _ensure_direct_directory(parent: Path, name: str) -> Path:
    if Path(name).name != name or name in {"", ".", ".."}:
        raise RemoteReceiveError("unsafe receive directory name")
    path = parent / name
    if path.is_symlink():
        raise RemoteReceiveError(f"{name} directory is a symbolic link")
    try:
        path.mkdir(mode=0o700, exist_ok=True)
    except OSError as exc:
        raise RemoteReceiveError(f"cannot establish {name} directory: {exc}") from exc
    _validate_direct_directory(path, parent, name)
    fsync_directory(path)
    fsync_directory(parent)
    return path


def _validate_direct_directory(path: Path, parent: Path, name: str) -> None:
    if path.is_symlink():
        raise RemoteReceiveError(f"{name} directory is a symbolic link")
    resolved_parent = parent.resolve()
    resolved = path.resolve()
    if resolved.parent != resolved_parent or resolved.name != name:
        raise RemoteReceiveError(f"{name} directory resolves outside expected parent")
    try:
        mode = path.stat(follow_symlinks=False).st_mode
    except OSError as exc:
        raise RemoteReceiveError(f"{name} directory is unavailable: {exc}") from exc
    if not stat.S_ISDIR(mode):
        raise RemoteReceiveError(f"{name} path is not a real directory")


def _resolve_exact_relative_file(
    root: Path,
    relative: str,
    *,
    expected_parent: str,
) -> Path:
    _require_relative_path(relative, "archive relative path")
    pure = PurePosixPath(relative)
    if len(pure.parts) != 2 or pure.parts[0] != expected_parent:
        raise RemoteReceiveError(
            f"archive path is not a direct child of {expected_parent}"
        )
    parent = root / expected_parent
    _validate_direct_directory(parent, root, expected_parent)
    candidate = parent / pure.name
    if candidate.parent.resolve() != parent.resolve():
        raise RemoteReceiveError("archive path escapes expected directory")
    return candidate


def _exclusive_create(path: Path) -> int:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    return os.open(path, flags, 0o600)


def _safe_read_regular_file(
    path: Path,
    *,
    progress: Callable[[], None] | None = None,
) -> bytes:
    descriptor = _open_regular_descriptor(path)
    try:
        body = bytearray()
        while True:
            block = os.read(descriptor, REMOTE_RECEIVE_BUFFER_BYTES)
            if not block:
                break
            body.extend(block)
            if progress is not None:
                progress()
        return bytes(body)
    finally:
        os.close(descriptor)


def _open_regular_descriptor(path: Path) -> int:
    flags = os.O_RDONLY | os.O_NONBLOCK
    if hasattr(os, "O_CLOEXEC"):
        flags |= os.O_CLOEXEC
    no_follow = getattr(os, "O_NOFOLLOW", None)
    if no_follow is None:
        raise RemoteReceiveError("safe no-follow file open is unavailable")
    flags |= no_follow
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if exc.errno == errno.ENOENT:
            raise FileNotFoundError(path) from exc
        raise RemoteReceiveError(f"cannot safely open immutable file {path.name}: {exc}") from exc
    try:
        status = os.fstat(descriptor)
    except Exception:
        os.close(descriptor)
        raise
    if not stat.S_ISREG(status.st_mode):
        os.close(descriptor)
        raise RemoteReceiveError(f"immutable path is not a regular file: {path.name}")
    return descriptor


def _verify_regular_file(
    path: Path,
    expected_bytes: int,
    expected_sha256: str,
    *,
    progress: Callable[[], None] | None = None,
) -> None:
    descriptor = _open_regular_descriptor(path)
    try:
        initial = os.fstat(descriptor)
        if initial.st_size != expected_bytes:
            raise RemoteReceiveError(f"immutable file size mismatch: {path.name}")
        observed_bytes = 0
        digest = hashlib.sha256()
        while True:
            block = os.read(descriptor, REMOTE_RECEIVE_BUFFER_BYTES)
            if not block:
                break
            observed_bytes += len(block)
            digest.update(block)
            if progress is not None:
                progress()
        final = os.fstat(descriptor)
        if observed_bytes != expected_bytes or final.st_size != expected_bytes:
            raise RemoteReceiveError(f"immutable file size mismatch: {path.name}")
        if digest.hexdigest() != expected_sha256:
            raise RemoteReceiveError(f"immutable file SHA-256 mismatch: {path.name}")
    finally:
        os.close(descriptor)


def _write_all(descriptor: int, body: bytes) -> None:
    remaining = memoryview(body)
    while remaining:
        written = os.write(descriptor, remaining)
        if written <= 0:
            raise RemoteReceiveError("filesystem write made no progress")
        remaining = remaining[written:]


def _decode_json_object(body: bytes, label: str) -> dict[str, object]:
    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RemoteReceiveError(f"{label} is not valid UTF-8 JSON") from exc
    if not isinstance(decoded, dict):
        raise RemoteReceiveError(f"{label} is not a JSON object")
    return cast(dict[str, object], decoded)


def _canonical_json(document: Mapping[str, object]) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode("utf-8")


def _require_exact_fields(
    document: Mapping[str, object], fields: tuple[str, ...], label: str
) -> None:
    if set(document) != set(fields):
        raise RemoteReceiveError(f"{label} fields are not exact")


def _require_supported_platform() -> None:
    if sys.platform not in {"linux", "darwin"}:
        raise RemoteReceiveError(
            f"M22.3 end-to-end durability is unsupported on {sys.platform!r}"
        )
    if not hasattr(os, "O_NOFOLLOW"):
        raise RemoteReceiveError("M22.3 requires O_NOFOLLOW")


def _require_text(value: object, field: str) -> None:
    if not isinstance(value, str) or not value:
        raise RemoteReceiveError(f"{field} must be non-empty text")


def _require_nonnegative_int(value: object, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool) or value < 0:
        raise RemoteReceiveError(f"{field} must be a non-negative integer")


def _require_sha256(value: object, field: str) -> None:
    if (
        not isinstance(value, str)
        or len(value) != 64
        or any(character not in "0123456789abcdef" for character in value)
    ):
        raise RemoteReceiveError(f"{field} must be a lowercase SHA-256 digest")


def _require_uuid4(value: object, field: str) -> None:
    _require_text(value, field)
    try:
        parsed = uuid.UUID(cast(str, value))
    except ValueError as exc:
        raise RemoteReceiveError(f"{field} must be a canonical UUID4") from exc
    if parsed.version != 4 or str(parsed) != value:
        raise RemoteReceiveError(f"{field} must be a canonical UUID4")


def _require_safe_segment(value: object, field: str) -> None:
    _require_text(value, field)
    text = cast(str, value)
    if text in {".", ".."} or "/" in text or "\\" in text or Path(text).name != text:
        raise RemoteReceiveError(f"{field} must be a safe path segment")


def _require_relative_path(
    value: object,
    field: str,
    *,
    allow_dot: bool = False,
) -> None:
    _require_text(value, field)
    text = cast(str, value)
    path = PurePosixPath(text)
    if (
        text != path.as_posix()
        or path.is_absolute()
        or "\\" in text
        or any(part in {"", ".", ".."} for part in path.parts)
        or (not allow_dot and text == ".")
    ):
        raise RemoteReceiveError(f"{field} must be a safe canonical relative path")
