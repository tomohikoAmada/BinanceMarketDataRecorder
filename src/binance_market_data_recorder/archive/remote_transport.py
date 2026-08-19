"""Byte/message-only RemoteTransport implementations for M22.5.

Transport never decides source eligibility, receipt correctness, deletion
authorization, or filesystem deletion.  Those decisions remain in M22.1,
M22.3, M22.4A, and M22.4B respectively.
"""

from __future__ import annotations

import io
import json
import re
import subprocess
import uuid
from collections.abc import Callable
from pathlib import PurePosixPath
from typing import BinaryIO, Protocol, cast
from weakref import WeakSet

from ..storage.catalog import Catalog, RemoteArchiveState
from ..storage.layout import StorageLayout
from .remote_authorization import RemoteAuthorizer
from .remote_delete import RemoteDeleter
from .remote_receive import RemoteArchiveReceipt
from .remote_source import (
    RemoteSourceExporter,
    RemoteSourceIdentity,
    RemoteSourceSelection,
    descriptor_sha256,
    portable_source_identity,
    remote_source_descriptor_from_bytes,
    validate_remote_source_identity,
)

_HOST_ALIAS = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*\Z")
_SAFE_REMOTE_TOKEN = re.compile(r"[A-Za-z0-9_./+-]+\Z")
_LOWER_SHA256 = re.compile(r"[0-9a-f]{64}\Z")
_AUTHORITY_FIELDS = (
    "receipt_id",
    "chunk_id",
    "state",
    "source_descriptor_sha256",
    "source_manifest_sha256",
    "stored_bytes",
    "stored_sha256",
)
_MAX_CONTROL_BYTES = 16 * 1024 * 1024


class RemoteTransportError(RuntimeError):
    """Remote byte/message movement failed without proving semantic outcome."""


class RemoteTransportProcessError(RemoteTransportError):
    """The SSH subprocess exited unsuccessfully."""


class RemoteTransportTimeout(RemoteTransportError):
    """An explicitly bounded transport or cleanup operation timed out."""


class RemoteTransport(Protocol):
    """Frozen M22.5 byte/message transport surface."""

    def select_oldest_source(self) -> RemoteSourceIdentity | None: ...

    def open_stored_bytes(self, source: RemoteSourceIdentity) -> BinaryIO: ...

    def authorize_receipt(
        self, source: RemoteSourceIdentity, receipt_bytes: bytes
    ) -> RemoteAuthorityStatus: ...

    def inspect_authority(self, receipt_id: str) -> RemoteAuthorityStatus | None: ...

    def delete_authorized(self, receipt_id: str) -> RemoteAuthorityStatus: ...


class RemoteAuthorityStatus:
    """Non-persisted validated view of existing remote Catalog authority."""

    __slots__ = (
        "chunk_id",
        "receipt_id",
        "source_descriptor_sha256",
        "source_manifest_sha256",
        "state",
        "stored_bytes",
        "stored_sha256",
    )

    def __init__(
        self,
        *,
        receipt_id: str,
        chunk_id: str,
        state: RemoteArchiveState,
        source_descriptor_sha256: str,
        source_manifest_sha256: str,
        stored_bytes: int,
        stored_sha256: str,
    ) -> None:
        require_receipt_id(receipt_id)
        require_chunk_id(chunk_id)
        require_sha256(source_descriptor_sha256, "source_descriptor_sha256")
        require_sha256(source_manifest_sha256, "source_manifest_sha256")
        require_sha256(stored_sha256, "stored_sha256")
        if not isinstance(state, RemoteArchiveState):
            raise RemoteTransportError("authority state is invalid")
        if not isinstance(stored_bytes, int) or isinstance(stored_bytes, bool) or stored_bytes < 0:
            raise RemoteTransportError("authority stored_bytes is invalid")
        self.receipt_id = receipt_id
        self.chunk_id = chunk_id
        self.state = state
        self.source_descriptor_sha256 = source_descriptor_sha256
        self.source_manifest_sha256 = source_manifest_sha256
        self.stored_bytes = stored_bytes
        self.stored_sha256 = stored_sha256

    def __eq__(self, other: object) -> bool:
        return isinstance(other, RemoteAuthorityStatus) and self.document() == other.document()

    def __repr__(self) -> str:
        return f"RemoteAuthorityStatus({self.document()!r})"

    def document(self) -> dict[str, object]:
        return {
            "receipt_id": self.receipt_id,
            "chunk_id": self.chunk_id,
            "state": self.state.value,
            "source_descriptor_sha256": self.source_descriptor_sha256,
            "source_manifest_sha256": self.source_manifest_sha256,
            "stored_bytes": self.stored_bytes,
            "stored_sha256": self.stored_sha256,
        }

    def canonical_bytes(self) -> bytes:
        return _canonical_json(self.document())


def authority_status_from_catalog(
    catalog: Catalog, receipt_id: str
) -> RemoteAuthorityStatus | None:
    """Read the existing validated Catalog row; do not create new authority."""

    require_receipt_id(receipt_id)
    row = catalog.remote_archive_transaction(receipt_id)
    if row is None:
        return None
    try:
        return RemoteAuthorityStatus(
            receipt_id=cast(str, row["receipt_id"]),
            chunk_id=cast(str, row["chunk_id"]),
            state=RemoteArchiveState(str(row["state"])),
            source_descriptor_sha256=cast(str, row["source_descriptor_sha256"]),
            source_manifest_sha256=cast(str, row["source_manifest_sha256"]),
            stored_bytes=cast(int, row["stored_bytes"]),
            stored_sha256=cast(str, row["stored_sha256"]),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise RemoteTransportError("malformed authoritative remote status") from exc


def remote_authority_status_from_bytes(body: bytes) -> RemoteAuthorityStatus | None:
    """Parse exact compact canonical control JSON (or exact ``null\n``)."""

    if body == b"null\n":
        return None
    try:
        decoded = json.loads(body)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise RemoteTransportError("authority response is not valid UTF-8 JSON") from exc
    if not isinstance(decoded, dict) or set(decoded) != set(_AUTHORITY_FIELDS):
        raise RemoteTransportError("authority response fields are not exact")
    document = cast(dict[str, object], decoded)
    try:
        status = RemoteAuthorityStatus(
            receipt_id=cast(str, document["receipt_id"]),
            chunk_id=cast(str, document["chunk_id"]),
            state=RemoteArchiveState(cast(str, document["state"])),
            source_descriptor_sha256=cast(str, document["source_descriptor_sha256"]),
            source_manifest_sha256=cast(str, document["source_manifest_sha256"]),
            stored_bytes=cast(int, document["stored_bytes"]),
            stored_sha256=cast(str, document["stored_sha256"]),
        )
    except (TypeError, ValueError) as exc:
        raise RemoteTransportError("authority response field types are invalid") from exc
    if status.canonical_bytes() != body:
        raise RemoteTransportError("authority response is not canonical")
    return status


class InProcessRemoteTransport:
    """Reference transport using the existing domain objects directly."""

    def __init__(self, *, layout: StorageLayout, catalog: Catalog) -> None:
        self.layout = layout
        self.catalog = catalog

    def select_oldest_source(self) -> RemoteSourceIdentity | None:
        selection = RemoteSourceExporter(
            layout=self.layout, catalog=self.catalog
        ).select_oldest()
        return None if selection is None else portable_source_identity(selection)

    def _selection(self, source: RemoteSourceIdentity) -> RemoteSourceSelection:
        validate_remote_source_identity(source)
        current = RemoteSourceExporter(
            layout=self.layout, catalog=self.catalog
        ).select_chunk(source.descriptor.chunk_id)
        if portable_source_identity(current) != source:
            raise RemoteTransportError("remote source identity changed")
        return current

    def open_stored_bytes(self, source: RemoteSourceIdentity) -> BinaryIO:
        selection = self._selection(source)
        try:
            return selection.sealed_path.open("rb", buffering=0)
        except OSError as exc:
            raise RemoteTransportError("cannot open selected stored bytes") from exc

    def authorize_receipt(
        self, source: RemoteSourceIdentity, receipt_bytes: bytes
    ) -> RemoteAuthorityStatus:
        selection = self._selection(source)
        pending = RemoteAuthorizer(
            layout=self.layout, catalog=self.catalog
        ).authorize(receipt_bytes, selection)
        status = authority_status_from_catalog(self.catalog, pending.receipt_id)
        if status is None:
            raise RemoteTransportError("authorization committed without readback")
        _require_status_binding(status, source, RemoteArchiveReceipt.from_bytes(receipt_bytes))
        return status

    def inspect_authority(self, receipt_id: str) -> RemoteAuthorityStatus | None:
        return authority_status_from_catalog(self.catalog, receipt_id)

    def delete_authorized(self, receipt_id: str) -> RemoteAuthorityStatus:
        result = RemoteDeleter(
            layout=self.layout, catalog=self.catalog
        ).delete_authorized(receipt_id)
        status = authority_status_from_catalog(self.catalog, result.receipt_id)
        if status is None:
            raise RemoteTransportError("delete completed without authoritative readback")
        return status


class OpenSSHRemoteTransport:
    """Ordinary system OpenSSH subprocess adapter with fixed remote verbs."""

    def __init__(
        self,
        *,
        host_alias: str,
        ssh_executable: str = "ssh",
        remote_recorder_executable: str = "binance-market-recorder",
        remote_config_path: PurePosixPath | str | None = None,
        connect_timeout_seconds: int = 30,
        operation_timeout_seconds: float | None = None,
        cleanup_timeout_seconds: float = 5.0,
    ) -> None:
        require_host_alias(host_alias)
        if not isinstance(ssh_executable, str) or not ssh_executable or "\0" in ssh_executable:
            raise RemoteTransportError("ssh executable is invalid")
        require_remote_executable(remote_recorder_executable)
        config = (
            None
            if remote_config_path is None
            else require_remote_config_path(remote_config_path)
        )
        if (
            not isinstance(connect_timeout_seconds, int)
            or isinstance(connect_timeout_seconds, bool)
            or not 1 <= connect_timeout_seconds <= 600
        ):
            raise RemoteTransportError("connect timeout must be an integer from 1 to 600")
        if operation_timeout_seconds is not None and operation_timeout_seconds <= 0:
            raise RemoteTransportError("operation timeout must be positive when supplied")
        if cleanup_timeout_seconds <= 0:
            raise RemoteTransportError("cleanup timeout must be positive")
        self.host_alias = host_alias
        self.ssh_executable = ssh_executable
        self.remote_recorder_executable = remote_recorder_executable
        self.remote_config_path = config
        self.connect_timeout_seconds = connect_timeout_seconds
        self.operation_timeout_seconds = operation_timeout_seconds
        self.cleanup_timeout_seconds = cleanup_timeout_seconds
        self._streams: WeakSet[_ProcessBackedStream] = WeakSet()

    def __enter__(self) -> OpenSSHRemoteTransport:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()

    def close(self) -> None:
        for stream in tuple(self._streams):
            stream.close()

    def select_oldest_source(self) -> RemoteSourceIdentity | None:
        descriptor_bytes = self._control("select-oldest")
        if descriptor_bytes == b"":
            return None
        descriptor = remote_source_descriptor_from_bytes(descriptor_bytes)
        digest = descriptor_sha256(descriptor_bytes)
        manifest_bytes = self._control("manifest", descriptor.chunk_id, digest)
        identity = RemoteSourceIdentity(
            descriptor=descriptor,
            descriptor_bytes=descriptor_bytes,
            descriptor_sha256=digest,
            manifest_bytes=manifest_bytes,
        )
        validate_remote_source_identity(identity)
        return identity

    def open_stored_bytes(self, source: RemoteSourceIdentity) -> BinaryIO:
        validate_remote_source_identity(source)
        command = self._remote_command(
            "raw", source.descriptor.chunk_id, source.descriptor_sha256
        )
        try:
            process = subprocess.Popen(
                self._argv(command),
                stdin=subprocess.DEVNULL,
                stdout=subprocess.PIPE,
                stderr=None,
                shell=False,
            )
        except OSError as exc:
            raise RemoteTransportError("cannot start OpenSSH Raw transport") from exc
        if process.stdout is None:
            process.kill()
            process.wait()
            raise RemoteTransportError("OpenSSH Raw transport lacks stdout")
        stream = _ProcessBackedStream(
            process,
            cleanup_timeout_seconds=self.cleanup_timeout_seconds,
            on_close=self._streams.discard,
        )
        self._streams.add(stream)
        return cast(BinaryIO, stream)

    def authorize_receipt(
        self, source: RemoteSourceIdentity, receipt_bytes: bytes
    ) -> RemoteAuthorityStatus:
        validate_remote_source_identity(source)
        receipt = RemoteArchiveReceipt.from_bytes(receipt_bytes)
        body = self._control(
            "authorize",
            source.descriptor.chunk_id,
            source.descriptor_sha256,
            stdin=receipt_bytes,
        )
        status = remote_authority_status_from_bytes(body)
        if status is None:
            raise RemoteTransportError("authorize response omitted authority")
        _require_status_binding(status, source, receipt)
        return status

    def inspect_authority(self, receipt_id: str) -> RemoteAuthorityStatus | None:
        require_receipt_id(receipt_id)
        body = self._control("authority", receipt_id)
        status = remote_authority_status_from_bytes(body)
        if status is not None and status.receipt_id != receipt_id:
            raise RemoteTransportError("authority response receipt_id mismatch")
        return status

    def delete_authorized(self, receipt_id: str) -> RemoteAuthorityStatus:
        require_receipt_id(receipt_id)
        body = self._control("delete", receipt_id)
        status = remote_authority_status_from_bytes(body)
        if status is None or status.receipt_id != receipt_id:
            raise RemoteTransportError("delete response lacks requested authority")
        return status

    def _control(self, verb: str, *identities: str, stdin: bytes | None = None) -> bytes:
        command = self._remote_command(verb, *identities)
        try:
            completed = subprocess.run(
                self._argv(command),
                input=stdin,
                capture_output=True,
                timeout=self.operation_timeout_seconds,
                check=False,
                shell=False,
            )
        except subprocess.TimeoutExpired as exc:
            raise RemoteTransportTimeout(f"remote {verb} operation timed out") from exc
        except OSError as exc:
            raise RemoteTransportError(f"cannot start OpenSSH {verb} operation") from exc
        if completed.returncode != 0:
            detail = completed.stderr[-4096:].decode("utf-8", errors="replace").strip()
            raise RemoteTransportProcessError(
                f"remote {verb} exited {completed.returncode}"
                + (f": {detail}" if detail else "")
            )
        if len(completed.stdout) > _MAX_CONTROL_BYTES:
            raise RemoteTransportError(f"remote {verb} response exceeds control limit")
        return completed.stdout

    def _remote_command(self, verb: str, *identities: str) -> str:
        if verb in {"manifest", "raw", "authorize"}:
            if len(identities) != 2:
                raise RemoteTransportError("chunk-bound remote command is malformed")
            require_chunk_id(identities[0])
            require_sha256(identities[1], "descriptor_sha256")
        elif verb in {"authority", "delete"}:
            if len(identities) != 1:
                raise RemoteTransportError("receipt-bound remote command is malformed")
            require_receipt_id(identities[0])
        elif verb == "select-oldest":
            if identities:
                raise RemoteTransportError("selection command takes no identity")
        else:
            raise RemoteTransportError("unsupported fixed remote command")
        tokens = [self.remote_recorder_executable]
        if self.remote_config_path is not None:
            tokens.extend(("--config", self.remote_config_path.as_posix()))
        tokens.extend(("_remote", verb, *identities))
        return " ".join(tokens)

    def _argv(self, remote_command: str) -> list[str]:
        return [
            self.ssh_executable,
            "-o",
            "BatchMode=yes",
            "-o",
            f"ConnectTimeout={self.connect_timeout_seconds}",
            self.host_alias,
            remote_command,
        ]


class _ProcessBackedStream(io.RawIOBase):
    """Own stdout and make successful EOF conditional on child exit zero."""

    def __init__(
        self,
        process: subprocess.Popen[bytes],
        *,
        cleanup_timeout_seconds: float,
        on_close: Callable[[_ProcessBackedStream], object],
    ) -> None:
        super().__init__()
        if process.stdout is None:
            raise RemoteTransportError("process-backed stream requires stdout")
        self._process = process
        self._stdout = process.stdout
        self._cleanup_timeout_seconds = cleanup_timeout_seconds
        self._on_close = on_close
        self._finished = False

    @property
    def process(self) -> subprocess.Popen[bytes]:
        """Expose process only for deterministic lifetime tests."""

        return self._process

    def readable(self) -> bool:
        return True

    def read(self, size: int = -1) -> bytes:
        if self.closed:
            raise ValueError("I/O operation on closed remote stream")
        body = self._stdout.read(size)
        if body:
            return body
        self._finish_at_eof()
        return b""

    def _finish_at_eof(self) -> None:
        if self._finished:
            return
        try:
            returncode = self._process.wait(timeout=self._cleanup_timeout_seconds)
        except subprocess.TimeoutExpired as exc:
            self._terminate_and_reap()
            raise RemoteTransportTimeout("Raw child did not exit after stdout EOF") from exc
        self._finished = True
        if returncode != 0:
            raise RemoteTransportProcessError(
                f"remote raw exited {returncode} after stdout EOF"
            )

    def _terminate_and_reap(self) -> None:
        if self._process.poll() is None:
            self._process.terminate()
            try:
                self._process.wait(timeout=self._cleanup_timeout_seconds)
            except subprocess.TimeoutExpired:
                self._process.kill()
                self._process.wait()
        else:
            self._process.wait()
        self._finished = True

    def close(self) -> None:
        if self.closed:
            return
        try:
            self._stdout.close()
            if not self._finished:
                self._terminate_and_reap()
        finally:
            self._on_close(self)
            super().close()


def require_host_alias(value: str) -> None:
    if not isinstance(value, str) or _HOST_ALIAS.fullmatch(value) is None or value.startswith("-"):
        raise RemoteTransportError("host_alias is not a safe OpenSSH Host alias")


def require_remote_executable(value: str) -> None:
    if not isinstance(value, str) or _SAFE_REMOTE_TOKEN.fullmatch(value) is None:
        raise RemoteTransportError("remote recorder executable is unsafe")
    path = PurePosixPath(value)
    if (
        value.startswith("-")
        or ".." in path.parts
        or path.as_posix() != value
        or ("/" in value and not path.is_absolute())
    ):
        raise RemoteTransportError("remote recorder executable is unsafe")


def require_remote_config_path(value: PurePosixPath | str) -> PurePosixPath:
    text = str(value)
    if _SAFE_REMOTE_TOKEN.fullmatch(text) is None:
        raise RemoteTransportError("remote config path is unsafe")
    path = PurePosixPath(text)
    if (
        not path.is_absolute()
        or ".." in path.parts
        or path.as_posix() != text
        or text.startswith("-")
    ):
        raise RemoteTransportError("remote config path must be a safe absolute POSIX path")
    return path


def require_chunk_id(value: str) -> None:
    if not isinstance(value, str):
        raise RemoteTransportError("chunk_id must be a canonical UUID")
    try:
        parsed = uuid.UUID(value)
    except ValueError as exc:
        raise RemoteTransportError("chunk_id must be a canonical UUID") from exc
    if str(parsed) != value:
        raise RemoteTransportError("chunk_id must be a canonical UUID")


def require_sha256(value: str, field: str) -> None:
    if not isinstance(value, str) or _LOWER_SHA256.fullmatch(value) is None:
        raise RemoteTransportError(f"{field} must be a lowercase SHA-256 digest")


def require_receipt_id(value: str) -> None:
    require_sha256(value, "receipt_id")


def _canonical_json(document: object) -> bytes:
    return (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()


def _require_status_binding(
    status: RemoteAuthorityStatus,
    source: RemoteSourceIdentity,
    receipt: RemoteArchiveReceipt,
) -> None:
    expected = {
        "receipt_id": receipt.receipt_id,
        "chunk_id": source.descriptor.chunk_id,
        "source_descriptor_sha256": source.descriptor_sha256,
        "source_manifest_sha256": source.descriptor.source_manifest_sha256,
        "stored_bytes": source.descriptor.stored_bytes,
        "stored_sha256": source.descriptor.stored_sha256,
    }
    if any(getattr(status, field) != value for field, value in expected.items()):
        raise RemoteTransportError("authority response/source/receipt binding mismatch")
