"""One-source bounded remote archive session orchestration for M22.5."""

from __future__ import annotations

from dataclasses import dataclass

from ..storage.catalog import RemoteArchiveState
from .remote_receive import (
    RemoteArchiveReceipt,
    RemoteReceiver,
    RemoteReceiveTarget,
    generate_archive_session_id,
)
from .remote_source import RemoteSourceIdentity
from .remote_transport import (
    RemoteAuthorityStatus,
    RemoteTransport,
    RemoteTransportError,
)


class RemoteArchiveSessionError(RuntimeError):
    """One exact source session failed closed."""


@dataclass(frozen=True, slots=True)
class RemoteArchiveSessionResult:
    """Result of exactly one source selection and no drain loop."""

    worked: bool
    source: RemoteSourceIdentity | None
    receipt: RemoteArchiveReceipt | None
    authority: RemoteAuthorityStatus | None


class RemoteArchiveSession:
    """Run selection, local receipt, authorization, and optional deletion once."""

    def __init__(
        self,
        *,
        transport: RemoteTransport,
        target: RemoteReceiveTarget,
    ) -> None:
        self.transport = transport
        self.target = target

    def run_one(
        self,
        *,
        delete: bool,
        session_id: str | None = None,
    ) -> RemoteArchiveSessionResult:
        source: RemoteSourceIdentity | None = None
        receipt: RemoteArchiveReceipt | None = None
        try:
            source = self.transport.select_oldest_source()
            if source is None:
                return RemoteArchiveSessionResult(False, None, None, None)
            receipt = RemoteReceiver(
                provider=self.transport,
                target=self.target,
            ).receive(
                source,
                session_id=session_id or generate_archive_session_id(),
            )
            receipt_bytes = receipt.canonical_bytes()
            authority = self._authorize_same_identity(source, receipt, receipt_bytes)
            if authority.state not in {
                RemoteArchiveState.REMOTE_DELETE_PENDING,
                RemoteArchiveState.REMOTE_DELETED,
            }:
                raise RemoteArchiveSessionError("authorization returned unsupported state")
            if delete and authority.state is RemoteArchiveState.REMOTE_DELETE_PENDING:
                authority = self._require_receipt_status(
                    self._delete_same_receipt(receipt.receipt_id), receipt
                )
            if delete and authority.state is not RemoteArchiveState.REMOTE_DELETED:
                raise RemoteArchiveSessionError("delete did not reach terminal authority")
            return RemoteArchiveSessionResult(True, source, receipt, authority)
        except RemoteArchiveSessionError:
            raise
        except Exception as exc:
            identity = receipt.receipt_id if receipt is not None else (
                source.descriptor.chunk_id if source is not None else "unselected"
            )
            raise RemoteArchiveSessionError(
                f"one-source remote archive session failed closed ({identity}): {exc}"
            ) from exc

    def _authorize_same_identity(
        self,
        source: RemoteSourceIdentity,
        receipt: RemoteArchiveReceipt,
        receipt_bytes: bytes,
    ) -> RemoteAuthorityStatus:
        try:
            return self._require_receipt_status(
                self.transport.authorize_receipt(source, receipt_bytes), receipt
            )
        except RemoteTransportError:
            observed = self.transport.inspect_authority(receipt.receipt_id)
            if observed is not None:
                return self._require_receipt_status(observed, receipt)
            try:
                return self._require_receipt_status(
                    self.transport.authorize_receipt(source, receipt_bytes), receipt
                )
            except RemoteTransportError:
                final = self.transport.inspect_authority(receipt.receipt_id)
                if final is None:
                    raise
                return self._require_receipt_status(final, receipt)

    def _delete_same_receipt(self, receipt_id: str) -> RemoteAuthorityStatus:
        try:
            return self.transport.delete_authorized(receipt_id)
        except RemoteTransportError:
            observed = self.transport.inspect_authority(receipt_id)
            if observed is None:
                raise RemoteArchiveSessionError(
                    "delete response ambiguous and authority is absent"
                ) from None
            if observed.state is RemoteArchiveState.REMOTE_DELETED:
                return observed
            if observed.state is not RemoteArchiveState.REMOTE_DELETE_PENDING:
                raise RemoteArchiveSessionError(
                    "delete authority state is invalid"
                ) from None
            try:
                return self.transport.delete_authorized(receipt_id)
            except RemoteTransportError:
                final = self.transport.inspect_authority(receipt_id)
                if final is None or final.state is not RemoteArchiveState.REMOTE_DELETED:
                    raise
                return final

    @staticmethod
    def _require_receipt_status(
        status: RemoteAuthorityStatus, receipt: RemoteArchiveReceipt
    ) -> RemoteAuthorityStatus:
        expected = {
            "receipt_id": receipt.receipt_id,
            "chunk_id": receipt.chunk_id,
            "source_descriptor_sha256": receipt.source_descriptor_sha256,
            "source_manifest_sha256": receipt.source_manifest_sha256,
            "stored_bytes": receipt.stored_bytes,
            "stored_sha256": receipt.stored_sha256,
        }
        if any(getattr(status, field) != value for field, value in expected.items()):
            raise RemoteArchiveSessionError("authority does not bind the same receipt")
        return status
