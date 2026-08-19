from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from binance_market_data_recorder.archive.remote_receive import (
    RemoteArchiveReceipt,
    RemoteReceiveTarget,
)
from binance_market_data_recorder.archive.remote_source import (
    RemoteSourceExporter,
    RemoteSourceSelection,
)
from binance_market_data_recorder.storage.catalog import Catalog
from tests.archive_support import PreparedArchive, prepare_archive


@dataclass(frozen=True, slots=True)
class RemoteAuthorizationFixture:
    prepared: PreparedArchive
    selections: tuple[RemoteSourceSelection, ...]
    target: RemoteReceiveTarget


def prepare_remote_authorization(
    root: Path, *, chunk_count: int = 1
) -> RemoteAuthorizationFixture:
    prepared = prepare_archive(root.resolve(), chunk_count=chunk_count, payload_bytes=128)
    with Catalog(prepared.layout.catalog) as catalog:
        exporter = RemoteSourceExporter(layout=prepared.layout, catalog=catalog)
        selections = tuple(exporter.select_chunk(chunk) for chunk in prepared.chunk_ids)
    target = RemoteReceiveTarget(
        archive_set_id="archive-set-m22-4a",
        storage_id=prepared.target.storage_id,
        volume_uuid=prepared.target.volume_uuid,
        registered_relative_path=prepared.target.registered_relative_path,
        marker_nonce=prepared.target.marker_nonce,
        root=prepared.target.root,
    )
    return RemoteAuthorizationFixture(prepared, selections, target)


def build_receipt(
    fixture: RemoteAuthorizationFixture,
    *,
    ordinal: int = 0,
    session_id: str = "12345678-1234-4234-9234-123456789abc",
) -> RemoteArchiveReceipt:
    selection = fixture.selections[ordinal]
    return RemoteArchiveReceipt.build(
        selection=selection,
        target=fixture.target,
        session_id=session_id,
        artifact_relative_path=f"raw/{selection.sealed_path.name}",
        archive_set_entry_sha256="e" * 64,
    )


def source_snapshot(
    fixture: RemoteAuthorizationFixture, *, ordinal: int = 0
) -> tuple[bytes, bytes, tuple[str, ...]]:
    selection = fixture.selections[ordinal]
    return (
        selection.sealed_path.read_bytes(),
        selection.manifest_path.read_bytes(),
        tuple(sorted(path.name for path in selection.sealed_path.parent.iterdir())),
    )
