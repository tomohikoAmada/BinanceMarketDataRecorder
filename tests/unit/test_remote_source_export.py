from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from pathlib import Path

import pytest

from binance_market_data_recorder.archive import (
    ArchiveManager,
    RemoteSourceDescriptor,
    RemoteSourceError,
    RemoteSourceExporter,
    canonical_descriptor_bytes,
)
from binance_market_data_recorder.storage.catalog import (
    ArchiveState,
    Catalog,
    ChunkState,
)
from binance_market_data_recorder.storage.layout import (
    StorageLayout,
    ensure_storage_layout,
)
from tests.archive_support import PreparedArchive, prepare_archive

GOLDEN_DESCRIPTOR_SHA256 = (
    "19d12404f64f282dbf6eccfd9c2c7e6d1779f7e9d868668f7c962fe4bfff33e2"
)


def _golden_descriptor() -> RemoteSourceDescriptor:
    return RemoteSourceDescriptor(
        descriptor_schema_version="remote-source-descriptor.v1",
        chunk_id="00112233-4455-6677-8899-aabbccddeeff",
        market="spot",
        stream="diff_depth",
        source_relative_path=(
            "data/sealed/00112233-4455-6677-8899-aabbccddeeff.bmdr.zst"
        ),
        stored_bytes=1234,
        stored_sha256="2" * 64,
        source_manifest_relative_path=(
            "data/manifests/00112233-4455-6677-8899-aabbccddeeff.manifest.json"
        ),
        source_manifest_sha256="1" * 64,
        manifest_schema_version="raw-chunk-manifest.v1",
        chunk_schema_version="raw-chunk.v1",
        envelope_schema_version="event-envelope.v1",
    )


def _manifest_path(prepared: PreparedArchive) -> Path:
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(prepared.chunk_ids[0])
        assert row is not None
        return prepared.layout.root / str(row["manifest_path"])


def _source_path(prepared: PreparedArchive) -> Path:
    with Catalog(prepared.layout.catalog) as catalog:
        row = catalog.chunk(prepared.chunk_ids[0])
        assert row is not None
        return prepared.layout.root / str(row["sealed_path"])


def _rewrite_manifest(path: Path, change: Callable[[dict[str, object]], None]) -> None:
    document = json.loads(path.read_bytes())
    assert isinstance(document, dict)
    change(document)
    path.write_bytes((json.dumps(document, sort_keys=True) + "\n").encode())


def _lifecycle_snapshot(catalog: Catalog, chunk_id: str) -> tuple[object, int, tuple[str, ...]]:
    return (
        catalog.state(chunk_id),
        catalog.transition_count(chunk_id),
        tuple(str(row["transaction_id"]) for row in catalog.archive_transactions()),
    )


def test_descriptor_canonical_bytes_match_golden_fixture() -> None:
    descriptor = _golden_descriptor()
    expected = (
        Path(__file__).parents[1] / "fixtures" / "remote_source_descriptor_v1.json"
    ).read_bytes()

    actual = canonical_descriptor_bytes(descriptor)

    assert actual == expected
    assert hashlib.sha256(actual).hexdigest() == GOLDEN_DESCRIPTOR_SHA256


def test_valid_sealed_source_produces_descriptor_v1_and_preserves_lifecycle(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        before = _lifecycle_snapshot(catalog, prepared.chunk_ids[0])
        selection = RemoteSourceExporter(
            layout=prepared.layout, catalog=catalog
        ).select_chunk(prepared.chunk_ids[0])
        after = _lifecycle_snapshot(catalog, prepared.chunk_ids[0])

        assert selection.descriptor.descriptor_schema_version == (
            "remote-source-descriptor.v1"
        )
        assert selection.descriptor.chunk_id == prepared.chunk_ids[0]
        assert selection.descriptor.source_relative_path.startswith("data/sealed/")
        assert selection.descriptor.source_manifest_relative_path.startswith(
            "data/manifests/"
        )
        assert selection.descriptor_sha256 == hashlib.sha256(
            selection.descriptor_bytes
        ).hexdigest()
        assert selection.manifest_bytes == selection.manifest_path.read_bytes()
        assert before == after
        assert catalog.state(prepared.chunk_ids[0]) is ChunkState.SEALED
        assert catalog.archive_transactions() == []


def test_repeated_selection_is_byte_identical(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        exporter = RemoteSourceExporter(layout=prepared.layout, catalog=catalog)
        first = exporter.select_chunk(prepared.chunk_ids[0])
        second = exporter.select_chunk(prepared.chunk_ids[0])

        assert first.descriptor_bytes == second.descriptor_bytes
        assert first.descriptor_sha256 == second.descriptor_sha256
        assert first.manifest_bytes == second.manifest_bytes


def test_manifest_digest_uses_exact_original_bytes(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    manifest_path = _manifest_path(prepared)
    original = manifest_path.read_bytes()
    manifest_path.write_bytes(
        (json.dumps(json.loads(original), indent=2, sort_keys=True) + "\n").encode()
    )

    with Catalog(prepared.layout.catalog) as catalog:
        selection = RemoteSourceExporter(
            layout=prepared.layout, catalog=catalog
        ).select_chunk(prepared.chunk_ids[0])

    assert selection.manifest_bytes == manifest_path.read_bytes()
    assert selection.descriptor.source_manifest_sha256 == hashlib.sha256(
        selection.manifest_bytes
    ).hexdigest()
    assert selection.descriptor.source_manifest_sha256 != hashlib.sha256(
        original
    ).hexdigest()


@pytest.mark.parametrize(
    "state",
    [ChunkState.ACTIVE, ChunkState.SEALING, ChunkState.QUARANTINED],
)
def test_unsealed_internal_states_are_rejected(tmp_path: Path, state: ChunkState) -> None:
    layout = ensure_storage_layout(tmp_path)
    chunk_id = "unsealed-chunk"
    with Catalog(layout.catalog) as catalog:
        catalog.register_active(
            chunk_id=chunk_id,
            partial_path="data/active/unsealed.bmdr.partial",
            created_at_utc_ns=1,
        )
        if state is not ChunkState.ACTIVE:
            catalog.transition(
                chunk_id,
                state,
                idempotency_key=f"test:{state}",
            )
        with pytest.raises(RemoteSourceError, match="source not eligible"):
            RemoteSourceExporter(layout=layout, catalog=catalog).select_chunk(chunk_id)


def _archive_state_fixture(
    root: Path, state: ArchiveState
) -> tuple[StorageLayout, str]:
    prepared = prepare_archive(root)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )

        def stop_after_reserve(point: str, _path: Path | None) -> None:
            if point == "after_reserve":
                raise RuntimeError("leave same-host transaction reserved")

        manager.fault_hook = stop_after_reserve
        with pytest.raises(RuntimeError, match="leave same-host"):
            manager.run_once()
        transaction = catalog.archive_transactions()[0]
        transaction_id = str(transaction["transaction_id"])
        progression = {
            ArchiveState.COPYING: [],
            ArchiveState.VERIFYING: [ArchiveState.VERIFYING],
            ArchiveState.VERIFIED: [ArchiveState.VERIFYING, ArchiveState.VERIFIED],
            ArchiveState.LOCAL_DELETE_PENDING: [
                ArchiveState.VERIFYING,
                ArchiveState.VERIFIED,
                ArchiveState.LOCAL_DELETE_PENDING,
            ],
            ArchiveState.LOCAL_DELETED: [
                ArchiveState.VERIFYING,
                ArchiveState.VERIFIED,
                ArchiveState.LOCAL_DELETE_PENDING,
                ArchiveState.LOCAL_DELETED,
            ],
        }
        for next_state in progression[state]:
            catalog.transition_archive(
                transaction_id,
                next_state,
                idempotency_key=f"test:{next_state}",
            )
        return prepared.layout, prepared.chunk_ids[0]


@pytest.mark.parametrize("state", list(ArchiveState))
def test_same_host_archive_states_are_rejected(
    tmp_path: Path, state: ArchiveState
) -> None:
    layout, chunk_id = _archive_state_fixture(tmp_path / str(state), state)
    with Catalog(layout.catalog) as catalog, pytest.raises(
        RemoteSourceError, match="source not eligible"
    ):
        RemoteSourceExporter(layout=layout, catalog=catalog).select_chunk(chunk_id)


def test_sealed_chunk_with_archive_transaction_fails_closed(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        manager = ArchiveManager(
            layout=prepared.layout, catalog=catalog, target=prepared.target
        )

        def stop_after_reserve(point: str, _path: Path | None) -> None:
            if point == "after_reserve":
                raise RuntimeError("reserved")

        manager.fault_hook = stop_after_reserve
        with pytest.raises(RuntimeError, match="reserved"):
            manager.run_once()
        with catalog._lock:
            catalog._connection.execute(
                "UPDATE chunks SET state = ? WHERE chunk_id = ?",
                (ChunkState.SEALED, prepared.chunk_ids[0]),
            )
        with pytest.raises(RemoteSourceError, match="contradiction"):
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                prepared.chunk_ids[0]
            )


def test_missing_source_fails_without_catalog_mutation(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    source = _source_path(prepared)
    source.unlink()
    with Catalog(prepared.layout.catalog) as catalog:
        before = _lifecycle_snapshot(catalog, prepared.chunk_ids[0])
        with pytest.raises(RemoteSourceError, match="source artifact missing"):
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                prepared.chunk_ids[0]
            )
        assert _lifecycle_snapshot(catalog, prepared.chunk_ids[0]) == before


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda document: document.__setitem__("chunk_id", "other"), "chunk_id mismatch"),
        (
            lambda document: document.__setitem__(
                "stored_bytes", int(document["stored_bytes"]) + 1
            ),
            "stored_bytes mismatch",
        ),
        (
            lambda document: document.__setitem__("stored_sha256", "0" * 64),
            "stored_sha256 mismatch",
        ),
        (
            lambda document: document.__setitem__("relative_path", "data/sealed/other.zst"),
            "relative_path",
        ),
        (
            lambda document: document.__setitem__(
                "manifest_schema_version", "raw-chunk-manifest.v2"
            ),
            "unsupported manifest schema",
        ),
    ],
)
def test_manifest_identity_and_schema_mismatches_fail_closed(
    tmp_path: Path,
    change: Callable[[dict[str, object]], None],
    message: str,
) -> None:
    prepared = prepare_archive(tmp_path)
    _rewrite_manifest(_manifest_path(prepared), change)
    with Catalog(prepared.layout.catalog) as catalog, pytest.raises(
        RemoteSourceError, match=message
    ):
        RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
            prepared.chunk_ids[0]
        )


def test_source_size_mismatch_fails_closed(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    source = _source_path(prepared)
    source.write_bytes(source.read_bytes() + b"x")
    with Catalog(prepared.layout.catalog) as catalog, pytest.raises(
        RemoteSourceError, match="source artifact validation failure"
    ):
        RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
            prepared.chunk_ids[0]
        )


def test_source_stored_sha_mismatch_fails_closed(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    source = _source_path(prepared)
    body = bytearray(source.read_bytes())
    body[0] ^= 1
    source.write_bytes(body)
    with Catalog(prepared.layout.catalog) as catalog, pytest.raises(
        RemoteSourceError, match="source artifact validation failure"
    ):
        RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
            prepared.chunk_ids[0]
        )


def test_source_decompressed_hash_mismatch_fails_closed(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    _rewrite_manifest(
        _manifest_path(prepared),
        lambda document: document.__setitem__("uncompressed_sha256", "0" * 64),
    )
    with Catalog(prepared.layout.catalog) as catalog, pytest.raises(
        RemoteSourceError, match="source artifact validation failure"
    ):
        RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
            prepared.chunk_ids[0]
        )


@pytest.mark.parametrize("field", ["sealed_path", "manifest_path"])
def test_catalog_path_escape_fails_closed(tmp_path: Path, field: str) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        with catalog._lock:
            catalog._connection.execute(
                f"UPDATE chunks SET {field} = ? WHERE chunk_id = ?",
                ("../outside", prepared.chunk_ids[0]),
            )
        with pytest.raises(RemoteSourceError, match="invalid Catalog"):
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                prepared.chunk_ids[0]
            )


def test_existing_nested_sealed_source_fails_path_authority(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    source = _source_path(prepared)
    nested = prepared.layout.sealed / "nested" / source.name
    nested.parent.mkdir()
    source.rename(nested)
    with Catalog(prepared.layout.catalog) as catalog:
        with catalog._lock:
            catalog._connection.execute(
                "UPDATE chunks SET sealed_path = ? WHERE chunk_id = ?",
                (
                    str(nested.relative_to(prepared.layout.root)),
                    prepared.chunk_ids[0],
                ),
            )
        assert nested.is_file()
        with pytest.raises(RemoteSourceError, match="exact Recorder directory"):
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                prepared.chunk_ids[0]
            )


def test_existing_nested_manifest_fails_path_authority(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    manifest = _manifest_path(prepared)
    nested = prepared.layout.manifests / "nested" / manifest.name
    nested.parent.mkdir()
    manifest.rename(nested)
    with Catalog(prepared.layout.catalog) as catalog:
        with catalog._lock:
            catalog._connection.execute(
                "UPDATE chunks SET manifest_path = ? WHERE chunk_id = ?",
                (
                    str(nested.relative_to(prepared.layout.root)),
                    prepared.chunk_ids[0],
                ),
            )
        assert nested.is_file()
        with pytest.raises(RemoteSourceError, match="exact Recorder directory"):
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                prepared.chunk_ids[0]
            )


def test_final_catalog_recheck_rejects_concurrent_state_change(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        original_snapshot = catalog.chunk_archive_snapshot
        calls = 0

        def racing_snapshot(
            chunk_id: str,
        ) -> tuple[dict[str, object] | None, dict[str, object] | None]:
            nonlocal calls
            result = original_snapshot(chunk_id)
            calls += 1
            if calls == 1:
                with catalog._lock:
                    catalog._connection.execute(
                        "UPDATE chunks SET state = ? WHERE chunk_id = ?",
                        (ChunkState.ACTIVE, chunk_id),
                    )
            return result

        monkeypatch.setattr(catalog, "chunk_archive_snapshot", racing_snapshot)
        with pytest.raises(RemoteSourceError, match="state changed during selection"):
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                prepared.chunk_ids[0]
            )


def test_failed_selection_does_not_change_lifecycle(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    with Catalog(prepared.layout.catalog) as catalog:
        before = _lifecycle_snapshot(catalog, prepared.chunk_ids[0])
        _rewrite_manifest(
            _manifest_path(prepared),
            lambda document: document.__setitem__("chunk_id", "other"),
        )
        with pytest.raises(RemoteSourceError):
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                prepared.chunk_ids[0]
            )
        assert _lifecycle_snapshot(catalog, prepared.chunk_ids[0]) == before


def test_oldest_selection_reuses_catalog_ordering(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path, chunk_count=2)
    with Catalog(prepared.layout.catalog) as catalog:
        selection = RemoteSourceExporter(
            layout=prepared.layout, catalog=catalog
        ).select_oldest()
        assert selection is not None
        assert selection.descriptor.chunk_id == prepared.chunk_ids[0]


def test_no_eligible_source_returns_none(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        assert RemoteSourceExporter(layout=layout, catalog=catalog).select_oldest() is None
