from __future__ import annotations

import base64
import hashlib
import io
import json
import multiprocessing
import os
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, replace
from pathlib import Path
from typing import BinaryIO

import pytest

from binance_market_data_recorder.archive import (
    ArchiveSetEntry,
    ArchiveSetError,
    ArchiveSetIndex,
    ArchiveSetStore,
    RemoteArchiveReceipt,
    RemoteReceiveError,
    RemoteReceiver,
    RemoteReceiveTarget,
    RemoteSourceExporter,
    RemoteSourceSelection,
    canonical_descriptor_bytes,
    generate_archive_set_id,
    revalidate_remote_archive_receipt,
)
from binance_market_data_recorder.archive import archive_set as archive_set_module
from binance_market_data_recorder.archive import remote_receive as receive_module
from binance_market_data_recorder.archive.remote_source import descriptor_sha256
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import (
    fsync_directory as actual_fsync_directory,
)
from tests.archive_support import PreparedArchive, prepare_archive


class SourceFileProvider:
    def open_stored_bytes(self, selection: RemoteSourceSelection) -> BinaryIO:
        return selection.sealed_path.open("rb", buffering=0)


class BodyProvider:
    def __init__(self, body: bytes) -> None:
        self.body = body

    def open_stored_bytes(self, selection: RemoteSourceSelection) -> BinaryIO:
        del selection
        return io.BytesIO(self.body)


class FailingProvider:
    def open_stored_bytes(self, selection: RemoteSourceSelection) -> BinaryIO:
        del selection
        raise OSError("injected provider failure")


@dataclass(frozen=True)
class ReceiveFixture:
    prepared: PreparedArchive
    selection: RemoteSourceSelection
    target: RemoteReceiveTarget


def _fixture(root: Path, *, payload_bytes: int = 128) -> ReceiveFixture:
    prepared = prepare_archive(root.resolve(), payload_bytes=payload_bytes)
    with Catalog(prepared.layout.catalog) as catalog:
        selection = RemoteSourceExporter(
            layout=prepared.layout, catalog=catalog
        ).select_chunk(prepared.chunk_ids[0])
    target = RemoteReceiveTarget(
        archive_set_id=generate_archive_set_id(),
        storage_id=prepared.target.storage_id,
        volume_uuid=prepared.target.volume_uuid,
        registered_relative_path=prepared.target.registered_relative_path,
        marker_nonce=prepared.target.marker_nonce,
        root=prepared.target.root,
    )
    return ReceiveFixture(prepared, selection, target)


def _receive(
    fixture: ReceiveFixture,
    *,
    session_id: str | None = None,
    provider: object | None = None,
    fault_hook: object | None = None,
) -> RemoteArchiveReceipt:
    return RemoteReceiver(
        provider=provider or SourceFileProvider(),  # type: ignore[arg-type]
        target=fixture.target,
        fault_hook=fault_hook,  # type: ignore[arg-type]
        utc_clock_ns=lambda: 1_800_000_000_000_000_000,
    ).receive(
        fixture.selection,
        session_id=session_id or "12345678-1234-4234-9234-123456789abc",
    )


def _artifact_path(fixture: ReceiveFixture) -> Path:
    return fixture.target.root / "raw" / fixture.selection.sealed_path.name


def _manifest_path(fixture: ReceiveFixture) -> Path:
    return (
        fixture.target.root
        / "manifests"
        / f"{fixture.selection.descriptor.chunk_id}.archive-manifest.json"
    )


def _entry_path(fixture: ReceiveFixture) -> Path:
    return (
        fixture.target.root
        / "archive-set"
        / "entries"
        / f"{fixture.selection.descriptor.chunk_id}.json"
    )


def _receipt_paths(fixture: ReceiveFixture) -> list[Path]:
    directory = fixture.target.root / "archive-set" / "receipts"
    return sorted(directory.glob("*.json")) if directory.exists() else []


def _source_snapshot(fixture: ReceiveFixture) -> tuple[bytes, bytes, object, object]:
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        return (
            fixture.selection.sealed_path.read_bytes(),
            fixture.selection.manifest_path.read_bytes(),
            catalog.chunk_archive_snapshot(fixture.selection.descriptor.chunk_id),
            tuple(catalog.archive_transactions()),
        )


def _rewrite_canonical(path: Path, change: object) -> None:
    document = json.loads(path.read_bytes())
    assert isinstance(document, dict)
    change(document)  # type: ignore[operator]
    path.write_bytes(
        (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
    )


def _marker_path(fixture: ReceiveFixture) -> Path:
    return fixture.target.root / ".binance-market-data-recorder-storage.json"


def test_end_to_end_receive_and_independent_revalidation(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    before = _source_snapshot(fixture)

    receipt = _receive(fixture)
    revalidated = revalidate_remote_archive_receipt(
        selection=fixture.selection,
        target=fixture.target,
        receipt_id=receipt.receipt_id,
    )

    assert revalidated == receipt
    assert _artifact_path(fixture).read_bytes() == fixture.selection.sealed_path.read_bytes()
    assert _source_snapshot(fixture) == before
    with Catalog(fixture.prepared.layout.catalog) as catalog:
        assert catalog.state(fixture.selection.descriptor.chunk_id) is ChunkState.SEALED
        assert catalog.archive_transactions() == []


@pytest.mark.parametrize(
    ("provider_factory", "message"),
    [
        (lambda body: BodyProvider(body[:-1]), "ended before expected"),
        (lambda body: BodyProvider(body + b"x"), "extra bytes"),
        (
            lambda body: BodyProvider(bytes([body[0] ^ 1]) + body[1:]),
            "SHA-256 mismatch",
        ),
        (lambda body: FailingProvider(), "provider failure"),
    ],
)
def test_transfer_failures_create_no_entry_or_receipt(
    tmp_path: Path, provider_factory: object, message: str
) -> None:
    fixture = _fixture(tmp_path)
    body = fixture.selection.sealed_path.read_bytes()
    before = _source_snapshot(fixture)
    provider = provider_factory(body)  # type: ignore[operator]

    with pytest.raises(RemoteReceiveError, match=message):
        _receive(fixture, provider=provider)

    assert not _entry_path(fixture).exists()
    assert _receipt_paths(fixture) == []
    assert _source_snapshot(fixture) == before


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("marker_nonce", "changed-nonce"),
        ("storage_id", "changed-storage"),
        ("volume_uuid", "changed-volume"),
        ("registered_relative_path", "changed/path"),
    ],
)
def test_registered_marker_identity_mismatch_fails_closed(
    tmp_path: Path, field: str, replacement: str
) -> None:
    fixture = _fixture(tmp_path)
    before = _source_snapshot(fixture)
    _rewrite_canonical(
        _marker_path(fixture),
        lambda document: document.__setitem__(field, replacement),
    )

    with pytest.raises(RemoteReceiveError, match="physical identity"):
        _receive(fixture)

    assert _receipt_paths(fixture) == []
    assert _source_snapshot(fixture) == before


def test_physical_identity_replaced_after_raw_commit_fails_before_manifest(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)

    def replace_marker(point: str, _path: Path | None) -> None:
        if point == "k7_after_raw_durable":
            _rewrite_canonical(
                _marker_path(fixture),
                lambda document: document.__setitem__("marker_nonce", "replacement"),
            )

    with pytest.raises(RemoteReceiveError, match="physical identity"):
        _receive(fixture, fault_hook=replace_marker)

    assert _artifact_path(fixture).is_file()
    assert not _manifest_path(fixture).exists()
    assert _receipt_paths(fixture) == []


def test_archive_set_mismatch_fails_without_rebinding(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    ArchiveSetStore.bind(
        fixture.target.root,
        archive_set_id="existing-set",
        storage_id=fixture.target.storage_id,
        volume_uuid=fixture.target.volume_uuid,
        registered_relative_path=fixture.target.registered_relative_path,
        marker_nonce=fixture.target.marker_nonce,
    )
    with pytest.raises(RemoteReceiveError, match="identity conflicts"):
        _receive(fixture)
    assert _receipt_paths(fixture) == []


def test_different_raw_collision_is_preserved_and_fails(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    final = _artifact_path(fixture)
    final.parent.mkdir()
    conflict = b"x" * fixture.selection.descriptor.stored_bytes
    final.write_bytes(conflict)

    with pytest.raises(RemoteReceiveError, match="SHA-256 mismatch"):
        _receive(fixture)

    assert final.read_bytes() == conflict
    assert _receipt_paths(fixture) == []


def test_identical_existing_raw_converges(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    final = _artifact_path(fixture)
    final.parent.mkdir()
    final.write_bytes(fixture.selection.sealed_path.read_bytes())

    receipt = _receive(fixture)

    assert receipt.artifact_relative_path == f"raw/{final.name}"
    assert revalidate_remote_archive_receipt(
        selection=fixture.selection,
        target=fixture.target,
        receipt_id=receipt.receipt_id,
    ) == receipt


def test_publication_window_conflict_never_overwrites_winner(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    final = _artifact_path(fixture)
    conflict = b"z" * fixture.selection.descriptor.stored_bytes

    def occupy(point: str, _path: Path | None) -> None:
        if point == "k5_after_artifact_verification_before_publish":
            final.write_bytes(conflict)

    with pytest.raises(RemoteReceiveError, match="SHA-256 mismatch"):
        _receive(fixture, fault_hook=occupy)

    assert final.read_bytes() == conflict


@pytest.mark.parametrize("object_kind", ["symlink", "directory", "fifo"])
def test_nonregular_raw_final_is_rejected_without_following(
    tmp_path: Path, object_kind: str
) -> None:
    fixture = _fixture(tmp_path)
    final = _artifact_path(fixture)
    final.parent.mkdir()
    outside = tmp_path / "outside"
    outside.write_bytes(b"outside")
    if object_kind == "symlink":
        final.symlink_to(outside)
    elif object_kind == "directory":
        final.mkdir()
    else:
        os.mkfifo(final)

    with pytest.raises(RemoteReceiveError, match=r"regular file|safely open"):
        _receive(fixture)

    assert outside.read_bytes() == b"outside"
    assert _receipt_paths(fixture) == []


def test_symlinked_receive_directory_is_rejected_without_outside_write(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (fixture.target.root / "raw").symlink_to(outside, target_is_directory=True)

    with pytest.raises(RemoteReceiveError, match="symbolic link"):
        _receive(fixture)

    assert list(outside.iterdir()) == []


def test_real_concurrent_identical_publishers_converge(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    sessions = (
        "12345678-1234-4234-9234-123456789abc",
        "87654321-4321-4321-8321-cba987654321",
    )

    with ThreadPoolExecutor(max_workers=2) as executor:
        receipts = tuple(
            executor.map(lambda session: _receive(fixture, session_id=session), sessions)
        )

    assert len({receipt.receipt_id for receipt in receipts}) == 2
    assert _artifact_path(fixture).read_bytes() == fixture.selection.sealed_path.read_bytes()
    assert len(_receipt_paths(fixture)) == 2


def test_real_concurrent_conflicting_publishers_preserve_one_winner(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path.resolve(), chunk_count=2, payload_bytes=128)
    with Catalog(prepared.layout.catalog) as catalog:
        selections = [
            RemoteSourceExporter(layout=prepared.layout, catalog=catalog).select_chunk(
                chunk_id
            )
            for chunk_id in prepared.chunk_ids
        ]
    first, second = selections
    second_manifest = json.loads(second.manifest_bytes)
    second_manifest["relative_path"] = first.descriptor.source_relative_path
    second_manifest_bytes = (
        json.dumps(second_manifest, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()
    second_descriptor = replace(
        second.descriptor,
        source_relative_path=first.descriptor.source_relative_path,
        source_manifest_sha256=hashlib.sha256(second_manifest_bytes).hexdigest(),
    )
    second_descriptor_bytes = canonical_descriptor_bytes(second_descriptor)
    second = replace(
        second,
        descriptor=second_descriptor,
        descriptor_bytes=second_descriptor_bytes,
        descriptor_sha256=descriptor_sha256(second_descriptor_bytes),
        manifest_bytes=second_manifest_bytes,
    )
    target = RemoteReceiveTarget(
        archive_set_id=generate_archive_set_id(),
        storage_id=prepared.target.storage_id,
        volume_uuid=prepared.target.volume_uuid,
        registered_relative_path=prepared.target.registered_relative_path,
        marker_nonce=prepared.target.marker_nonce,
        root=prepared.target.root,
    )

    def run(selection_and_session: tuple[RemoteSourceSelection, str]) -> object:
        selection, session = selection_and_session
        try:
            return RemoteReceiver(
                provider=SourceFileProvider(), target=target
            ).receive(selection, session_id=session)
        except RemoteReceiveError as exc:
            return exc

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = tuple(
            executor.map(
                run,
                (
                    (first, "12345678-1234-4234-9234-123456789abc"),
                    (second, "87654321-4321-4321-8321-cba987654321"),
                ),
            )
        )
    receipts = [item for item in outcomes if isinstance(item, RemoteArchiveReceipt)]
    errors = [item for item in outcomes if isinstance(item, RemoteReceiveError)]

    assert len(receipts) == 1
    assert len(errors) == 1
    winner = first if receipts[0].chunk_id == first.descriptor.chunk_id else second
    final = target.root / receipts[0].artifact_relative_path
    assert final.read_bytes() == winner.sealed_path.read_bytes()
    assert len(list((target.root / "archive-set" / "receipts").glob("*.json"))) == 1


def test_external_manifest_structure_and_distinct_digests(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    receipt = _receive(fixture)
    manifest_body = _manifest_path(fixture).read_bytes()
    manifest = json.loads(manifest_body)
    entry = ArchiveSetStore.open(fixture.target.root).read_entry(receipt.chunk_id)

    assert set(manifest) == set(receive_module._ARCHIVE_MANIFEST_FIELDS)
    assert manifest["archive_manifest_schema_version"] == (
        "external-archive-manifest.v1"
    )
    assert base64.b64decode(manifest["raw_manifest_bytes_base64"], validate=True) == (
        fixture.selection.manifest_bytes
    )
    assert manifest["source_manifest_sha256"] == hashlib.sha256(
        fixture.selection.manifest_bytes
    ).hexdigest()
    assert entry.archive_manifest_sha256 == hashlib.sha256(manifest_body).hexdigest()
    assert entry.source_manifest_sha256 == manifest["source_manifest_sha256"]
    assert entry.archive_manifest_sha256 != entry.source_manifest_sha256
    assert not (
        fixture.target.root / "manifests" / fixture.selection.manifest_path.name
    ).exists()


def test_existing_semantically_same_manifest_with_new_timestamp_is_adopted(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)

    def stop(point: str, _path: Path | None) -> None:
        if point == "k7d_after_archive_manifest_durable":
            raise RuntimeError("stop after manifest")

    with pytest.raises(RemoteReceiveError, match="stop after manifest"):
        _receive(fixture, fault_hook=stop)
    _rewrite_canonical(
        _manifest_path(fixture),
        lambda document: document.__setitem__("verified_at_utc_ns", 99),
    )
    winner = _manifest_path(fixture).read_bytes()

    receipt = _receive(fixture)

    assert _manifest_path(fixture).read_bytes() == winner
    entry = ArchiveSetStore.open(fixture.target.root).read_entry(receipt.chunk_id)
    assert entry.archive_manifest_sha256 == hashlib.sha256(winner).hexdigest()


def test_conflicting_manifest_is_preserved(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    def stop(point: str, _path: Path | None) -> None:
        if point == "k7_after_raw_durable":
            raise RuntimeError("stop before manifest")

    with pytest.raises(RemoteReceiveError, match="stop before manifest"):
        _receive(fixture, fault_hook=stop)
    conflict = b'{"unrelated":"preserve"}\n'
    _manifest_path(fixture).parent.mkdir(exist_ok=True)
    _manifest_path(fixture).write_bytes(conflict)

    with pytest.raises(RemoteReceiveError):
        _receive(fixture)
    assert _manifest_path(fixture).read_bytes() == conflict
    assert _receipt_paths(fixture) == []


def test_archive_set_entry_digest_is_exact(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    receipt = _receive(fixture)
    entry = ArchiveSetStore.open(fixture.target.root).read_entry(receipt.chunk_id)
    assert hashlib.sha256(entry.canonical_bytes()).hexdigest() == (
        receipt.archive_set_entry_sha256
    )


def test_archive_set_entry_commit_failure_retains_artifact_and_manifest(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)

    def fail_commit(self: ArchiveSetStore, entry: ArchiveSetEntry) -> ArchiveSetEntry:
        del self, entry
        raise ArchiveSetError("injected entry commit failure")

    monkeypatch.setattr(ArchiveSetStore, "commit_entry", fail_commit)
    with pytest.raises(RemoteReceiveError, match="entry commit failure"):
        _receive(fixture)

    assert _artifact_path(fixture).is_file()
    assert _manifest_path(fixture).is_file()
    assert _receipt_paths(fixture) == []


def test_archive_set_entry_temp_fsync_failure_reconciles_on_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    before = _source_snapshot(fixture)
    armed = False
    failed = False
    post_k8_fsync_calls = 0
    actual_fsync = os.fsync

    def hook(point: str, _path: Path | None) -> None:
        nonlocal armed
        if point == "k8_before_archive_set_entry_commit":
            armed = True

    def fail_entry_temp_fsync(descriptor: int) -> None:
        nonlocal armed, failed, post_k8_fsync_calls
        if armed:
            post_k8_fsync_calls += 1
            # commit_entry() fsyncs entries/, archive-set/, and the medium root
            # before _atomic_publish() fsyncs the entry temporary file.
            if post_k8_fsync_calls == 4:
                armed = False
                failed = True
                raise OSError("injected Archive Set entry temp file fsync failure")
        actual_fsync(descriptor)

    # archive_set.os and remote_receive.os are the shared Python os module. The
    # K8 arm and exact post-K8 ordering keep earlier Raw/manifest fsyncs out.
    monkeypatch.setattr(os, "fsync", fail_entry_temp_fsync)

    with pytest.raises(RemoteReceiveError, match="entry temp file fsync failure"):
        _receive(fixture, fault_hook=hook)

    assert failed
    assert post_k8_fsync_calls == 4
    assert _artifact_path(fixture).is_file()
    assert _manifest_path(fixture).is_file()
    assert not _entry_path(fixture).exists()
    assert _receipt_paths(fixture) == []
    assert _source_snapshot(fixture) == before

    receipt = _receive(fixture)
    assert revalidate_remote_archive_receipt(
        selection=fixture.selection,
        target=fixture.target,
        receipt_id=receipt.receipt_id,
    ) == receipt


def test_archive_set_entries_parent_fsync_failure_reconciles_on_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    before = _source_snapshot(fixture)
    entries_directory = fixture.target.root / "archive-set" / "entries"
    armed = False
    failed = False
    entries_fsync_calls = 0
    entry_visible_at_failure = False

    def hook(point: str, _path: Path | None) -> None:
        nonlocal armed, entries_fsync_calls
        if point == "k8_before_archive_set_entry_commit" and not failed:
            armed = True
            entries_fsync_calls = 0

    def fail_post_publication_entries_fsync(path: Path) -> None:
        nonlocal armed, failed, entries_fsync_calls, entry_visible_at_failure
        if armed and path == entries_directory:
            entries_fsync_calls += 1
            # _ensure_inventory_directories() is the first entries/ fsync;
            # _atomic_publish() reaches this second one after publication.
            if entries_fsync_calls == 2:
                entry_visible_at_failure = _entry_path(fixture).is_file()
                armed = False
                failed = True
                raise OSError("injected post-publication entries directory fsync failure")
        actual_fsync_directory(path)

    monkeypatch.setattr(
        archive_set_module,
        "fsync_directory",
        fail_post_publication_entries_fsync,
    )

    with pytest.raises(
        RemoteReceiveError, match="post-publication entries directory fsync failure"
    ):
        _receive(fixture, fault_hook=hook)

    assert failed
    assert entries_fsync_calls == 2
    assert entry_visible_at_failure
    assert _entry_path(fixture).is_file()
    assert _receipt_paths(fixture) == []
    assert _source_snapshot(fixture) == before

    receipt = _receive(fixture)
    assert revalidate_remote_archive_receipt(
        selection=fixture.selection,
        target=fixture.target,
        receipt_id=receipt.receipt_id,
    ) == receipt


def test_conflicting_archive_set_entry_is_preserved(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)

    def stop(point: str, _path: Path | None) -> None:
        if point == "k7d_after_archive_manifest_durable":
            raise RuntimeError("stop before entry")

    with pytest.raises(RemoteReceiveError, match="stop before entry"):
        _receive(fixture, fault_hook=stop)
    manifest_sha = hashlib.sha256(_manifest_path(fixture).read_bytes()).hexdigest()
    store = ArchiveSetStore.open(fixture.target.root)
    conflicting = ArchiveSetEntry(
        archive_set_id=fixture.target.archive_set_id,
        storage_id=fixture.target.storage_id,
        chunk_id=fixture.selection.descriptor.chunk_id,
        artifact_relative_path=f"raw/{fixture.selection.sealed_path.name}",
        archive_manifest_relative_path=(
            f"manifests/{fixture.selection.descriptor.chunk_id}.archive-manifest.json"
        ),
        archive_manifest_sha256=manifest_sha,
        stored_bytes=fixture.selection.descriptor.stored_bytes,
        stored_sha256="0" * 64,
        source_manifest_sha256=fixture.selection.descriptor.source_manifest_sha256,
    )
    store.commit_entry(conflicting)

    with pytest.raises(RemoteReceiveError, match="entry conflicts"):
        _receive(fixture)
    assert store.read_entry(conflicting.chunk_id) == conflicting
    assert _receipt_paths(fixture) == []


def test_same_session_receipt_is_idempotent_and_different_session_is_distinct(
    tmp_path: Path,
) -> None:
    fixture = _fixture(tmp_path)
    session = "12345678-1234-4234-9234-123456789abc"
    first = _receive(fixture, session_id=session)
    second = _receive(fixture, session_id=session)
    third = _receive(
        fixture, session_id="87654321-4321-4321-8321-cba987654321"
    )

    assert first == second
    assert first.receipt_id != third.receipt_id
    assert len(_receipt_paths(fixture)) == 2


def test_corrupted_existing_receipt_is_never_overwritten(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    receipt = _receive(fixture)
    path = _receipt_paths(fixture)[0]
    corrupted = receipt.canonical_bytes()[:-2] + b"x\n"
    path.write_bytes(corrupted)

    with pytest.raises(RemoteReceiveError):
        _receive(fixture)
    assert path.read_bytes() == corrupted


@pytest.mark.parametrize("kind", ["raw", "manifest", "entry", "receipt", "marker"])
def test_independent_revalidation_rejects_corruption(
    tmp_path: Path, kind: str
) -> None:
    fixture = _fixture(tmp_path)
    receipt = _receive(fixture)
    if kind == "raw":
        body = bytearray(_artifact_path(fixture).read_bytes())
        body[-1] ^= 1
        _artifact_path(fixture).write_bytes(body)
    elif kind == "manifest":
        _manifest_path(fixture).write_bytes(b'{"corrupt":true}\n')
    elif kind == "entry":
        _rewrite_canonical(
            _entry_path(fixture),
            lambda document: document.__setitem__("stored_sha256", "0" * 64),
        )
    elif kind == "receipt":
        _receipt_paths(fixture)[0].write_bytes(b'{"corrupt":true}\n')
    else:
        _rewrite_canonical(
            _marker_path(fixture),
            lambda document: document.__setitem__("marker_nonce", "changed"),
        )

    with pytest.raises(RemoteReceiveError):
        revalidate_remote_archive_receipt(
            selection=fixture.selection,
            target=fixture.target,
            receipt_id=receipt.receipt_id,
        )


def test_workspace_index_is_not_receipt_authority(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    receipt = _receive(fixture)
    index_path = tmp_path / "workspace" / "archive-set.sqlite"
    ArchiveSetIndex(index_path).rebuild([fixture.target.root])
    index_path.unlink()

    assert revalidate_remote_archive_receipt(
        selection=fixture.selection,
        target=fixture.target,
        receipt_id=receipt.receipt_id,
    ) == receipt


def test_unknown_receiving_and_partial_files_are_never_swept(tmp_path: Path) -> None:
    fixture = _fixture(tmp_path)
    raw = fixture.target.root / "raw"
    raw.mkdir()
    unknown_raw = raw / f".{fixture.selection.sealed_path.name}.unknown.receiving"
    unknown_raw.write_bytes(b"unknown")
    archive_set = ArchiveSetStore.bind(
        fixture.target.root,
        archive_set_id=fixture.target.archive_set_id,
        storage_id=fixture.target.storage_id,
        volume_uuid=fixture.target.volume_uuid,
        registered_relative_path=fixture.target.registered_relative_path,
        marker_nonce=fixture.target.marker_nonce,
    )
    receipts = archive_set.archive_set_directory / "receipts"
    receipts.mkdir()
    unknown_receipt = receipts / ".unknown.json.old.partial"
    unknown_receipt.write_bytes(b"unknown")

    _receive(fixture)

    assert unknown_raw.read_bytes() == b"unknown"
    assert unknown_receipt.read_bytes() == b"unknown"


@pytest.mark.parametrize(
    "fault_point",
    [
        "k0_before_artifact_temp",
        "k1_during_artifact_transfer",
        "k2_after_artifact_writes_before_fsync",
        "k3_after_artifact_fsync_before_close",
        "k4_during_artifact_temp_readback",
        "k5_after_artifact_verification_before_publish",
        "k6_after_raw_rename_before_parent_fsync",
        "k7_after_raw_durable",
        "k7m_after_archive_manifest_rename_before_parent_fsync",
        "k7d_after_archive_manifest_durable",
        "k8_before_archive_set_entry_commit",
        "k9_after_archive_set_entry_durable",
        "k10_after_receipt_file_fsync_before_publish",
        "k11_after_receipt_rename_before_parent_fsync",
    ],
)
def test_k0_through_k11_exception_never_returns_committed_receipt(
    tmp_path: Path, fault_point: str
) -> None:
    fixture = _fixture(tmp_path)
    before = _source_snapshot(fixture)

    def stop(point: str, _path: Path | None) -> None:
        if point == fault_point:
            raise RuntimeError(f"stop at {fault_point}")

    with pytest.raises(RemoteReceiveError, match="stop at"):
        _receive(fixture, fault_hook=stop)
    assert _source_snapshot(fixture) == before


@pytest.mark.parametrize(
    ("armed_at", "fail_kind"),
    [
        ("k2_after_artifact_writes_before_fsync", "file"),
        ("k6_after_raw_rename_before_parent_fsync", "raw-dir"),
        ("k7_after_raw_durable", "file"),
        ("k7m_after_archive_manifest_rename_before_parent_fsync", "manifest-dir"),
        # K9 follows the real Archive Set entry commit; the next file fsync is
        # the receipt temporary-file fsync, not the entry temporary-file fsync.
        ("k9_after_archive_set_entry_durable", "file"),
        ("k11_after_receipt_rename_before_parent_fsync", "receipt-dir"),
    ],
)
def test_each_fsync_boundary_fails_closed_without_source_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    armed_at: str,
    fail_kind: str,
) -> None:
    fixture = _fixture(tmp_path)
    before = _source_snapshot(fixture)
    armed = False
    actual_fsync = os.fsync

    def hook(point: str, _path: Path | None) -> None:
        nonlocal armed
        if point == armed_at:
            armed = True

    def fail_file_fsync(descriptor: int) -> None:
        nonlocal armed
        if armed and fail_kind == "file":
            armed = False
            raise OSError("injected file fsync failure")
        actual_fsync(descriptor)

    def fail_directory_fsync(path: Path) -> None:
        nonlocal armed
        selected = {
            "raw-dir": "raw",
            "manifest-dir": "manifests",
            "receipt-dir": "receipts",
        }.get(fail_kind)
        if armed and selected == path.name:
            armed = False
            raise OSError("injected directory fsync failure")
        actual_fsync_directory(path)

    monkeypatch.setattr(
        "binance_market_data_recorder.archive.remote_receive.os.fsync",
        fail_file_fsync,
    )
    monkeypatch.setattr(
        "binance_market_data_recorder.archive.remote_receive.fsync_directory",
        fail_directory_fsync,
    )
    with pytest.raises(RemoteReceiveError, match="fsync failure"):
        _receive(fixture, fault_hook=hook)

    assert _receipt_paths(fixture) == [] or armed_at == (
        "k11_after_receipt_rename_before_parent_fsync"
    )
    assert _source_snapshot(fixture) == before


def test_receipt_parent_fsync_failure_reconciles_on_retry(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fixture = _fixture(tmp_path)
    actual = actual_fsync_directory
    armed = False

    def hook(point: str, _path: Path | None) -> None:
        nonlocal armed
        if point == "k11_after_receipt_rename_before_parent_fsync":
            armed = True

    def fail_once(path: Path) -> None:
        nonlocal armed
        if armed and path.name == "receipts":
            armed = False
            raise OSError("receipt directory fsync failure")
        actual(path)

    monkeypatch.setattr(
        "binance_market_data_recorder.archive.remote_receive.fsync_directory",
        fail_once,
    )
    with pytest.raises(RemoteReceiveError, match="fsync failure"):
        _receive(fixture, fault_hook=hook)
    assert len(_receipt_paths(fixture)) == 1

    monkeypatch.setattr(
        "binance_market_data_recorder.archive.remote_receive.fsync_directory",
        actual,
    )
    assert _receive(fixture).receipt_id == _receipt_paths(fixture)[0].stem


def _kill_receive_process(
    selection: RemoteSourceSelection,
    target: RemoteReceiveTarget,
    session_id: str,
    kill_point: str,
    result_path: str,
) -> None:
    def kill(point: str, _path: Path | None) -> None:
        if point == kill_point:
            os._exit(77)

    receipt = RemoteReceiver(
        provider=SourceFileProvider(),
        target=target,
        fault_hook=kill,
        utc_clock_ns=lambda: 1_800_000_000_000_000_000,
    ).receive(selection, session_id=session_id)
    Path(result_path).write_text(receipt.receipt_id, encoding="utf-8")


@pytest.mark.parametrize(
    ("kill_point", "raw_final", "manifest_final", "entry_final", "receipt_final"),
    [
        ("k0_before_artifact_temp", False, False, False, False),
        ("k1_during_artifact_transfer", False, False, False, False),
        ("k3_after_artifact_fsync_before_close", False, False, False, False),
        ("k5_after_artifact_verification_before_publish", False, False, False, False),
        ("k6_after_raw_rename_before_parent_fsync", True, False, False, False),
        ("k7_after_raw_durable", True, False, False, False),
        (
            "k7m_after_archive_manifest_rename_before_parent_fsync",
            True,
            True,
            False,
            False,
        ),
        ("k7d_after_archive_manifest_durable", True, True, False, False),
        ("k9_after_archive_set_entry_durable", True, True, True, False),
        ("k10_after_receipt_file_fsync_before_publish", True, True, True, False),
        ("k11_after_receipt_rename_before_parent_fsync", True, True, True, True),
        ("k12_after_receipt_parent_durable", True, True, True, True),
    ],
)
def test_abrupt_process_death_materializes_ordered_state_and_restart_converges(
    tmp_path: Path,
    kill_point: str,
    raw_final: bool,
    manifest_final: bool,
    entry_final: bool,
    receipt_final: bool,
) -> None:
    fixture = _fixture(tmp_path)
    session = "12345678-1234-4234-9234-123456789abc"
    result_path = tmp_path / "child-result"
    context = multiprocessing.get_context("spawn")
    child = context.Process(
        target=_kill_receive_process,
        args=(
            fixture.selection,
            fixture.target,
            session,
            kill_point,
            str(result_path),
        ),
    )
    child.start()
    child.join(timeout=30)
    try:
        assert not child.is_alive()
        assert child.exitcode == 77
        assert not result_path.exists()
        assert _artifact_path(fixture).exists() is raw_final
        assert _manifest_path(fixture).exists() is manifest_final
        assert _entry_path(fixture).exists() is entry_final
        assert bool(_receipt_paths(fixture)) is receipt_final

        unknown_temps = tuple(
            path
            for path in fixture.target.root.rglob("*")
            if path.is_file() and path.name.startswith(".")
            and path.suffix in {".receiving", ".partial"}
        )
        receipt = _receive(fixture, session_id=session)
        assert revalidate_remote_archive_receipt(
            selection=fixture.selection,
            target=fixture.target,
            receipt_id=receipt.receipt_id,
        ) == receipt
        assert all(path.exists() for path in unknown_temps)
    finally:
        if child.is_alive():
            child.terminate()
            child.join(timeout=5)
