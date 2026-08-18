from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest

from binance_market_data_recorder.archive import (
    ARCHIVE_SET_ENTRY_SCHEMA,
    REMOTE_ARCHIVE_RECEIPT_SCHEMA,
    REMOTE_RECEIVE_VERIFICATION_OUTCOME,
    REMOTE_RECEIVE_VERIFICATION_VERSION,
    ArchiveSetEntry,
    RemoteArchiveReceipt,
    RemoteReceiveError,
    RemoteReceiver,
    RemoteReceiveTarget,
    RemoteSourceDescriptor,
    RemoteSourceSelection,
    canonical_descriptor_bytes,
    generate_archive_session_id,
    receive_transaction_id,
)
from binance_market_data_recorder.archive import remote_receive as receive_module
from binance_market_data_recorder.archive.remote_source import descriptor_sha256


def _canonical(document: dict[str, object]) -> bytes:
    return (
        json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n"
    ).encode()


def _selection(**descriptor_changes: object) -> RemoteSourceSelection:
    manifest: dict[str, object] = {
        "manifest_schema_version": "raw-chunk-manifest.v1",
        "chunk_schema_version": "raw-chunk.v1",
        "envelope_schema_version": "event-envelope.v1",
        "chunk_id": "00112233-4455-6677-8899-aabbccddeeff",
        "market": "spot",
        "stream": "diff_depth",
        "relative_path": (
            "data/sealed/00112233-4455-6677-8899-aabbccddeeff.bmdr.zst"
        ),
        "stored_bytes": 3,
        "stored_sha256": hashlib.sha256(b"raw").hexdigest(),
    }
    manifest_bytes = _canonical(manifest)
    values: dict[str, object] = {
        "descriptor_schema_version": "remote-source-descriptor.v1",
        "chunk_id": manifest["chunk_id"],
        "market": manifest["market"],
        "stream": manifest["stream"],
        "source_relative_path": manifest["relative_path"],
        "stored_bytes": manifest["stored_bytes"],
        "stored_sha256": manifest["stored_sha256"],
        "source_manifest_relative_path": (
            "data/manifests/00112233-4455-6677-8899-aabbccddeeff.manifest.json"
        ),
        "source_manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
        "manifest_schema_version": manifest["manifest_schema_version"],
        "chunk_schema_version": manifest["chunk_schema_version"],
        "envelope_schema_version": manifest["envelope_schema_version"],
    }
    values.update(descriptor_changes)
    descriptor = RemoteSourceDescriptor(**values)  # type: ignore[arg-type]
    descriptor_bytes = canonical_descriptor_bytes(descriptor)
    return RemoteSourceSelection(
        descriptor=descriptor,
        descriptor_bytes=descriptor_bytes,
        descriptor_sha256=descriptor_sha256(descriptor_bytes),
        manifest_bytes=manifest_bytes,
        manifest_path=Path("/source/manifest.json"),
        sealed_path=Path("/source/raw.zst"),
    )


def _target(**changes: object) -> RemoteReceiveTarget:
    values: dict[str, object] = {
        "archive_set_id": "set-1",
        "storage_id": "storage-1",
        "volume_uuid": "volume-1",
        "registered_relative_path": "Archive/Recorder",
        "marker_nonce": "nonce-1",
        "root": Path("/archive"),
    }
    values.update(changes)
    return RemoteReceiveTarget(**values)  # type: ignore[arg-type]


def _receipt(
    *, session_id: str | None = None, entry_sha256: str = "e" * 64
) -> RemoteArchiveReceipt:
    return RemoteArchiveReceipt.build(
        selection=_selection(),
        target=_target(),
        session_id=session_id or "12345678-1234-4234-9234-123456789abc",
        artifact_relative_path=(
            "raw/00112233-4455-6677-8899-aabbccddeeff.bmdr.zst"
        ),
        archive_set_entry_sha256=entry_sha256,
    )


def test_receipt_canonical_serialization_and_exact_field_set() -> None:
    receipt = _receipt()
    body = receipt.canonical_bytes()
    document = json.loads(body)

    assert body.endswith(b"\n") and not body.endswith(b"\n\n")
    assert set(document) == set(receive_module._RECEIPT_FIELDS)
    assert document["receipt_schema_version"] == REMOTE_ARCHIVE_RECEIPT_SCHEMA
    assert document["verification_version"] == REMOTE_RECEIVE_VERIFICATION_VERSION
    assert document["verification_outcome"] == REMOTE_RECEIVE_VERIFICATION_OUTCOME
    assert RemoteArchiveReceipt.from_bytes(body) == receipt


def test_receipt_id_is_digest_of_all_fields_except_receipt_id() -> None:
    receipt = _receipt()
    expected = hashlib.sha256(_canonical(receipt.identity_document())).hexdigest()
    assert receipt.receipt_id == expected


def test_receipt_rejects_noncanonical_and_wrong_identity() -> None:
    receipt = _receipt()
    document = receipt.document()
    document["receipt_id"] = "0" * 64
    with pytest.raises(RemoteReceiveError, match="receipt_id"):
        RemoteArchiveReceipt.from_bytes(_canonical(document))
    with pytest.raises(RemoteReceiveError, match="not canonical"):
        RemoteArchiveReceipt.from_bytes(
            (json.dumps(receipt.document(), indent=2) + "\n").encode()
        )


@pytest.mark.parametrize(
    "session_id",
    [
        "not-a-uuid",
        "12345678-1234-1234-9234-123456789abc",
        "12345678-1234-4234-9234-123456789ABC",
    ],
)
def test_invalid_session_uuid_fails_closed(session_id: str) -> None:
    with pytest.raises(RemoteReceiveError, match="canonical UUID4"):
        _receipt(session_id=session_id)


def test_generate_archive_session_id_returns_canonical_uuid4() -> None:
    session_id = generate_archive_session_id()
    parsed = UUID(session_id)
    assert parsed.version == 4
    assert str(parsed) == session_id


def test_descriptor_bytes_and_digest_are_revalidated() -> None:
    selection = _selection()
    with pytest.raises(RemoteReceiveError, match="descriptor bytes"):
        receive_transaction_id(
            replace(selection, descriptor_bytes=selection.descriptor_bytes + b" "),
            _target(),
        )
    with pytest.raises(RemoteReceiveError, match="descriptor digest"):
        receive_transaction_id(
            replace(selection, descriptor_sha256="0" * 64),
            _target(),
        )


def test_source_manifest_digest_and_descriptor_fields_are_revalidated() -> None:
    selection = _selection()
    with pytest.raises(RemoteReceiveError, match="manifest digest"):
        receive_transaction_id(
            replace(selection, manifest_bytes=selection.manifest_bytes + b" "),
            _target(),
        )

    mismatched = _selection(market="usdm")
    with pytest.raises(RemoteReceiveError, match="identity mismatch"):
        receive_transaction_id(mismatched, _target())


def test_unsupported_source_manifest_schema_fails_closed() -> None:
    with pytest.raises(RemoteReceiveError, match="unsupported source Raw manifest"):
        receive_transaction_id(
            _selection(manifest_schema_version="raw-chunk-manifest.v999"),
            _target(),
        )


@pytest.mark.parametrize(
    "path", ["../sealed/x", "/sealed/x", "data//sealed/x", "data\\sealed\\x"]
)
def test_source_descriptor_path_must_be_canonical(path: str) -> None:
    with pytest.raises(RemoteReceiveError, match="canonical relative path"):
        receive_transaction_id(_selection(source_relative_path=path), _target())


def test_receive_transaction_id_is_stable_and_session_independent() -> None:
    selection = _selection()
    target = _target()
    first = receive_transaction_id(selection, target)
    second = receive_transaction_id(selection, target)
    assert first == second
    assert str(UUID(first)) == first

    receipt_one = _receipt(session_id="12345678-1234-4234-9234-123456789abc")
    receipt_two = _receipt(session_id="87654321-4321-4321-8321-cba987654321")
    assert receipt_one.receipt_id != receipt_two.receipt_id
    assert receive_transaction_id(selection, target) == first


def test_same_session_and_chain_produce_same_receipt_id() -> None:
    assert _receipt().receipt_id == _receipt().receipt_id


@pytest.mark.parametrize(
    "path",
    ["../escape", "/absolute", "Archive\\Recorder", "Archive//Recorder", "./x"],
)
def test_target_registered_path_must_be_canonical(path: str) -> None:
    with pytest.raises(RemoteReceiveError, match="canonical relative path"):
        _target(registered_relative_path=path)


def test_registered_root_dot_remains_valid() -> None:
    assert _target(registered_relative_path=".").registered_relative_path == "."


def test_windows_end_to_end_gate_fails_before_any_receive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        "binance_market_data_recorder.archive.remote_receive.sys.platform", "win32"
    )

    class NeverProvider:
        def open_stored_bytes(self, selection: RemoteSourceSelection) -> object:
            raise AssertionError(selection)

    with pytest.raises(RemoteReceiveError, match="unsupported"):
        RemoteReceiver(
            provider=NeverProvider(),  # type: ignore[arg-type]
            target=_target(),
        ).receive(_selection(), session_id=generate_archive_session_id())


def test_archive_and_source_manifest_authorities_are_distinct() -> None:
    selection = _selection()
    target = _target()
    archive_manifest = {
        **receive_module._archive_manifest_expected(
            selection,
            target,
            transaction_id=receive_transaction_id(selection, target),
            artifact_relative_path=(
                "raw/00112233-4455-6677-8899-aabbccddeeff.bmdr.zst"
            ),
        ),
        "raw_manifest": json.loads(selection.manifest_bytes),
        "raw_manifest_bytes_base64": "unused-for-digest-comparison",
        "verification": {
            "full_readback": True,
            "size_match": True,
            "sha256_match": True,
        },
        "verified_at_utc_ns": 1,
    }
    archive_sha = hashlib.sha256(_canonical(archive_manifest)).hexdigest()
    assert archive_sha != selection.descriptor.source_manifest_sha256

    entry = ArchiveSetEntry(
        archive_set_id=target.archive_set_id,
        storage_id=target.storage_id,
        chunk_id=selection.descriptor.chunk_id,
        artifact_relative_path=(
            "raw/00112233-4455-6677-8899-aabbccddeeff.bmdr.zst"
        ),
        archive_manifest_relative_path=(
            "manifests/00112233-4455-6677-8899-aabbccddeeff.archive-manifest.json"
        ),
        archive_manifest_sha256=archive_sha,
        stored_bytes=selection.descriptor.stored_bytes,
        stored_sha256=selection.descriptor.stored_sha256,
        source_manifest_sha256=selection.descriptor.source_manifest_sha256,
    )
    assert entry.as_dict()["schema"] == ARCHIVE_SET_ENTRY_SCHEMA
    assert entry.archive_manifest_sha256 == archive_sha
    assert entry.source_manifest_sha256 == selection.descriptor.source_manifest_sha256
    assert entry.archive_manifest_sha256 != entry.source_manifest_sha256
