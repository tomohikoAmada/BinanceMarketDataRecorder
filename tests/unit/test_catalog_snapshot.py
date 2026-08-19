from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from dataclasses import replace
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.archive.catalog_snapshot import (
    CATALOG_SNAPSHOT_MANIFEST_SCHEMA,
    CATALOG_SNAPSHOT_RETENTION_SCHEMA,
    CATALOG_SNAPSHOT_VERIFICATION_VERSION,
    CatalogSnapshotError,
    CatalogSnapshotManifest,
    CatalogSnapshotReference,
    CatalogSnapshotRetention,
)
from binance_market_data_recorder.archive.remote_transport import (
    OpenSSHRemoteTransport,
    RemoteTransportError,
)
from binance_market_data_recorder.storage.catalog import (
    Catalog,
    CatalogStateError,
    RemoteArchiveState,
)


def _manifest() -> CatalogSnapshotManifest:
    return CatalogSnapshotManifest(
        schema=CATALOG_SNAPSHOT_MANIFEST_SCHEMA,
        snapshot_id=str(uuid.uuid4()),
        receipt_id="a" * 64,
        chunk_id=str(uuid.uuid4()),
        required_remote_state=RemoteArchiveState.REMOTE_DELETE_PENDING.value,
        observed_remote_state=RemoteArchiveState.REMOTE_DELETED.value,
        stored_bytes=4096,
        sha256="b" * 64,
        verification_version=CATALOG_SNAPSHOT_VERIFICATION_VERSION,
        verified_at_utc_ns=1,
    )


def test_manifest_exact_fields_and_canonical_round_trip() -> None:
    manifest = _manifest()
    body = manifest.canonical_bytes()
    assert CatalogSnapshotManifest.from_bytes(body) == manifest
    assert set(json.loads(body)) == {
        "schema",
        "snapshot_id",
        "receipt_id",
        "chunk_id",
        "required_remote_state",
        "observed_remote_state",
        "stored_bytes",
        "sha256",
        "verification_version",
        "verified_at_utc_ns",
    }
    with pytest.raises(CatalogSnapshotError):
        CatalogSnapshotManifest.from_bytes(body.rstrip(b"\n"))
    document = json.loads(body)
    document["extra"] = True
    with pytest.raises(CatalogSnapshotError, match="fields"):
        CatalogSnapshotManifest.from_bytes(
            (json.dumps(document, sort_keys=True, separators=(",", ":")) + "\n").encode()
        )


def test_manifest_deleted_requirement_rejects_pending_observation() -> None:
    manifest = _manifest()
    invalid = replace(
        manifest,
        required_remote_state=RemoteArchiveState.REMOTE_DELETED.value,
        observed_remote_state=RemoteArchiveState.REMOTE_DELETE_PENDING.value,
    )
    with pytest.raises(CatalogSnapshotError, match="below required"):
        invalid.validate()


def test_retention_schema_is_exact_canonical_and_distinguishes_ids() -> None:
    first = CatalogSnapshotReference(str(uuid.uuid4()), "a" * 64)
    second = CatalogSnapshotReference(str(uuid.uuid4()), "a" * 64)
    retention = CatalogSnapshotRetention(
        CATALOG_SNAPSHOT_RETENTION_SCHEMA, 2, second, first
    )
    body = retention.canonical_bytes()
    assert CatalogSnapshotRetention.from_bytes(body) == retention
    assert first.snapshot_id != second.snapshot_id
    assert first.manifest_sha256 == second.manifest_sha256
    assert set(json.loads(body)) == {"schema", "generation", "latest", "previous"}


def test_live_read_only_uri_never_contains_immutable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path):
        pass
    calls: list[tuple[object, dict[str, object]]] = []
    original = sqlite3.connect

    def recording(database: str, *args: Any, **kwargs: Any) -> sqlite3.Connection:
        calls.append((database, kwargs))
        return cast(sqlite3.Connection, original(database, *args, **kwargs))

    monkeypatch.setattr(sqlite3, "connect", recording)
    with Catalog.open_live_read_only(path) as catalog:
        assert catalog.live_read_only
    database, arguments = calls[-1]
    assert "mode=ro" in str(database)
    assert "immutable=1" not in str(database)
    assert arguments["uri"] is True


def test_backup_api_rejects_static_read_only_source(tmp_path: Path) -> None:
    path = tmp_path / "catalog.sqlite"
    with Catalog(path):
        pass
    destination = sqlite3.connect(tmp_path / "destination.sqlite")
    try:
        with Catalog(path, read_only=True) as catalog, pytest.raises(
            CatalogStateError, match="live read-only"
        ):
            catalog.backup_to(destination)
    finally:
        destination.close()


@pytest.mark.parametrize(
    "state", [RemoteArchiveState.REMOTE_DELETE_PENDING, RemoteArchiveState.REMOTE_DELETED]
)
def test_ssh_snapshot_command_is_fixed_and_state_allowlisted(
    state: RemoteArchiveState,
) -> None:
    transport = OpenSSHRemoteTransport(host_alias="archive-vps")
    command = transport._remote_command("catalog-snapshot", "a" * 64, state.value)
    assert command == (
        "binance-market-recorder _remote catalog-snapshot "
        f"{'a' * 64} {state.value}"
    )


@pytest.mark.parametrize("state", ["PENDING", "REMOTE_DELETED;rm", ""])
def test_ssh_snapshot_command_rejects_arbitrary_state(state: str) -> None:
    transport = OpenSSHRemoteTransport(host_alias="archive-vps")
    with pytest.raises(RemoteTransportError):
        transport._remote_command("catalog-snapshot", "a" * 64, state)


def test_manifest_sha_is_content_identity_not_snapshot_identity() -> None:
    first = _manifest()
    second = replace(first, snapshot_id=str(uuid.uuid4()))
    assert first.snapshot_id != second.snapshot_id
    assert first.sha256 == second.sha256
    assert hashlib.sha256(first.canonical_bytes()).digest() != hashlib.sha256(
        second.canonical_bytes()
    ).digest()
