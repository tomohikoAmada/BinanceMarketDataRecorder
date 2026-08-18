from __future__ import annotations

import json
import sqlite3
from pathlib import Path
from uuid import UUID

import pytest

from binance_market_data_recorder.archive import (
    ARCHIVE_SET_ENTRY_SCHEMA,
    ARCHIVE_SET_MEDIUM_FILENAME,
    ArchiveMediumIdentity,
    ArchiveSetEntry,
    ArchiveSetError,
    ArchiveSetIndex,
    ArchiveSetStore,
    generate_archive_set_id,
    read_archive_medium_identity,
    rebuild_archive_set_index,
    scan_archive_medium,
)
from binance_market_data_recorder.archive import archive_set as archive_set_module
from binance_market_data_recorder.storage.layout import (
    fsync_directory as actual_fsync_directory,
)

MARKER_NAME = ".binance-market-data-recorder-storage.json"
MARKER_SCHEMA = "registered-storage.v1"
HASH_A = "a" * 64
HASH_B = "b" * 64
HASH_C = "c" * 64


def _medium(
    root: Path, name: str, *, archive_set_id: str, storage_id: str | None = None
) -> tuple[Path, dict[str, str]]:
    folder = root / name
    folder.mkdir(parents=True)
    identity = {
        "storage_id": storage_id or f"storage-{name}",
        "marker_nonce": f"nonce-{name}",
        "volume_uuid": f"volume-{name}",
        "registered_relative_path": name,
    }
    marker = {
        "schema": MARKER_SCHEMA,
        **identity,
        "created_at_utc_ns": 1,
    }
    (folder / MARKER_NAME).write_text(
        json.dumps(marker, sort_keys=True, separators=(",", ":")) + "\n",
        encoding="utf-8",
    )
    return folder, identity


def _bind(
    root: Path, name: str, archive_set_id: str, *, storage_id: str | None = None
) -> tuple[ArchiveSetStore, dict[str, str]]:
    folder, physical = _medium(
        root, name, archive_set_id=archive_set_id, storage_id=storage_id
    )
    store = ArchiveSetStore.bind(
        folder,
        archive_set_id=archive_set_id,
        **physical,
    )
    return store, physical


def _entry(
    store: ArchiveSetStore, chunk_id: str = "chunk-001", **overrides: object
) -> ArchiveSetEntry:
    values: dict[str, object] = {
        "archive_set_id": store.identity.archive_set_id,
        "storage_id": store.identity.storage_id,
        "chunk_id": chunk_id,
        "artifact_relative_path": f"raw/{chunk_id}.bmdr.zst",
        "archive_manifest_relative_path": f"manifests/{chunk_id}.json",
        "archive_manifest_sha256": HASH_A,
        "stored_bytes": 123,
        "stored_sha256": HASH_B,
        "source_manifest_sha256": HASH_C,
    }
    values.update(overrides)
    return ArchiveSetEntry(**values)  # type: ignore[arg-type]


def _tree_snapshot(root: Path) -> dict[str, tuple[str, bytes]]:
    return {
        path.relative_to(root).as_posix(): (
            ("file", path.read_bytes()) if path.is_file() else ("directory", b"")
        )
        for path in sorted(root.rglob("*"))
    }


def _sqlite_paths(path: Path) -> tuple[Path, ...]:
    return (path, Path(f"{path}-journal"), Path(f"{path}-wal"), Path(f"{path}-shm"))


def test_01_generate_valid_archive_set_id() -> None:
    generated = generate_archive_set_id()
    assert str(UUID(generated)) == generated
    assert generated != "storage-1"


def test_02_bind_first_physical_medium(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    assert store.identity_path.is_file()
    assert store.entries_directory.is_dir()


def test_03_bind_second_medium_to_same_set(tmp_path: Path) -> None:
    archive_set_id = generate_archive_set_id()
    first, _ = _bind(tmp_path, "one", archive_set_id)
    second, _ = _bind(tmp_path, "two", archive_set_id)
    assert first.identity.archive_set_id == second.identity.archive_set_id
    assert first.identity.storage_id != second.identity.storage_id


def test_04_medium_canonical_bytes_are_deterministic() -> None:
    identity = ArchiveMediumIdentity("set", "storage", "volume", "folder", "nonce")
    expected = (
        b'{"archive_set_id":"set","marker_nonce":"nonce",'
        b'"registered_relative_path":"folder","schema":"archive-set-medium.v1",'
        b'"storage_id":"storage","volume_uuid":"volume"}\n'
    )
    assert identity.canonical_bytes() == expected


def test_05_reopen_unchanged_medium_identity(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    assert ArchiveSetStore.open(store.root).identity == store.identity
    assert read_archive_medium_identity(store.root) == store.identity


def test_06_rebind_same_storage_to_different_set_fails_closed(tmp_path: Path) -> None:
    store, physical = _bind(tmp_path, "one", generate_archive_set_id())
    with pytest.raises(ArchiveSetError, match="identity conflicts"):
        ArchiveSetStore.bind(
            store.root,
            archive_set_id=generate_archive_set_id(),
            **physical,
        )


def test_07_physical_marker_mismatch_fails_closed(tmp_path: Path) -> None:
    folder, physical = _medium(tmp_path, "one", archive_set_id="set")
    physical["volume_uuid"] = "different-volume"
    with pytest.raises(ArchiveSetError, match="physical identity unavailable"):
        ArchiveSetStore.bind(folder, archive_set_id="set", **physical)
    assert not (folder / ARCHIVE_SET_MEDIUM_FILENAME).exists()


def test_08_unsupported_medium_schema_fails_closed(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    path = store.identity_path
    document = json.loads(path.read_text(encoding="utf-8"))
    document["schema"] = "archive-set-medium.v999"
    path.write_text(json.dumps(document), encoding="utf-8")
    with pytest.raises(ArchiveSetError, match="unsupported"):
        ArchiveSetStore.open(store.root)


def test_09_corrupt_medium_json_fails_closed(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.identity_path.write_text("{not json", encoding="utf-8")
    with pytest.raises(ArchiveSetError, match="invalid"):
        ArchiveSetStore.open(store.root)


def test_10_commit_one_immutable_entry(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    entry = store.commit_entry(_entry(store))
    assert entry == store.read_entry("chunk-001")
    assert (store.entries_directory / "chunk-001.json").is_file()


def test_11_entry_canonical_bytes_are_deterministic(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    encoded = _entry(store).canonical_bytes()
    assert encoded.endswith(b"\n")
    assert json.loads(encoded) == {
        "archive_manifest_relative_path": "manifests/chunk-001.json",
        "archive_manifest_sha256": HASH_A,
        "archive_set_id": store.identity.archive_set_id,
        "artifact_relative_path": "raw/chunk-001.bmdr.zst",
        "chunk_id": "chunk-001",
        "schema": ARCHIVE_SET_ENTRY_SCHEMA,
        "source_manifest_sha256": HASH_C,
        "storage_id": store.identity.storage_id,
        "stored_bytes": 123,
        "stored_sha256": HASH_B,
    }


def test_12_exact_duplicate_entry_is_idempotent(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    entry = _entry(store)
    assert store.commit_entry(entry) == store.commit_entry(entry)
    assert len(list(store.entries_directory.iterdir())) == 1


def test_13_conflicting_entry_is_never_overwritten(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.commit_entry(_entry(store))
    with pytest.raises(ArchiveSetError, match="entry conflicts"):
        store.commit_entry(_entry(store, stored_sha256=HASH_C))
    assert (
        json.loads((store.entries_directory / "chunk-001.json").read_text())[
            "stored_sha256"
        ]
        == HASH_B
    )


def test_14_entry_set_mismatch_fails_closed(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    with pytest.raises(ArchiveSetError, match="archive_set_id"):
        store.commit_entry(_entry(store, archive_set_id=generate_archive_set_id()))


def test_15_entry_storage_mismatch_fails_closed(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    with pytest.raises(ArchiveSetError, match="storage_id"):
        store.commit_entry(_entry(store, storage_id="other-storage"))


def test_16_unsafe_entry_chunk_id_fails_closed(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    with pytest.raises(ArchiveSetError, match="safe path segment"):
        store.commit_entry(_entry(store, chunk_id="../escape"))


def test_17_entry_model_has_whole_chunk_identity_only(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    fields = set(_entry(store).__dataclass_fields__)
    assert "chunk_id" in fields
    assert not {"segment", "segments", "stripe", "shard", "replica"} & fields


def test_18_two_media_distinct_chunks_rebuild(tmp_path: Path) -> None:
    archive_set_id = generate_archive_set_id()
    first, _ = _bind(tmp_path, "one", archive_set_id)
    second, _ = _bind(tmp_path, "two", archive_set_id)
    first.commit_entry(_entry(first, chunk_id="one"))
    second.commit_entry(_entry(second, chunk_id="two"))
    index = ArchiveSetIndex(tmp_path / "workspace" / "index.sqlite")
    assert index.rebuild([first.root, second.root]) == {
        "archive_sets": 1,
        "media": 2,
        "artifacts": 2,
    }
    assert {
        row["chunk_id"] for row in index.artifacts(archive_set_id=archive_set_id)
    } == {"one", "two"}


def test_19_deleted_index_rebuilds_from_media(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.commit_entry(_entry(store))
    index_path = tmp_path / "index.sqlite"
    index = ArchiveSetIndex(index_path)
    index.rebuild([store.root])
    index_path.unlink()
    rebuilt = ArchiveSetIndex(index_path)
    rebuilt.rebuild([store.root])
    assert rebuilt.artifacts(chunk_id="chunk-001")[0]["storage_id"] == store.identity.storage_id


def test_20_cross_media_same_chunk_different_storage_fails_closed(tmp_path: Path) -> None:
    archive_set_id = generate_archive_set_id()
    first, _ = _bind(tmp_path, "one", archive_set_id)
    second, _ = _bind(tmp_path, "two", archive_set_id)
    first.commit_entry(_entry(first))
    second.commit_entry(_entry(second))
    with pytest.raises(ArchiveSetError, match="collision"):
        ArchiveSetIndex(tmp_path / "index.sqlite").rebuild([first.root, second.root])


def test_21_cross_media_hash_path_collision_fails_closed(tmp_path: Path) -> None:
    archive_set_id = generate_archive_set_id()
    first, _ = _bind(tmp_path, "one", archive_set_id)
    second, _ = _bind(tmp_path, "two", archive_set_id)
    first.commit_entry(_entry(first))
    second.commit_entry(
        _entry(
            second,
            storage_id=second.identity.storage_id,
            artifact_relative_path="raw/other.bmdr.zst",
        )
    )
    with pytest.raises(ArchiveSetError, match="collision"):
        ArchiveSetIndex(tmp_path / "index.sqlite").rebuild([first.root, second.root])


def test_22_one_detached_medium_remains_independently_readable(tmp_path: Path) -> None:
    archive_set_id = generate_archive_set_id()
    first, _ = _bind(tmp_path, "one", archive_set_id)
    second, _ = _bind(tmp_path, "two", archive_set_id)
    first.commit_entry(_entry(first, chunk_id="one"))
    second.commit_entry(_entry(second, chunk_id="two"))
    assert [item.chunk_id for item in scan_archive_medium(second.root).entries] == ["two"]


def test_23_rebuild_does_not_require_all_set_media(tmp_path: Path) -> None:
    archive_set_id = generate_archive_set_id()
    first, _ = _bind(tmp_path, "one", archive_set_id)
    second, _ = _bind(tmp_path, "two", archive_set_id)
    first.commit_entry(_entry(first, chunk_id="one"))
    second.commit_entry(_entry(second, chunk_id="two"))
    index = ArchiveSetIndex(tmp_path / "index.sqlite")
    assert index.rebuild([second.root])["artifacts"] == 1
    assert index.artifacts()[0]["chunk_id"] == "two"


def test_24_scan_is_read_only(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.commit_entry(_entry(store))
    before = {
        path.relative_to(store.root): path.read_bytes() if path.is_file() else None
        for path in store.root.rglob("*")
    }
    scan_archive_medium(store.root)
    after = {
        path.relative_to(store.root): path.read_bytes() if path.is_file() else None
        for path in store.root.rglob("*")
    }
    assert after == before


def test_25_malformed_entry_fails_closed(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    (store.entries_directory / "bad.json").write_text("not json", encoding="utf-8")
    with pytest.raises(ArchiveSetError, match="invalid"):
        store.scan()


def test_26_failed_rebuild_does_not_replace_existing_index(tmp_path: Path) -> None:
    archive_set_id = generate_archive_set_id()
    store, _ = _bind(tmp_path, "one", archive_set_id)
    store.commit_entry(_entry(store))
    index = ArchiveSetIndex(tmp_path / "index.sqlite")
    index.rebuild([store.root])
    conflicting, _ = _bind(tmp_path, "two", archive_set_id)
    conflicting.commit_entry(_entry(conflicting, stored_sha256=HASH_C))
    with pytest.raises(ArchiveSetError):
        index.rebuild([store.root, conflicting.root])
    assert index.artifacts(chunk_id="chunk-001")[0]["stored_sha256"] == HASH_B


def test_27_registered_marker_is_byte_identical_after_bind(tmp_path: Path) -> None:
    folder, physical = _medium(tmp_path, "one", archive_set_id="set")
    marker = folder / MARKER_NAME
    before = marker.read_bytes()
    ArchiveSetStore.bind(folder, archive_set_id="set", **physical)
    assert marker.read_bytes() == before


def test_28_existing_archive_manifest_is_not_rewritten(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    manifest = store.root / "manifests" / "chunk-001.json"
    manifest.parent.mkdir()
    manifest.write_text('{"schema":"external-archive-manifest.v1"}\n', encoding="utf-8")
    before = manifest.read_bytes()
    store.commit_entry(_entry(store))
    assert manifest.read_bytes() == before


def test_29_index_queries_by_set_storage_and_chunk(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.commit_entry(_entry(store))
    index = ArchiveSetIndex(tmp_path / "index.sqlite")
    index.rebuild([store.root])
    assert len(index.artifacts(archive_set_id=store.identity.archive_set_id)) == 1
    assert len(index.artifacts(storage_id=store.identity.storage_id)) == 1
    assert len(index.artifacts(chunk_id="chunk-001")) == 1


def test_30_index_has_no_live_catalog_tables_or_mountpoints(tmp_path: Path) -> None:
    index = ArchiveSetIndex(tmp_path / "index.sqlite")
    assert not index.path.exists()
    assert index.archive_sets() == []
    with sqlite3.connect(index.path) as connection:
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
    assert tables == {"archive_sets", "archive_media", "archive_artifacts"}
    assert "mountpoint" not in {
        column[1]
        for column in connection.execute("PRAGMA table_info(archive_media)")
    }


def test_31_medium_conflict_in_publication_window_never_clobbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder, physical = _medium(tmp_path, "one", archive_set_id="loser-set")
    winner = ArchiveMediumIdentity(
        archive_set_id="winner-set",
        storage_id=physical["storage_id"],
        volume_uuid=physical["volume_uuid"],
        registered_relative_path=physical["registered_relative_path"],
        marker_nonce=physical["marker_nonce"],
    )
    winner_bytes = winner.canonical_bytes()
    original = archive_set_module._publish_no_clobber

    def occupy_then_publish(source: Path, destination: Path) -> bool:
        destination.write_bytes(winner_bytes)
        return original(source, destination)

    monkeypatch.setattr(
        archive_set_module, "_publish_no_clobber", occupy_then_publish
    )
    with pytest.raises(ArchiveSetError, match="identity conflicts"):
        ArchiveSetStore.bind(folder, archive_set_id="loser-set", **physical)

    identity_path = folder / ARCHIVE_SET_MEDIUM_FILENAME
    assert identity_path.read_bytes() == winner_bytes
    assert read_archive_medium_identity(folder) == winner
    assert list(folder.glob(f".{ARCHIVE_SET_MEDIUM_FILENAME}.*.partial")) == []


def test_32_entry_conflict_in_publication_window_never_clobbers(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    loser = _entry(store)
    winner = _entry(store, stored_sha256=HASH_C)
    winner_bytes = winner.canonical_bytes()
    original = archive_set_module._publish_no_clobber

    def occupy_then_publish(source: Path, destination: Path) -> bool:
        destination.write_bytes(winner_bytes)
        return original(source, destination)

    monkeypatch.setattr(
        archive_set_module, "_publish_no_clobber", occupy_then_publish
    )
    with pytest.raises(ArchiveSetError, match="entry conflicts"):
        store.commit_entry(loser)

    final = store.entries_directory / "chunk-001.json"
    assert final.read_bytes() == winner_bytes
    assert store.read_entry("chunk-001") == winner
    assert list(store.entries_directory.glob(".chunk-001.json.*.partial")) == []


def test_33_same_medium_identity_in_publication_window_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder, physical = _medium(tmp_path, "one", archive_set_id="same-set")
    expected = ArchiveMediumIdentity(archive_set_id="same-set", **physical)
    original = archive_set_module._publish_no_clobber

    def occupy_then_publish(source: Path, destination: Path) -> bool:
        destination.write_bytes(expected.canonical_bytes())
        return original(source, destination)

    monkeypatch.setattr(
        archive_set_module, "_publish_no_clobber", occupy_then_publish
    )
    store = ArchiveSetStore.bind(folder, archive_set_id="same-set", **physical)

    assert store.identity == expected
    assert store.identity_path.read_bytes() == expected.canonical_bytes()
    assert list(folder.glob(f".{ARCHIVE_SET_MEDIUM_FILENAME}.*.partial")) == []


def test_34_same_entry_in_publication_window_is_idempotent(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    entry = _entry(store)
    original = archive_set_module._publish_no_clobber
    fsynced: list[Path] = []

    def occupy_then_publish(source: Path, destination: Path) -> bool:
        destination.write_bytes(entry.canonical_bytes())
        return original(source, destination)

    def record_fsync(path: Path) -> None:
        fsynced.append(path)
        actual_fsync_directory(path)

    monkeypatch.setattr(
        archive_set_module, "_publish_no_clobber", occupy_then_publish
    )
    monkeypatch.setattr(archive_set_module, "fsync_directory", record_fsync)
    assert store.commit_entry(entry) == entry
    assert fsynced[-1] == store.entries_directory
    assert list(store.entries_directory.glob(".chunk-001.json.*.partial")) == []


def test_35_successful_publication_fsyncs_parent_directory(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    folder, physical = _medium(tmp_path, "one", archive_set_id="set")
    fsynced: list[Path] = []
    monkeypatch.setattr(archive_set_module, "fsync_directory", fsynced.append)

    ArchiveSetStore.bind(folder, archive_set_id="set", **physical)

    assert fsynced[0] == folder.resolve()


@pytest.mark.parametrize(
    ("field", "alias"),
    [
        ("artifact_relative_path", "raw//x"),
        ("artifact_relative_path", "raw/./x"),
        ("artifact_relative_path", "raw/x/"),
        ("artifact_relative_path", "./raw/x"),
        ("archive_manifest_relative_path", "manifests//x"),
        ("archive_manifest_relative_path", "manifests/./x"),
        ("archive_manifest_relative_path", "manifests/x/"),
        ("archive_manifest_relative_path", "./manifests/x"),
    ],
)
def test_36_entry_paths_must_already_be_canonical(
    tmp_path: Path, field: str, alias: str
) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    with pytest.raises(ArchiveSetError, match="safe relative path"):
        store.commit_entry(_entry(store, **{field: alias}))


@pytest.mark.parametrize(
    ("field", "unsafe"),
    [
        ("artifact_relative_path", "../escape"),
        ("artifact_relative_path", "/raw/x"),
        ("artifact_relative_path", "raw\\x"),
        ("archive_manifest_relative_path", "../escape"),
        ("archive_manifest_relative_path", "/manifests/x"),
        ("archive_manifest_relative_path", "manifests\\x"),
    ],
)
def test_37_existing_unsafe_entry_paths_remain_rejected(
    tmp_path: Path, field: str, unsafe: str
) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    with pytest.raises(ArchiveSetError, match="safe relative path"):
        store.commit_entry(_entry(store, **{field: unsafe}))


def test_38_canonical_entry_and_registered_paths_remain_valid(tmp_path: Path) -> None:
    folder, physical = _medium(tmp_path, "registered/folder", archive_set_id="set")
    store = ArchiveSetStore.bind(folder, archive_set_id="set", **physical)
    entry = _entry(
        store,
        artifact_relative_path="raw/x",
        archive_manifest_relative_path="manifests/x",
    )
    assert store.commit_entry(entry) == entry
    assert store.identity.registered_relative_path == "registered/folder"


@pytest.mark.parametrize(
    "relative_index",
    [
        "index.sqlite",
        "workspace/index.sqlite",
        "archive-set/index.sqlite",
        "archive-set/entries/index.sqlite",
    ],
)
def test_39_index_inside_medium_is_rejected_before_any_write(
    tmp_path: Path, relative_index: str
) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.commit_entry(_entry(store))
    before = _tree_snapshot(store.root)
    index_path = store.root / relative_index
    index = ArchiveSetIndex(index_path)
    assert _tree_snapshot(store.root) == before

    with pytest.raises(ArchiveSetError, match="inside archive media"):
        index.rebuild([store.root])

    assert _tree_snapshot(store.root) == before
    assert all(not path.exists() for path in _sqlite_paths(index_path))
    assert scan_archive_medium(store.root).entries == (store.read_entry("chunk-001"),)


def test_40_wrapper_rejects_index_inside_medium_before_any_write(tmp_path: Path) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.commit_entry(_entry(store))
    before = _tree_snapshot(store.root)
    index_path = store.entries_directory / "index.sqlite"

    with pytest.raises(ArchiveSetError, match="inside archive media"):
        rebuild_archive_set_index(index_path, (root for root in [store.root]))

    assert _tree_snapshot(store.root) == before
    assert all(not path.exists() for path in _sqlite_paths(index_path))
    assert scan_archive_medium(store.root).entries == (store.read_entry("chunk-001"),)


def test_41_symlink_alias_index_inside_medium_is_rejected_before_write(
    tmp_path: Path,
) -> None:
    store, _ = _bind(tmp_path, "one", generate_archive_set_id())
    store.commit_entry(_entry(store))
    alias = tmp_path / "medium-alias"
    alias.symlink_to(store.root, target_is_directory=True)
    index_path = alias / "workspace" / "index.sqlite"
    before = _tree_snapshot(store.root)

    with pytest.raises(ArchiveSetError, match="inside archive media"):
        ArchiveSetIndex(index_path).rebuild([store.root])

    assert _tree_snapshot(store.root) == before
    assert all(not path.exists() for path in _sqlite_paths(index_path))
