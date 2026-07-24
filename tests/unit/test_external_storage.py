from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.macos import (
    StorageRegistrationError,
    StorageRegistry,
    StorageState,
    VolumeInfo,
    inspect_path,
)
from binance_market_data_recorder.storage.macos.registry import MARKER_NAME, PROBE_PREFIX


class FakeVolumes:
    def __init__(self, *volumes: VolumeInfo) -> None:
        self.volumes = list(volumes)

    def inventory(self) -> list[VolumeInfo]:
        return list(self.volumes)


def volume(mountpoint: Path | None, *, writable: bool = True) -> VolumeInfo:
    return VolumeInfo(
        disk_id="disk9s1",
        volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        name="Archive One",
        filesystem_type="apfs",
        mountpoint=mountpoint,
        writable=writable,
        internal=False,
        removable=True,
        total_bytes=1_000_000,
        free_bytes=750_000,
        observed_at_utc_ns=1,
    )


def tree(root: Path) -> set[str]:
    return {str(path.relative_to(root)) for path in root.rglob("*")}


def outside_snapshot(root: Path, excluded: Path) -> dict[str, bytes | None]:
    snapshot: dict[str, bytes | None] = {}
    for path in root.rglob("*"):
        if path == excluded or excluded in path.parents:
            continue
        snapshot[str(path.relative_to(root))] = path.read_bytes() if path.is_file() else None
    return snapshot


def test_public_storage_state_contract_is_complete() -> None:
    assert {state.value for state in StorageState} == {
        "ABSENT",
        "PRESENT_UNMOUNTED",
        "MOUNTED",
        "UNREGISTERED",
        "PROBING",
        "READY",
        "READ_ONLY",
        "LOW_SPACE",
        "COPYING",
        "VERIFYING",
        "EJECT_PENDING",
        "SAFE_TO_REMOVE",
        "DISAPPEARED_DURING_COPY",
        "DEGRADED",
        "ERROR",
    }


def test_register_probes_and_writes_only_inside_selected_folder(tmp_path: Path) -> None:
    mount = tmp_path / "External"
    folder = mount / "QuantData" / "BinanceRecorder"
    sibling = mount / "unrelated.txt"
    unrelated_directory = mount / "Photos"
    folder.mkdir(parents=True)
    unrelated_directory.mkdir()
    (unrelated_directory / "photo.txt").write_text("untouched", encoding="utf-8")
    sibling.write_text("user data", encoding="utf-8")
    before_outside = outside_snapshot(mount, folder)

    with Catalog(tmp_path / "internal" / "catalog.sqlite") as catalog:
        result = StorageRegistry(catalog=catalog, volumes=FakeVolumes(volume(mount))).register(
            folder
        )
        targets = catalog.storage_targets()

    assert result["state"] == "READY"
    assert result["registered_relative_path"] == "QuantData/BinanceRecorder"
    assert cast(dict[str, Any], result["probe"])["residual_files"] == 0
    assert len(targets) == 1
    assert outside_snapshot(mount, folder) == before_outside
    assert {path.name for path in folder.iterdir()} == {MARKER_NAME}
    assert not any(path.name.startswith(PROBE_PREFIX) for path in folder.iterdir())
    assert not (mount / MARKER_NAME).exists()


def test_volume_root_and_internal_or_read_only_paths_are_rejected(tmp_path: Path) -> None:
    mount = tmp_path / "External"
    folder = mount / "Recorder"
    folder.mkdir(parents=True)
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        root_registry = StorageRegistry(catalog=catalog, volumes=FakeVolumes(volume(mount)))
        with pytest.raises(StorageRegistrationError, match="root"):
            root_registry.register(mount)
        read_only = StorageRegistry(
            catalog=catalog, volumes=FakeVolumes(volume(mount, writable=False))
        )
        with pytest.raises(StorageRegistrationError, match="read-only"):
            read_only.register(folder)
        internal = replace(volume(mount), internal=True)
        with pytest.raises(StorageRegistrationError, match="external"):
            StorageRegistry(catalog=catalog, volumes=FakeVolumes(internal)).register(folder)
    assert list(folder.iterdir()) == []


def test_uuid_resolves_new_name_and_mountpoint_after_reinsert(tmp_path: Path) -> None:
    first_mount = tmp_path / "Old Name"
    folder = first_mount / "QuantData" / "Recorder"
    folder.mkdir(parents=True)
    fake = FakeVolumes(volume(first_mount))
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=fake)
        registered = registry.register(folder)
        second_mount = tmp_path / "Renamed Volume"
        first_mount.rename(second_mount)
        fake.volumes = [replace(volume(second_mount), name="Archive Renamed")]
        status = registry.statuses()[0]

    assert status["storage_id"] == registered["storage_id"]
    assert status["state"] == "READY"
    assert cast(dict[str, Any], status["current_volume"])["volume_name"] == (
        "Archive Renamed"
    )
    assert status["resolved_path"] == str(second_mount / "QuantData" / "Recorder")


def test_absent_unmounted_and_read_only_never_claim_ready(tmp_path: Path) -> None:
    mount = tmp_path / "External"
    folder = mount / "Recorder"
    folder.mkdir(parents=True)
    fake = FakeVolumes(volume(mount))
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=fake)
        registry.register(folder)
        fake.volumes = []
        assert registry.statuses()[0]["state"] == "ABSENT"
        fake.volumes = [replace(volume(None), writable=None)]
        assert registry.statuses()[0]["state"] == "PRESENT_UNMOUNTED"
        fake.volumes = [volume(mount, writable=False)]
        contents = tree(folder)
        assert registry.statuses()[0]["state"] == "READ_ONLY"
        assert tree(folder) == contents


def test_registered_external_target_reports_low_space_severity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    mount = tmp_path / "External"
    folder = mount / "Recorder"
    folder.mkdir(parents=True)
    fake = FakeVolumes(volume(mount))
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=fake)
        registry.register(folder)
        monkeypatch.setattr(
            "binance_market_data_recorder.storage.macos.registry.shutil.disk_usage",
            lambda _path: SimpleNamespace(total=100 * 1024**3, free=14 * 1024**3),
        )
        status = registry.statuses()[0]
    assert status["state"] == "LOW_SPACE"
    assert status["space_severity"] == "CRITICAL"


def test_marker_mismatch_blocks_ready_and_unregister_preserves_marker(tmp_path: Path) -> None:
    mount = tmp_path / "External"
    folder = mount / "Recorder"
    folder.mkdir(parents=True)
    fake = FakeVolumes(volume(mount))
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=fake)
        result = registry.register(folder)
        marker_path = folder / MARKER_NAME
        marker = json.loads(marker_path.read_text(encoding="utf-8"))
        marker["volume_uuid"] = "WRONG"
        marker_path.write_text(json.dumps(marker), encoding="utf-8")
        status = registry.statuses()[0]
        assert status["state"] == "ERROR"
        assert "identity mismatch" in str(status["reason"])
        unregistered = registry.unregister(str(result["storage_id"]))
        assert unregistered["marker_deleted"] is False
        assert marker_path.is_file()
        assert catalog.storage_targets() == []


def test_inspect_is_read_only_and_reports_unregistered_volume(tmp_path: Path) -> None:
    mount = tmp_path / "External"
    folder = mount / "Recorder"
    folder.mkdir(parents=True)
    before = tree(mount)
    result = inspect_path(folder, [volume(mount)])
    assert result["registrable"] is True
    assert cast(dict[str, Any], result["volume"])["state"] == "UNREGISTERED"
    assert result["filesystem_mutated"] is False
    assert tree(mount) == before


def test_replaced_registered_path_symlink_cannot_escape_volume(tmp_path: Path) -> None:
    mount = tmp_path / "External"
    folder = mount / "Recorder"
    outside = tmp_path / "outside"
    folder.mkdir(parents=True)
    outside.mkdir()
    fake = FakeVolumes(volume(mount))
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=fake)
        registry.register(folder)
        moved = mount / "OriginalRecorder"
        folder.rename(moved)
        folder.symlink_to(outside, target_is_directory=True)
        before = tree(outside)
        status = registry.statuses()[0]
    assert status["state"] == "ERROR"
    assert "alias" in str(status["reason"])
    assert tree(outside) == before


def test_registration_rejects_selected_folder_or_marker_symlink(tmp_path: Path) -> None:
    mount = tmp_path / "External"
    real_folder = mount / "RealRecorder"
    alias = mount / "RecorderAlias"
    real_folder.mkdir(parents=True)
    alias.symlink_to(real_folder, target_is_directory=True)
    fake = FakeVolumes(volume(mount))
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=fake)
        with pytest.raises(StorageRegistrationError, match="symbolic link"):
            registry.register(alias)
        inspected = registry.inspect(alias)
        assert inspected["registrable"] is False
        assert inspected["is_symbolic_link"] is True

        marker_target = real_folder / "marker-target.json"
        marker_target.write_text("{}", encoding="utf-8")
        (real_folder / MARKER_NAME).symlink_to(marker_target)
        with pytest.raises(StorageRegistrationError, match=r"marker.*symbolic link"):
            registry.register(real_folder)
