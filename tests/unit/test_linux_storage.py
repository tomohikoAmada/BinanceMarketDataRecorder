from __future__ import annotations

import json
from pathlib import Path

import pytest

from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.linux import (
    LinuxVolumeAdapter,
    parse_mountinfo,
)
from binance_market_data_recorder.storage.macos import (
    PlatformVolumeError,
    StorageRegistry,
)

FIXTURES = Path(__file__).parents[1] / "fixtures" / "linux"


def _fixture(name: str) -> str:
    return (FIXTURES / name).read_text(encoding="utf-8")


def test_linux_fixture_reports_source_uuid_filesystem_and_mountpoint() -> None:
    adapter = LinuxVolumeAdapter(
        mountinfo_text=_fixture("mountinfo.txt"),
        findmnt_json=_fixture("findmnt.json"),
        lsblk_json=_fixture("lsblk.json"),
        utc_clock_ns=lambda: 123,
    )
    volumes = adapter.inventory()
    assert len(volumes) == 1
    document = volumes[0].public_dict()
    assert document["disk_id"] == "/dev/sdb1"
    assert document["volume_uuid"] == "0123-ABCD"
    assert document["filesystem_type"] == "ext4"
    assert document["mountpoint"] == "/media/orangepi/archive"
    assert document["internal"] is False
    assert document["observed_at_utc_ns"] == 123


def test_linux_registration_uses_only_an_existing_mounted_directory(
    tmp_path: Path,
) -> None:
    mountpoint = tmp_path / "mounted"
    folder = mountpoint / "sealed-archive"
    folder.mkdir(parents=True)
    mountinfo = (
        f"42 31 8:17 / {mountpoint} rw,nosuid,nodev "
        "- ext4 /dev/sdb1 rw\n"
    )
    findmnt = json.dumps(
        {
            "filesystems": [
                {
                    "source": "/dev/sdb1",
                    "target": str(mountpoint),
                    "fstype": "ext4",
                    "options": "rw,nosuid,nodev",
                }
            ]
        }
    )
    adapter = LinuxVolumeAdapter(
        mountinfo_text=mountinfo,
        findmnt_json=findmnt,
        lsblk_json=_fixture("lsblk.json"),
    )
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=adapter)
        registered = registry.register(folder)
        assert registered["state"] in {"READY", "LOW_SPACE"}
        assert registered["volume_uuid"] == "0123-ABCD"
        assert registered["registered_relative_path"] == "sealed-archive"
        assert registry.statuses()[0]["state"] in {"READY", "LOW_SPACE"}


def test_linux_disappearance_is_reported_absent_without_deleting_source(
    tmp_path: Path,
) -> None:
    mountpoint = tmp_path / "mounted"
    folder = mountpoint / "sealed-archive"
    folder.mkdir(parents=True)
    present = LinuxVolumeAdapter(
        mountinfo_text=(
            f"42 31 8:17 / {mountpoint} rw - ext4 /dev/sdb1 rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/sdb1",
                        "target": str(mountpoint),
                        "fstype": "ext4",
                        "options": "rw",
                    }
                ]
            }
        ),
        lsblk_json=_fixture("lsblk.json"),
    )
    absent = LinuxVolumeAdapter(
        mountinfo_text="",
        findmnt_json='{"filesystems":[]}',
        lsblk_json='{"blockdevices":[]}',
    )
    internal_source = tmp_path / "internal-source.raw"
    internal_source.write_bytes(b"immutable-source")
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        StorageRegistry(catalog=catalog, volumes=present).register(folder)
        status = StorageRegistry(catalog=catalog, volumes=absent).statuses()[0]
    assert status["state"] == "ABSENT"
    assert internal_source.read_bytes() == b"immutable-source"


def test_linux_eject_never_claims_success_without_platform_capability() -> None:
    adapter = LinuxVolumeAdapter(
        mountinfo_text="",
        findmnt_json='{"filesystems":[]}',
        lsblk_json='{"blockdevices":[]}',
    )
    with pytest.raises(PlatformVolumeError, match="automatic Linux eject"):
        adapter.request_eject(
            # The adapter rejects before using volume-specific fields.
            object(),  # type: ignore[arg-type]
        )


def test_invalid_mountinfo_fails_closed() -> None:
    with pytest.raises(PlatformVolumeError, match="invalid"):
        parse_mountinfo("malformed")
