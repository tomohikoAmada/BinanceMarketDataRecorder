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
    StorageRegistrationError,
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


def test_linux_nested_findmnt_excludes_hotplug_root_device(
    tmp_path: Path,
) -> None:
    mountpoint = tmp_path / "external"
    mountpoint.mkdir()
    adapter = LinuxVolumeAdapter(
        mountinfo_text=(
            "31 1 179:2 / / rw - ext4 /dev/mmcblk0p2 rw\n"
            f"42 31 8:17 / {mountpoint} rw - ext4 /dev/sdb1 rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/mmcblk0p2",
                        "target": "/",
                        "fstype": "ext4",
                        "options": "rw",
                        "children": [
                            {
                                "source": "/dev/sdb1",
                                "target": str(mountpoint),
                                "fstype": "ext4",
                                "options": "rw",
                            }
                        ],
                    }
                ]
            }
        ),
        lsblk_json=json.dumps(
            {
                "blockdevices": [
                    {
                        "name": "mmcblk0",
                        "path": "/dev/mmcblk0",
                        "pkname": None,
                        "rm": False,
                        "hotplug": True,
                        "children": [
                            {
                                "name": "mmcblk0p1",
                                "path": "/dev/mmcblk0p1",
                                "pkname": "mmcblk0",
                                "uuid": "BOOT",
                                "rm": False,
                                "hotplug": True,
                            },
                            {
                                "name": "mmcblk0p2",
                                "path": "/dev/mmcblk0p2",
                                "pkname": "mmcblk0",
                                "uuid": "ROOT",
                                "rm": False,
                                "hotplug": True,
                            },
                        ],
                    },
                    {
                        "name": "sdb",
                        "path": "/dev/sdb",
                        "pkname": None,
                        "rm": False,
                        "hotplug": True,
                        "children": [
                            {
                                "name": "sdb1",
                                "path": "/dev/sdb1",
                                "pkname": "sdb",
                                "uuid": "0123-abcd",
                                "fstype": "ext4",
                                "rm": False,
                                "hotplug": False,
                            }
                        ],
                    },
                ]
            }
        ),
    )

    volumes = adapter.inventory()

    assert [volume.disk_id for volume in volumes] == ["/dev/sdb1"]


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


def test_root_device_excluded_even_when_hotplug_true(
    tmp_path: Path,
) -> None:
    mountpoint = tmp_path / "external"
    mountpoint.mkdir()
    adapter = LinuxVolumeAdapter(
        mountinfo_text=(
            "31 1 179:2 / / rw - ext4 /dev/sda1 rw\n"
            f"42 31 8:17 / {mountpoint} rw - ext4 /dev/sdb1 rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/sda1",
                        "target": "/",
                        "fstype": "ext4",
                        "options": "rw",
                    },
                    {
                        "source": "/dev/sdb1",
                        "target": str(mountpoint),
                        "fstype": "ext4",
                        "options": "rw",
                    },
                ]
            }
        ),
        lsblk_json=json.dumps(
            {
                "blockdevices": [
                    {
                        "name": "sda",
                        "path": "/dev/sda",
                        "pkname": None,
                        "rm": False,
                        "hotplug": True,
                        "children": [
                            {
                                "name": "sda1",
                                "path": "/dev/sda1",
                                "pkname": "sda",
                                "uuid": "ROOT-UUID",
                                "rm": False,
                                "hotplug": True,
                            }
                        ],
                    },
                    {
                        "name": "sdb",
                        "path": "/dev/sdb",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "children": [
                            {
                                "name": "sdb1",
                                "path": "/dev/sdb1",
                                "pkname": "sdb",
                                "uuid": "DATA-UUID",
                                "fstype": "ext4",
                                "rm": False,
                                "hotplug": False,
                            }
                        ],
                    },
                ]
            }
        ),
    )
    volumes = adapter.inventory()
    assert [volume.disk_id for volume in volumes] == ["/dev/sdb1"]


def test_usb_standard_device_included(tmp_path: Path) -> None:
    mountpoint = tmp_path / "usb"
    mountpoint.mkdir()
    adapter = LinuxVolumeAdapter(
        mountinfo_text=(
            "31 1 179:2 / / rw - ext4 /dev/sda1 rw\n"
            f"42 31 8:1 / {mountpoint} rw - ext4 /dev/sdb1 rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/sda1",
                        "target": "/",
                        "fstype": "ext4",
                        "options": "rw",
                    },
                    {
                        "source": "/dev/sdb1",
                        "target": str(mountpoint),
                        "fstype": "ext4",
                        "options": "rw",
                    },
                ]
            }
        ),
        lsblk_json=json.dumps(
            {
                "blockdevices": [
                    {
                        "name": "sda",
                        "path": "/dev/sda",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "children": [
                            {
                                "name": "sda1",
                                "path": "/dev/sda1",
                                "pkname": "sda",
                                "uuid": "ROOT",
                                "rm": False,
                                "hotplug": False,
                            }
                        ],
                    },
                    {
                        "name": "sdb",
                        "path": "/dev/sdb",
                        "pkname": None,
                        "rm": True,
                        "hotplug": True,
                        "children": [
                            {
                                "name": "sdb1",
                                "path": "/dev/sdb1",
                                "pkname": "sdb",
                                "uuid": "USB-UUID",
                                "fstype": "ext4",
                                "rm": True,
                                "hotplug": True,
                            }
                        ],
                    },
                ]
            }
        ),
    )
    volumes = adapter.inventory()
    assert len(volumes) == 1
    assert volumes[0].disk_id == "/dev/sdb1"
    assert volumes[0].volume_uuid == "USB-UUID"


def test_bridged_disk_without_rm_hotplug_included(
    tmp_path: Path,
) -> None:
    mountpoint = tmp_path / "bridge-disk"
    mountpoint.mkdir()
    adapter = LinuxVolumeAdapter(
        mountinfo_text=(
            "31 1 179:2 / / rw - ext4 /dev/sda1 rw\n"
            f"42 31 8:33 / {mountpoint} rw - ext4 /dev/sdc1 rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/sda1",
                        "target": "/",
                        "fstype": "ext4",
                        "options": "rw",
                    },
                    {
                        "source": "/dev/sdc1",
                        "target": str(mountpoint),
                        "fstype": "ext4",
                        "options": "rw",
                    },
                ]
            }
        ),
        lsblk_json=json.dumps(
            {
                "blockdevices": [
                    {
                        "name": "sda",
                        "path": "/dev/sda",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "children": [
                            {
                                "name": "sda1",
                                "path": "/dev/sda1",
                                "pkname": "sda",
                                "uuid": "ROOT",
                                "rm": False,
                                "hotplug": False,
                            }
                        ],
                    },
                    {
                        "name": "sdc",
                        "path": "/dev/sdc",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "tran": "usb",
                        "children": [
                            {
                                "name": "sdc1",
                                "path": "/dev/sdc1",
                                "pkname": "sdc",
                                "uuid": "BRIDGE-UUID",
                                "fstype": "ext4",
                                "rm": False,
                                "hotplug": False,
                                "tran": "usb",
                            }
                        ],
                    },
                ]
            }
        ),
    )
    volumes = adapter.inventory()
    assert len(volumes) == 1
    assert volumes[0].disk_id == "/dev/sdc1"
    assert volumes[0].volume_uuid == "BRIDGE-UUID"


def test_root_sibling_partition_excluded(tmp_path: Path) -> None:
    mountpoint = tmp_path / "shared-disk"
    mountpoint.mkdir()
    adapter = LinuxVolumeAdapter(
        mountinfo_text=(
            "31 1 179:2 / / rw - ext4 /dev/sda1 rw\n"
            f"42 31 179:3 / {mountpoint} rw - ext4 /dev/sda2 rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/sda1",
                        "target": "/",
                        "fstype": "ext4",
                        "options": "rw",
                    },
                    {
                        "source": "/dev/sda2",
                        "target": str(mountpoint),
                        "fstype": "ext4",
                        "options": "rw",
                    },
                ]
            }
        ),
        lsblk_json=json.dumps(
            {
                "blockdevices": [
                    {
                        "name": "sda",
                        "path": "/dev/sda",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "children": [
                            {
                                "name": "sda1",
                                "path": "/dev/sda1",
                                "pkname": "sda",
                                "uuid": "ROOT",
                                "rm": False,
                                "hotplug": False,
                            },
                            {
                                "name": "sda2",
                                "path": "/dev/sda2",
                                "pkname": "sda",
                                "uuid": "SIBLING",
                                "fstype": "ext4",
                                "rm": False,
                                "hotplug": False,
                            },
                        ],
                    },
                ]
            }
        ),
    )
    volumes = adapter.inventory()
    assert len(volumes) == 0


def test_pseudo_filesystems_excluded(tmp_path: Path) -> None:
    adapter = LinuxVolumeAdapter(
        mountinfo_text=(
            "31 1 179:2 / / rw - ext4 /dev/sda1 rw\n"
            "50 31 0:5 / /proc rw - proc proc rw\n"
            "51 31 0:6 / /sys rw - sysfs sysfs rw\n"
            "52 31 0:7 / /dev rw - devtmpfs udev rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/sda1",
                        "target": "/",
                        "fstype": "ext4",
                        "options": "rw",
                    },
                    {
                        "source": "proc",
                        "target": "/proc",
                        "fstype": "proc",
                        "options": "rw",
                    },
                    {
                        "source": "sysfs",
                        "target": "/sys",
                        "fstype": "sysfs",
                        "options": "rw",
                    },
                    {
                        "source": "udev",
                        "target": "/dev",
                        "fstype": "devtmpfs",
                        "options": "rw",
                    },
                ]
            }
        ),
        lsblk_json=json.dumps(
            {
                "blockdevices": [
                    {
                        "name": "sda",
                        "path": "/dev/sda",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "children": [
                            {
                                "name": "sda1",
                                "path": "/dev/sda1",
                                "pkname": "sda",
                                "uuid": "ROOT",
                                "rm": False,
                                "hotplug": False,
                            }
                        ],
                    },
                ]
            }
        ),
    )
    volumes = adapter.inventory()
    assert len(volumes) == 0


def test_no_uuid_device_registration_rejected(
    tmp_path: Path,
) -> None:
    mountpoint = tmp_path / "no-uuid"
    folder = mountpoint / "archive-folder"
    folder.mkdir(parents=True)
    adapter = LinuxVolumeAdapter(
        mountinfo_text=(
            "31 1 179:2 / / rw - ext4 /dev/sda1 rw\n"
            f"42 31 8:17 / {mountpoint} rw - ext4 /dev/sdb1 rw\n"
        ),
        findmnt_json=json.dumps(
            {
                "filesystems": [
                    {
                        "source": "/dev/sda1",
                        "target": "/",
                        "fstype": "ext4",
                        "options": "rw",
                    },
                    {
                        "source": "/dev/sdb1",
                        "target": str(mountpoint),
                        "fstype": "ext4",
                        "options": "rw",
                    },
                ]
            }
        ),
        lsblk_json=json.dumps(
            {
                "blockdevices": [
                    {
                        "name": "sda",
                        "path": "/dev/sda",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "children": [
                            {
                                "name": "sda1",
                                "path": "/dev/sda1",
                                "pkname": "sda",
                                "uuid": "ROOT",
                                "rm": False,
                                "hotplug": False,
                            }
                        ],
                    },
                    {
                        "name": "sdb",
                        "path": "/dev/sdb",
                        "pkname": None,
                        "rm": False,
                        "hotplug": False,
                        "children": [
                            {
                                "name": "sdb1",
                                "path": "/dev/sdb1",
                                "pkname": "sdb",
                                "uuid": None,
                                "fstype": "ext4",
                                "rm": False,
                                "hotplug": False,
                            }
                        ],
                    },
                ]
            }
        ),
    )
    with Catalog(tmp_path / "catalog.sqlite") as catalog:
        registry = StorageRegistry(catalog=catalog, volumes=adapter)
        with pytest.raises(StorageRegistrationError, match="UUID"):
            registry.register(folder)
