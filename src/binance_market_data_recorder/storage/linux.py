"""Read-only Linux discovery for already-mounted external block filesystems."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from .macos.model import PlatformEjectResult, VolumeInfo
from .macos.volumes import PlatformVolumeError

CommandRunner = Callable[[Sequence[str]], subprocess.CompletedProcess[str]]
_MOUNT_ESCAPE = re.compile(r"\\([0-7]{3})")


def _run_command(arguments: Sequence[str]) -> subprocess.CompletedProcess[str]:
    try:
        return subprocess.run(
            list(arguments),
            check=False,
            capture_output=True,
            text=True,
            timeout=30,
        )
    except (OSError, subprocess.SubprocessError) as exc:
        raise PlatformVolumeError(
            f"cannot execute {' '.join(arguments[:2])}: {type(exc).__name__}"
        ) from exc


def _unescape_mount(value: str) -> str:
    return _MOUNT_ESCAPE.sub(lambda match: chr(int(match.group(1), 8)), value)


def parse_mountinfo(document: str) -> list[dict[str, str]]:
    """Parse the kernel's current mount namespace without changing it."""

    mounts: list[dict[str, str]] = []
    for raw_line in document.splitlines():
        line = raw_line.strip()
        if not line:
            continue
        left, separator, right = line.partition(" - ")
        left_fields = left.split()
        right_fields = right.split()
        if not separator or len(left_fields) < 6 or len(right_fields) < 2:
            raise PlatformVolumeError("invalid /proc/self/mountinfo record")
        mounts.append(
            {
                "mountpoint": _unescape_mount(left_fields[4]),
                "options": left_fields[5],
                "filesystem_type": right_fields[0],
                "source": _unescape_mount(right_fields[1]),
            }
        )
    return mounts


def _json_object(document: str, *, source: str) -> dict[str, Any]:
    try:
        value = json.loads(document)
    except json.JSONDecodeError as exc:
        raise PlatformVolumeError(f"invalid {source} JSON") from exc
    if not isinstance(value, dict):
        raise PlatformVolumeError(f"invalid {source} JSON root")
    return value


_PSEUDO_FS_TYPES = frozenset(
    {
        "autofs",
        "bpf",
        "cgroup",
        "cgroup2",
        "configfs",
        "debugfs",
        "devpts",
        "devtmpfs",
        "efivarfs",
        "fusectl",
        "hugetlbfs",
        "mqueue",
        "overlay",
        "proc",
        "pstore",
        "ramfs",
        "securityfs",
        "sysfs",
        "tmpfs",
        "tracefs",
    }
)


def _flatten_lsblk(
    devices: object,
) -> list[dict[str, object]]:
    if not isinstance(devices, list):
        raise PlatformVolumeError("invalid lsblk blockdevices")
    output: list[dict[str, object]] = []
    for item in devices:
        if not isinstance(item, dict):
            raise PlatformVolumeError("invalid lsblk device")
        row = dict(item)
        row["_external"] = True
        output.append(row)
        output.extend(_flatten_lsblk(item.get("children", [])))
    return output


def _flatten_findmnt(filesystems: object) -> list[dict[str, object]]:
    if not isinstance(filesystems, list):
        raise PlatformVolumeError("invalid findmnt filesystems")
    output: list[dict[str, object]] = []
    for item in filesystems:
        if not isinstance(item, dict):
            raise PlatformVolumeError("invalid findmnt filesystem")
        output.append(item)
        output.extend(_flatten_findmnt(item.get("children", [])))
    return output


def _root_backing_names(
    *,
    kernel_by_target: Mapping[str, Mapping[str, str]],
    block_by_path: Mapping[str, Mapping[str, object]],
    block_by_name: Mapping[str, Mapping[str, object]],
) -> frozenset[str]:
    root = kernel_by_target.get("/")
    if root is None:
        return frozenset()
    source_path = root["source"].split("[", 1)[0]
    row = block_by_path.get(source_path)
    names: set[str] = set()
    while row is not None:
        name = row.get("name")
        if not isinstance(name, str) or name in names:
            break
        names.add(name)
        parent = row.get("pkname")
        row = block_by_name.get(parent) if isinstance(parent, str) else None
    return frozenset(names)


def _shares_root_device(
    row: Mapping[str, object],
    *,
    root_names: frozenset[str],
    block_by_name: Mapping[str, Mapping[str, object]],
) -> bool:
    current: Mapping[str, object] | None = row
    visited: set[str] = set()
    while current is not None:
        name = current.get("name")
        if not isinstance(name, str) or name in visited:
            return False
        if name in root_names:
            return True
        visited.add(name)
        parent = current.get("pkname")
        current = block_by_name.get(parent) if isinstance(parent, str) else None
    return False


class LinuxVolumeAdapter:
    """Combine mountinfo, findmnt, and lsblk evidence for external volumes."""

    def __init__(
        self,
        *,
        command_runner: CommandRunner = _run_command,
        mountinfo_text: str | None = None,
        findmnt_json: str | None = None,
        lsblk_json: str | None = None,
        utc_clock_ns: Callable[[], int] = time.time_ns,
    ) -> None:
        self._command_runner = command_runner
        self._mountinfo_text = mountinfo_text
        self._findmnt_json = findmnt_json
        self._lsblk_json = lsblk_json
        self._utc_clock_ns = utc_clock_ns

    @staticmethod
    def capability() -> dict[str, object]:
        return {
            "available": Path("/proc/self/mountinfo").is_file(),
            "discovery": ["/proc/self/mountinfo", "findmnt --json", "lsblk --json"],
            "automatic_eject": False,
            "filesystem_mutated": False,
        }

    def _command_json(self, arguments: Sequence[str], supplied: str | None) -> str:
        if supplied is not None:
            return supplied
        result = self._command_runner(arguments)
        if result.returncode != 0:
            raise PlatformVolumeError(
                f"{' '.join(arguments[:2])} failed ({result.returncode})"
            )
        return result.stdout

    def inventory(self) -> list[VolumeInfo]:
        mountinfo = (
            self._mountinfo_text
            if self._mountinfo_text is not None
            else Path("/proc/self/mountinfo").read_text(encoding="utf-8")
        )
        kernel_mounts = parse_mountinfo(mountinfo)
        findmnt = _json_object(
            self._command_json(
                (
                    "/usr/bin/findmnt",
                    "--json",
                    "--bytes",
                    "--output",
                    "SOURCE,TARGET,FSTYPE,OPTIONS",
                ),
                self._findmnt_json,
            ),
            source="findmnt",
        )
        lsblk = _json_object(
            self._command_json(
                (
                    "/usr/bin/lsblk",
                    "--json",
                    "--bytes",
                    "--output",
                    "NAME,PATH,PKNAME,UUID,FSTYPE,TYPE,RM,HOTPLUG,MOUNTPOINTS",
                ),
                self._lsblk_json,
            ),
            source="lsblk",
        )
        mount_rows = _flatten_findmnt(findmnt.get("filesystems"))
        kernel_by_target = {
            str(row["mountpoint"]): row for row in kernel_mounts
        }
        block_rows = _flatten_lsblk(lsblk.get("blockdevices"))
        block_by_path: dict[str, Mapping[str, object]] = {}
        block_by_name: dict[str, Mapping[str, object]] = {}
        for row in block_rows:
            path = row.get("path")
            if isinstance(path, str):
                block_by_path[path] = row
            name = row.get("name")
            if isinstance(name, str):
                block_by_name[name] = row
        root_names = _root_backing_names(
            kernel_by_target=kernel_by_target,
            block_by_path=block_by_path,
            block_by_name=block_by_name,
        )

        observations: list[VolumeInfo] = []
        for raw_mount in mount_rows:
            source = raw_mount.get("source")
            target = raw_mount.get("target")
            fstype = str(raw_mount.get("fstype") or "")
            if not isinstance(source, str) or not isinstance(target, str):
                continue
            if fstype in _PSEUDO_FS_TYPES:
                continue
            source_path = source.split("[", 1)[0]
            block = block_by_path.get(source_path)
            if block is None or _shares_root_device(
                block,
                root_names=root_names,
                block_by_name=block_by_name,
            ):
                continue
            kernel = kernel_by_target.get(target)
            if kernel is None or kernel["source"].split("[", 1)[0] != source_path:
                continue
            mountpoint = Path(target).resolve()
            options = str(raw_mount.get("options") or kernel["options"])
            writable = "rw" in options.split(",")
            total_bytes: int | None = None
            free_bytes: int | None = None
            try:
                usage = shutil.disk_usage(mountpoint)
                total_bytes = usage.total
                free_bytes = usage.free
                writable = writable and os.access(mountpoint, os.W_OK)
            except OSError:
                writable = False
            uuid_value = block.get("uuid")
            filesystem_uuid = (
                uuid_value.upper()
                if isinstance(uuid_value, str) and uuid_value
                else None
            )
            observations.append(
                VolumeInfo(
                    disk_id=source_path,
                    volume_uuid=filesystem_uuid or "",
                    name=Path(target).name or None,
                    filesystem_type=str(
                        raw_mount.get("fstype")
                        or block.get("fstype")
                        or kernel["filesystem_type"]
                    ),
                    mountpoint=mountpoint,
                    writable=writable,
                    internal=False,
                    removable=bool(block.get("rm") or block.get("hotplug")),
                    total_bytes=total_bytes,
                    free_bytes=free_bytes,
                    observed_at_utc_ns=self._utc_clock_ns(),
                )
            )
        return sorted(observations, key=lambda volume: volume.disk_id)

    def request_eject(
        self,
        _volume: VolumeInfo,
        *,
        timeout_seconds: float = 30.0,
    ) -> PlatformEjectResult:
        if timeout_seconds <= 0:
            raise ValueError("eject timeout must be positive")
        raise PlatformVolumeError(
            "automatic Linux eject is unavailable; stop archive work and use "
            "trusted OS tooling before reporting safe removal"
        )
