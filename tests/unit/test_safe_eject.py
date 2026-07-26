from __future__ import annotations

from dataclasses import replace
from pathlib import Path
from typing import Literal

import pytest

from binance_market_data_recorder.archive import ArchiveError, ArchiveManager
from binance_market_data_recorder.storage.catalog import Catalog, CatalogStateError
from binance_market_data_recorder.storage.macos import (
    EjectError,
    PlatformEjectResult,
    SafeEjectCoordinator,
    StorageRegistry,
    VolumeInfo,
)
from tests.archive_support import prepare_archive


class FakeEjectPlatform:
    def __init__(self, volume: VolumeInfo, result: PlatformEjectResult) -> None:
        self.volumes = [volume]
        self.result = result
        self.calls = 0
        self.disappear_after_request = False

    def inventory(self) -> list[VolumeInfo]:
        return list(self.volumes)

    def request_eject(
        self, volume: VolumeInfo, *, timeout_seconds: float = 30.0
    ) -> PlatformEjectResult:
        assert volume == self.volumes[0]
        assert timeout_seconds > 0
        self.calls += 1
        if self.disappear_after_request:
            self.volumes = []
        return self.result


def _volume(prepared_root: Path, volume_uuid: str) -> VolumeInfo:
    return VolumeInfo(
        disk_id="disk9s1",
        volume_uuid=volume_uuid,
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=prepared_root / "external-volume",
        writable=True,
        internal=False,
        removable=True,
        total_bytes=100 * 1024**3,
        free_bytes=90 * 1024**3,
        observed_at_utc_ns=1,
    )


def _platform_result(
    *,
    unmounted: bool,
    ejected: bool,
    failed_stage: Literal["unmount", "eject", "timeout"] | None = None,
) -> PlatformEjectResult:
    return PlatformEjectResult(
        disk_id="disk9s1",
        unmounted=unmounted,
        ejected=ejected,
        failed_stage=failed_stage,
        dissenter_status=None if failed_stage is None else 49153,
        dissenter_message=None if failed_stage is None else "busy",
    )


def test_idle_eject_blocks_allocation_until_reinsertion(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    volume = _volume(tmp_path, prepared.target.volume_uuid)
    platform = FakeEjectPlatform(
        volume, _platform_result(unmounted=True, ejected=True)
    )
    source = next(prepared.layout.sealed.iterdir())

    with Catalog(prepared.layout.catalog) as catalog:
        result = SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
            prepared.target.storage_id
        )
        assert result.safe_to_remove is True
        assert result.status == "SAFE_TO_REMOVE"
        assert "可以拔出" in result.message
        assert source.is_file()
        assert catalog.storage_control(prepared.target.storage_id)["state"] == (
            "SAFE_TO_REMOVE"
        )
        with pytest.raises(CatalogStateError, match="blocks new archive allocation"):
            ArchiveManager(
                layout=prepared.layout,
                catalog=catalog,
                target=prepared.target,
            ).run_once()

        platform.volumes = []
        status = StorageRegistry(catalog=catalog, volumes=platform).statuses()[0]
        assert status["state"] == "ABSENT"
        control = status["control"]
        assert isinstance(control, dict)
        assert control["state"] == "SAFE_TO_REMOVE"

        platform.volumes = [replace(volume, mountpoint=volume.mountpoint)]
        reinserted = StorageRegistry(catalog=catalog, volumes=platform).statuses()[0]
        assert reinserted["state"] == "READY"
        assert catalog.storage_control(prepared.target.storage_id)["state"] == "ACTIVE"


@pytest.mark.parametrize(
    ("fault_point", "expected_state"),
    [
        ("after_reserve", "COPYING"),
        ("after_copy_catalog_transition", "VERIFYING"),
    ],
)
def test_copy_or_verify_in_progress_refuses_immediate_eject(
    tmp_path: Path, fault_point: str, expected_state: str
) -> None:
    prepared = prepare_archive(tmp_path)
    volume = _volume(tmp_path, prepared.target.volume_uuid)
    platform = FakeEjectPlatform(
        volume, _platform_result(unmounted=True, ejected=True)
    )

    def fail(point: str, _path: Path | None) -> None:
        if point == fault_point:
            raise RuntimeError("simulated active transaction")

    with Catalog(prepared.layout.catalog) as catalog:
        with pytest.raises((ArchiveError, RuntimeError)):
            ArchiveManager(
                layout=prepared.layout,
                catalog=catalog,
                target=prepared.target,
                fault_hook=fail,
            ).run_once()
        result = SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
            prepared.target.storage_id
        )
        assert result.status == "BUSY"
        assert result.safe_to_remove is False
        assert result.active_transactions[0]["state"] == expected_state
        assert platform.calls == 0
        assert catalog.storage_control(prepared.target.storage_id)["state"] == "ACTIVE"
        assert next(prepared.layout.sealed.iterdir()).is_file()


@pytest.mark.parametrize(
    "result",
    [
        _platform_result(
            unmounted=False, ejected=False, failed_stage="unmount"
        ),
        _platform_result(unmounted=True, ejected=False, failed_stage="eject"),
    ],
)
def test_system_refusal_never_claims_safe_and_reopens_allocation(
    tmp_path: Path, result: PlatformEjectResult
) -> None:
    prepared = prepare_archive(tmp_path)
    platform = FakeEjectPlatform(
        _volume(tmp_path, prepared.target.volume_uuid), result
    )
    with Catalog(prepared.layout.catalog) as catalog:
        outcome = SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
            prepared.target.storage_id
        )
        assert outcome.status == "EJECT_REFUSED"
        assert outcome.safe_to_remove is False
        assert catalog.storage_control(prepared.target.storage_id)["state"] == "ACTIVE"
        events = catalog.operational_events(event_type="STORAGE_EJECT_REFUSED")
        assert len(events) == 1
        assert next(prepared.layout.sealed.iterdir()).is_file()


def test_explicit_retry_can_eject_after_confirmed_unmount_refusal(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    mounted = _volume(tmp_path, prepared.target.volume_uuid)
    platform = FakeEjectPlatform(
        mounted,
        _platform_result(unmounted=True, ejected=False, failed_stage="eject"),
    )
    with Catalog(prepared.layout.catalog) as catalog:
        first = SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
            prepared.target.storage_id
        )
        assert first.status == "EJECT_REFUSED"
        platform.volumes = [replace(mounted, mountpoint=None)]
        platform.result = _platform_result(unmounted=True, ejected=True)

        retried = SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
            prepared.target.storage_id
        )

        assert retried.safe_to_remove is True
        assert retried.status == "SAFE_TO_REMOVE"
        assert platform.calls == 2
        assert catalog.storage_control(prepared.target.storage_id)["state"] == (
            "SAFE_TO_REMOVE"
        )


def test_unmounted_volume_without_prior_refusal_evidence_is_rejected(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    volume = replace(
        _volume(tmp_path, prepared.target.volume_uuid),
        mountpoint=None,
    )
    platform = FakeEjectPlatform(
        volume, _platform_result(unmounted=True, ejected=True)
    )
    with Catalog(prepared.layout.catalog) as catalog:
        with pytest.raises(EjectError, match="no confirmed prior unmount"):
            SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
                prepared.target.storage_id
            )
        assert platform.calls == 0


def test_forced_disappearance_is_not_reported_as_safe(tmp_path: Path) -> None:
    prepared = prepare_archive(tmp_path)
    volume = _volume(tmp_path, prepared.target.volume_uuid)
    platform = FakeEjectPlatform(
        volume,
        _platform_result(unmounted=False, ejected=False, failed_stage="timeout"),
    )
    platform.disappear_after_request = True
    with Catalog(prepared.layout.catalog) as catalog:
        outcome = SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
            prepared.target.storage_id
        )
        assert outcome.status == "FORCED_REMOVAL"
        assert outcome.forced_removal is True
        assert outcome.safe_to_remove is False
        assert next(prepared.layout.sealed.iterdir()).is_file()
        assert catalog.storage_control(prepared.target.storage_id)["state"] == "ACTIVE"
        assert len(
            catalog.operational_events(event_type="STORAGE_FORCED_REMOVAL")
        ) == 1
        assert StorageRegistry(catalog=catalog, volumes=platform).statuses()[0][
            "state"
        ] == "ABSENT"
        platform.volumes = [volume]
        assert StorageRegistry(catalog=catalog, volumes=platform).statuses()[0][
            "state"
        ] == "READY"
        reconciled = ArchiveManager(
            layout=prepared.layout,
            catalog=catalog,
            target=prepared.target,
        ).run_once()
        assert reconciled.state == "LOCAL_DELETED"


def test_timeout_keeps_allocation_blocked_until_explicit_retry(
    tmp_path: Path,
) -> None:
    prepared = prepare_archive(tmp_path)
    platform = FakeEjectPlatform(
        _volume(tmp_path, prepared.target.volume_uuid),
        _platform_result(unmounted=False, ejected=False, failed_stage="timeout"),
    )
    with Catalog(prepared.layout.catalog) as catalog:
        outcome = SafeEjectCoordinator(catalog=catalog, platform=platform).eject(
            prepared.target.storage_id
        )
        assert outcome.status == "EJECT_TIMEOUT"
        assert outcome.safe_to_remove is False
        assert catalog.storage_control(prepared.target.storage_id)["state"] == (
            "EJECT_PENDING"
        )
        with pytest.raises(CatalogStateError, match="blocks new archive allocation"):
            ArchiveManager(
                layout=prepared.layout,
                catalog=catalog,
                target=prepared.target,
            ).run_once()
