from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from binance_market_data_recorder.archive import ArchiveTarget
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout, ensure_storage_layout
from binance_market_data_recorder.storage.macos import StorageRegistry, VolumeInfo
from tests.factories import event


class FixedVolumes:
    def __init__(self, volume: VolumeInfo) -> None:
        self.volume = volume

    def inventory(self) -> list[VolumeInfo]:
        return [self.volume]


@dataclass(frozen=True)
class PreparedArchive:
    layout: StorageLayout
    target: ArchiveTarget
    chunk_ids: tuple[str, ...]


def prepare_archive(
    root: Path,
    *,
    chunk_count: int = 1,
    payload_bytes: int = 0,
) -> PreparedArchive:
    layout = ensure_storage_layout(root / "internal")
    mountpoint = root / "external-volume"
    target_root = mountpoint / "QuantData" / "BinanceRecorder"
    target_root.mkdir(parents=True)
    volume = VolumeInfo(
        disk_id="disk9s1",
        volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        name="Test Archive",
        filesystem_type="apfs",
        mountpoint=mountpoint,
        writable=True,
        internal=False,
        removable=True,
        total_bytes=100 * 1024**3,
        free_bytes=90 * 1024**3,
        observed_at_utc_ns=1,
    )
    chunk_ids: list[str] = []
    with Catalog(layout.catalog) as catalog:
        registration = StorageRegistry(
            catalog=catalog, volumes=FixedVolumes(volume)
        ).register(target_root)
        target_row = catalog.storage_targets()[0]
        for ordinal in range(chunk_count):
            writer = RawChunkWriter(
                layout=layout,
                catalog=catalog,
                market="spot",
                symbol="BTCUSDT",
                stream="diff_depth",
                collector_instance_id="archive-fixture",
                collector_version="0.1.0+test",
                durability_interval_seconds=0,
                created_at_utc_ns=1_700_000_000_000_000_000 + ordinal,
            )
            payload = b"x" * payload_bytes if payload_bytes else None
            writer.append(event(ordinal + 1, payload=payload))
            writer.close()
            manifest = seal_partial(writer.path, layout=layout, catalog=catalog)
            chunk_ids.append(str(manifest["chunk_id"]))
    target = ArchiveTarget(
        storage_id=str(registration["storage_id"]),
        volume_uuid=volume.volume_uuid,
        registered_relative_path=str(target_row["relative_path"]),
        marker_nonce=str(target_row["marker_nonce"]),
        root=target_root,
    )
    return PreparedArchive(layout, target, tuple(chunk_ids))
