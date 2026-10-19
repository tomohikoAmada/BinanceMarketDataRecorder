"""Unpaced heterogeneous 20-product storage fixture, entirely local/offline.

This measures the actual Raw/Seal/Archive/Normalize/Replay paths on the current
host. It does not model Binance arrival rates or certify the target VPS. The
caller selects an existing disposable parent; a fresh child is retained.
"""

from __future__ import annotations

import argparse
import json
import platform
import resource
import tempfile
import time
from collections import Counter
from pathlib import Path
from typing import Any

from binance_market_data_recorder.archive import ArchiveManager, archive_drain
from binance_market_data_recorder.domain.event import EventEnvelope, Market
from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.replay import GapPolicy, ManifestCatalog, ReplayQuery
from binance_market_data_recorder.spool.seal import validate_sealed_artifact
from binance_market_data_recorder.spool.stream import StreamSpool
from binance_market_data_recorder.spool.writer import RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from binance_market_data_recorder.storage.macos import StorageRegistry, VolumeInfo

SYMBOLS = (
    "BTCUSDT",
    "ETHUSDT",
    "BNBUSDT",
    "SOLUSDT",
    "XRPUSDT",
    "DOGEUSDT",
    "ADAUSDT",
    "AVAXUSDT",
    "LINKUSDT",
    "LTCUSDT",
)
MARKETS: tuple[Market, ...] = ("spot", "um_perpetual")
STREAMS = ("diff_depth", "agg_trade", "book_ticker")


class FixtureVolumes:
    def __init__(self, volume: VolumeInfo | None) -> None:
        self.volume = volume

    def inventory(self) -> list[VolumeInfo]:
        return [] if self.volume is None else [self.volume]


def fixture_event(
    market: Market, symbol: str, stream: str, ordinal: int, levels: int
) -> EventEnvelope:
    timestamp = 1792368000000 + ordinal
    payload: dict[str, object] = {
        "e": "depthUpdate",
        "s": symbol,
        "E": timestamp,
        "T": timestamp - 1,
    }
    sequence: dict[str, int | str]
    if stream == "diff_depth":
        payload.update(
            U=ordinal,
            u=ordinal,
            pu=ordinal - 1,
            b=[[str(1000 - n), f"{ordinal % 19 + 1}.0123"] for n in range(levels)],
            a=[[str(1001 + n), f"{ordinal % 23 + 1}.0456"] for n in range(levels)],
        )
        sequence = {"U": ordinal, "u": ordinal}
        if market == "um_perpetual":
            sequence["pu"] = ordinal - 1
    elif stream == "agg_trade":
        payload.update(
            e="aggTrade",
            a=ordinal,
            f=ordinal,
            l=ordinal,
            p="1000.01",
            q=f"{ordinal % 13 + 1}.001",
            m=False,
            M=True,
        )
        sequence = {"a": ordinal, "f": ordinal, "l": ordinal}
    else:
        payload.update(e="bookTicker", u=ordinal, b="1000.01", B="2.001", a="1000.02", A="3.002")
        sequence = {"u": ordinal}
    return EventEnvelope(
        market=market,
        symbol=symbol,
        stream=stream,
        module=f"binance.{'spot' if market == 'spot' else 'usdm'}.websocket.v1",
        connection_id=f"r3-{market}-{symbol}-{stream}",
        collector_instance_id=f"r3-{market}-{symbol}",
        collector_version="r3-offline",
        receive_time_utc_ns=timestamp * 1_000_000 + 123,
        receive_monotonic_ns=ordinal * 1_000_000,
        exchange_event_time=timestamp,
        exchange_transaction_time=timestamp - 1 if market == "um_perpetual" else None,
        exchange_trade_time=timestamp - 1 if stream == "agg_trade" else None,
        source_sequence=sequence,
        raw_payload=json.dumps(payload, separators=(",", ":")).encode(),
        capture_flags=("reconnect_gap",) if symbol == "LTCUSDT" and ordinal == 1 else (),
    )


def tree_bytes(root: Path) -> int:
    return sum(p.stat().st_size for p in root.rglob("*") if p.is_file())


def run(parent: Path, *, rounds: int, base_frames: int) -> dict[str, Any]:
    root = Path(tempfile.mkdtemp(prefix="r3-storage-", dir=parent))
    layout = ensure_storage_layout(root / "writer")
    archive_root = root / "fixture-volume" / "archive"
    archive_root.mkdir(parents=True)
    volume = VolumeInfo(
        disk_id="fixture",
        volume_uuid="AAAAAAAA-BBBB-CCCC-DDDD-EEEEEEEEEEEE",
        name="R3 fixture",
        filesystem_type="apfs",
        mountpoint=archive_root.parent,
        writable=True,
        internal=False,
        removable=True,
        total_bytes=100 * 1024**3,
        free_bytes=90 * 1024**3,
        observed_at_utc_ns=1,
    )
    volumes = FixtureVolumes(volume)
    report: dict[str, Any] = {
        "kind": "R3_LOCAL_SYNTHETIC_NOT_VPS_QUALIFICATION",
        "root": str(root),
        "platform": platform.platform(),
        "python": platform.python_version(),
        "symbols": list(SYMBOLS),
        "products": 20,
        "core_streams": 60,
        "rounds": rounds,
        "base_frames": base_frames,
        "rotation": "default phased time/size rotation plus round close; no elapsed-60s claim",
        "durability_interval_seconds": 1.0,
        "round_results": [],
    }
    expected: Counter[tuple[str, str, str]] = Counter()
    max_active = 0
    with Catalog(layout.catalog) as catalog:
        registration = StorageRegistry(catalog=catalog, volumes=volumes).register(archive_root)
        storage_id = str(registration["storage_id"])
        for round_index in range(rounds):
            wall, cpu = time.perf_counter(), time.process_time()
            chunks_before = len(catalog.chunks_in_states(ChunkState.SEALED))
            spools = []
            for market in MARKETS:
                for index, symbol in enumerate(SYMBOLS):
                    tier = (
                        4
                        if symbol == "ETHUSDT"
                        else 3
                        if symbol == "BTCUSDT"
                        else (2 if index < 6 else 1)
                    )
                    for stream_index, stream in enumerate(STREAMS):
                        spool = StreamSpool(
                            layout=layout,
                            catalog=catalog,
                            market=market,
                            symbol=symbol,
                            stream=stream,
                            collector_instance_id=f"r3-{market}-{symbol}",
                            collector_version="r3-offline",
                            queue_capacity=16,
                            rotation=RotationPolicy(),
                            durability_interval_seconds=1.0,
                            max_frame_bytes=16 * 1024**2,
                        )
                        count = base_frames * tier * (stream_index + 1)
                        for frame in range(count):
                            ordinal = round_index * 1_000_000 + frame + 1
                            spool.enqueue(
                                fixture_event(market, symbol, stream, ordinal, 5 + index * 4)
                            )
                            spool.drain_one()
                        expected[market, symbol, stream] += count
                        spools.append(spool)
            active = tree_bytes(layout.active)
            max_active = max(max_active, active)
            append_wall = time.perf_counter() - wall
            seal_start = time.perf_counter()
            sealed_results = [spool.close_and_seal() for spool in spools]
            # A final admitted frame can already have triggered natural
            # rotation, leaving close_and_seal() with no active writer.
            chunks_after = len(catalog.chunks_in_states(ChunkState.SEALED))
            assert chunks_after - chunks_before >= 60
            report["round_results"].append(
                {
                    "append_wall_seconds": append_wall,
                    "new_chunks": chunks_after - chunks_before,
                    "chunks_closed_at_round_end": sum(item is not None for item in sealed_results),
                    "seal_wall_seconds": time.perf_counter() - seal_start,
                    "cpu_seconds": time.process_time() - cpu,
                    "active_raw_bytes_before_seal": active,
                    "sealed_backlog": catalog.source_lifecycle_aggregate(),
                    "catalog_bytes": layout.catalog.stat().st_size,
                    "wal_bytes": Path(str(layout.catalog) + "-wal").stat().st_size,
                }
            )
        manifests = [json.loads(p.read_text()) for p in layout.manifests.glob("*.json")]
        assert len(manifests) >= 60 * rounds
        report["frames"] = sum(expected.values())
        report["stored_bytes"] = sum(m["stored_bytes"] for m in manifests)
        report["decoded_bytes"] = sum(m["uncompressed_bytes"] for m in manifests)
        report["peak_active_raw_bytes"] = max_active
        report["retained_internal_manifest_bytes"] = tree_bytes(layout.manifests)
        absent = archive_drain(
            layout=layout,
            catalog=catalog,
            storage_id=storage_id,
            max_runtime_seconds=50,
            max_files=10000,
            volumes=FixtureVolumes(None),
        )
        assert absent["processed_files"] == 0
        assert len(catalog.chunks_in_states(ChunkState.SEALED)) == len(manifests)
        report["absent_target"] = absent
        cpu, wall = time.process_time(), time.perf_counter()
        drained = archive_drain(
            layout=layout,
            catalog=catalog,
            storage_id=storage_id,
            max_runtime_seconds=50,
            max_files=len(manifests),
            volumes=volumes,
        )
        archive_wall = time.perf_counter() - wall
        assert drained["successful_transactions"] == len(manifests), drained
        assert drained["backlog_files_after"] == 0
        assert not list(layout.sealed.glob("*"))
        report["archive"] = {
            **drained,
            "wall_seconds": archive_wall,
            "cpu_seconds": time.process_time() - cpu,
            "bytes_per_work_second": report["stored_bytes"] / archive_wall,
            "chunks_per_work_second": len(manifests) / archive_wall,
        }
        # This independent read verifies the source-retired archive now consumed
        # by Normalize, including flags/counts/identity; it is not a copy hash.
        for row in catalog.archive_transactions(storage_id=storage_id):
            manifest = json.loads(
                (layout.root / str(row["source_manifest_relative_path"])).read_text()
            )
            validate_sealed_artifact(archive_root / str(row["target_relative_path"]), manifest)
        manager_row = catalog.storage_targets()[0]
        from binance_market_data_recorder.archive import ArchiveTarget

        verified = ArchiveManager(
            layout=layout,
            catalog=catalog,
            target=ArchiveTarget(
                storage_id=storage_id,
                volume_uuid=volume.volume_uuid,
                registered_relative_path=str(manager_row["relative_path"]),
                marker_nonce=str(manager_row["marker_nonce"]),
                root=archive_root,
            ),
        ).verify_all()
        assert verified["status"] == "VERIFIED", verified
        assert verified["verified_files"] == len(manifests)
        assert verified["failed_files"] == verified["pending_files"] == 0
        report["archive_verification"] = verified
        report["archive_target_total_bytes"] = tree_bytes(archive_root)
        report["archive_target_non_raw_bytes"] = tree_bytes(archive_root) - report["stored_bytes"]
        report["per_identity_frames"] = [
            {"market": market, "symbol": symbol, "stream": stream, "frames": count}
            for (market, symbol, stream), count in sorted(expected.items())
        ]
        report["writer_bytes_after_archive_before_derived"] = tree_bytes(layout.root)
        catalog._connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        report["catalog_after_checkpoint_bytes"] = layout.catalog.stat().st_size
        report["wal_after_checkpoint_bytes"] = Path(str(layout.catalog) + "-wal").stat().st_size
        wall = time.perf_counter()
        normalized = Normalizer(
            layout=layout, catalog=catalog, external_roots={storage_id: archive_root}
        ).run()
        report["normalization"] = {
            **normalized.public_dict(),
            "wall_seconds": time.perf_counter() - wall,
        }
        assert normalized.normalized_rows == sum(expected.values())
        assert normalized.build_id is not None
    dataset = ManifestCatalog(layout.root).open_build(normalized.build_id)
    observed: Counter[tuple[str, str, str]] = Counter()
    unreliable = 0
    for symbol in SYMBOLS:
        for event in dataset.replay(ReplayQuery(symbol=symbol, gap_policy=GapPolicy.INCLUDE)):
            observed[str(event.row["market"]), symbol, str(event.row["stream"])] += 1
            unreliable += event.is_unreliable
    assert observed == expected
    assert unreliable > 0
    report["replay"] = {
        "frames": sum(observed.values()),
        "identities": len(observed),
        "unreliable_rows": unreliable,
        "exact_counts": True,
    }
    report["peak_rss_bytes"] = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss * (
        1 if platform.system() == "Darwin" else 1024
    )
    report["complete"] = True
    (root / "result.json").write_text(json.dumps(report, indent=2))
    return report


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--parent", type=Path, required=True)
    parser.add_argument("--rounds", type=int, default=3)
    parser.add_argument("--base-frames", type=int, default=40)
    args = parser.parse_args()
    if not 1 <= args.rounds <= 10 or not 1 <= args.base_frames <= 1000:
        parser.error("rounds must be 1..10, base-frames 1..1000")
    report = run(args.parent.resolve(strict=True), rounds=args.rounds, base_frames=args.base_frames)
    print(
        json.dumps(
            {"root": report["root"], "frames": report["frames"], "complete": report["complete"]}
        )
    )


if __name__ == "__main__":
    main()
