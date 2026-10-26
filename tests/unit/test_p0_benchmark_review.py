from __future__ import annotations

import asyncio
import hashlib
import importlib.util
import json
import struct
from pathlib import Path
from types import ModuleType, SimpleNamespace
from typing import Any

import pytest

from binance_market_data_recorder.normalize.raw import load_source_chunks
from binance_market_data_recorder.spool.seal import seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from docs.milestones.P0.benchmark_checks import (
    WorkloadProgress,
    classify_steady,
    verify_physical_records,
)
from tests.factories import event

ROOT = Path(__file__).resolve().parents[2]
LABEL = "spot/BTCUSDT/diff_depth"


def expected_payloads(ordinals: list[int]) -> dict[str, Any]:
    digest = hashlib.sha256()
    for ordinal in ordinals:
        payload = event(ordinal).raw_payload
        digest.update(struct.pack("!I", len(payload)))
        digest.update(payload)
    return {LABEL: {"frames": len(ordinals), "sha256": digest.hexdigest()}}


def write_chunk(layout: Any, catalog: Catalog, ordinals: list[int], created: int) -> None:
    writer = RawChunkWriter(
        layout=layout,
        catalog=catalog,
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        collector_instance_id="collector-1",
        collector_version="test",
        created_at_utc_ns=created,
    )
    for ordinal in ordinals:
        writer.append(event(ordinal))
    writer.close()
    seal_partial(writer.path, layout=layout, catalog=catalog)


def test_physical_two_one_three_fails_despite_valid_crc_and_hashes(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        write_chunk(layout, catalog, [2, 1, 3], 10)
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        with pytest.raises(ValueError, match="physical receive order inversion"):
            verify_physical_records(chunks, layout.catalog, expected_payloads([1, 2, 3]))


def test_cross_chunk_sequence_uses_lifecycle_not_hash_or_creation_wall_clock(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        # A backwards wall clock must not change append chronology.
        write_chunk(layout, catalog, [1, 2], 200)
        write_chunk(layout, catalog, [3], 100)
        chunks = load_source_chunks(layout=layout, catalog=catalog)
        result, _, _, proof = verify_physical_records(
            list(reversed(chunks)), layout.catalog, expected_payloads([1, 2, 3])
        )
        assert result == expected_payloads([1, 2, 3])
        assert [row["physical_records"] for row in proof] == [2, 1]
        assert proof[0]["sealed_transition_id"] < proof[1]["active_transition_id"]


def test_cross_chunk_physical_inversion_fails(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        write_chunk(layout, catalog, [2], 100)
        write_chunk(layout, catalog, [1, 3], 200)
        with pytest.raises(ValueError, match="physical receive order inversion"):
            verify_physical_records(
                load_source_chunks(layout=layout, catalog=catalog),
                layout.catalog,
                expected_payloads([1, 2, 3]),
            )


def test_tail_catchup_does_not_make_slow_window_steady() -> None:
    async def exercise() -> None:
        progress = WorkloadProgress(
            {("spot", "BTCUSDT", "diff_depth"): [b"x"] * 3}, {"diff_depth": 1.0}
        )
        release = asyncio.Event()
        queue: asyncio.Queue[int] = asyncio.Queue()

        async def slow_writer() -> None:
            await release.wait()
            for _ in range(3):
                await queue.get()
                progress.written("writer", LABEL, 1)
                queue.task_done()
            progress.synced("writer", progress.sync_target("writer"))

        task = asyncio.create_task(slow_writer())
        points = [progress.snapshot(0)]
        for index in range(3):
            progress.record("sent", LABEL, 1, float(index))
            progress.record("received", LABEL, 1)
            progress.record("enqueued", LABEL, 1)
            await queue.put(index)
            points.append(progress.snapshot(float(index + 1)))
        window = classify_steady(points, 3)
        assert window["status"] == "NON_STEADY"
        assert not window["performance_comparable"]
        assert points[-1]["streams"][LABEL]["pending_write_payload_bytes"] == 3
        release.set()
        await task
        final = progress.snapshot(4)["streams"][LABEL]
        assert final["persisted_frames"] == final["total_input_frames"] == 3
        assert final["pending_write_frames"] == 0
        assert classify_steady(points, 3) == window

    asyncio.run(exercise())


def test_input_delivered_after_deadline_is_nonsteady_even_if_final_counts_match() -> None:
    p = WorkloadProgress({("spot", "BTCUSDT", "diff_depth"): [b"x"]}, {"diff_depth": 1.0})
    for phase in ("sent", "received", "enqueued"):
        p.record(phase, LABEL, 1, 2.0)
    p.written("writer", LABEL, 1)
    p.synced("writer", p.sync_target("writer"))
    assert not classify_steady([p.snapshot(3)], 1)["performance_comparable"]


def test_written_is_not_persisted_until_successful_fsync_ack() -> None:
    p = WorkloadProgress({("spot", "BTCUSDT", "diff_depth"): [b"x"]}, {"diff_depth": 1.0})
    for phase in ("sent", "received", "enqueued"):
        p.record(phase, LABEL, 1)
    p.written("writer", LABEL, 1)
    row = p.snapshot(0.5)["streams"][LABEL]
    assert row["written_frames"] == 1 and row["persisted_frames"] == 0
    p.synced("writer", p.sync_target("writer"))
    assert p.snapshot(0.5)["streams"][LABEL]["persisted_frames"] == 1
    assert classify_steady([p.snapshot(0.5)] * 3, 1)["performance_comparable"]


def profile_module() -> ModuleType:
    spec = importlib.util.spec_from_file_location(
        "p0_profile_test", ROOT / "docs/milestones/P0/profile_steady.py"
    )
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize(
    "conflict",
    [
        "profile-steady-n2.json",
        "profile-steady-n2.stdout",
        "profile-steady-n2.stderr",
        "profile-steady-n2.folded",
        "profile-steady-n2.execution.json",
        "profile-steady-sampler.stdout",
        "profile-steady-sampler.stderr",
    ],
)
def test_existing_output_refused_before_open_or_process(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, conflict: str
) -> None:
    module = profile_module()
    old = tmp_path / conflict
    old.write_bytes(b"ORIGINAL_EVIDENCE")
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **k: pytest.fail("process launched"))
    args = SimpleNamespace(root=tmp_path, profiler=Path("/unused"), commit="test", wait_suite=False)
    with pytest.raises(FileExistsError):
        module.run_profile(args)
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == {conflict: b"ORIGINAL_EVIDENCE"}


def test_all_old_profile_outputs_stay_byte_identical(tmp_path: Path) -> None:
    module = profile_module()
    before = {name: ("ORIGINAL_" + name).encode() for name in module.OUTPUTS}
    for name, data in before.items():
        (tmp_path / name).write_bytes(data)
    with pytest.raises(FileExistsError):
        module.run_profile(
            SimpleNamespace(
                root=tmp_path, profiler=Path("/unused"), commit="test", wait_suite=False
            )
        )
    assert {p.name: p.read_bytes() for p in tmp_path.iterdir()} == before


def test_start_failure_preserves_failed_execution_receipt(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    module = profile_module()
    child = SimpleNamespace(pid=123, returncode=1, poll=lambda: 1)
    monkeypatch.setattr(module.subprocess, "Popen", lambda *a, **k: child)
    with pytest.raises(RuntimeError, match="start was not observed"):
        module.run_profile(
            SimpleNamespace(
                root=tmp_path, profiler=Path("/unused"), commit="test", wait_suite=False
            )
        )
    receipt = json.loads((tmp_path / "profile-steady-n2.execution.json").read_text())
    assert receipt["status"] == "FAILED" and receipt["exit_code"] == 1
    assert receipt["error_type"] == "RuntimeError"
    assert receipt["profiler_argv"] is None


def test_payload_reorder_rejected_even_if_timestamps_were_rewritten(tmp_path: Path) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market="spot",
            symbol="BTCUSDT",
            stream="diff_depth",
            collector_instance_id="collector-1",
            collector_version="test",
        )
        for physical, original in enumerate([2, 1, 3], 1):
            writer.append(
                event(original).model_copy(
                    update={"receive_monotonic_ns": 5_000_000_000 + physical}
                )
            )
        writer.close()
        seal_partial(writer.path, layout=layout, catalog=catalog)
        with pytest.raises(ValueError, match="physical Raw payload/order/count"):
            verify_physical_records(
                load_source_chunks(layout=layout, catalog=catalog),
                layout.catalog,
                expected_payloads([1, 2, 3]),
            )


def test_model_ingress_accounted_without_claiming_wire_http_time() -> None:
    p = WorkloadProgress({("spot", "BTCUSDT", "diff_depth"): [b"x"]}, {"diff_depth": 1.0})
    key = "spot/BTCUSDT/exchange_info"
    p.register_model_source(key)
    for phase in ("sent", "received", "enqueued"):
        p.record(phase, key, 100)
    row = p.snapshot(0.5)["streams"][key]
    assert row["protocol_kind"] == "model_at_raw_ingress"
    assert row["total_input_frames"] is None
    assert row["pending_write_payload_bytes"] == row["spool_owned_payload_bytes"] == 100
    p.written("metadata-writer", key, 100)
    p.synced("metadata-writer", p.sync_target("metadata-writer"))
    assert p.snapshot(0.5)["streams"][key]["persisted_payload_bytes"] == 100


def test_post_write_consumer_backlog_is_not_hidden_by_fsync() -> None:
    p = WorkloadProgress({("spot", "BTCUSDT", "diff_depth"): [b"x"]}, {"diff_depth": 1.0})
    points = []
    for size in (1, 2, 3):
        point = p.snapshot(float(size))
        point["streams"][LABEL]["receipt_owned_payload_bytes"] = size
        points.append(point)
    result = classify_steady(points, 3)
    assert "sustained_backlog_growth:receipt_owned_payload_bytes" in result["reasons"]
