from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

import pytest

from binance_market_data_recorder.spool.format import scan_chunk
from binance_market_data_recorder.spool.recovery import recover_partials
from binance_market_data_recorder.storage.catalog import Catalog, ChunkState
from binance_market_data_recorder.storage.layout import ensure_storage_layout

WORKER = r'''
import json
import os
import sys
import time

from binance_market_data_recorder.spool.format import encode_frame
from binance_market_data_recorder.spool.writer import RawChunkWriter
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event

root = __import__('pathlib').Path(sys.argv[1])
marker = __import__('pathlib').Path(sys.argv[2])
mode = sys.argv[3]
layout = ensure_storage_layout(root)
catalog = Catalog(layout.catalog)
writer = RawChunkWriter(
    layout=layout,
    catalog=catalog,
    market='spot',
    symbol='BTCUSDT',
    stream='diff_depth',
    collector_instance_id='kill9-worker',
    collector_version='0.1.0+test',
    durability_interval_seconds=0,
)
if mode in {'complete_frame', 'truncated_frame'}:
    writer.append(event(1))
if mode == 'truncated_frame':
    writer.close()
    frame = encode_frame(event(2), max_frame_bytes=writer.header.max_frame_bytes)
    descriptor = os.open(writer.path, os.O_WRONLY | os.O_APPEND)
    os.write(descriptor, frame[:len(frame) // 2])
    os.fsync(descriptor)
marker.write_text(json.dumps({'chunk_id': str(writer.header.chunk_id), 'path': str(writer.path)}))
while True:
    time.sleep(1)
'''


@pytest.mark.parametrize(
    ("mode", "expected_action", "expected_records", "expected_state"),
    [
        ("header_only", "unchanged", 0, ChunkState.ACTIVE),
        ("complete_frame", "unchanged", 1, ChunkState.ACTIVE),
        ("truncated_frame", "tail_truncated", 1, ChunkState.RECOVERED),
    ],
)
def test_kill9_matrix_recovers_without_false_seal(
    tmp_path: Path,
    mode: str,
    expected_action: str,
    expected_records: int,
    expected_state: ChunkState,
) -> None:
    data_root = tmp_path / "data-root"
    marker = tmp_path / "ready.json"
    process = subprocess.Popen(
        [sys.executable, "-c", WORKER, str(data_root), str(marker), mode],
        cwd=Path(__file__).resolve().parents[2],
    )
    deadline = time.monotonic() + 15
    while not marker.exists() and process.poll() is None and time.monotonic() < deadline:
        time.sleep(0.01)
    assert marker.exists(), f"worker failed with {process.poll()}"

    os.kill(process.pid, signal.SIGKILL)
    assert process.wait(timeout=5) == -signal.SIGKILL
    evidence = json.loads(marker.read_text())

    layout = ensure_storage_layout(data_root)
    assert not list(layout.sealed.iterdir())
    assert not list(layout.manifests.iterdir())
    with Catalog(layout.catalog) as catalog:
        actions = recover_partials(layout=layout, catalog=catalog)
        assert actions[0].action == expected_action
        assert catalog.state(evidence["chunk_id"]) is expected_state
        recovered = scan_chunk(Path(evidence["path"]))
        assert recovered.is_clean
        assert recovered.statistics.record_count == expected_records
        assert not list(layout.sealed.iterdir())
        assert not list(layout.manifests.iterdir())
