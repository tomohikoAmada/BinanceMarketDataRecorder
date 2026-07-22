from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast
from uuid import UUID

from binance_market_data_recorder.spool.format import (
    ChunkHeader,
    encode_chunk_header,
    encode_frame,
    scan_chunk,
)
from tests.factories import event

ROOT = Path(__file__).resolve().parents[2]


def test_python_bytes_match_language_neutral_golden_vector(tmp_path: Path) -> None:
    vector = cast(
        dict[str, Any],
        json.loads((ROOT / "tests/golden/raw_chunk_v1.json").read_text()),
    )
    header = ChunkHeader(
        chunk_id=UUID("00112233-4455-6677-8899-aabbccddeeff"),
        created_at_utc_ns=1_700_000_000_000_000_000,
        collector_instance_id="collector-1",
        collector_version="0.1.0+golden",
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
        max_frame_bytes=16 * 1024 * 1024,
    )
    chunk = encode_chunk_header(header) + encode_frame(
        event(7, payload=b'{  "u" : 7 }'), max_frame_bytes=header.max_frame_bytes
    )
    assert chunk.hex() == vector["chunk_hex"]
    path = tmp_path / "golden.bmdr.partial"
    path.write_bytes(chunk)
    result = scan_chunk(path)
    assert result.is_clean
    assert result.statistics.record_count == 1
