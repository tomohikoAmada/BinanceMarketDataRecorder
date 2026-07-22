from __future__ import annotations

import random
from pathlib import Path
from uuid import UUID

from binance_market_data_recorder.spool.format import (
    ChunkHeader,
    ScanIssue,
    encode_chunk_header,
    encode_frame,
    scan_chunk,
)
from tests.factories import event


def _chunk_bytes() -> tuple[bytes, int]:
    header = ChunkHeader(
        chunk_id=UUID("11223344-5566-7788-99aa-bbccddeeff00"),
        created_at_utc_ns=1_700_000_000_000_000_000,
        collector_instance_id="property-test",
        collector_version="0.1.0+test",
        market="spot",
        symbol="BTCUSDT",
        stream="diff_depth",
    )
    header_bytes = encode_chunk_header(header)
    frames = b"".join(
        encode_frame(event(index), max_frame_bytes=header.max_frame_bytes)
        for index in range(20)
    )
    return header_bytes + frames, len(header_bytes)


def test_random_tail_truncations_never_appear_clean(tmp_path: Path) -> None:
    complete, header_end = _chunk_bytes()
    randomizer = random.Random(20260722)
    positions = {randomizer.randrange(1, len(complete)) for _ in range(200)}
    for index, position in enumerate(sorted(positions)):
        path = tmp_path / f"truncated-{index}.partial"
        path.write_bytes(complete[:position])
        result = scan_chunk(path)
        assert not result.is_clean
        if position >= header_end:
            assert result.issue is ScanIssue.TRUNCATED_TAIL
            assert result.valid_end <= position
        else:
            assert result.issue is ScanIssue.INVALID_HEADER


def test_random_frame_bit_flips_are_detected(tmp_path: Path) -> None:
    complete, header_end = _chunk_bytes()
    randomizer = random.Random(10101)
    for index in range(100):
        corrupted = bytearray(complete)
        position = randomizer.randrange(header_end + 12, len(corrupted))
        corrupted[position] ^= 1 << randomizer.randrange(8)
        path = tmp_path / f"corrupt-{index}.partial"
        path.write_bytes(corrupted)
        result = scan_chunk(path)
        assert not result.is_clean
        assert result.issue is not ScanIssue.NONE
