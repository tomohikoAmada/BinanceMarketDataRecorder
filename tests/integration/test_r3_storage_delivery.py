from pathlib import Path

import pytest

from binance_market_data_recorder.spool.writer import RawChunkWriter
from tools.benchmark_r3_storage import run


@pytest.mark.parametrize("rotate_each_frame", [False, True])
def test_twenty_product_storage_archive_retirement_and_replay(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    rotate_each_frame: bool,
) -> None:
    if rotate_each_frame:
        monkeypatch.setattr(RawChunkWriter, "should_rotate", lambda self: True)
    result = run(tmp_path, rounds=1, base_frames=1)
    assert result["complete"] is True
    assert result["frames"] == 228
    assert result["archive"]["successful_transactions"] >= 60
    assert result["absent_target"]["backlog_files_after"] >= 60
    assert result["replay"]["identities"] == 60
    assert result["replay"]["unreliable_rows"] > 0
    assert result["normalization"]["source_chunks"] >= 60
