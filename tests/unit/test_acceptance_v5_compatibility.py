from __future__ import annotations

from pathlib import Path
from typing import cast

import pytest

from binance_market_data_recorder.service.acceptance import AcceptanceError
from tests.unit.test_m22_9_acceptance import _v4_observer, _verify_v4_sample_chain


def test_historical_v4_627602869_second_gap_remains_rejected(tmp_path: Path) -> None:
    observer, clock, _manager, _evaluator = _v4_observer(tmp_path)
    observer.start()
    clock.boot += 627_602_869_270
    clock.utc += 627_602_869_270
    _path, _sha, sample = observer.sample()
    assert sample["result"] == "INCOMPLETE"
    assert "acceptance_observation_gap" in cast(list[str], sample["blocking_findings"])
    with pytest.raises(AcceptanceError, match="excessive gap"):
        _verify_v4_sample_chain(observer)
