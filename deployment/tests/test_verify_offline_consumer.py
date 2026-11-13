from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import SimpleNamespace

MODULE_PATH = (
    Path(__file__).resolve().parents[1] / "verify_offline_consumer.py"
)
SPEC = importlib.util.spec_from_file_location(
    "verify_offline_consumer", MODULE_PATH
)
assert SPEC is not None and SPEC.loader is not None
consumer = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = consumer
SPEC.loader.exec_module(consumer)


class FakeLayout:
    @classmethod
    def from_root(cls, root: Path):
        return SimpleNamespace(catalog=root / "catalog.sqlite")


class FakeCatalog:
    def __init__(self, *args, **kwargs):
        pass

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False


class FakeRegistry:
    def __init__(self, *args, **kwargs):
        pass

    def observe_statuses(self):
        return []


def _envelope(
    market: str,
    receive_ns: int,
    *,
    symbol: str = "BTCUSDT",
    stream: str = "agg_trade",
    valid: bool = True,
):
    return SimpleNamespace(
        market=market,
        symbol=symbol,
        stream=stream,
        receive_time_utc_ns=receive_ns,
        raw_payload=b'{"data": {}}',
        capture_flags=(),
        valid=valid,
    )


def _run(monkeypatch, records):
    monkeypatch.setattr(consumer, "StorageLayout", FakeLayout)
    monkeypatch.setattr(consumer, "Catalog", FakeCatalog)
    monkeypatch.setattr(consumer, "StorageRegistry", FakeRegistry)
    monkeypatch.setattr(consumer, "volume_adapter", lambda: None)
    monkeypatch.setattr(
        consumer,
        "load_source_chunks",
        lambda **kwargs: [SimpleNamespace(manifest={"created_at_utc_ns": 0})],
    )
    monkeypatch.setattr(
        consumer,
        "iter_source_records",
        lambda chunk: iter([SimpleNamespace(envelope=item) for item in records]),
    )
    monkeypatch.setattr(
        consumer,
        "parse_capture_envelope",
        lambda envelope: [SimpleNamespace(valid=envelope.valid)],
    )
    monkeypatch.setattr(
        consumer,
        "aggregate",
        lambda trades, *args: [{"capture_gap": False}]
        if list(trades)
        else [],
    )
    return consumer.run_check(
        root=Path("/fake"),
        market="spot",
        symbol="BTCUSDT",
        start_ms=1000,
        end_ms=2000,
        interval_seconds=60,
    )


def test_empty_data_fails(monkeypatch) -> None:
    result = _run(monkeypatch, [])
    assert result["status"] == "FAIL"
    assert consumer.exit_code(result) == 1


def test_wrong_market_fails(monkeypatch) -> None:
    result = _run(
        monkeypatch,
        [_envelope("um_perpetual", 1_500_000_000)],
    )
    assert result["wrong_market_records"] == 1
    assert result["status"] == "FAIL"


def test_out_of_window_record_is_not_counted(monkeypatch) -> None:
    result = _run(
        monkeypatch,
        [_envelope("spot", 500_000_000)],
    )
    assert result["records"] == 0
    assert result["status"] == "FAIL"


def test_normal_window_passes(monkeypatch) -> None:
    result = _run(
        monkeypatch,
        [_envelope("spot", 1_500_000_000)],
    )
    assert result["records"] == 1
    assert result["valid"] == 1
    assert result["agg_trades"] == 1
    assert result["status"] == "PASS"
    assert consumer.exit_code(result) == 0


def test_main_returns_nonzero_for_failed_result(monkeypatch) -> None:
    monkeypatch.setattr(consumer, "run_check", lambda **kwargs: {"status": "FAIL"})
    assert (
        consumer.main(
            [
                "/fake",
                "--market",
                "spot",
                "--start-ms",
                "1000",
                "--end-ms",
                "2000",
            ]
        )
        == 1
    )


def test_um_shared_records_do_not_fail_a_spot_window(monkeypatch) -> None:
    result = _run(
        monkeypatch,
        [
            _envelope("spot", 1_500_000_000),
            _envelope("um_shared", 1_500_000_000),
        ],
    )
    assert result["wrong_market_records"] == 0
    assert result["status"] == "PASS"
