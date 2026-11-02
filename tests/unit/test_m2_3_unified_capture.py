from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest

from binance_market_data_recorder.binance.public_websocket import ReceivedFrame
from binance_market_data_recorder.collector.coverage import SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.factories import event
from tests.unit.test_ms2_configurable_products import make_groups
from tests.unit.test_p2_coverage import Response


@pytest.mark.parametrize("kind", ["open_interest_statistics", "global_long_short_ratio"])
@pytest.mark.parametrize("empty", [False, True])
def test_explicit_statistics_resume_cursor_after_durable_response(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    empty: bool,
) -> None:
    from binance_market_data_recorder.binance.usdm.side_data_rest import RestSideDataKind
    from tests.unit.test_usdm_side_data_collector import _cursor_model

    now = 1_800_000_000_000
    saved = now - 4 * 300_000
    stream = kind + "_5m"
    expected_start = saved if kind == "open_interest_statistics" else saved + 300_000
    layout = ensure_storage_layout(tmp_path)
    monkeypatch.setattr(
        "binance_market_data_recorder.collector.coverage.time.time_ns", lambda: now * 1_000_000
    )
    requests = []
    synced = False

    class Client:
        def request(self, source: Any, params: Any) -> Response:
            requests.append(params)
            rows = (
                []
                if empty
                else [
                    _cursor_model(RestSideDataKind(stream), t)
                    for t in range(expected_start, now, 300_000)
                ]
            )
            for row in rows:
                row["symbol"] = "ETHUSDT"
            return Response(rows)

    async def exercise(catalog: Catalog) -> None:
        nonlocal synced
        catalog.advance_side_data_cursor(
            kind=stream,
            symbol="ETHUSDT",
            last_persisted_period_timestamp=saved,
            updated_at_utc_ns=1,
            source_retention_window="latest_30_days",
            retention_window_ms=30 * 86400_000,
        )
        config = RecorderConfig(data_root=tmp_path, usdm_symbols=("ETHUSDT",))
        owner = SelectedAssetCapture(
            config,
            logging.getLogger("m2.3.cursor"),
            "v",
            "i",
            layout,
            catalog,
            clients={"usdm": Client()},
        )
        state = owner.sources["usdm", "ETHUSDT", stream, "rest"]
        spool = owner.spool("um_perpetual", "ETHUSDT", "rest_observations")
        original_sync = spool.sync

        def sync() -> None:
            nonlocal synced
            original_sync()
            synced = True

        monkeypatch.setattr(spool, "sync", sync)
        await owner.poll_once(state)
        assert requests[0]["startTime"] == expected_start
        assert requests[0]["endTime"] == now - 1
        cursor = catalog.side_data_cursor(stream, "ETHUSDT")
        assert cursor is not None
        accepted = not empty or kind == "open_interest_statistics"
        assert cursor["last_persisted_period_timestamp"] == (now - 300_000 if accepted else saved)
        assert synced
        assert (state.error is None) == accepted
        assert catalog.side_data_cursor(stream, "SOLUSDT") is None
        spool.close_and_seal()
        if accepted:
            restarted = SelectedAssetCapture(
                config, owner.logger, "v", "next", layout, catalog, clients={}
            )
            assert restarted.sources[state.source.key].next_period_ms == (
                now - 300_000 if kind == "open_interest_statistics" else now
            )

    with Catalog(layout.catalog) as catalog:
        asyncio.run(exercise(catalog))


def test_common_factory_errors_are_not_retried_and_receipt_flags_survive(
    tmp_path: Path,
) -> None:
    async def exercise(catalog: Catalog) -> None:
        owner = SelectedAssetCapture(
            RecorderConfig(data_root=tmp_path, spot_symbols=("ETHUSDT",)),
            logging.getLogger("m2.3.factory"),
            "v",
            "i",
            layout,
            catalog,
            clients={},
        )
        make_groups(owner)
        stream = next(iter(owner.collectors.values())).groups[0]
        calls = 0

        def broken(**kwargs: Any) -> Any:
            nonlocal calls
            calls += 1
            raise TypeError("callback implementation error")

        stream.envelope_factory = broken
        receipt = ReceivedFrame(b"exact bytes", "c", 1, 2, ("sequence_gap",))
        with pytest.raises(TypeError, match="callback implementation error"):
            await stream._persist_batch([receipt])
        assert calls == 1
        stream.envelope_factory = lambda **kwargs: event().model_copy(
            update={
                "symbol": "ETHUSDT",
                "stream": stream.stream_name,
            }
        )
        persisted = await stream._persist_batch([receipt])
        assert "sequence_gap" in persisted[0].capture_flags
        stream.spool.close_and_seal()

    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        asyncio.run(exercise(catalog))


def test_initial_empty_statistics_window_is_retried_after_restart(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = 1_800_000_000_000
    layout = ensure_storage_layout(tmp_path)
    monkeypatch.setattr(
        "binance_market_data_recorder.collector.coverage.time.time_ns", lambda: now * 1_000_000
    )

    class EmptyClient:
        def request(self, source: Any, params: Any) -> Response:
            return Response([])

    async def exercise(catalog: Catalog) -> None:
        nonlocal now
        config = RecorderConfig(data_root=tmp_path, usdm_symbols=("ETHUSDT",))
        owner = SelectedAssetCapture(
            config,
            logging.getLogger("m2.3.empty"),
            "v",
            "i",
            layout,
            catalog,
            clients={"usdm": EmptyClient()},
        )
        key = "usdm", "ETHUSDT", "global_long_short_ratio_5m", "rest"
        state = owner.sources[key]
        state.next_period_ms = now - 300_000
        await owner.poll_once(state)
        attempted = state.next_period_ms
        assert attempted == now - 300_000
        assert catalog.side_data_cursor(state.source.stream, "ETHUSDT") is None
        for spool in owner.spools.values():
            spool.close_and_seal()
        now += 300_000
        assert owner._parameters(state.source)["startTime"] == attempted
        restarted = SelectedAssetCapture(
            config, owner.logger, "v", "next", layout, catalog, clients={}
        )
        assert restarted._parameters(restarted.sources[key].source)["startTime"] == attempted

    with Catalog(layout.catalog) as catalog:
        asyncio.run(exercise(catalog))
