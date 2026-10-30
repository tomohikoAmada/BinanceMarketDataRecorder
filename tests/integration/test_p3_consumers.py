from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, cast

import pytest

from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.normalize import parser as parser_module
from binance_market_data_recorder.normalize.pipeline import _candidate
from binance_market_data_recorder.normalize.raw import SourceRecord, load_source_chunks
from binance_market_data_recorder.replay import GapPolicy, ManifestCatalog, ReplayQuery
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.normalization_support import BASE_NS, envelope, fixture, provenance, seal_events


def test_subrecords_and_malformed_identity_share_one_raw_digest(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    rows = [
        {
            "symbol": "BTCUSDT",
            "fundingTime": ordinal,
            "fundingRate": "0.00010000",
            "markPrice": "42.0",
        }
        for ordinal in (1, 2, 3)
    ]
    observation = envelope(
        market="um_perpetual",
        stream="funding_history",
        ordinal=1,
        module="binance.usdm.rest.v1",
        raw_payload=provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=rows,
            path="/fapi/v1/fundingRate",
            kind="funding_history",
        ),
    )
    with Catalog(layout.catalog) as catalog:
        seal_events(layout=layout, catalog=catalog, events=[observation])
        chunk = load_source_chunks(layout=layout, catalog=catalog)[0]
        original = hashlib.sha256
        payload_calls = 0

        def counted(data: Any = b"", *args: Any, **kwargs: Any) -> Any:
            nonlocal payload_calls
            if data == observation.raw_payload:
                payload_calls += 1
            return original(data, *args, **kwargs)

        monkeypatch.setattr(hashlib, "sha256", counted)
        record = SourceRecord(chunk, 0, observation)
        parsed = parser_module.parse_envelope(observation, payload_sha256=record.payload_sha256)
        assert len(parsed) == 3
        candidates = [_candidate(record, item) for item in parsed]
        assert payload_calls == 1
        assert all(
            cast(dict[str, object], c["row"])["raw_payload_sha256"]
            == original(observation.raw_payload).hexdigest()
            for c in candidates
        )
        malformed = observation.model_copy(update={"capture_flags": ("malformed",)})
        record = SourceRecord(chunk, 0, malformed)
        parsed = parser_module.parse_envelope(malformed, payload_sha256=record.payload_sha256)
        _candidate(record, parsed[0])
        assert payload_calls == 2


@pytest.mark.parametrize("stream", ["funding_info", "exchange_info"])
def test_poll_model_digest_only_exists_for_real_overlap_identity_consumer(
    stream: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    model = json.loads(fixture("usdm", stream + ".json"))
    observation = envelope(
        market="um_perpetual",
        stream=stream,
        ordinal=1,
        module="binance.usdm.rest.v1",
        raw_payload=provenance(
            schema_version="binance-usdm-side-rest-provenance.v1",
            model=model,
            path="/fixture",
            kind=stream,
        ),
    )
    original = parser_module.sha256_json
    calls: list[object] = []

    def counted(value: object) -> str:
        calls.append(value)
        return original(value)

    monkeypatch.setattr(parser_module, "sha256_json", counted)
    parsed = parser_module.parse_envelope(observation)
    assert parsed[0].valid and calls == []
    overlapped = observation.model_copy(
        update={
            "capture_flags": ("blue_green_overlap", "deployment_id=deployment"),
        }
    )
    parsed = parser_module.parse_envelope(overlapped)
    assert calls == [model]
    assert parsed[0].semantic_identity["model_sha256"] == original(model)


def test_selected_normalization_skips_unrelated_unavailable_raw_and_filters_rows(
    tmp_path: Path,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        selected = [
            envelope(
                market="spot",
                stream="agg_trade",
                raw_payload=fixture("spot", "agg_trade.json"),
                ordinal=i,
                receive_time_utc_ns=BASE_NS + i * 86_400_000_000_000,
            )
            for i in (1, 2)
        ]
        manifest = seal_events(layout=layout, catalog=catalog, events=selected)
        unrelated = envelope(
            market="spot",
            stream="book_ticker",
            raw_payload=fixture("spot", "book_ticker.json"),
            ordinal=3,
        ).model_copy(update={"symbol": "ETHUSDT"})
        other = seal_events(layout=layout, catalog=catalog, events=[unrelated])
        (layout.root / str(other["relative_path"])).unlink()
        chunks = load_source_chunks(
            layout=layout,
            catalog=catalog,
            products=frozenset({("spot", "BTCUSDT")}),
        )
        assert [c.chunk_id for c in chunks] == [manifest["chunk_id"]]
        normalizer = Normalizer(
            layout=layout,
            catalog=catalog,
            products=frozenset({("spot", "BTCUSDT")}),
            start_utc_ns=BASE_NS + 86_400_000_000_000,
            end_utc_ns=BASE_NS + 2 * 86_400_000_000_000,
        )
        result = normalizer.run()
        assert result.build_id is not None
        dataset = ManifestCatalog(layout.root).open_build(result.build_id)
        replay = list(
            dataset.replay(ReplayQuery(streams=("agg_trade",), gap_policy=GapPolicy.INCLUDE))
        )
        assert len(replay) == 1
        assert replay[0].row["receive_time_utc_ns"] == selected[0].receive_time_utc_ns
        original_id = result.build_id
        result = Normalizer(
            layout=layout,
            catalog=catalog,
            manifest_paths=[
                layout.manifests / (str(manifest["chunk_id"]).replace("-", "") + ".manifest.json")
            ],
            start_utc_ns=BASE_NS,
            end_utc_ns=BASE_NS + 3 * 86_400_000_000_000,
        ).run()
        assert result.build_id != original_id
