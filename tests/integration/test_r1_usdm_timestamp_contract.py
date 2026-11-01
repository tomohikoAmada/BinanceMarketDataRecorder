from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest

import binance_market_data_recorder.normalize.model as model
import binance_market_data_recorder.normalize.pipeline as pipeline
from binance_market_data_recorder.binance.usdm.schema import (
    UsdMStream,
    envelope_from_websocket_frame,
)
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.normalize import Normalizer
from binance_market_data_recorder.normalize.parser import parse_envelope
from binance_market_data_recorder.replay import ManifestCatalog, ReplayClock, ReplayQuery
from binance_market_data_recorder.spool.format import decode_envelope, encode_envelope
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.normalization_support import BASE_NS, fixture, seal_events

CONTRACT = json.loads(
    (Path(__file__).parents[1] / "fixtures/contracts/usdm-depth-time.v1.json").read_bytes()
)


def capture(stream: UsdMStream, payload: dict[str, object]) -> EventEnvelope:
    raw = json.dumps(payload, separators=(",", ":")).encode()
    return envelope_from_websocket_frame(
        symbol="BTCUSDT",
        raw_payload=raw,
        stream=stream,
        connection_id="r1-test",
        collector_instance_id="r1-test",
        collector_version="r1-candidate",
        receive_time_utc_ns=BASE_NS,
        receive_monotonic_ns=1,
    )


@pytest.mark.parametrize("stream", list(UsdMStream))
@pytest.mark.parametrize("transaction_time", [0, 1690000002000])
def test_distinct_event_and_transaction_or_trade_times_round_trip(
    stream: UsdMStream,
    transaction_time: int,
    tmp_path: Path,
) -> None:
    payload = json.loads(fixture("usdm", f"{stream.value}.json"))
    payload.update(E=1690000002007, T=transaction_time)
    event = capture(stream, payload)
    restored = decode_envelope(encode_envelope(event))
    assert restored == event
    assert restored.raw_payload == event.raw_payload
    assert restored.source_sequence == event.source_sequence
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_events(layout=layout, catalog=catalog, events=[restored])
        build = Normalizer(layout=layout, catalog=catalog).run()
    assert build.build_id is not None
    dataset = ManifestCatalog(layout.root).open_build(build.build_id)
    [replayed] = list(dataset.replay(ReplayQuery(clock=ReplayClock.EXCHANGE_TIME)))
    assert replayed.event_time_ns == transaction_time * 1_000_000
    assert replayed.row["exchange_event_time_ms"] == 1690000002007
    transaction_slot = (
        "exchange_trade_time_ms"
        if stream is UsdMStream.AGG_TRADE
        else "exchange_transaction_time_ms"
    )
    empty_slot = (
        "exchange_transaction_time_ms"
        if stream is UsdMStream.AGG_TRADE
        else "exchange_trade_time_ms"
    )
    assert replayed.row[transaction_slot] == transaction_time
    assert replayed.row[empty_slot] is None
    assert replayed.row["valid"] is True


@pytest.mark.parametrize("patch", CONTRACT["invalid_source_patches"] + [{"T": 2**63}])
def test_invalid_time_retains_exact_raw_and_is_explicitly_malformed(patch: dict[str, Any]) -> None:
    payload = {**CONTRACT["raw"], **patch}
    event = capture(UsdMStream.DIFF_DEPTH, payload)
    assert json.loads(event.raw_payload) == payload
    assert event.capture_flags == ("malformed",)
    assert event.exchange_transaction_time is None
    assert event.exchange_trade_time is None
    assert event.exchange_event_time is None
    assert parse_envelope(event)[0].valid is False


def test_absent_required_time_is_not_silently_inferred_from_event_time() -> None:
    payload = dict(CONTRACT["raw"])
    del payload["T"]
    assert capture(UsdMStream.DIFF_DEPTH, payload).capture_flags == ("malformed",)


@pytest.mark.parametrize("field", ["E", "T"])
def test_persisted_legacy_out_of_range_time_retains_invalid_row_without_build_abort(
    tmp_path: Path,
    field: str,
) -> None:
    payload = {**CONTRACT["raw"], field: 2**63}
    # The exact base extractor accepted nonnegative unbounded Python integers.
    event = capture(UsdMStream.DIFF_DEPTH, CONTRACT["raw"]).model_copy(
        update={
            "module": "binance.usdm.websocket.v1",
            "raw_payload": json.dumps(payload, separators=(",", ":")).encode(),
            "exchange_event_time": payload["E"],
            "exchange_transaction_time": None,
            "exchange_trade_time": payload["T"],
        }
    )
    assert decode_envelope(encode_envelope(event)) == event
    layout = ensure_storage_layout(tmp_path)
    with Catalog(layout.catalog) as catalog:
        seal_events(layout=layout, catalog=catalog, events=[event])
        build = Normalizer(layout=layout, catalog=catalog).run()
    assert build.build_id is not None
    [saved] = list(ManifestCatalog(layout.root).open_build(build.build_id).replay(ReplayQuery()))
    assert saved.row["valid"] is False
    assert "exceeds int64" in str(saved.row["error_code"])
    assert all(
        saved.row[name] is None
        for name in (
            "exchange_event_time_ms",
            "exchange_transaction_time_ms",
            "exchange_trade_time_ms",
        )
    )
    assert saved.row["raw_payload_sha256"] == hashlib.sha256(event.raw_payload).hexdigest()


def test_legacy_raw_rederives_v2_while_legacy_build_and_replay_remain_immutable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    layout = ensure_storage_layout(tmp_path)
    event = capture(UsdMStream.DIFF_DEPTH, CONTRACT["raw"])
    legacy = event.model_copy(
        update={
            "module": "binance.usdm.websocket.v1",
            "exchange_transaction_time": None,
            "exchange_trade_time": CONTRACT["raw"]["T"],
        }
    )
    with Catalog(layout.catalog) as catalog:
        seal_events(layout=layout, catalog=catalog, events=[legacy])
        original_parse = parse_envelope
        with monkeypatch.context() as old_code:
            old_code.setattr(model, "DATASET_VERSION", "normalized-dataset.v1")
            old_code.setattr(pipeline, "DATASET_VERSION", "normalized-dataset.v1")
            old_code.setattr(
                pipeline,
                "parse_envelope",
                lambda raw, **_kwargs: [
                    replace(parsed, exchange_times=None) for parsed in original_parse(raw)
                ],
            )
            old_build = Normalizer(layout=layout, catalog=catalog).run()
        preserved = {
            path: hashlib.sha256(path.read_bytes()).hexdigest()
            for path in layout.root.rglob("*")
            if path.is_file()
            and (path.suffix in {".zst", ".parquet"} or path.name.endswith(".manifest.json"))
        }
        new_build = Normalizer(layout=layout, catalog=catalog).run()
    assert old_build.build_id is not None and new_build.build_id is not None
    assert old_build.build_id != new_build.build_id
    assert all(
        hashlib.sha256(path.read_bytes()).hexdigest() == sha for path, sha in preserved.items()
    )
    old_catalog = ManifestCatalog(layout.root, dataset_version="normalized-dataset.v1")
    new_catalog = ManifestCatalog(layout.root)
    assert [build.build_id for build in old_catalog.list_builds()] == [old_build.build_id]
    assert [build.build_id for build in new_catalog.list_builds()] == [new_build.build_id]
    query = ReplayQuery(clock=ReplayClock.EXCHANGE_TIME)
    [old] = list(old_catalog.open_build(old_build.build_id).replay(query))
    [new] = list(new_catalog.open_build(new_build.build_id).replay(query))
    assert old.event_time_ns == CONTRACT["raw"]["E"] * 1_000_000
    assert old.row["exchange_trade_time_ms"] == CONTRACT["raw"]["T"]
    assert new.event_time_ns == CONTRACT["raw"]["T"] * 1_000_000
    assert new.row["exchange_trade_time_ms"] is None
    assert new.row["module"] == "binance.usdm.websocket.v1"
    assert new.row["raw_payload_sha256"] == hashlib.sha256(legacy.raw_payload).hexdigest()
