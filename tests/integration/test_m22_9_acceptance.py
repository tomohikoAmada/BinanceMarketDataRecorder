from __future__ import annotations

from pathlib import Path
from typing import Literal

import pytest

from binance_market_data_recorder.audit.reconnect_boundaries import (
    EXPLICIT_SEQUENCE_GAP,
    UNMARKED_RECONNECT,
    audit_data_root,
    incremental_audit_data_root,
    strict_manifest_inventory,
)
from binance_market_data_recorder.domain.event import EventEnvelope
from binance_market_data_recorder.spool.seal import SealError, seal_partial
from binance_market_data_recorder.spool.writer import RawChunkWriter, RotationPolicy
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import ensure_storage_layout
from tests.unit.test_historical_reconnect_audit import build_fixture
from tools.audit_reconnect_boundaries import audit_data_root as historical_audit_data_root


def test_installed_strict_audit_reuses_real_raw_manifest_fixture(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    result = incremental_audit_data_root(tmp_path)
    payload = result
    assert result["schema_version"] == "m22.9-reconnect-audit.v2"
    assert payload["manifest_inventory"]["count"] == 4
    assert payload["summary"]["explicit_gap"] == 2
    assert payload["summary"]["unmarked_reconnect"] == 2
    assert payload["summary"]["unknown"] == 1
    assert EXPLICIT_SEQUENCE_GAP in {
        item["kind"] for stream in payload["streams"] for item in stream["transitions"]
    }
    assert UNMARKED_RECONNECT in {
        item["kind"] for stream in payload["streams"] for item in stream["transitions"]
    }


@pytest.mark.parametrize("body", [b"{", b"[]\n", b'{"manifest_schema_version":"future"}\n'])
def test_strict_inventory_never_skips_malformed_manifest(tmp_path: Path, body: bytes) -> None:
    manifests = tmp_path / "data" / "manifests"
    manifests.mkdir(parents=True)
    (manifests / "bad.manifest.json").write_bytes(body)
    with pytest.raises(SealError):
        strict_manifest_inventory(tmp_path)


def test_acceptance_source_contains_no_direct_catalog_sql(tmp_path: Path) -> None:
    source = Path("src/binance_market_data_recorder/service/acceptance.py").read_text()
    assert "sqlite3.connect" not in source
    assert "cursor.execute" not in source
    assert "SELECT " not in source


def _seal_stream(
    root: Path,
    *,
    market: Literal["spot", "um_perpetual"],
    stream: str,
    connections: list[str],
    receive_start: int,
) -> None:
    layout = ensure_storage_layout(root)
    with Catalog(layout.catalog) as catalog:
        writer = RawChunkWriter(
            layout=layout,
            catalog=catalog,
            market=market,
            symbol="BTCUSDT",
            stream=stream,
            collector_instance_id="shared-engine-test",
            collector_version="0.1.0+test",
            rotation=RotationPolicy(seconds=60),
            durability_interval_seconds=0,
        )
        for ordinal, connection in enumerate(connections):
            writer.append(
                EventEnvelope(
                    market=market,
                    symbol="BTCUSDT",
                    stream=stream,
                    module="shared-engine-test",
                    connection_id=connection,
                    collector_instance_id="shared-engine-test",
                    collector_version="0.1.0+test",
                    receive_time_utc_ns=receive_start + ordinal,
                    receive_monotonic_ns=receive_start + ordinal,
                    raw_payload=b"{}",
                )
            )
        writer.close()
        seal_partial(writer.path, layout=layout, catalog=catalog)


def test_interleaved_market_stream_chunks_never_form_cross_stream_reconnects(
    tmp_path: Path,
) -> None:
    _seal_stream(
        tmp_path,
        market="spot",
        stream="diff_depth",
        connections=["spot-depth"],
        receive_start=100,
    )
    _seal_stream(
        tmp_path,
        market="spot",
        stream="agg_trade",
        connections=["spot-trade"],
        receive_start=101,
    )
    _seal_stream(
        tmp_path,
        market="um_perpetual",
        stream="book_ticker",
        connections=["um-book"],
        receive_start=102,
    )
    result = incremental_audit_data_root(tmp_path)
    assert result["summary"]["transitions_total"] == 0


def test_rest_per_request_connection_ids_are_not_reconnect_boundaries(tmp_path: Path) -> None:
    _seal_stream(
        tmp_path,
        market="spot",
        stream="depth_snapshot",
        connections=["request-a", "request-b", "request-c"],
        receive_start=200,
    )
    result = incremental_audit_data_root(tmp_path)
    assert result["summary"]["transitions_total"] == 0


def test_historical_tool_and_installed_acceptance_share_exact_engine(tmp_path: Path) -> None:
    build_fixture(tmp_path)
    assert historical_audit_data_root is audit_data_root
    historical = historical_audit_data_root(tmp_path)
    assert historical == audit_data_root(tmp_path)
    acceptance = incremental_audit_data_root(tmp_path)

    def classifications(document: dict[str, object]) -> list[tuple[object, ...]]:
        streams = document["streams"]
        assert isinstance(streams, list)
        return sorted(
            (
                stream["market"],
                stream["stream"],
                transition["boundary_kind"],
                transition["old_chunk_id"],
                transition["new_chunk_id"],
                transition["kind"],
            )
            for stream in streams
            if isinstance(stream, dict)
            for transition in stream["transitions"]
            if isinstance(transition, dict)
        )

    assert classifications(historical) == classifications(acceptance)
