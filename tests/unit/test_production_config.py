"""Unit tests for the M3.3 production deployment configuration."""

from __future__ import annotations

from pathlib import Path

from binance_market_data_recorder.config import load_config

ROOT = Path(__file__).resolve().parents[2]
DEPLOY_CONFIG = ROOT / "deploy" / "recorder.toml"
EXAMPLE_CONFIG = ROOT / "examples" / "recorder.production.toml"


def test_production_config_files_exist_and_match() -> None:
    assert DEPLOY_CONFIG.is_file(), "deploy/recorder.toml must exist"
    assert EXAMPLE_CONFIG.is_file(), "examples/recorder.production.toml must exist"
    assert DEPLOY_CONFIG.read_text(encoding="utf-8") == EXAMPLE_CONFIG.read_text(
        encoding="utf-8"
    ), "deploy/recorder.toml and examples/recorder.production.toml must be identical"


def test_production_config_loads_and_verifies_m3_3_contract() -> None:
    loaded = load_config(config_file=DEPLOY_CONFIG)
    config = loaded.config

    # Host and environment paths
    assert config.data_root.resolve() == Path("/var/lib/binance-market-data-recorder").resolve()
    assert config.capacity_profile == "vps-production-v1"
    assert config.network_proxy_mode == "direct"
    assert config.log_level == "INFO"

    # M1.1: 15m / 128 MiB chunk rotation thresholds
    assert config.rotation_seconds == 900.0
    assert config.rotation_bytes == 128 * 1024 * 1024
    assert config.durability_interval_seconds == 1.0

    # Ingress queue and frame sizes
    assert config.ingress_queue_capacity == 8192
    assert config.max_frame_bytes == 16 * 1024 * 1024
    assert config.heartbeat_seconds == 5.0
    assert config.prevent_sleep is False

    # Coverage specification
    assert config.coverage is not None
    coverage = config.coverage
    assert coverage.underlying_assets == ("BTC", "ETH")

    # ADR-0055: dated premium klines omitted
    assert coverage.capture_dated_premium_klines is False

    # ADR-0057: USD-M delivery adlQuantile omitted
    assert coverage.capture_dated_adl_risk is False

    # ADR-0058 / D1: closed bar persistence only + REST backfill
    assert coverage.kline_forming_policy == "closed_only"

    # ADR-0058 / D2: S12 covers only 6 distinct timezone intervals
    assert coverage.spot_utc8_intervals == (
        "6h",
        "12h",
        "1d",
        "3d",
        "1w",
        "1M",
    )

    # ADR-0054 / P2: D06 cadences and derivative weights
    assert coverage.recent_trades_interval_seconds == 30.0
    assert coverage.max_steady_derivative_weight_per_minute == 1000.0

    # D06 physical derivative cadences
    cadences = coverage.recent_trades_intervals
    assert cadences["um_perpetual:BTCUSDT"] == 2.0
    assert cadences["um_perpetual:ETHUSDT"] == 2.0
    assert cadences["um_perpetual:BTCUSDC"] == 3.0
    assert cadences["um_perpetual:ETHUSDC"] == 3.0
    assert cadences["cm_perpetual:BTCUSD_PERP"] == 30.0
    assert cadences["cm_perpetual:ETHUSD_PERP"] == 30.0
    assert cadences["cm_delivery:BTCUSD_261225"] == 30.0
    assert cadences["cm_delivery:ETHUSD_261225"] == 30.0
