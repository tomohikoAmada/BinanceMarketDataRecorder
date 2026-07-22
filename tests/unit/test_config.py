from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.config import ConfigurationError, RecorderConfig, load_config


def test_defaults_are_credential_free() -> None:
    fake_home = Path("/Users/config-test-user")
    loaded = load_config(environ={}, home=fake_home)

    assert loaded.config.data_root == (
        fake_home / "Library" / "Application Support" / "BinanceMarketDataRecorder"
    ).resolve()
    assert loaded.config.log_level == "INFO"
    assert loaded.config.rotation_seconds == 60.0
    assert loaded.config.rotation_bytes == 128 * 1024 * 1024
    assert loaded.config.durability_interval_seconds == 1.0
    assert loaded.config.ingress_queue_capacity == 8192
    assert loaded.config.max_frame_bytes == 16 * 1024 * 1024
    field_names = {name.casefold() for name in RecorderConfig.model_fields}
    assert not field_names & {"api_key", "secret", "account", "order", "trading"}


def test_file_then_environment_override(tmp_path: Path) -> None:
    config_file = tmp_path / "settings.toml"
    config_file.write_text(
        '[recorder]\ndata_root = "/var/lib/binance-recorder"\nlog_level = "warning"\n',
        encoding="utf-8",
    )
    loaded = load_config(
        config_file=config_file,
        environ={"BINANCE_MARKET_RECORDER_LOG_LEVEL": "ERROR"},
        home=tmp_path / "home",
        repository_root=tmp_path / "workspace" / "repo",
    )

    assert loaded.config.data_root == Path("/var/lib/binance-recorder").resolve()
    assert loaded.config.log_level == "ERROR"
    assert loaded.sources["data_root"] == "config_file"
    assert loaded.sources["log_level"] == "environment"
    assert loaded.sources["rotation_seconds"] == "default"


def test_spool_settings_support_file_and_environment_overrides(tmp_path: Path) -> None:
    config_file = tmp_path / "settings.toml"
    config_file.write_text(
        "[recorder]\nrotation_seconds = 30.0\nrotation_bytes = 2097152\n",
        encoding="utf-8",
    )
    loaded = load_config(
        config_file=config_file,
        environ={
            "BINANCE_MARKET_RECORDER_DURABILITY_INTERVAL_SECONDS": "0.25",
            "BINANCE_MARKET_RECORDER_INGRESS_QUEUE_CAPACITY": "128",
        },
        home=tmp_path / "home",
        repository_root=tmp_path / "workspace" / "repo",
    )
    assert loaded.config.rotation_seconds == 30.0
    assert loaded.config.rotation_bytes == 2 * 1024 * 1024
    assert loaded.config.durability_interval_seconds == 0.25
    assert loaded.config.ingress_queue_capacity == 128


def test_durability_window_cannot_exceed_one_second() -> None:
    with pytest.raises(ConfigurationError, match="invalid configuration"):
        load_config(
            environ={"BINANCE_MARKET_RECORDER_DURABILITY_INTERVAL_SECONDS": "1.01"}
        )


def test_unknown_secret_like_environment_setting_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="unknown environment settings"):
        load_config(environ={"BINANCE_MARKET_RECORDER_API_KEY": "must-not-exist"})


def test_unknown_file_field_is_rejected(tmp_path: Path) -> None:
    config_file = tmp_path / "settings.toml"
    config_file.write_text('[recorder]\nsecret = "must-not-exist"\n', encoding="utf-8")

    with pytest.raises(ConfigurationError, match="invalid configuration file"):
        load_config(config_file=config_file, environ={})
