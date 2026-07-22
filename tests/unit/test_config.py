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
    assert loaded.sources == {"data_root": "config_file", "log_level": "environment"}


def test_unknown_secret_like_environment_setting_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="unknown environment settings"):
        load_config(environ={"BINANCE_MARKET_RECORDER_API_KEY": "must-not-exist"})


def test_unknown_file_field_is_rejected(tmp_path: Path) -> None:
    config_file = tmp_path / "settings.toml"
    config_file.write_text('[recorder]\nsecret = "must-not-exist"\n', encoding="utf-8")

    with pytest.raises(ConfigurationError, match="invalid configuration file"):
        load_config(config_file=config_file, environ={})
