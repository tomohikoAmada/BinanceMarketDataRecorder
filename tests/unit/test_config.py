from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.config import ConfigurationError, RecorderConfig, load_config
from binance_market_data_recorder.paths import default_data_root


def test_defaults_are_credential_free() -> None:
    fake_home = Path("/Users/config-test-user")
    loaded = load_config(environ={}, home=fake_home)

    assert loaded.config.data_root == default_data_root(home=fake_home).resolve()
    assert loaded.config.capacity_profile is None
    assert loaded.config.log_level == "INFO"
    assert loaded.config.rotation_seconds == 60.0
    assert loaded.config.rotation_bytes == 128 * 1024 * 1024
    assert loaded.config.durability_interval_seconds == 1.0
    assert loaded.config.ingress_queue_capacity == 8192
    assert loaded.config.max_frame_bytes == 16 * 1024 * 1024
    assert loaded.config.heartbeat_seconds == 5.0
    assert loaded.config.sleep_gap_threshold_seconds == 30.0
    assert loaded.config.prevent_sleep is False
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


def test_service_power_settings_support_strict_overrides(tmp_path: Path) -> None:
    config_file = tmp_path / "settings.toml"
    config_file.write_text(
        "[recorder]\nheartbeat_seconds = 2.0\nprevent_sleep = true\n",
        encoding="utf-8",
    )
    loaded = load_config(
        config_file=config_file,
        environ={
            "BINANCE_MARKET_RECORDER_SLEEP_GAP_THRESHOLD_SECONDS": "45",
            "BINANCE_MARKET_RECORDER_PREVENT_SLEEP": "false",
        },
        home=tmp_path / "home",
        repository_root=tmp_path / "workspace" / "repo",
    )
    assert loaded.config.heartbeat_seconds == 2.0
    assert loaded.config.sleep_gap_threshold_seconds == 45.0
    assert loaded.config.prevent_sleep is False


def test_invalid_prevent_sleep_environment_is_rejected() -> None:
    with pytest.raises(ConfigurationError, match="invalid boolean"):
        load_config(environ={"BINANCE_MARKET_RECORDER_PREVENT_SLEEP": "sometimes"})


def test_vps_capacity_profile_is_selected_only_by_explicit_toml(
    tmp_path: Path,
) -> None:
    config_file = tmp_path / "recorder.toml"
    config_file.write_text(
        '[recorder]\ncapacity_profile = "vps-production-v1"\n',
        encoding="utf-8",
    )

    loaded = load_config(
        config_file=config_file,
        environ={},
        home=tmp_path / "home",
        repository_root=tmp_path / "workspace" / "repo",
    )

    assert loaded.config.capacity_profile == "vps-production-v1"
    assert loaded.sources["capacity_profile"] == "config_file"


def test_unknown_capacity_profile_fails_configuration_load(tmp_path: Path) -> None:
    config_file = tmp_path / "recorder.toml"
    config_file.write_text(
        '[recorder]\ncapacity_profile = "looks-like-a-vps"\n',
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="invalid configuration file"):
        load_config(config_file=config_file, environ={})


@pytest.mark.parametrize(
    "environment",
    [
        {"HOSTNAME": "production-vps"},
        {"SYSTEMD_EXEC_PID": "123"},
        {"BINANCE_MARKET_RECORDER_DATA_ROOT": "/var/lib/recorder-test"},
    ],
)
def test_host_platform_systemd_and_environment_do_not_select_vps_profile(
    tmp_path: Path,
    environment: dict[str, str],
) -> None:
    loaded = load_config(
        environ=environment,
        home=tmp_path / "home",
        repository_root=tmp_path / "workspace" / "repo",
    )

    assert loaded.config.capacity_profile is None


def test_vps_profile_rejects_all_recorder_environment_overrides(
    tmp_path: Path,
) -> None:
    config_file = tmp_path / "recorder.toml"
    config_file.write_text(
        '[recorder]\ncapacity_profile = "vps-production-v1"\n',
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="rejects BINANCE_MARKET_RECORDER"):
        load_config(
            config_file=config_file,
            environ={"BINANCE_MARKET_RECORDER_LOG_LEVEL": "ERROR"},
        )


@pytest.mark.parametrize("proxy_mode", ["environment", "explicit"])
def test_vps_profile_requires_direct_network_mode(
    tmp_path: Path,
    proxy_mode: str,
) -> None:
    config_file = tmp_path / "recorder.toml"
    proxy_url = (
        '\nnetwork_proxy_url = "http://127.0.0.1:7890"'
        if proxy_mode == "explicit"
        else ""
    )
    config_file.write_text(
        "[recorder]\n"
        'capacity_profile = "vps-production-v1"\n'
        f'network_proxy_mode = "{proxy_mode}"{proxy_url}\n',
        encoding="utf-8",
    )

    with pytest.raises(ConfigurationError, match="requires direct"):
        load_config(config_file=config_file, environ={})


def test_capacity_profile_environment_selector_does_not_exist() -> None:
    with pytest.raises(ConfigurationError, match="unknown environment settings"):
        load_config(
            environ={
                "BINANCE_MARKET_RECORDER_CAPACITY_PROFILE": "vps-production-v1"
            }
        )
