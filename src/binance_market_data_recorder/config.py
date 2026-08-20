"""Strict, credential-free Recorder configuration."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Self

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    ValidationError,
    field_validator,
    model_validator,
)

from .network import ProxyConfigurationError, ProxyMode, ProxyPolicy
from .paths import UnsafeDataRootError, default_data_root, validate_data_root

ENV_PREFIX = "BINANCE_MARKET_RECORDER_"
CONFIG_FILE_ENV = f"{ENV_PREFIX}CONFIG_FILE"
ALLOWED_ENV_SETTINGS = {
    CONFIG_FILE_ENV,
    f"{ENV_PREFIX}GIT_COMMIT",
    f"{ENV_PREFIX}DATA_ROOT",
    f"{ENV_PREFIX}LOG_LEVEL",
    f"{ENV_PREFIX}ROTATION_SECONDS",
    f"{ENV_PREFIX}ROTATION_BYTES",
    f"{ENV_PREFIX}DURABILITY_INTERVAL_SECONDS",
    f"{ENV_PREFIX}INGRESS_QUEUE_CAPACITY",
    f"{ENV_PREFIX}MAX_FRAME_BYTES",
    f"{ENV_PREFIX}HEARTBEAT_SECONDS",
    f"{ENV_PREFIX}SLEEP_GAP_THRESHOLD_SECONDS",
    f"{ENV_PREFIX}PREVENT_SLEEP",
    f"{ENV_PREFIX}NETWORK_PROXY_MODE",
    f"{ENV_PREFIX}NETWORK_PROXY_URL",
}
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
CapacityProfileId = Literal["vps-production-v1"]


class ConfigurationError(ValueError):
    """A deterministic user-facing configuration failure."""


class RecorderConfig(BaseModel):
    """Credential-free Recorder configuration through the current milestone."""

    model_config = ConfigDict(
        extra="forbid", frozen=True, strict=True, hide_input_in_errors=True
    )

    data_root: Path
    capacity_profile: CapacityProfileId | None = None
    log_level: LogLevel = "INFO"
    network_proxy_mode: ProxyMode = "direct"
    network_proxy_url: str | None = None
    rotation_seconds: float = Field(default=60.0, gt=0)
    rotation_bytes: int = Field(default=128 * 1024 * 1024, ge=1024 * 1024)
    durability_interval_seconds: float = Field(default=1.0, ge=0, le=1.0)
    ingress_queue_capacity: int = Field(default=8192, ge=1)
    max_frame_bytes: int = Field(default=16 * 1024 * 1024, ge=1024, le=64 * 1024 * 1024)
    heartbeat_seconds: float = Field(default=5.0, ge=1.0, le=60.0)
    sleep_gap_threshold_seconds: float = Field(default=30.0, ge=5.0, le=600.0)
    prevent_sleep: bool = False
    side_mark_price_enabled: bool = True
    side_liquidation_enabled: bool = True
    side_premium_index_enabled: bool = True
    side_funding_history_enabled: bool = True
    side_funding_info_enabled: bool = True
    side_open_interest_enabled: bool = True
    side_exchange_info_enabled: bool = True
    side_premium_index_interval_seconds: float = Field(default=60.0, gt=0)
    side_funding_history_interval_seconds: float = Field(default=300.0, gt=0)
    side_funding_info_interval_seconds: float = Field(default=3600.0, gt=0)
    side_open_interest_interval_seconds: float = Field(default=60.0, gt=0)
    side_exchange_info_interval_seconds: float = Field(default=3600.0, gt=0)
    side_degraded_after_seconds: float = Field(default=900.0, gt=0)
    spot_exchange_info_enabled: bool = True
    spot_exchange_info_interval_seconds: float = Field(default=3600.0, gt=0)
    side_open_interest_statistics_enabled: bool = True
    side_taker_buy_sell_volume_enabled: bool = True
    side_global_long_short_ratio_enabled: bool = True
    side_top_long_short_account_ratio_enabled: bool = True
    side_top_long_short_position_ratio_enabled: bool = True
    side_basis_enabled: bool = True
    side_open_interest_statistics_interval_seconds: float = Field(default=300.0, gt=0)
    side_taker_buy_sell_volume_interval_seconds: float = Field(default=300.0, gt=0)
    side_global_long_short_ratio_interval_seconds: float = Field(default=300.0, gt=0)
    side_top_long_short_account_ratio_interval_seconds: float = Field(
        default=300.0, gt=0
    )
    side_top_long_short_position_ratio_interval_seconds: float = Field(
        default=300.0, gt=0
    )
    side_basis_interval_seconds: float = Field(default=300.0, gt=0)

    @field_validator("data_root", mode="before")
    @classmethod
    def _path_from_text(cls, value: object) -> object:
        return Path(value) if isinstance(value, str) else value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    @model_validator(mode="after")
    def _proxy_policy_is_valid(self) -> Self:
        try:
            ProxyPolicy(self.network_proxy_mode, self.network_proxy_url)
        except ProxyConfigurationError as exc:
            raise ValueError(str(exc)) from exc
        if (
            self.capacity_profile == "vps-production-v1"
            and self.network_proxy_mode != "direct"
        ):
            raise ValueError(
                "vps-production-v1 requires direct network proxy mode"
            )
        return self

    def proxy_policy(
        self, *, environment: Mapping[str, str] | None = None
    ) -> ProxyPolicy:
        return ProxyPolicy(
            self.network_proxy_mode,
            self.network_proxy_url,
            environment=environment,
        )

    def public_dict(self) -> dict[str, object]:
        """Return the complete safe-to-display configuration."""

        return {
            "data_root": str(self.data_root),
            "capacity_profile": self.capacity_profile,
            "log_level": self.log_level,
            "network_proxy_mode": self.network_proxy_mode,
            **self.proxy_policy().status().public_dict(),
            "rotation_seconds": self.rotation_seconds,
            "rotation_bytes": self.rotation_bytes,
            "durability_interval_seconds": self.durability_interval_seconds,
            "ingress_queue_capacity": self.ingress_queue_capacity,
            "max_frame_bytes": self.max_frame_bytes,
            "heartbeat_seconds": self.heartbeat_seconds,
            "sleep_gap_threshold_seconds": self.sleep_gap_threshold_seconds,
            "prevent_sleep": self.prevent_sleep,
            **{
                name: getattr(self, name)
                for name in self.__class__.model_fields
                if name.startswith(("side_", "spot_"))
            },
        }


class _RecorderOverrides(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, hide_input_in_errors=True
    )

    data_root: Path | None = None
    capacity_profile: CapacityProfileId | None = None
    log_level: LogLevel | None = None
    network_proxy_mode: ProxyMode | None = None
    network_proxy_url: str | None = None
    rotation_seconds: float | None = Field(default=None, gt=0)
    rotation_bytes: int | None = Field(default=None, ge=1024 * 1024)
    durability_interval_seconds: float | None = Field(default=None, ge=0, le=1.0)
    ingress_queue_capacity: int | None = Field(default=None, ge=1)
    max_frame_bytes: int | None = Field(default=None, ge=1024, le=64 * 1024 * 1024)
    heartbeat_seconds: float | None = Field(default=None, ge=1.0, le=60.0)
    sleep_gap_threshold_seconds: float | None = Field(default=None, ge=5.0, le=600.0)
    prevent_sleep: bool | None = None
    side_mark_price_enabled: bool | None = None
    side_liquidation_enabled: bool | None = None
    side_premium_index_enabled: bool | None = None
    side_funding_history_enabled: bool | None = None
    side_funding_info_enabled: bool | None = None
    side_open_interest_enabled: bool | None = None
    side_exchange_info_enabled: bool | None = None
    side_premium_index_interval_seconds: float | None = Field(default=None, gt=0)
    side_funding_history_interval_seconds: float | None = Field(default=None, gt=0)
    side_funding_info_interval_seconds: float | None = Field(default=None, gt=0)
    side_open_interest_interval_seconds: float | None = Field(default=None, gt=0)
    side_exchange_info_interval_seconds: float | None = Field(default=None, gt=0)
    side_degraded_after_seconds: float | None = Field(default=None, gt=0)
    spot_exchange_info_enabled: bool | None = None
    spot_exchange_info_interval_seconds: float | None = Field(default=None, gt=0)
    side_open_interest_statistics_enabled: bool | None = None
    side_taker_buy_sell_volume_enabled: bool | None = None
    side_global_long_short_ratio_enabled: bool | None = None
    side_top_long_short_account_ratio_enabled: bool | None = None
    side_top_long_short_position_ratio_enabled: bool | None = None
    side_basis_enabled: bool | None = None
    side_open_interest_statistics_interval_seconds: float | None = Field(
        default=None, gt=0
    )
    side_taker_buy_sell_volume_interval_seconds: float | None = Field(
        default=None, gt=0
    )
    side_global_long_short_ratio_interval_seconds: float | None = Field(
        default=None, gt=0
    )
    side_top_long_short_account_ratio_interval_seconds: float | None = Field(
        default=None, gt=0
    )
    side_top_long_short_position_ratio_interval_seconds: float | None = Field(
        default=None, gt=0
    )
    side_basis_interval_seconds: float | None = Field(default=None, gt=0)

    @field_validator("data_root", mode="before")
    @classmethod
    def _path_from_text(cls, value: object) -> object:
        return Path(value) if isinstance(value, str) else value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class _ConfigFile(BaseModel):
    model_config = ConfigDict(
        extra="forbid", strict=True, hide_input_in_errors=True
    )

    recorder: _RecorderOverrides = Field(default_factory=_RecorderOverrides)


@dataclass(frozen=True)
class LoadedConfig:
    config: RecorderConfig
    config_file: Path | None
    sources: Mapping[str, str]


def _read_config_file(path: Path) -> _RecorderOverrides:
    try:
        with path.open("rb") as handle:
            document = tomllib.load(handle)
    except (OSError, tomllib.TOMLDecodeError) as exc:
        raise ConfigurationError(f"cannot read configuration file {path}: {exc}") from exc
    try:
        return _ConfigFile.model_validate(document).recorder
    except ValidationError as exc:
        raise ConfigurationError(f"invalid configuration file {path}: {exc}") from exc


def _validate_environment(environ: Mapping[str, str]) -> None:
    unknown = sorted(
        key
        for key in environ
        if key.startswith(ENV_PREFIX) and key not in ALLOWED_ENV_SETTINGS
    )
    if unknown:
        raise ConfigurationError(f"unknown environment settings: {', '.join(unknown)}")


def load_config(
    *,
    config_file: str | Path | None = None,
    environ: Mapping[str, str] | None = None,
    repository_root: Path | None = None,
    home: Path | None = None,
) -> LoadedConfig:
    """Resolve defaults < TOML file < environment, then enforce path safety."""

    environment = os.environ if environ is None else environ
    _validate_environment(environment)
    selected_file_value = config_file or environment.get(CONFIG_FILE_ENV)
    selected_file = (
        Path(selected_file_value).expanduser().resolve() if selected_file_value else None
    )

    values: dict[str, object] = {
        "data_root": default_data_root(home=home),
        "capacity_profile": None,
        "log_level": "INFO",
        "network_proxy_mode": "direct",
        "network_proxy_url": None,
        "rotation_seconds": 60.0,
        "rotation_bytes": 128 * 1024 * 1024,
        "durability_interval_seconds": 1.0,
        "ingress_queue_capacity": 8192,
        "max_frame_bytes": 16 * 1024 * 1024,
        "heartbeat_seconds": 5.0,
        "sleep_gap_threshold_seconds": 30.0,
        "prevent_sleep": False,
        "side_mark_price_enabled": True,
        "side_liquidation_enabled": True,
        "side_premium_index_enabled": True,
        "side_funding_history_enabled": True,
        "side_funding_info_enabled": True,
        "side_open_interest_enabled": True,
        "side_exchange_info_enabled": True,
        "side_premium_index_interval_seconds": 60.0,
        "side_funding_history_interval_seconds": 300.0,
        "side_funding_info_interval_seconds": 3600.0,
        "side_open_interest_interval_seconds": 60.0,
        "side_exchange_info_interval_seconds": 3600.0,
        "side_degraded_after_seconds": 900.0,
        "spot_exchange_info_enabled": True,
        "spot_exchange_info_interval_seconds": 3600.0,
        "side_open_interest_statistics_enabled": True,
        "side_taker_buy_sell_volume_enabled": True,
        "side_global_long_short_ratio_enabled": True,
        "side_top_long_short_account_ratio_enabled": True,
        "side_top_long_short_position_ratio_enabled": True,
        "side_basis_enabled": True,
        "side_open_interest_statistics_interval_seconds": 300.0,
        "side_taker_buy_sell_volume_interval_seconds": 300.0,
        "side_global_long_short_ratio_interval_seconds": 300.0,
        "side_top_long_short_account_ratio_interval_seconds": 300.0,
        "side_top_long_short_position_ratio_interval_seconds": 300.0,
        "side_basis_interval_seconds": 300.0,
    }
    sources = {name: "default" for name in values}

    if selected_file is not None:
        overrides = _read_config_file(selected_file)
        if overrides.data_root is not None:
            values["data_root"] = overrides.data_root
            sources["data_root"] = "config_file"
        if overrides.capacity_profile is not None:
            values["capacity_profile"] = overrides.capacity_profile
            sources["capacity_profile"] = "config_file"
        if overrides.log_level is not None:
            values["log_level"] = overrides.log_level
            sources["log_level"] = "config_file"
        if overrides.network_proxy_mode is not None:
            values["network_proxy_mode"] = overrides.network_proxy_mode
            sources["network_proxy_mode"] = "config_file"
        if overrides.network_proxy_url is not None:
            values["network_proxy_url"] = overrides.network_proxy_url
            sources["network_proxy_url"] = "config_file"
        for name in (
            "rotation_seconds",
            "rotation_bytes",
            "durability_interval_seconds",
            "ingress_queue_capacity",
            "max_frame_bytes",
            "heartbeat_seconds",
            "sleep_gap_threshold_seconds",
            "prevent_sleep",
            "side_mark_price_enabled",
            "side_liquidation_enabled",
            "side_premium_index_enabled",
            "side_funding_history_enabled",
            "side_funding_info_enabled",
            "side_open_interest_enabled",
            "side_exchange_info_enabled",
            "side_premium_index_interval_seconds",
            "side_funding_history_interval_seconds",
            "side_funding_info_interval_seconds",
            "side_open_interest_interval_seconds",
            "side_exchange_info_interval_seconds",
            "side_degraded_after_seconds",
            "spot_exchange_info_enabled",
            "spot_exchange_info_interval_seconds",
            "side_open_interest_statistics_enabled",
            "side_taker_buy_sell_volume_enabled",
            "side_global_long_short_ratio_enabled",
            "side_top_long_short_account_ratio_enabled",
            "side_top_long_short_position_ratio_enabled",
            "side_basis_enabled",
            "side_open_interest_statistics_interval_seconds",
            "side_taker_buy_sell_volume_interval_seconds",
            "side_global_long_short_ratio_interval_seconds",
            "side_top_long_short_account_ratio_interval_seconds",
            "side_top_long_short_position_ratio_interval_seconds",
            "side_basis_interval_seconds",
        ):
            value = getattr(overrides, name)
            if value is not None:
                values[name] = value
                sources[name] = "config_file"

    if values["capacity_profile"] == "vps-production-v1":
        operational_overrides = sorted(
            key for key in environment if key.startswith(ENV_PREFIX)
        )
        if operational_overrides:
            raise ConfigurationError(
                "vps-production-v1 rejects BINANCE_MARKET_RECORDER_* "
                "environment settings: " + ", ".join(operational_overrides)
            )

    data_root_env = environment.get(f"{ENV_PREFIX}DATA_ROOT")
    if data_root_env is not None:
        values["data_root"] = data_root_env
        sources["data_root"] = "environment"
    log_level_env = environment.get(f"{ENV_PREFIX}LOG_LEVEL")
    if log_level_env is not None:
        values["log_level"] = log_level_env
        sources["log_level"] = "environment"
    proxy_mode_env = environment.get(f"{ENV_PREFIX}NETWORK_PROXY_MODE")
    if proxy_mode_env is not None:
        values["network_proxy_mode"] = proxy_mode_env.strip().casefold()
        sources["network_proxy_mode"] = "environment"
    proxy_url_env = environment.get(f"{ENV_PREFIX}NETWORK_PROXY_URL")
    if proxy_url_env is not None:
        values["network_proxy_url"] = proxy_url_env
        sources["network_proxy_url"] = "environment"
    numeric_environment = {
        "rotation_seconds": float,
        "rotation_bytes": int,
        "durability_interval_seconds": float,
        "ingress_queue_capacity": int,
        "max_frame_bytes": int,
        "heartbeat_seconds": float,
        "sleep_gap_threshold_seconds": float,
    }
    for name, parser in numeric_environment.items():
        environment_name = f"{ENV_PREFIX}{name.upper()}"
        raw_value = environment.get(environment_name)
        if raw_value is not None:
            try:
                values[name] = parser(raw_value)
            except ValueError as exc:
                raise ConfigurationError(
                    f"invalid numeric environment setting {environment_name}"
                ) from exc
            sources[name] = "environment"

    prevent_sleep_env = environment.get(f"{ENV_PREFIX}PREVENT_SLEEP")
    if prevent_sleep_env is not None:
        normalized = prevent_sleep_env.strip().casefold()
        if normalized not in {"true", "false", "1", "0"}:
            raise ConfigurationError(
                f"invalid boolean environment setting {ENV_PREFIX}PREVENT_SLEEP"
            )
        values["prevent_sleep"] = normalized in {"true", "1"}
        sources["prevent_sleep"] = "environment"

    try:
        parsed = RecorderConfig.model_validate(values)
    except ValidationError as exc:
        raise ConfigurationError(f"invalid configuration: {exc}") from exc
    try:
        safe_root = validate_data_root(
            parsed.data_root,
            repository_root=repository_root,
            home=home,
        )
    except UnsafeDataRootError as exc:
        raise ConfigurationError(str(exc)) from exc
    return LoadedConfig(
        config=parsed.model_copy(update={"data_root": safe_root}),
        config_file=selected_file,
        sources=sources,
    )
