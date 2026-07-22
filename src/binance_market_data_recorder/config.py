"""Strict, credential-free Recorder configuration."""

from __future__ import annotations

import os
import tomllib
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .paths import UnsafeDataRootError, default_data_root, validate_data_root

ENV_PREFIX = "BINANCE_MARKET_RECORDER_"
CONFIG_FILE_ENV = f"{ENV_PREFIX}CONFIG_FILE"
ALLOWED_ENV_SETTINGS = {
    CONFIG_FILE_ENV,
    f"{ENV_PREFIX}DATA_ROOT",
    f"{ENV_PREFIX}LOG_LEVEL",
}
LogLevel = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


class ConfigurationError(ValueError):
    """A deterministic user-facing configuration failure."""


class RecorderConfig(BaseModel):
    """M1 configuration; intentionally contains no credential/account fields."""

    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)

    data_root: Path
    log_level: LogLevel = "INFO"

    @field_validator("data_root", mode="before")
    @classmethod
    def _path_from_text(cls, value: object) -> object:
        return Path(value) if isinstance(value, str) else value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value

    def public_dict(self) -> dict[str, str]:
        """Return the complete safe-to-display M1 configuration."""

        return {"data_root": str(self.data_root), "log_level": self.log_level}


class _RecorderOverrides(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

    data_root: Path | None = None
    log_level: LogLevel | None = None

    @field_validator("data_root", mode="before")
    @classmethod
    def _path_from_text(cls, value: object) -> object:
        return Path(value) if isinstance(value, str) else value

    @field_validator("log_level", mode="before")
    @classmethod
    def _normalize_log_level(cls, value: object) -> object:
        return value.upper() if isinstance(value, str) else value


class _ConfigFile(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)

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
        "log_level": "INFO",
    }
    sources = {"data_root": "default", "log_level": "default"}

    if selected_file is not None:
        overrides = _read_config_file(selected_file)
        if overrides.data_root is not None:
            values["data_root"] = overrides.data_root
            sources["data_root"] = "config_file"
        if overrides.log_level is not None:
            values["log_level"] = overrides.log_level
            sources["log_level"] = "config_file"

    data_root_env = environment.get(f"{ENV_PREFIX}DATA_ROOT")
    if data_root_env is not None:
        values["data_root"] = data_root_env
        sources["data_root"] = "environment"
    log_level_env = environment.get(f"{ENV_PREFIX}LOG_LEVEL")
    if log_level_env is not None:
        values["log_level"] = log_level_env
        sources["log_level"] = "environment"

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
