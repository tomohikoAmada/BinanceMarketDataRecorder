from __future__ import annotations

from pathlib import Path

import pytest

from binance_market_data_recorder.paths import (
    UnsafeDataRootError,
    default_data_root,
    validate_data_root,
)


def test_default_macos_data_root() -> None:
    assert default_data_root(home=Path("/Users/example"), platform="darwin") == Path(
        "/Users/example/Library/Application Support/BinanceMarketDataRecorder"
    )


def test_default_linux_data_root() -> None:
    assert default_data_root(home=Path("/home/example"), platform="linux") == Path(
        "/home/example/.local/share/BinanceMarketDataRecorder"
    )


@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/Users/example",
        "/Users/example/Desktop/data",
        "/Users/example/Documents/data",
        "/Users/example/Library/Mobile Documents/data",
        "/Users/example/Library/CloudStorage/data",
        "/tmp/data",
        "/private/tmp/data",
    ],
)
def test_dangerous_persistent_roots_are_rejected(path: str) -> None:
    with pytest.raises(UnsafeDataRootError):
        validate_data_root(
            path,
            home=Path("/Users/example"),
            repository_root=Path("/opt/work/repo"),
        )


def test_repository_and_workspace_are_rejected(tmp_path: Path) -> None:
    repository = tmp_path / "workspace" / "BinanceMarketDataRecorder"
    repository.mkdir(parents=True)
    (repository / ".git").mkdir()

    for candidate in (repository, repository / "var", repository.parent / "other-data"):
        with pytest.raises(UnsafeDataRootError, match="repository_or_workspace"):
            validate_data_root(candidate, repository_root=repository, home=tmp_path / "home")


def test_safe_absolute_root_is_normalized(tmp_path: Path) -> None:
    candidate = tmp_path / "safe" / "data"
    assert validate_data_root(
        candidate,
        repository_root=tmp_path / "different-workspace" / "repo",
        home=tmp_path / "home",
    ) == candidate.resolve()


def test_relative_root_is_rejected() -> None:
    with pytest.raises(UnsafeDataRootError, match="path_must_be_absolute"):
        validate_data_root("relative/data")
