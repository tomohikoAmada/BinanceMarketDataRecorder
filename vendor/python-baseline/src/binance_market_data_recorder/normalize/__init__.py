"""Rebuildable content-addressed normalized Parquet datasets."""

from importlib import import_module
from typing import Any

__all__ = [
    "DATASET_VERSION",
    "NormalizationError",
    "NormalizationResult",
    "Normalizer",
    "normalization_status",
]

_EXPORTS = {
    "DATASET_VERSION": ".model",
    "NormalizationError": ".pipeline",
    "NormalizationResult": ".pipeline",
    "Normalizer": ".pipeline",
    "normalization_status": ".pipeline",
}


def __getattr__(name: str) -> Any:
    submodule = _EXPORTS.get(name)
    if submodule is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(submodule, __name__), name)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted([*globals().keys(), *__all__])
