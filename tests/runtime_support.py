"""Service tests inject product workers into the same production supervisor."""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from typing import cast

from binance_market_data_recorder.collector.coverage import ProductCapture, SelectedAssetCapture
from binance_market_data_recorder.config import RecorderConfig
from binance_market_data_recorder.domain.product import ProductKey
from binance_market_data_recorder.service.runtime import RuntimeCollector, SupervisorFactory
from binance_market_data_recorder.storage.catalog import Catalog
from binance_market_data_recorder.storage.layout import StorageLayout


def with_collectors(
    factory: Callable[..., Mapping[ProductKey, RuntimeCollector]],
) -> SupervisorFactory:
    def create(
        config: RecorderConfig,
        logger: logging.Logger,
        version: str,
        instance_id: str,
        layout: StorageLayout,
        catalog: Catalog,
    ) -> SelectedAssetCapture:
        owner = SelectedAssetCapture(
            config, logger, version, instance_id, layout, catalog, clients={}
        )
        owner._active_sources.clear()
        owner.collectors = cast(
            dict[ProductKey, ProductCapture],
            dict(factory(config, logger, version, instance_id, None, None)),
        )
        return owner

    return create
