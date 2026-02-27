"""Kukur source for a PI AF Template using PI Web API."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Generator
from datetime import datetime

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import (
    InvalidSourceException,
    MissingModuleException,
)
from kukur.source.piwebapi_af.pi_asset_framework import (
    PIAssetFramework,
    PIWebAPIConnection,
)

try:
    import requests  # noqa: F401

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

logger = logging.getLogger(__name__)


class PIWebAPIAssetFrameworkTemplateSource:
    """Query element templates in PI Asset Framework."""

    def __init__(self, config: dict):
        self._config = config

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Return all attributes of the selected elements in the Asset Framework."""
        with PIWebAPIConnection(self._config) as connection:
            af = PIAssetFramework(connection, self._config)
            yield from af.search(selector)

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for one tag."""
        raise NotImplementedError()

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        with PIWebAPIConnection(self._config) as connection:
            af = PIAssetFramework(connection, self._config)
            return af.get_data(selector, start_date, end_date)

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> pa.Table:
        """Return plot data for the given time series in the given time period."""
        with PIWebAPIConnection(self._config) as connection:
            af = PIAssetFramework(connection, self._config)
            return af.get_plot_data(selector, start_date, end_date, interval_count)


def from_config(config: dict) -> PIWebAPIAssetFrameworkTemplateSource:
    """Create a new PIWebAPIAssetFrameworkSource."""
    if "database_uri" not in config:
        raise InvalidSourceException(
            'piwebapi-af-template sources require a "database_uri" entry'
        )
    if not HAS_REQUESTS:
        raise MissingModuleException("requests", "piwebapi-af-template")
    return PIWebAPIAssetFrameworkTemplateSource(config)
