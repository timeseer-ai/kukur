"""The Kukur application dispatches data requests to the correct data source."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Any, Dict, Generator, List, Union
from pathlib import Path

import json
import pyarrow as pa

from kukur import Metadata, SeriesSelector, Source
from kukur.exceptions import UnknownSourceException
from kukur.source import SourceFactory
from kukur.api_key.app import ApiKeys

from kukur.repository import MigrationRunner, RepositoryRegistry


class Kukur:
    """Kukur queries the sources it's configured with for time series data and metadata.

    It implements the Source interface."""

    __repository: RepositoryRegistry

    def __init__(self, config: Dict[str, Any]):
        self.__source_factory = SourceFactory(config)
        self.__repository = RepositoryRegistry(
            data_dir=Path(config.get("data_dir", "."))
        )
        migration_runner = MigrationRunner()
        migration_runner.register(self.__repository.api_key().migrations())
        migration_runner.migrate()

    def search(
        self, selector: SeriesSelector
    ) -> Generator[Union[SeriesSelector, Metadata], None, None]:
        """Return all time series or even the metadata of them in this source matching the selector."""
        return self._get_source(selector.source).search(selector)

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for the given time series."""
        return self._get_source(selector.source).get_metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        return self._get_source(selector.source).get_data(
            selector, start_date, end_date
        )

    def get_api_keys(self) -> ApiKeys:
        """Return the api keys."""
        return ApiKeys(self.__repository)

    def list_sources(self, *_) -> List[bytes]:
        """Return all the configured sources."""
        sources = self.__source_factory.get_source_names()
        return [json.dumps(sources).encode()]

    def _get_source(self, source_name: str) -> Source:
        source = self.__source_factory.get_source(source_name)
        if source is None:
            raise UnknownSourceException(source_name)
        return source
