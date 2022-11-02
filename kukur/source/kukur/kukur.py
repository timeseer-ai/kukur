"""Connect to another Kukur instance using Arrow Flight."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Any, Dict, Generator, Optional, Tuple, Union

import pyarrow as pa

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.client import Client


def from_config(config: Dict[str, Any]):
    """Create a new Kukur source"""
    source_name = config["source"]
    host = config.get("host", "localhost")
    port = config.get("port", 8081)
    api_key = (config.get("api_key_name", ""), config.get("api_key", ""))
    return KukurSource(source_name, host, port, api_key)


class KukurSource:
    """KukurSource connects to another Kukur instance using Arrow Flight."""

    __client: Client
    __source_name: str

    # pylint: disable=too-many-arguments
    def __init__(
        self, source_name: str, host: str, port: int, api_key: Tuple[str, str]
    ):
        self.__client = Client(api_key, host, port)
        self.__source_name = source_name

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Search time series using the Flight service."""
        query = SeriesSearch(self.__source_name, selector.tags, selector.field)
        for result in self.__client.search(query):
            if isinstance(result, SeriesSelector):
                yield SeriesSelector.from_tags(
                    selector.source, result.tags, result.field
                )
            else:
                result.series = SeriesSelector.from_tags(
                    selector.source, result.series.tags, result.series.field
                )
                yield result

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Get metadata from the Flight service."""
        remote_selector = SeriesSelector.from_tags(
            self.__source_name, selector.tags, selector.field
        )
        metadata = self.__client.get_metadata(remote_selector)
        metadata.series = SeriesSelector(selector.source, selector.tags, selector.field)
        return metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Get data from the Flight service."""
        remote_selector = SeriesSelector.from_tags(
            self.__source_name, selector.tags, selector.field
        )
        return self.__client.get_data(remote_selector, start_date, end_date)

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> pa.Table:
        """Get plot data from the Flight service."""
        remote_selector = SeriesSelector.from_tags(
            self.__source_name, selector.tags, selector.field
        )
        return self.__client.get_plot_data(
            remote_selector, start_date, end_date, interval_count
        )

    def get_source_structure(
        self, selector: SeriesSelector
    ) -> Optional[SourceStructure]:
        """Return the source structure using the Flight service."""
        remote_selector = SeriesSelector.from_tags(
            self.__source_name, selector.tags, selector.field
        )
        return self.__client.get_source_structure(remote_selector)
