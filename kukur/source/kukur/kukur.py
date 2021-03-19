"""Connect to another Kukur instance using Arrow Flight."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Any, Dict, Generator, Tuple, Union

import pyarrow as pa

from kukur import Metadata, SeriesSelector
from kukur.client import Client
from kukur.exceptions import InvalidDataError


class TimeseerNotInstalledError(Exception):
    """Raised when the timeseer is not available."""

    def __init__(self):
        Exception.__init__(self, "the timeseer is not available. Install timeseer.")


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
        self, selector: SeriesSelector
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Search time series using the Flight service."""
        query = SeriesSelector(self.__source_name, selector.name)
        for result in self.__client.search(query):
            if isinstance(result, SeriesSelector):
                yield SeriesSelector(selector.source, result.name)
            else:
                result.series = SeriesSelector(selector.source, result.series.name)
                yield result

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Get metadata from the Flight service."""
        if selector.name is None:
            raise InvalidDataError("No series name")
        remote_selector = SeriesSelector(self.__source_name, selector.name)
        metadata = self.__client.get_metadata(remote_selector)
        metadata.series = selector
        return metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Get data from the Flight service."""
        remote_selector = SeriesSelector(self.__source_name, selector.name)
        return self.__client.get_data(remote_selector, start_date, end_date)
