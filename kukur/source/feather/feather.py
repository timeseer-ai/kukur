"""
feather contains the Feather data source for Timeseer.

Three formats are supported:
- row based, with many series in one file containing one row per data point
- directory based, with one file per series
- pivot, with many series as columns in one file
"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from datetime import datetime
from typing import Dict, Generator

import pyarrow as pa
import pyarrow.compute
import pyarrow.feather as feather

from kukur import Metadata, SeriesSelector

from kukur.loader import Loader, from_config as loader_from_config
from kukur.exceptions import InvalidDataError, InvalidSourceException


def from_config(config: Dict[str, str]):
    """Create a new Feather data source from the given configuration dictionary."""
    data_format = config.get("format", "row")
    if "path" not in config:
        raise InvalidSourceException('Feather sources require a "path" entry')
    loader = loader_from_config(config, files_as_path=True)
    return FeatherSource(data_format, loader)


class FeatherSource:
    """A Feather data source for Timeseer."""

    __loader: Loader
    __data_format: str

    def __init__(self, data_format: str, loader: Loader):
        """Create a new Feather data source."""
        self.__loader = loader
        self.__data_format = data_format

    def search(self, selector: SeriesSelector) -> Generator[Metadata, None, None]:
        """Feather does not support searching for time series."""

    # pylint: disable=no-self-use
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Feather currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Read data in one of the predefined formats.

        The complete Feather file will be loaded in an Arrow table during processing.
        """
        data = self.__read_all_data(selector)
        # pylint: disable=no-member
        on_or_after = pyarrow.compute.greater_equal(data["ts"], pa.scalar(start_date))
        before = pyarrow.compute.less(data["ts"], pa.scalar(end_date))
        return data.filter(pyarrow.compute.and_(on_or_after, before))

    def __read_all_data(self, selector: SeriesSelector) -> pa.Table:
        if self.__data_format == "pivot":
            return _read_pivot_data(self.__loader, selector)

        if self.__data_format == "dir":
            return _read_directory_data(self.__loader, selector)

        return _read_row_data(self.__loader, selector)


def _read_pivot_data(loader: Loader, selector: SeriesSelector) -> pa.Table:
    all_data = feather.read_table(loader.open())
    if selector.name not in all_data.column_names:
        raise InvalidDataError(f'column "{selector.name}" not found')
    schema = pa.schema([("ts", pa.timestamp("us", "utc")), ("value", pa.float64())])
    return (
        all_data.select([0, selector.name]).rename_columns(["ts", "value"]).cast(schema)
    )


def _read_row_data(loader: Loader, selector: SeriesSelector) -> pa.Table:
    schema = pa.schema(
        [
            ("series name", pa.string()),
            ("ts", pa.timestamp("us", "utc")),
            ("value", pa.float64()),
        ]
    )
    all_data = (
        feather.read_table(loader.open())
        .rename_columns(["series name", "ts", "value"])
        .cast(schema)
    )
    # pylint: disable=no-member
    data = all_data.filter(
        pyarrow.compute.equal(all_data["series name"], pa.scalar(selector.name))
    )
    return data.drop(["series name"])


def _read_directory_data(loader: Loader, selector: SeriesSelector) -> pa.Table:
    data = feather.read_table(loader.open_child(f"{selector.name}.feather"))
    schema = pa.schema([("ts", pa.timestamp("us", "utc")), ("value", pa.float64())])
    return data.rename_columns(["ts", "value"]).cast(schema)
