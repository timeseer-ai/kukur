"""Common logic for the file formats supported by pyarrow."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from datetime import datetime
from typing import Generator

import pyarrow as pa
import pyarrow.compute
import pyarrow.types

from kukur import Metadata, SeriesSelector
from kukur.exceptions import InvalidDataError
from kukur.loader import Loader
from kukur.source.quality import QualityMapper


class BaseArrowSource(ABC):
    """Base class for pyarrow file format data sources."""

    __loader: Loader
    __data_format: str
    __quality_mapper: QualityMapper

    def __init__(self, data_format: str, loader: Loader, quality_mapper: QualityMapper):
        """Create a new data source."""
        self.__loader = loader
        self.__data_format = data_format
        self.__quality_mapper = quality_mapper

    @abstractmethod
    def read_file(self, file_like) -> pa.Table:
        """Read the given file-like object using the pyarrow format reader."""
        ...

    @abstractmethod
    def get_file_extension(self) -> str:
        """Return the file extension for the supported pyarrow format."""
        ...

    def search(self, selector: SeriesSelector) -> Generator[Metadata, None, None]:
        """ArrowSource does not support searching for time series."""

    # pylint: disable=no-self-use
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Feather currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Read data in one of the predefined formats.

        The complete file will be loaded in an Arrow table during processing.
        """
        data = self.__read_all_data(selector)
        # pylint: disable=no-member
        on_or_after = pyarrow.compute.greater_equal(data["ts"], pa.scalar(start_date))
        before = pyarrow.compute.less(data["ts"], pa.scalar(end_date))
        return data.filter(pyarrow.compute.and_(on_or_after, before))

    def __read_all_data(self, selector: SeriesSelector) -> pa.Table:
        if self.__data_format == "pivot":
            return self._read_pivot_data(selector)

        if self.__data_format == "dir":
            return self._read_directory_data(selector)

        return self._read_row_data(selector)

    def _read_pivot_data(self, selector: SeriesSelector) -> pa.Table:
        all_data = self.read_file(self.__loader.open())
        if selector.name not in all_data.column_names:
            raise InvalidDataError(f'column "{selector.name}" not found')
        data = all_data.select([0, selector.name]).rename_columns(["ts", "value"])
        schema = pa.schema(
            [
                ("ts", pa.timestamp("us", "utc")),
                ("value", _get_value_schema_type(data)),
            ]
        )
        return data.cast(schema)

    def _read_row_data(self, selector: SeriesSelector) -> pa.Table:
        columns = ["series name", "ts", "value"]
        if self.__quality_mapper.is_present():
            columns.append("quality")

        all_data = self.read_file(self.__loader.open()).rename_columns(columns)
        schema = pa.schema(
            [
                ("ts", pa.timestamp("us", "utc")),
                ("value", _get_value_schema_type(all_data)),
            ]
        )
        if self.__quality_mapper.is_present():
            schema = schema.append(pa.field("quality", pa.int8()))
            kukur_quality_values = self._map_quality(all_data["quality"])
            all_data = all_data.set_column(
                3, "quality", pa.array(kukur_quality_values, type=pa.int8())
            )

        # pylint: disable=no-member
        data = all_data.filter(
            pyarrow.compute.equal(all_data["series name"], pa.scalar(selector.name))
        ).drop(["series name"])

        return data.cast(schema)

    def _read_directory_data(self, selector: SeriesSelector) -> pa.Table:
        columns = ["ts", "value"]
        if self.__quality_mapper.is_present():
            columns.append("quality")

        data = self.read_file(
            self.__loader.open_child(f"{selector.name}.{self.get_file_extension()}")
        ).rename_columns(columns)
        schema = pa.schema(
            [
                ("ts", pa.timestamp("us", "utc")),
                ("value", _get_value_schema_type(data)),
            ]
        )
        if self.__quality_mapper.is_present():
            schema = schema.append(pa.field("quality", pa.int8()))
            kukur_quality_values = self._map_quality(data["quality"])
            data = data.set_column(
                2, "quality", pa.array(kukur_quality_values, type=pa.int8())
            )
        return data.cast(schema)

    def _map_quality(self, quality_data: pa.array) -> pa.Table:
        return [
            self.__quality_mapper.from_source(source_quality_value.as_py())
            for source_quality_value in quality_data
        ]


def _get_value_schema_type(data: pa.Table):
    value_type = pa.float64()
    if len(data) > 0:
        if pyarrow.types.is_string(data["value"][0].type):
            value_type = pa.string()
    return value_type
