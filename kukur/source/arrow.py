"""Common logic for the file formats supported by pyarrow."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from abc import ABC, abstractmethod
from base64 import b64encode
from dataclasses import dataclass
from datetime import datetime
from pathlib import PurePath
from typing import Any, Dict, Generator, List, Optional

import pyarrow as pa
import pyarrow.compute
import pyarrow.types

from kukur import Metadata, SeriesSelector
from kukur.base import SeriesSearch
from kukur.exceptions import InvalidDataError, InvalidSourceException
from kukur.loader import Loader
from kukur.source.quality import QualityMapper


@dataclass
class SourcePartition:
    """A partition in a data set."""

    origin: str
    key: str
    path_encoding: Optional[str] = None
    format: Optional[str] = None
    column: Optional[str] = None

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "SourcePartition":
        """Create a partition from a data dictionary."""
        if "origin" not in data:
            raise InvalidSourceException("No partition origin")
        if "key" not in data:
            raise InvalidSourceException("No partition key")
        return SourcePartition(
            data["origin"],
            data["key"],
            data.get("path_encoding"),
            data.get("format"),
            data.get("column"),
        )


@dataclass
class BaseArrowSourceOptions:
    """Options for a BaseArrowSource."""

    data_format: str
    column_mapping: Dict[str, str]
    tag_columns: List[str]
    field_columns: List[str]
    data_datetime_format: Optional[str] = None
    data_timezone: Optional[str] = None
    path_encoding: Optional[str] = None
    partitions: Optional[List[SourcePartition]] = None

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "BaseArrowSourceOptions":
        """Create source options from a data dictionary."""
        data_format = data.get("format", "row")
        options = cls(
            data_format,
            data.get("column_mapping", {}),
            data.get("tag_columns", ["series name"]),
            data.get("field_columns", ["value"]),
            data.get("data_datetime_format"),
            data.get("data_timezone"),
            data.get("path_encoding"),
        )

        if data_format == "dir":
            options.partitions = [
                SourcePartition.from_data(partition)
                for partition in data.get("partitions", [])
            ]
            if len(options.partitions) == 0:
                options.partitions.append(SourcePartition("tag", "series name", None))
        return options


class BaseArrowSource(ABC):
    """Base class for pyarrow file format data sources."""

    __loader: Loader
    __options: BaseArrowSourceOptions
    __quality_mapper: QualityMapper
    __sort_by_timestamp: bool = False

    def __init__(
        self,
        options: BaseArrowSourceOptions,
        loader: Loader,
        quality_mapper: QualityMapper,
        *,
        sort_by_timestamp: bool = False,
    ):
        """Create a new data source."""
        self.__loader = loader
        self.__options = options
        self.__quality_mapper = quality_mapper
        self.__sort_by_timestamp = sort_by_timestamp

    @abstractmethod
    def read_file(self, file_like, selector=None) -> pa.Table:
        """Read the given file-like object using the pyarrow format reader."""
        ...

    @abstractmethod
    def get_file_extension(self) -> str:
        """Return the file extension for the supported pyarrow format."""
        ...

    def search(self, selector: SeriesSearch) -> Generator[SeriesSelector, None, None]:
        """Detect series in the data."""
        if self.__options.data_format == "pivot":
            yield from self._search_pivot(selector.source)

        if self.__options.data_format == "row":
            yield from self._search_row(selector.source)

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
        if self.__sort_by_timestamp is True:
            data = data.sort_by("ts")
        # pylint: disable=no-member
        on_or_after = pyarrow.compute.greater_equal(data["ts"], pa.scalar(start_date))
        before = pyarrow.compute.less(data["ts"], pa.scalar(end_date))
        return data.filter(pyarrow.compute.and_(on_or_after, before))

    def __read_all_data(self, selector: SeriesSelector) -> pa.Table:
        if self.__options.data_format == "pivot":
            return self._read_pivot_data(selector)

        if self.__options.data_format == "dir":
            return self._read_directory_data(selector)

        return self._read_row_data(selector)

    def _search_pivot(self, source_name: str) -> Generator[SeriesSelector, None, None]:
        all_data = self._load_pivot_data()
        for name in all_data.column_names[1:]:
            yield SeriesSelector(source_name, name)

    def _read_pivot_data(self, selector: SeriesSelector) -> pa.Table:
        all_data = self._load_pivot_data()
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

    def _load_pivot_data(self) -> pa.Table:
        all_data = self.read_file(self.__loader.open())
        all_data = _map_pivot_columns(self.__options.column_mapping, all_data)
        all_data = _cast_ts_column(
            all_data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return all_data

    def _search_row(self, source_name: str) -> Generator[SeriesSelector, None, None]:
        all_data = self._load_row_data()
        for tags in (
            all_data.group_by(self.__options.tag_columns).aggregate([]).to_pylist()
        ):
            for field_name in self.__options.field_columns:
                yield SeriesSelector(source_name, tags, field_name)

    def _read_row_data(self, selector: SeriesSelector) -> pa.Table:
        all_data = self._load_row_data(selector)

        for k, v in selector.tags.items():
            all_data = all_data.filter(pyarrow.compute.equal(all_data[k], pa.scalar(v)))
        data = pa.Table.from_arrays(
            [
                all_data["ts"],
                all_data[selector.field],
            ],
            ["ts", "value"],
        )
        schema = pa.schema(
            [
                ("ts", pa.timestamp("us", "utc")),
                ("value", _get_value_schema_type(data)),
            ]
        )
        if self.__quality_mapper.is_present():
            schema = schema.append(pa.field("quality", pa.int8()))
            data = data.set_column(2, "quality", self._map_quality(all_data["quality"]))

        return data.cast(schema)

    def _load_row_data(self, selector: Optional[SeriesSelector] = None) -> pa.Table:
        columns = self.__options.tag_columns + ["ts"] + self.__options.field_columns
        if self.__quality_mapper.is_present():
            columns.append("quality")

        data_columns = {}
        if len(self.__options.column_mapping) > 0:
            data_columns = {
                column_name: self.__options.column_mapping.get(column_name, column_name)
                for column_name in columns
            }

        all_data = self.read_file(self.__loader.open(), selector)
        all_data = _map_columns(data_columns, all_data)
        all_data = all_data.rename_columns(columns)
        all_data = _cast_ts_column(
            all_data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return all_data

    def _read_directory_data(self, selector: SeriesSelector) -> pa.Table:
        columns = ["ts", "value"]
        if self.__quality_mapper.is_present():
            columns.append("quality")

        if self.__options.partitions is None:
            raise InvalidSourceException(
                'Sources with `format = "dir"` require at least one partition'
            )

        data_path = None
        for partition in self.__options.partitions:
            partition_path = selector.tags[partition.key]
            if partition.path_encoding is not None:
                if partition.path_encoding == "base64":
                    partition_path = b64encode(partition_path.encode()).decode()
            if data_path is None:
                data_path = PurePath(partition_path)
            else:
                data_path = data_path / partition_path

        data = self.read_file(
            self.__loader.open_child(f"{data_path}.{self.get_file_extension()}")
        )

        data = _map_columns(self.__options.column_mapping, data)
        data = data.rename_columns(columns)
        data = _cast_ts_column(
            data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        schema = pa.schema(
            [
                ("ts", pa.timestamp("us", "utc")),
                ("value", _get_value_schema_type(data)),
            ]
        )
        if self.__quality_mapper.is_present():
            schema = schema.append(pa.field("quality", pa.int8()))
            data = data.set_column(2, "quality", self._map_quality(data["quality"]))
        return data.cast(schema)

    def _map_quality(self, quality_data: pa.Array) -> pa.Array:
        return self.__quality_mapper.map_array(quality_data)


def _get_value_schema_type(data: pa.Table):
    value_type = pa.float64()
    if len(data) > 0:
        if pyarrow.types.is_string(data["value"][0].type):
            value_type = pa.string()
    return value_type


def _map_columns(column_mapping: Dict[str, str], data: pa.Table) -> pa.Table:
    if len(column_mapping) == 0:
        return data

    return pa.Table.from_pydict(
        {
            column_name: data[data_name]
            for column_name, data_name in column_mapping.items()
        }
    )


def _map_pivot_columns(column_mapping: Dict[str, str], data: pa.Table) -> pa.Table:
    ts_column_name = data.column_names[0]
    if "ts" in column_mapping:
        ts_column_name = column_mapping["ts"]

    ts_column = data[ts_column_name]
    return data.drop([ts_column_name]).add_column(0, "ts", ts_column)


def _cast_ts_column(
    data: pa.Table, data_datetime_format: Optional[str], data_timezone: Optional[str]
) -> pa.Table:
    if data_datetime_format is not None:
        # pylint: disable=no-member
        data = data.set_column(
            data.column_names.index("ts"),
            "ts",
            pyarrow.compute.strptime(data["ts"], data_datetime_format, "us"),
        )
    if data_timezone is not None:
        # pylint: disable=no-member
        data = data.set_column(
            data.column_names.index("ts"),
            "ts",
            pyarrow.compute.assume_timezone(data["ts"], data_timezone),
        )
    return data
