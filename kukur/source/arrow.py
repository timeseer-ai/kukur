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
from kukur.base import DataSelector, SeriesSearch
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
    def read_file(self, file_like) -> pa.Table:
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
        self, selector: DataSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Read data in one of the predefined formats.

        The complete file will be loaded in an Arrow table during processing.
        """
        data = self.__read_all_data(selector)
        if self.__sort_by_timestamp is True:
            data = data.sort_by("ts")

        return filter_by_timerange(data, start_date, end_date)

    def __read_all_data(self, selector: DataSelector) -> pa.Table:
        if self.__options.data_format == "pivot":
            all_data = self._read_pivot_data()
            return filter_pivot_data(all_data, selector)

        if self.__options.data_format == "dir":
            return self._read_directory_data(selector)

        all_data = self._read_row_data()
        return filter_row_data(all_data, selector, self.__quality_mapper)

    def _search_pivot(self, source_name: str) -> Generator[SeriesSelector, None, None]:
        all_data = self._read_pivot_data()
        for name in all_data.column_names[1:]:
            yield SeriesSelector(source_name, name)

    def _read_row_data(self) -> pa.Table:
        column_names = (
            self.__options.tag_columns + ["ts"] + self.__options.field_columns
        )
        all_data = self.read_file(self.__loader.open())
        all_data = map_row_columns(
            all_data, column_names, self.__options.column_mapping, self.__quality_mapper
        )
        all_data = cast_ts_column(
            all_data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return all_data

    def _read_pivot_data(self) -> pa.Table:
        all_data = self.read_file(self.__loader.open())
        all_data = map_pivot_columns(self.__options.column_mapping, all_data)
        all_data = cast_ts_column(
            all_data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return all_data

    def _search_row(self, source_name: str) -> Generator[SeriesSelector, None, None]:
        all_data = self._read_row_data()
        for tags in (
            all_data.group_by(self.__options.tag_columns).aggregate([]).to_pylist()
        ):
            for field_name in self.__options.field_columns:
                yield SeriesSelector(source_name, tags, field_name)

    def _read_directory_data(self, selector: DataSelector) -> pa.Table:
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
        if selector.fields is not None:
            keep_columns = ["ts"] + selector.fields
            if self.__quality_mapper.is_present():
                keep_columns.append("quality")
            columns_to_drop: List[str] = []
            for column_name in data.column_names:
                if column_name not in keep_columns:
                    columns_to_drop.append(column_name)
            data = data.drop_columns(columns_to_drop)

        data = cast_ts_column(
            data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return conform_to_schema(data, self.__quality_mapper)


def map_pivot_columns(column_mapping: Dict[str, str], data: pa.Table) -> pa.Table:
    """Map pivot format columns according to the mapping definition."""
    ts_column_name = data.column_names[0]
    if "ts" in column_mapping:
        ts_column_name = column_mapping["ts"]

    ts_column = data[ts_column_name]
    return data.drop([ts_column_name]).add_column(0, "ts", ts_column)


def cast_ts_column(
    data: pa.Table, data_datetime_format: Optional[str], data_timezone: Optional[str]
) -> pa.Table:
    """Cast the timestamp column considering format and timezone."""
    return data.set_column(
        data.column_names.index("ts"),
        "ts",
        cast_timestamp(data["ts"], data_datetime_format, data_timezone),
    )


def cast_timestamp(
    array: pa.Array, data_datetime_format: Optional[str], data_timezone: Optional[str]
) -> pa.Array:
    """Cast a timestamp column considering format and timezone."""
    if data_datetime_format is not None:
        # pylint: disable=no-member
        array = pa.compute.strptime(array, data_datetime_format, "us")

    if data_timezone is not None:
        # pylint: disable=no-member
        array = pa.compute.assume_timezone(array, data_timezone)

    if not pa.types.is_timestamp(array.type):
        array = pyarrow.compute.cast(array, pa.timestamp("us", "UTC"))

    return array


def filter_pivot_data(all_data: pa.Table, selector: DataSelector) -> pa.Table:
    """Filter resulting pivot data based on selector tags and field."""
    column_names = all_data.column_names[1:]
    if selector.fields is not None:
        for field_name in selector.fields:
            if field_name not in all_data.column_names:
                raise InvalidDataError(f'column "{field_name}" not found')
        column_names = selector.fields
    data = all_data.select([0] + column_names).rename_columns(
        {all_data.column_names[0], "ts"}
    )

    return data.cast(determine_schema(data, include_quality=False))


def filter_row_data(
    all_data: pa.Table, selector: DataSelector, quality_mapper: QualityMapper
) -> pa.Table:
    """Filter resulting row data based on selector tags and field."""
    for k, v in selector.tags.items():
        all_data = all_data.filter(pyarrow.compute.equal(all_data[k], pa.scalar(v)))
    data = {"ts": all_data["ts"]}
    if selector.fields is not None:
        for column_name in selector.fields:
            data[column_name] = all_data[column_name]
    else:
        for column_name in all_data.column_names:
            if column_name not in ["ts", "quality"]:
                data[column_name] = all_data[column_name]
    if quality_mapper.is_present():
        data["quality"] = all_data["quality"]

    filtered_data = pa.Table.from_pydict(data)
    return conform_to_schema(filtered_data, quality_mapper)


def conform_to_schema(table: pa.Table, quality_mapper: QualityMapper) -> pa.Table:
    """Conform the table to the schema expected in Kukur."""
    schema = determine_schema(table, include_quality=quality_mapper.is_present())
    if quality_mapper.is_present():
        table = table.set_column(
            table.schema.get_field_index("quality"),
            "quality",
            _map_quality(table["quality"], quality_mapper),
        )

    return table.cast(schema)


def _map_quality(quality_data: pa.Array, quality_mapper: QualityMapper) -> pa.Array:
    return quality_mapper.map_array(quality_data)


def map_row_columns(
    all_data: pa.Table,
    column_names: List[str],
    column_mapping: Dict[str, str],
    quality_mapper: QualityMapper,
) -> pa.Table:
    """Map columns according to the provided column mapping."""
    if quality_mapper.is_present():
        column_names.append("quality")

    data_columns = {
        column_name: column_mapping.get(column_name, column_name)
        for column_name in column_names
    }

    all_data = _map_columns(data_columns, all_data)
    all_data = all_data.rename_columns(column_names)
    return all_data


def _map_columns(column_mapping: Dict[str, str], data: pa.Table) -> pa.Table:
    if len(column_mapping) == 0:
        return data

    return pa.Table.from_pydict(
        {
            column_name: data[data_name]
            for column_name, data_name in column_mapping.items()
        }
    )


def filter_by_timerange(
    all_data: pa.Table, start_date: datetime, end_date: datetime
) -> pa.Table:
    """Remove data before start_date and after end_date."""
    # pylint: disable=no-member
    on_or_after = pyarrow.compute.greater_equal(all_data["ts"], pa.scalar(start_date))
    before = pyarrow.compute.less(all_data["ts"], pa.scalar(end_date))
    return all_data.filter(pyarrow.compute.and_(on_or_after, before))


def empty_table(
    field_names: Optional[List[str]] = None, *, include_quality: bool = True
) -> pa.Table:
    """Return an empty table with a column for each data field."""
    data: Dict[str, List] = {
        "ts": [],
    }
    if field_names is not None:
        for field_name in field_names:
            data[field_name] = []
    if include_quality:
        data["quality"] = []
    return pa.Table.from_pydict(data)


def determine_schema(data: pa.Table, *, include_quality: bool = True) -> pa.Schema:
    """Determine a schema for data type casting to supported data types."""
    schema_data = [
        ("ts", pa.timestamp("us", "UTC")),
    ]
    for column_name in data.column_names[1:]:
        schema_data.append((column_name, get_value_schema_type(data[column_name])))
    if include_quality:
        schema_data.append(("quality", pa.int8()))
    return pa.schema(schema_data)


def get_value_schema_type(column: pa.Array):
    """Get the type of a value column."""
    value_type = pa.float64()
    if len(column) > 0:
        if pa.types.is_string(column[0].type):
            value_type = pa.string()
    return value_type
