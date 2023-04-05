"""Delta Lake source for Kukur.

Two formats are supported:
- row based, with many series in one file containing one row per data point
- pivot, with many series as columns in one file
"""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False

from dataclasses import dataclass
from dataclasses import field as data_field
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Tuple

import pyarrow as pa
from dateutil.relativedelta import relativedelta

from kukur.base import SeriesSearch, SeriesSelector
from kukur.exceptions import (
    InvalidDataError,
    InvalidSourceException,
    MissingModuleException,
)
from kukur.loader import Loader
from kukur.metadata import Metadata
from kukur.source.quality import QualityMapper


class PartitionOrigin(Enum):
    """Allowed origins for delta lake partitions."""

    TAG = "tag"
    TIMESTAMP = "timestamp"


class Resolution(Enum):
    """Resultion for timestamp partioning."""

    DAY = "DAY"
    MONTH = "MONTH"
    YEAR = "YEAR"


@dataclass
class DeltaLakeTagPartition:
    """A tag based partition in delta lake."""

    key: str

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "DeltaLakeTagPartition":
        """Create a partition from a data dictionary."""
        if "key" not in data:
            raise InvalidSourceException("No partition key")
        return DeltaLakeTagPartition(data["key"])


@dataclass
class DeltaLakeTimestampPartition:
    """A timestamp based partition in delta lake."""

    resolution: Resolution
    format: Optional[str] = None
    column: Optional[str] = None

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "DeltaLakeTimestampPartition":
        """Create a partition from a data dictionary."""
        if "key" not in data:
            raise InvalidSourceException("No partition key")
        return DeltaLakeTimestampPartition(
            Resolution(data["key"]),
            data.get("format"),
            data.get("column"),
        )


@dataclass
class DeltaSourceOptions:
    """Options for a DeltaSource."""

    data_format: str
    column_mapping: Dict[str, str]
    tag_columns: List[str]
    field_columns: List[str]
    tag_partitions: List[DeltaLakeTagPartition] = data_field(default_factory=list)
    timestamp_partitions: List[DeltaLakeTimestampPartition] = data_field(
        default_factory=list
    )
    data_datetime_format: Optional[str] = None
    data_timezone: Optional[str] = None
    path_encoding: Optional[str] = None

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "DeltaSourceOptions":
        """Create source options from a data dictionary."""
        data_format = data.get("format", "row")
        tag_partitions = []
        timestamp_partitions = []
        for partition_data in data.get("partitions", []):
            if "origin" not in partition_data:
                raise InvalidSourceException("No partition origin")
            if PartitionOrigin(partition_data["origin"]) == PartitionOrigin.TAG:
                tag_partitions.append(DeltaLakeTagPartition.from_data(partition_data))
            if PartitionOrigin(partition_data["origin"]) == PartitionOrigin.TIMESTAMP:
                timestamp_partitions.append(
                    DeltaLakeTimestampPartition.from_data(partition_data)
                )

        options = cls(
            data_format,
            data.get("column_mapping", {}),
            data.get("tag_columns", ["series name"]),
            data.get("field_columns", ["value"]),
            tag_partitions,
            timestamp_partitions,
            data.get("data_datetime_format"),
            data.get("data_timezone"),
            data.get("path_encoding"),
        )

        return options


class DeltaLakeLoader:
    """Fakes a loader for Delta Lake tables.

    It does not really load files, as the other loaders do.
    """

    def __init__(self, config: dict) -> None:
        self.__uri = config["uri"]

    def open(self):
        """Return the URI to connect to."""
        return self.__uri

    def has_child(self, subpath: str) -> bool:
        """Not supported for Delta Lake."""
        raise NotImplementedError()

    def open_child(self, subpath: str):
        """Not supported for Delta Lake."""
        raise NotImplementedError()


class DeltaLakeSource:
    """Connect to a Delta Lake."""

    __loader: Loader
    __options: DeltaSourceOptions
    __quality_mapper: QualityMapper
    __sort_by_timestamp: bool = False

    def __init__(
        self,
        options: DeltaSourceOptions,
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

    def read_file(self, file_like) -> pa.Table:
        """Return a PyArrow Table for the Delta Table at the given URI."""
        return DeltaTable(file_like).to_pyarrow_table()

    def _format_tag_partition(
        self,
        partition: DeltaLakeTagPartition,
        selector: SeriesSelector,
    ) -> Tuple[str, str, str]:
        column_name = self.__options.column_mapping.get(partition.key, partition.key)
        return (column_name, "=", selector.tags[partition.key])

    def _format_timestamp_partition(
        self,
        partition: DeltaLakeTimestampPartition,
        start_date: datetime,
        end_date: datetime,
    ) -> Tuple[str, str, list[str]]:
        column = partition.resolution.value
        if partition.column is not None:
            column = partition.column

        format = partition.format
        partition_values = []

        if partition.resolution == Resolution.YEAR:
            start_date = start_date.replace(month=1, day=1, hour=0, minute=0, second=0)
            interval = relativedelta(years=1)
            if format is None:
                format = "%Y"

        if partition.resolution == Resolution.MONTH:
            start_date = start_date.replace(day=1, hour=0, minute=0, second=0)
            if format is None:
                format = "%Y-%m"
            interval = relativedelta(months=1)

        if partition.resolution == Resolution.DAY:
            start_date = start_date.replace(hour=0, minute=0, second=0)
            if format is None:
                format = "%Y-%m-%d"
            interval = relativedelta(days=1)

        assert format is not None
        while start_date < end_date:
            partition_values.append(start_date.strftime(format))
            start_date = start_date + interval

        return (column, "in", partition_values)

    def read_partitioned_file(
        self,
        file_like,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
    ) -> pa.Table:
        """Return a PyArrow Table for the Delta Table using the defined partitions."""
        tag_partitions = [
            self._format_tag_partition(partition, selector)
            for partition in self.__options.tag_partitions
        ]
        timestamp_partitions = [
            self._format_timestamp_partition(partition, start_date, end_date)
            for partition in self.__options.timestamp_partitions
        ]

        return DeltaTable(file_like).to_pyarrow_table(
            tag_partitions + timestamp_partitions
        )

    def get_file_extension(self) -> str:
        """Delta lakes do not support row-based formats."""
        raise NotImplementedError()

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
        data = self.__read_all_data(selector, start_date, end_date)
        if self.__sort_by_timestamp is True:
            data = data.sort_by("ts")
        # pylint: disable=no-member
        on_or_after = pa.compute.greater_equal(data["ts"], pa.scalar(start_date))
        before = pa.compute.less(data["ts"], pa.scalar(end_date))
        return data.filter(pa.compute.and_(on_or_after, before))

    def __read_all_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        if self.__options.data_format == "pivot":
            return self._read_pivot_data(selector)

        return self._read_row_data(selector, start_date, end_date)

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
        all_data = self.read_file(
            self.__loader.open(),
        )
        all_data = self._map_row_data(all_data)
        for tags in (
            all_data.group_by(self.__options.tag_columns).aggregate([]).to_pylist()
        ):
            for field_name in self.__options.field_columns:
                yield SeriesSelector(source_name, tags, field_name)

    def _read_row_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        all_data = self.read_partitioned_file(
            self.__loader.open(), selector, start_date, end_date
        )
        all_data = self._map_row_data(all_data)

        for k, v in selector.tags.items():
            all_data = all_data.filter(pa.compute.equal(all_data[k], pa.scalar(v)))
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

    def _map_row_data(
        self,
        all_data: pa.Table,
    ) -> pa.Table:
        columns = self.__options.tag_columns + ["ts"] + self.__options.field_columns
        if self.__quality_mapper.is_present():
            columns.append("quality")

        data_columns = {}
        if len(self.__options.column_mapping) > 0:
            data_columns = {
                column_name: self.__options.column_mapping.get(column_name, column_name)
                for column_name in columns
            }

        all_data = _map_columns(data_columns, all_data)
        all_data = all_data.rename_columns(columns)
        all_data = _cast_ts_column(
            all_data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return all_data

    def _map_quality(self, quality_data: pa.Array) -> pa.Array:
        return self.__quality_mapper.map_array(quality_data)


def from_config(config: dict, quality_mapper: QualityMapper) -> DeltaLakeSource:
    """Create a new delta lake data source from the given configuration dictionary."""
    if not HAS_DELTA_LAKE:
        raise MissingModuleException("deltalake", "delta")

    data_format = config.get("format", "row")
    tag_partitions = []
    timestamp_partitions = []
    for partition_data in config.get("partitions", []):
        if "origin" not in partition_data:
            raise InvalidSourceException("No partition origin")
        if PartitionOrigin(partition_data["origin"]) == PartitionOrigin.TAG:
            tag_partitions.append(DeltaLakeTagPartition.from_data(partition_data))
        if PartitionOrigin(partition_data["origin"]) == PartitionOrigin.TIMESTAMP:
            timestamp_partitions.append(
                DeltaLakeTimestampPartition.from_data(partition_data)
            )
    options = DeltaSourceOptions(
        data_format,
        config.get("column_mapping", {}),
        config.get("tag_columns", ["series name"]),
        config.get("field_columns", ["value"]),
        tag_partitions,
        timestamp_partitions,
    )

    if data_format not in ["row", "pivot"]:
        raise InvalidSourceException(
            'Delta lake sources support only the "row" and "pivot" format.'
        )
    if "uri" not in config:
        raise InvalidSourceException('Delta lake sources require an "uri" entry')
    return DeltaLakeSource(
        options,
        DeltaLakeLoader(config),
        quality_mapper,
        sort_by_timestamp=config.get("sort_by_timestamp", False),
    )


def _get_value_schema_type(data: pa.Table):
    value_type = pa.float64()
    if len(data) > 0:
        if pa.types.is_string(data["value"][0].type):
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
            pa.compute.strptime(data["ts"], data_datetime_format, "us"),
        )
    if data_timezone is not None:
        # pylint: disable=no-member
        data = data.set_column(
            data.column_names.index("ts"),
            "ts",
            pa.compute.assume_timezone(data["ts"], data_timezone),
        )
    return data
