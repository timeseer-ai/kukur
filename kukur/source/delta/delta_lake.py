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
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, Dict, Generator, List, Optional, Tuple, Union

import pyarrow as pa
from dateutil.relativedelta import relativedelta

from kukur.base import SeriesSearch, SeriesSelector
from kukur.exceptions import (
    InvalidSourceException,
    MissingModuleException,
)
from kukur.loader import Loader
from kukur.metadata import Metadata
from kukur.source.arrow import (
    cast_ts_column,
    filter_by_timerange,
    filter_pivot_data,
    filter_row_data,
    map_pivot_columns,
    map_row_columns,
)
from kukur.source.quality import QualityMapper


class PartitionOrigin(Enum):
    """Allowed origins for delta lake partitions."""

    TAG = "tag"
    TIMESTAMP = "timestamp"


class Resolution(Enum):
    """Resultion for timestamp partioning."""

    HOUR = "HOUR"
    DAY = "DAY"
    MONTH = "MONTH"
    YEAR = "YEAR"


@dataclass
class DeltaLakePartition:
    """A partition in delta lake."""

    origin: PartitionOrigin
    key: str
    format: Optional[str] = None
    column: Optional[str] = None

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "DeltaLakePartition":
        """Create a partition from a data dictionary."""
        if "origin" not in data:
            raise InvalidSourceException("No partition origin")
        if "key" not in data:
            raise InvalidSourceException("No partition key")
        return DeltaLakePartition(
            PartitionOrigin(data["origin"]),
            data["key"],
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
    partitions: List[DeltaLakePartition] = data_field(default_factory=list)
    data_datetime_format: Optional[str] = None
    data_timezone: Optional[str] = None
    path_encoding: Optional[str] = None

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "DeltaSourceOptions":
        """Create source options from a data dictionary."""
        data_format = data.get("format", "row")
        partitions = []
        for partition_data in data.get("partitions", []):
            if "origin" not in partition_data:
                raise InvalidSourceException("No partition origin")
            partitions.append(DeltaLakePartition.from_data(partition_data))

        options = cls(
            data_format,
            data.get("column_mapping", {}),
            data.get("tag_columns", ["series name"]),
            data.get("field_columns", ["value"]),
            partitions,
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
        return filter_by_timerange(data, start_date, end_date)

    def get_file_extension(self) -> str:
        """Delta lakes do not support row-based formats."""
        raise NotImplementedError()

    def __read_all_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        if self.__options.data_format == "pivot":
            all_data = self._read_pivot_data()
            return filter_pivot_data(all_data, selector)

        all_data = self._read_row_partitioned_data(selector, start_date, end_date)
        return filter_row_data(all_data, selector, self.__quality_mapper)

    def _search_pivot(self, source_name: str) -> Generator[SeriesSelector, None, None]:
        all_data = self._read_pivot_data()
        for name in all_data.column_names[1:]:
            yield SeriesSelector(source_name, name)

    def _read_row_data(self) -> pa.Table:
        column_names = (
            self.__options.tag_columns + ["ts"] + self.__options.field_columns
        )
        row_data = self._read_file(self.__loader.open())
        row_data = map_row_columns(
            row_data, column_names, self.__options.column_mapping, self.__quality_mapper
        )
        row_data = cast_ts_column(
            row_data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return row_data

    def _read_row_partitioned_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        column_names = (
            self.__options.tag_columns + ["ts"] + self.__options.field_columns
        )
        row_data = self._read_partitioned_file(
            self.__loader.open(), selector, start_date, end_date
        )
        row_data = map_row_columns(
            row_data, column_names, self.__options.column_mapping, self.__quality_mapper
        )
        row_data = cast_ts_column(
            row_data, self.__options.data_datetime_format, self.__options.data_timezone
        )
        return row_data

    def _read_pivot_data(self) -> pa.Table:
        all_data = self._read_file(self.__loader.open())
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

    def _read_file(self, file_like) -> pa.Table:
        return DeltaTable(file_like).to_pyarrow_table()

    def _read_partitioned_file(
        self,
        file_like,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
    ) -> pa.Table:
        """Return a PyArrow Table for the Delta Table using the defined partitions."""
        partitions: List[Tuple[str, str, Union[List[str], str]]] = []
        for partition in self.__options.partitions:
            if partition.origin == PartitionOrigin.TAG:
                partitions.append(self._format_tag_partition(partition, selector))
            if partition.origin == PartitionOrigin.TIMESTAMP:
                partitions.append(
                    self._format_timestamp_partition(partition, start_date, end_date)
                )

        return DeltaTable(file_like).to_pyarrow_table(partitions)

    def _format_tag_partition(
        self,
        partition: DeltaLakePartition,
        selector: SeriesSelector,
    ) -> Tuple[str, str, str]:
        column_name = self.__options.column_mapping.get(partition.key, partition.key)
        return (column_name, "=", selector.tags[partition.key])

    def _format_timestamp_partition(
        self,
        partition: DeltaLakePartition,
        start_date: datetime,
        end_date: datetime,
    ) -> Tuple[str, str, List[str]]:
        resolution = Resolution(partition.key)
        column = resolution.value
        if partition.column is not None:
            column = partition.column

        format = partition.format
        partition_values = []
        max_partition_values_date = None

        if resolution == Resolution.YEAR:
            start_date = start_date.replace(month=1, day=1, hour=0, minute=0, second=0)
            interval: Union[relativedelta, timedelta] = relativedelta(years=1)
            if format is None:
                format = "%Y"
        elif resolution == Resolution.MONTH:
            start_date = start_date.replace(day=1, hour=0, minute=0, second=0)
            max_partition_values_date = start_date.replace(year=start_date.year + 1)
            interval = relativedelta(months=1)
        elif resolution == Resolution.DAY:
            start_date = start_date.replace(hour=0, minute=0, second=0)
            max_partition_values_date = start_date + timedelta(days=31)
            interval = relativedelta(days=1)
        elif resolution == Resolution.HOUR:
            start_date = start_date.replace(minute=0, second=0)
            max_partition_values_date = start_date + timedelta(days=1)
            interval = timedelta(hours=1)

        if (
            max_partition_values_date is not None
            and end_date > max_partition_values_date
        ):
            end_date = max_partition_values_date

        while start_date < end_date:
            if format is not None:
                partition_values.append(start_date.strftime(format))
            else:
                if resolution == Resolution.MONTH:
                    partition_values.append(f"{start_date.month}")
                elif resolution == Resolution.DAY:
                    partition_values.append(f"{start_date.day}")
                elif resolution == Resolution.HOUR:
                    partition_values.append(f"{start_date.hour}")
            start_date = start_date + interval

        return (column, "in", partition_values)


def from_config(config: dict, quality_mapper: QualityMapper) -> DeltaLakeSource:
    """Create a new delta lake data source from the given configuration dictionary."""
    if not HAS_DELTA_LAKE:
        raise MissingModuleException("deltalake", "delta")

    data_format = config.get("format", "row")
    partitions = []
    for partition_data in config.get("partitions", []):
        partitions.append(DeltaLakePartition.from_data(partition_data))
    options = DeltaSourceOptions(
        data_format,
        config.get("column_mapping", {}),
        config.get("tag_columns", ["series name"]),
        config.get("field_columns", ["value"]),
        partitions,
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
        sort_by_timestamp=config.get("sort_by_timestamp", True),
    )
