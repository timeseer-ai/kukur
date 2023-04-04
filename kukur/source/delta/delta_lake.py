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

from base64 import b64encode
from datetime import datetime
from pathlib import PurePath
from typing import Dict, Generator, List, Optional, Tuple, Union

import pyarrow as pa

from kukur.base import SeriesSearch, SeriesSelector
from kukur.exceptions import (
    InvalidDataError,
    InvalidSourceException,
    MissingModuleException,
)
from kukur.loader import Loader
from kukur.metadata import Metadata
from kukur.source.arrow import BaseArrowSourceOptions, SourcePartition
from kukur.source.quality import QualityMapper


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

    def read_file(
        self,
        file_like,
        selector: Optional[SeriesSelector] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pa.Table:
        """Return a PyArrow Table for the Delta Table at the given URI."""
        partitions: List[Tuple[str, str, Union[str, List[str]]]] = []
        if self.__options.partitions is not None and selector is not None:
            for partition in self.__options.partitions:
                if partition.origin == "tag":
                    column_name = self.__options.column_mapping.get(
                        partition.key, partition.key
                    )
                    partitions.append((column_name, "=", selector.tags[partition.key]))
                else:
                    assert start_date is not None
                    assert end_date is not None

                    start = start_date.replace(
                        month=1, day=1, hour=0, minute=0, second=0
                    )
                    years = []
                    while start < end_date:
                        format = "%Y"
                        if partition.format is not None:
                            format = partition.format
                        column = partition.key
                        if partition.column is not None:
                            column = partition.column
                        years.append(start.strftime(format))
                        start = start.replace(year=start.year + 1)
                    partitions.append((column, "in", years))
        return DeltaTable(file_like).to_pyarrow_table(partitions)

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

        if self.__options.data_format == "dir":
            return self._read_directory_data(selector)

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
        all_data = self._load_row_data()
        for tags in (
            all_data.group_by(self.__options.tag_columns).aggregate([]).to_pylist()
        ):
            for field_name in self.__options.field_columns:
                yield SeriesSelector(source_name, tags, field_name)

    def _read_row_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        all_data = self._load_row_data(selector, start_date, end_date)

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

    def _load_row_data(
        self,
        selector: Optional[SeriesSelector] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
    ) -> pa.Table:
        all_data = self.read_file(self.__loader.open(), selector, start_date, end_date)
        columns = self.__options.tag_columns + ["ts"] + self.__options.field_columns
        if self.__quality_mapper.is_present():
            columns.append("quality")

        data_columns = {}
        if len(self.__options.column_mapping) > 0 or len(all_data.column_names) > len(
            columns
        ):
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


def from_config(config: dict, quality_mapper: QualityMapper) -> DeltaLakeSource:
    """Create a new delta lake data source from the given configuration dictionary."""
    if not HAS_DELTA_LAKE:
        raise MissingModuleException("deltalake", "delta")

    data_format = config.get("format", "row")
    options = BaseArrowSourceOptions(
        data_format,
        config.get("column_mapping", {}),
        config.get("tag_columns", ["series name"]),
        config.get("field_columns", ["value"]),
    )
    options.partitions = [
        SourcePartition.from_data(partition)
        for partition in config.get("partitions", [])
    ]
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
