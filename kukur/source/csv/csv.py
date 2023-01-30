"""
csv contains the CSV data source for Timeseer.

Three formats are supported:
- row based, with many series in one file containing one line per data point
- directory based, with one file per series
- pivot, with many series as columns in one file
"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import csv

from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Union

import pyarrow as pa
import pyarrow.csv
import pyarrow.compute

from kukur import Dictionary, Metadata, SeriesSearch, SeriesSelector

from kukur.loader import Loader, from_config as loader_from_config
from kukur.exceptions import InvalidDataError, InvalidSourceException, KukurException
from kukur.metadata import fields
from kukur.source.metadata import MetadataMapper, MetadataValueMapper
from kukur.source.quality import QualityMapper


class InvalidMetadataError(KukurException):
    """Raised when the metadata is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid metadata: {message}")


def from_config(
    config: Dict[str, Any],
    metadata_mapper: MetadataMapper,
    metadata_value_mapper: MetadataValueMapper,
    quality_mapper: QualityMapper,
):
    """Create a new CSV data source from the given configuration dictionary."""
    loaders = CSVLoaders()
    if "path" in config:
        loaders.data = loader_from_config(config, "path", files_as_path=True)
    if "metadata" in config:
        loaders.metadata = loader_from_config(config, "metadata", "r")
    if "dictionary_dir" in config:
        loaders.dictionary = loader_from_config(config, "dictionary_dir", "r")
    data_format = config.get("format", "row")
    column_mapping = config.get("column_mapping", {})
    data_datetime_format = config.get("data_datetime_format", None)
    data_timezone = config.get("data_timezone", None)
    options = CSVSourceOptions(
        data_format,
        config.get("header_row", False),
        column_mapping,
        data_datetime_format,
        data_timezone,
    )
    metadata_fields: List[str] = config.get("metadata_fields", [])
    if len(metadata_fields) == 0:
        metadata_fields = config.get("fields", [])
    mappers = CSVMappers(metadata_mapper, metadata_value_mapper, quality_mapper)
    return CSVSource(options, metadata_fields, loaders, mappers)


@dataclass
class CSVLoaders:
    """Data loaders for CSV sources."""

    data: Optional[Loader] = None
    metadata: Optional[Loader] = None
    dictionary: Optional[Loader] = None


@dataclass
class CSVMappers:
    """Value mappers for CSV sources."""

    metadata: MetadataMapper
    metadata_values: MetadataValueMapper
    quality: QualityMapper


@dataclass
class CSVSourceOptions:
    """Options for a CSV source."""

    data_format: str
    header_row: bool
    column_mapping: Dict[str, str]
    data_datetime_format: Optional[str] = None
    data_timezone: Optional[str] = None


class CSVSource:
    """A CSV data source."""

    __loaders: CSVLoaders
    __options: CSVSourceOptions
    __metadata_fields: List[str]
    __mappers: CSVMappers

    def __init__(
        self,
        options: CSVSourceOptions,
        metadata_fields: List[str],
        loaders: CSVLoaders,
        mappers: CSVMappers,
    ):
        """Create a new CSV data source."""
        self.__loaders = loaders
        self.__metadata_fields = metadata_fields
        self.__options = options
        self.__mappers = mappers

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Search for series matching the given selector."""
        if self.__loaders.metadata is None:
            yield from self._search_in_data(selector)
            return
        with self.__loaders.metadata.open() as metadata_file:
            reader = csv.DictReader(metadata_file)
            for row in reader:
                if self.__mappers.metadata.from_kukur("series name") not in row:
                    raise InvalidMetadataError('column "series name" not found')
                series_name = row[self.__mappers.metadata.from_kukur("series name")]
                metadata = None
                if "series name" in selector.tags:
                    if series_name == selector.name:
                        metadata = Metadata(
                            SeriesSelector.from_tags(
                                selector.source, selector.tags, selector.field
                            )
                        )
                else:
                    selector_tags = selector.tags.copy()
                    selector_tags["series name"] = series_name
                    metadata = Metadata(
                        SeriesSelector.from_tags(
                            selector.source, selector_tags, selector.field
                        )
                    )

                if metadata is not None:
                    field_names = [field for field, _ in metadata.iter_names()]
                    if len(self.__metadata_fields) > 0:
                        field_names = self.__metadata_fields
                    for field in field_names:
                        if self.__mappers.metadata.from_kukur(field) in row:
                            try:
                                value = row[self.__mappers.metadata.from_kukur(field)]
                                metadata.coerce_field(
                                    field,
                                    self.__mappers.metadata_values.from_source(
                                        field, value
                                    ),
                                )
                            except ValueError:
                                pass
                    dictionary_name = metadata.get_field(fields.DictionaryName)
                    if dictionary_name is not None:
                        metadata.set_field(
                            fields.Dictionary, self.__get_dictionary(dictionary_name)
                        )
                    yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata, taking any configured metadata mapping into account."""
        metadata = Metadata(selector)
        if self.__loaders.metadata is None:
            return metadata

        with self.__loaders.metadata.open() as metadata_file:
            reader = csv.DictReader(metadata_file)
            for row in reader:
                if self.__mappers.metadata.from_kukur("series name") not in row:
                    raise InvalidMetadataError('column "series name" not found')
                if (
                    row[self.__mappers.metadata.from_kukur("series name")]
                    != selector.name
                ):
                    continue
                field_names = [field for field, _ in metadata.iter_names()]
                if len(self.__metadata_fields) > 0:
                    field_names = self.__metadata_fields
                for field in field_names:
                    if self.__mappers.metadata.from_kukur(field) in row:
                        try:
                            value = row[self.__mappers.metadata.from_kukur(field)]
                            metadata.coerce_field(
                                field,
                                self.__mappers.metadata_values.from_source(
                                    field, value
                                ),
                            )
                        except ValueError:
                            pass

            dictionary_name = metadata.get_field(fields.DictionaryName)
            if dictionary_name is not None:
                metadata.set_field(
                    fields.Dictionary, self.__get_dictionary(dictionary_name)
                )

        return metadata

    def __get_dictionary(self, set_name: str) -> Optional[Dictionary]:
        if self.__loaders.dictionary is None:
            return None
        if not self.__loaders.dictionary.has_child(f"{set_name}.csv"):
            return None
        with self.__loaders.dictionary.open_child(f"{set_name}.csv") as mapping_file:
            reader = csv.reader(mapping_file)
            mapping = {int(row[0]): row[1] for row in reader}
            return Dictionary(mapping)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Read data in one of the predefined formats.

        The complete CSV file will be loaded in an Arrow table during processing.
        """
        data = self.__read_all_data(selector)
        # pylint: disable=no-member
        on_or_after = pyarrow.compute.greater_equal(data["ts"], pa.scalar(start_date))
        before = pyarrow.compute.less(data["ts"], pa.scalar(end_date))
        return data.filter(pyarrow.compute.and_(on_or_after, before))

    def _search_in_data(
        self, search: SeriesSearch
    ) -> Generator[SeriesSelector, None, None]:
        if self.__loaders.data is None:
            return

        if self.__options.data_format == "row":
            yield from self._search_row(self.__loaders.data, search)
        elif self.__options.data_format == "pivot":
            yield from self._search_pivot(self.__loaders.data, search)

    def _search_row(
        self, loader: Loader, search: SeriesSearch
    ) -> Generator[SeriesSelector, None, None]:
        all_data = self._open_row_data(loader)
        # pylint: disable=no-member
        for name in pyarrow.compute.unique(all_data["series name"]):
            yield SeriesSelector(search.source, name.as_py())

    def _search_pivot(
        self, loader: Loader, search: SeriesSearch
    ) -> Generator[SeriesSelector, None, None]:
        all_data = self._open_pivot_data(loader)
        for name in all_data.column_names[1:]:
            yield SeriesSelector(search.source, name)

    def __read_all_data(self, selector: SeriesSelector) -> pa.Table:
        if self.__loaders.data is None:
            raise InvalidSourceException("No data path configured.")
        if self.__options.data_format == "pivot":
            return self._read_pivot_data(self.__loaders.data, selector)

        if self.__options.data_format == "dir":
            return self._read_directory_data(self.__loaders.data, selector)

        return self._read_row_data(self.__loaders.data, selector)

    def _read_row_data(self, loader: Loader, selector: SeriesSelector) -> pa.Table:
        all_data = self._open_row_data(loader)
        # pylint: disable=no-member
        data = all_data.filter(
            pyarrow.compute.equal(all_data["series name"], pa.scalar(selector.name))
        )
        return data.drop(["series name"])

    def _open_row_data(self, loader: Loader) -> pa.Table:
        columns = ["series name", "ts", "value"]
        timestamp_column = "ts"
        if "ts" in self.__options.column_mapping:
            timestamp_column = self.__options.column_mapping["ts"]

        if self.__mappers.quality.is_present():
            columns.append("quality")

        if not self.__options.header_row:
            read_options = pyarrow.csv.ReadOptions(column_names=columns)
        else:
            read_options = pyarrow.csv.ReadOptions()

        convert_options = _get_convert_options(
            timestamp_column,
            self.__options.data_datetime_format,
            self.__options.data_timezone,
        )

        try:
            all_data = pyarrow.csv.read_csv(
                loader.open(),
                read_options=read_options,
                convert_options=convert_options,
            )

            all_data = _map_columns(self.__options.column_mapping, all_data)
            all_data = _cast_ts_column(all_data, self.__options.data_timezone)
        except pa.lib.ArrowInvalid as arrow_invalid_exception:
            if self.__options.data_datetime_format is None:
                raise arrow_invalid_exception

            column_types = {timestamp_column: pa.string()}
            convert_options = pyarrow.csv.ConvertOptions(
                column_types=column_types,
            )
            all_data = pyarrow.csv.read_csv(
                loader.open(),
                read_options=read_options,
                convert_options=convert_options,
            )
            all_data = _map_columns(self.__options.column_mapping, all_data)
            all_data = _convert_timestamp(
                all_data,
                self.__options.data_datetime_format,
                self.__options.data_timezone,
            )

        if self.__mappers.quality.is_present():
            all_data = all_data.set_column(
                3, "quality", self._map_quality(all_data["quality"])
            )
        return all_data

    def _read_directory_data(
        self, loader: Loader, selector: SeriesSelector
    ) -> pa.Table:
        columns = ["ts", "value"]
        timestamp_column = "ts"
        if "ts" in self.__options.column_mapping:
            timestamp_column = self.__options.column_mapping["ts"]

        if self.__mappers.quality.is_present():
            columns.append("quality")
        if not self.__options.header_row:
            read_options = pyarrow.csv.ReadOptions(column_names=columns)
        else:
            read_options = pyarrow.csv.ReadOptions()

        convert_options = _get_convert_options(
            "ts", self.__options.data_datetime_format, self.__options.data_timezone
        )

        try:
            data = pyarrow.csv.read_csv(
                loader.open_child(f"{selector.tags['series name']}.csv"),
                read_options=read_options,
                convert_options=convert_options,
            )
            data = _map_columns(self.__options.column_mapping, data)
            data = _cast_ts_column(data, self.__options.data_timezone)
        except pa.lib.ArrowInvalid as arrow_invalid_exception:
            if self.__options.data_datetime_format is None:
                raise arrow_invalid_exception

            column_types = {timestamp_column: pa.string()}
            convert_options = pyarrow.csv.ConvertOptions(
                column_types=column_types,
            )
            data = pyarrow.csv.read_csv(
                loader.open_child(f"{selector.tags['series name']}.csv"),
                read_options=read_options,
                convert_options=convert_options,
            )
            data = _map_columns(self.__options.column_mapping, data)
            data = _convert_timestamp(
                data,
                self.__options.data_datetime_format,
                self.__options.data_timezone,
            )
        if self.__mappers.quality.is_present():
            return data.set_column(2, "quality", self._map_quality(data["quality"]))
        return data

    def _read_pivot_data(self, loader: Loader, selector: SeriesSelector) -> pa.Table:
        all_data = self._open_pivot_data(loader)
        if selector.name not in all_data.column_names:
            raise InvalidDataError(f'column "{selector.name}" not found')
        data = all_data.select(["ts", selector.name]).rename_columns(["ts", "value"])
        return data

    def _open_pivot_data(self, loader: Loader) -> pa.Table:
        timestamp_column = "ts"
        if "ts" in self.__options.column_mapping:
            timestamp_column = self.__options.column_mapping["ts"]

        convert_options = _get_convert_options(
            timestamp_column,
            self.__options.data_datetime_format,
            self.__options.data_timezone,
        )
        try:
            all_data = pyarrow.csv.read_csv(
                loader.open(), convert_options=convert_options
            )
            all_data = _map_pivot_columns(self.__options.column_mapping, all_data)
            all_data = _cast_ts_column(all_data, self.__options.data_timezone)
        except pa.lib.ArrowInvalid as arrow_invalid_exception:
            if self.__options.data_datetime_format is None:
                raise arrow_invalid_exception

            column_types = {timestamp_column: pa.string()}
            convert_options = pyarrow.csv.ConvertOptions(
                column_types=column_types,
            )
            all_data = pyarrow.csv.read_csv(
                loader.open(),
                convert_options=convert_options,
            )
            all_data = _map_pivot_columns(self.__options.column_mapping, all_data)
            all_data = _convert_timestamp(
                all_data,
                self.__options.data_datetime_format,
                self.__options.data_timezone,
            )
        return all_data

    def _map_quality(self, quality_data: pa.Array) -> pa.Array:
        return self.__mappers.quality.map_array(quality_data)


def _get_convert_options(
    timestamp_column: str,
    data_datetime_format: Optional[str],
    data_timezone: Optional[str],
) -> pyarrow.csv.ConvertOptions:
    column_types = {
        timestamp_column: pa.timestamp("us")
        if data_timezone is not None
        else pa.timestamp("us", "utc")
    }

    timestamp_parsers = (
        [data_datetime_format] if data_datetime_format is not None else None
    )
    return pyarrow.csv.ConvertOptions(
        column_types=column_types,
        timestamp_parsers=timestamp_parsers,
    )


def _cast_ts_column(data: pa.Table, data_timezone: Optional[str]) -> pa.Table:
    if data_timezone is None:
        return data

    # pylint: disable=no-member
    return data.set_column(
        data.column_names.index("ts"),
        "ts",
        pyarrow.compute.assume_timezone(data["ts"], data_timezone),
    )


def _convert_timestamp(
    data: pa.Table, data_datetime_format: str, data_timezone: Optional[str]
) -> pa.Table:
    # pylint: disable=no-member
    data = data.set_column(
        data.column_names.index("ts"),
        "ts",
        [
            [
                datetime.strptime(timestamp.as_py(), data_datetime_format)
                for timestamp in data["ts"]
            ]
        ],
    )

    if data_timezone is None:
        if data["ts"][0].as_py().tzinfo is None:
            # pylint: disable=no-member
            return data.set_column(
                data.column_names.index("ts"),
                "ts",
                pyarrow.compute.assume_timezone(data["ts"], "UTC"),
            )
        return data

    # pylint: disable=no-member
    return data.set_column(
        data.column_names.index("ts"),
        "ts",
        pyarrow.compute.assume_timezone(data["ts"], data_timezone),
    )


def _map_columns(column_mapping: Dict[str, str], data: pa.Table) -> pa.Table:
    if len(column_mapping) == 0:
        return data

    columns = {
        "ts": data[column_mapping["ts"]],
        "value": data[column_mapping["value"]],
    }

    if "series name" in column_mapping:
        columns["series name"] = data[column_mapping["series name"]]

    if "quality" in column_mapping:
        columns["quality"] = data[column_mapping["quality"]]

    return pa.Table.from_pydict(columns)


def _map_pivot_columns(column_mapping: Dict[str, str], data: pa.Table) -> pa.Table:
    ts_column_name = data.column_names[0]
    if "ts" in column_mapping:
        ts_column_name = column_mapping["ts"]

    ts_column = data[ts_column_name]
    return data.drop([ts_column_name]).add_column(0, "ts", ts_column)
