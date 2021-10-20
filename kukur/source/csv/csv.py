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
from typing import Any, Dict, Generator, List, Optional

import pyarrow as pa
import pyarrow.csv
import pyarrow.compute

from kukur import Dictionary, Metadata, SeriesSelector

from kukur.loader import Loader, from_config as loader_from_config
from kukur.exceptions import InvalidDataError, InvalidSourceException
from kukur.metadata import fields
from kukur.source.metadata import MetadataMapper, MetadataValueMapper
from kukur.source.quality import QualityMapper


class InvalidMetadataError(Exception):
    """Raised when the metadata is invalid."""

    def __init__(self, message: str):
        Exception.__init__(self, f"invalid metadata: {message}")


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
    metadata_fields: List[str] = config.get("metadata_fields", [])
    if len(metadata_fields) == 0:
        metadata_fields = config.get("fields", [])
    mappers = CSVMappers(metadata_mapper, metadata_value_mapper, quality_mapper)
    return CSVSource(data_format, metadata_fields, loaders, mappers)


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


class CSVSource:
    """A CSV data source."""

    __loaders: CSVLoaders
    __data_format: str
    __metadata_fields: List[str]
    __mappers: CSVMappers

    def __init__(
        self,
        data_format: str,
        metadata_fields: List[str],
        loaders: CSVLoaders,
        mappers: CSVMappers,
    ):
        """Create a new CSV data source."""
        self.__loaders = loaders
        self.__metadata_fields = metadata_fields
        self.__data_format = data_format
        self.__mappers = mappers

    def search(self, selector: SeriesSelector) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        if self.__loaders.metadata is None:
            return

        with self.__loaders.metadata.open() as metadata_file:
            reader = csv.DictReader(metadata_file)
            for row in reader:
                if self.__mappers.metadata.from_kukur("series name") not in row:
                    raise InvalidMetadataError('column "series name" not found')
                series_name = row[self.__mappers.metadata.from_kukur("series name")]
                metadata = None
                if selector.name is not None:
                    if series_name == selector.name:
                        metadata = Metadata(
                            SeriesSelector(selector.source, series_name)
                        )
                else:
                    metadata = Metadata(SeriesSelector(selector.source, series_name))

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

    def __read_all_data(self, selector: SeriesSelector) -> pa.Table:
        if self.__loaders.data is None:
            raise InvalidSourceException("No data path configured.")
        if self.__data_format == "pivot":
            return _read_pivot_data(self.__loaders.data, selector)

        if self.__data_format == "dir":
            return self._read_directory_data(self.__loaders.data, selector)

        return self._read_row_data(self.__loaders.data, selector)

    def _read_row_data(self, loader: Loader, selector: SeriesSelector) -> pa.Table:
        columns = ["series name", "ts", "value"]
        if self.__mappers.quality.is_present():
            columns.append("quality")
        read_options = pyarrow.csv.ReadOptions(column_names=columns)
        convert_options = pyarrow.csv.ConvertOptions(
            column_types={"ts": pa.timestamp("us", "utc")},
        )
        all_data = pyarrow.csv.read_csv(
            loader.open(), read_options=read_options, convert_options=convert_options
        )
        if self.__mappers.quality.is_present():
            kukur_quality_values = self._map_quality(all_data["quality"])
            all_data = all_data.set_column(
                3, "quality", pa.array(kukur_quality_values, type=pa.int8())
            )

        # pylint: disable=no-member
        data = all_data.filter(
            pyarrow.compute.equal(all_data["series name"], pa.scalar(selector.name))
        )
        return data.drop(["series name"])

    def _read_directory_data(
        self, loader: Loader, selector: SeriesSelector
    ) -> pa.Table:
        columns = ["ts", "value"]
        if self.__mappers.quality.is_present():
            columns.append("quality")
        read_options = pyarrow.csv.ReadOptions(column_names=columns)
        convert_options = pyarrow.csv.ConvertOptions(
            column_types={"ts": pa.timestamp("us", "utc")},
        )
        all_data = pyarrow.csv.read_csv(
            loader.open_child(f"{selector.name}.csv"),
            read_options=read_options,
            convert_options=convert_options,
        )
        if self.__mappers.quality.is_present():
            kukur_quality_values = self._map_quality(all_data["quality"])
            return all_data.set_column(
                2, "quality", pa.array(kukur_quality_values, type=pa.int8())
            )
        return all_data

    def _map_quality(self, quality_data: pa.array) -> pa.Table:
        return [
            self.__mappers.quality.from_source(source_quality_value.as_py())
            for source_quality_value in quality_data
        ]


def _read_pivot_data(loader: Loader, selector: SeriesSelector) -> pa.Table:
    all_data = pyarrow.csv.read_csv(loader.open())
    if selector.name not in all_data.column_names:
        raise InvalidDataError(f'column "{selector.name}" not found')
    columns = ["ts", "value"]
    schema = pa.schema([("ts", pa.timestamp("us", "utc")), ("value", pa.float64())])
    return all_data.select([0, selector.name]).rename_columns(columns).cast(schema)
