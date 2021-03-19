"""
csv contains the CSV data source for Timeseer.

Three formats are supported:
- row based, with many series in one file containing one line per data point
- directory based, with one file per series
- pivot, with many series as columns in one file
"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import csv

from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Generator, Optional

import pyarrow as pa
import pyarrow.csv
import pyarrow.compute

from kukur import Dictionary, Metadata, SeriesSelector

from kukur.loader import Loader, from_config as loader_from_config
from kukur.exceptions import InvalidDataError, InvalidSourceException


class InvalidMetadataError(Exception):
    """Raised when the metadata is invalid."""

    def __init__(self, message: str):
        Exception.__init__(self, f"invalid metadata: {message}")


class InvalidMetadataMappingError(Exception):
    """Raised when the metadata mapping is invalid."""

    def __init__(self, message: str):
        Exception.__init__(self, f"invalid metadata mapping: {message}")


def from_config(config: Dict[str, str]):
    """Create a new CSV data source from the given configuration dictionary."""
    loaders = CSVLoaders()
    if "path" in config:
        loaders.data = loader_from_config(config, "path", files_as_path=True)
    if "metadata" in config:
        loaders.metadata = loader_from_config(config, "metadata", "r")
    if "metadata_mapping" in config:
        loaders.metadata_mapping = loader_from_config(config, "metadata_mapping", "r")
    if "dictionary_dir" in config:
        loaders.dictionary = loader_from_config(config, "dictionary_dir", "r")
    data_format = config.get("format", "row")
    return CSVSource(data_format, loaders)


@dataclass
class CSVLoaders:
    """Data loaders for CSV sources."""

    data: Optional[Loader] = None
    metadata: Optional[Loader] = None
    metadata_mapping: Optional[Loader] = None
    dictionary: Optional[Loader] = None


class CSVSource:
    """A CSV data source for Timeseer."""

    __loaders: CSVLoaders
    __data_format: str

    def __init__(
        self,
        data_format: str,
        loaders: CSVLoaders,
    ):
        """Create a new CSV data source."""
        self.__loaders = loaders
        self.__data_format = data_format

    def search(self, selector: SeriesSelector) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        if self.__loaders.metadata is None:
            return

        dummy_metadata = Metadata(selector)
        mapping = self.__get_metadata_mapping(dummy_metadata)

        with self.__loaders.metadata.open() as metadata_file:
            reader = csv.DictReader(metadata_file)
            for row in reader:
                if mapping["series name"] not in row:
                    raise InvalidMetadataError('column "series name" not found')
                series_name = row[mapping["series name"]]
                metadata = None
                if selector.name is not None:
                    if series_name == selector.name:
                        metadata = Metadata(
                            SeriesSelector(selector.source, series_name)
                        )
                else:
                    metadata = Metadata(SeriesSelector(selector.source, series_name))

                if metadata is not None:
                    for field, _ in metadata:
                        if mapping[field] in row:
                            try:
                                metadata.set_field(field, row[mapping[field]])
                            except ValueError:
                                pass
                    if metadata.dictionary_name is not None:
                        metadata.dictionary = self.__get_dictionary(
                            metadata.dictionary_name
                        )
                    yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata, taking any configured metadata mapping into account."""
        metadata = Metadata(selector)
        if self.__loaders.metadata is None:
            return metadata

        mapping = self.__get_metadata_mapping(metadata)

        with self.__loaders.metadata.open() as metadata_file:
            reader = csv.DictReader(metadata_file)
            for row in reader:
                if mapping["series name"] not in row:
                    raise InvalidMetadataError('column "series name" not found')
                if row[mapping["series name"]] != selector.name:
                    continue
                for field, _ in metadata:
                    if mapping[field] in row:
                        try:
                            metadata.set_field(field, row[mapping[field]])
                        except ValueError:
                            pass

            if metadata.dictionary_name is not None:
                metadata.dictionary = self.__get_dictionary(metadata.dictionary_name)

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
            return _read_directory_data(self.__loaders.data, selector)

        return _read_row_data(self.__loaders.data, selector)

    def __get_metadata_mapping(self, metadata: Metadata) -> Dict[str, str]:
        mapping = {k: k for k, _ in metadata}
        mapping["series name"] = "series name"

        if self.__loaders.metadata_mapping is None:
            return mapping

        with self.__loaders.metadata_mapping.open() as mapping_file:
            reader = csv.reader(mapping_file)
            for row in reader:
                if len(row) != 2:
                    raise InvalidMetadataMappingError("row length is not 2")
                if row[0] in mapping:
                    mapping[row[0]] = row[1]

        return mapping


def _read_pivot_data(loader: Loader, selector: SeriesSelector) -> pa.Table:
    all_data = pyarrow.csv.read_csv(loader.open())
    if selector.name not in all_data.column_names:
        raise InvalidDataError(f'column "{selector.name}" not found')
    schema = pa.schema([("ts", pa.timestamp("us", "utc")), ("value", pa.float64())])
    return (
        all_data.select([0, selector.name]).rename_columns(["ts", "value"]).cast(schema)
    )


def _read_row_data(loader: Loader, selector: SeriesSelector) -> pa.Table:
    read_options = pyarrow.csv.ReadOptions(column_names=["series name", "ts", "value"])
    convert_options = pyarrow.csv.ConvertOptions(
        column_types={"ts": pa.timestamp("us", "utc")}
    )
    all_data = pyarrow.csv.read_csv(
        loader.open(), read_options=read_options, convert_options=convert_options
    )
    # pylint: disable=no-member
    data = all_data.filter(
        pyarrow.compute.equal(all_data["series name"], pa.scalar(selector.name))
    )
    return data.drop(["series name"])


def _read_directory_data(loader: Loader, selector: SeriesSelector) -> pa.Table:
    read_options = pyarrow.csv.ReadOptions(column_names=["ts", "value"])
    convert_options = pyarrow.csv.ConvertOptions(
        column_types={"ts": pa.timestamp("us", "utc")}
    )
    return pyarrow.csv.read_csv(
        loader.open_child(f"{selector.name}.csv"),
        read_options=read_options,
        convert_options=convert_options,
    )
