"""Base classes for connections to SQL-like sources from Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timezone, tzinfo
from typing import Generator, Optional, Union

import dateutil.parser
import pyarrow as pa

from kukur import Dictionary, Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import KukurException
from kukur.metadata import fields
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper

logger = logging.getLogger(__name__)


class InvalidMetadataError(KukurException):
    """Raised when the metadata is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid metadata: {message}")


@dataclass
class SQLConfig:  # pylint: disable=too-many-instance-attributes
    """Configuration settings for a SQL connection."""

    connection_string: str
    query_string_parameters: bool = False
    list_query: Optional[str] = None
    list_columns: list[str] = field(default_factory=list)
    metadata_query: Optional[str] = None
    metadata_columns: list[str] = field(default_factory=list)
    dictionary_query: Optional[str] = None
    data_query: Optional[str] = None
    data_query_datetime_format: Optional[str] = None
    data_timezone: Optional[tzinfo] = None
    data_query_timezone: Optional[tzinfo] = None
    enable_trace_logging: bool = False
    query_timeout_seconds: int = 0

    @classmethod
    def from_dict(cls, data):
        """Create a new SQL data source from a configuration dict."""
        if "connection_string" in data:
            connection_string = data["connection_string"]
        else:
            with open(data["connection_string_path"], encoding="utf-8") as f:
                connection_string = f.read().strip()

        config = SQLConfig(connection_string)

        config.list_query = data.get("list_query")
        if config.list_query is None and "list_query_path" in data:
            with open(data["list_query_path"], encoding="utf-8") as f:
                config.list_query = f.read()
        config.list_columns = data.get("list_columns", [])
        config.metadata_query = data.get("metadata_query")
        if config.metadata_query is None and "metadata_query_path" in data:
            with open(data["metadata_query_path"], encoding="utf-8") as f:
                config.metadata_query = f.read()
        config.metadata_columns = data.get("metadata_columns", [])
        config.dictionary_query = data.get("dictionary_query")
        if config.dictionary_query is None and "dictionary_query_path" in data:
            with open(data["dictionary_query_path"], encoding="utf-8") as f:
                config.dictionary_query = f.read()
        config.data_query = data.get("data_query")
        if config.data_query is None and "data_query_path" in data:
            with open(data["data_query_path"], encoding="utf-8") as f:
                config.data_query = f.read()

        config.data_query_datetime_format = data.get("data_query_datetime_format")
        config.query_string_parameters = data.get("query_string_parameters", False)
        if "data_timezone" in data:
            config.data_timezone = dateutil.tz.gettz(data.get("data_timezone"))
        if "data_query_timezone" in data:
            config.data_query_timezone = dateutil.tz.gettz(
                data.get("data_query_timezone")
            )
        if "enable_trace_logging" in data:
            config.enable_trace_logging = data.get("enable_trace_logging", False)
        config.query_timeout_seconds = data.get("query_timeout_seconds", 0)

        return config


class BaseSQLSource(ABC):
    """A SQL data source.

    Subclasses should implement a connect() method.
    """

    _config: SQLConfig
    _metadata_value_mapper: MetadataValueMapper
    _quality_mapper: QualityMapper

    def __init__(
        self,
        config: SQLConfig,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        self._config = config
        self._metadata_value_mapper = metadata_value_mapper
        self._quality_mapper = quality_mapper

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[SeriesSelector, Metadata], None, None]:
        """Search for time series matching the given selector."""
        if self._config.list_query is None:
            return
        if len(self._config.list_columns) == 0:
            for result in self.__search_names(selector):
                yield result
            return
        for metadata in self.__search_metadata(selector):
            yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata from the DB-API connection."""
        metadata = Metadata(selector)
        if self._config.metadata_query is None:
            return metadata
        connection = self.connect()
        cursor = connection.cursor()

        query = self._config.metadata_query
        params = [selector.name]
        if self._config.query_string_parameters:
            query = query.format(*params)
            params = []
        cursor.execute(query, params)
        row = cursor.fetchone()
        if row:
            for i, name in enumerate(self._config.metadata_columns):
                value = row[i]
                if value is None:
                    continue
                if isinstance(value, str) and value == "":
                    continue
                try:
                    metadata.coerce_field(
                        name, self._metadata_value_mapper.from_source(name, value)
                    )
                except ValueError:
                    pass

        dictionary_name = metadata.get_field(fields.DictionaryName)
        if dictionary_name is not None:
            metadata.set_field(
                fields.Dictionary, self.__query_dictionary(cursor, dictionary_name)
            )
        return metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data using the specified DB-API query."""
        if self._config.data_query is None:
            return pa.Table.from_pydict({"ts": [], "value": [], "quality": []})
        connection = self.connect()
        cursor = connection.cursor()

        query = self._config.data_query
        params = [
            selector.name,
            self.__format_date(start_date),
            self.__format_date(end_date),
        ]
        if self._config.query_string_parameters:
            query = query.format(*params)
            params = []

        cursor.execute(query, params)

        timestamps = []
        values = []
        qualities = []
        for row in cursor:
            if self._config.enable_trace_logging:
                logger.info(
                    'Data from "%s (%s)" at %s has value %s with type %s',
                    selector.source,
                    selector.name,
                    row[0],
                    row[1],
                    type(row[1]),
                )
            if isinstance(row[0], datetime):
                ts = row[0]
            else:
                ts = dateutil.parser.parse(row[0])
            if self._config.data_timezone:
                ts = ts.replace(tzinfo=self._config.data_timezone)
            ts = ts.astimezone(timezone.utc)
            value = row[1]
            if value is None:
                value = float("nan")
            if isinstance(value, bytes):
                continue
            if isinstance(value, datetime):
                value = value.isoformat()
            if self._quality_mapper.is_present():
                quality = self._quality_mapper.from_source(row[2])
                qualities.append(quality)
            timestamps.append(ts)
            values.append(value)
        if self._quality_mapper.is_present():
            return pa.Table.from_pydict(
                {"ts": timestamps, "value": values, "quality": qualities}
            )
        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def __search_names(
        self, selector: SeriesSearch
    ) -> Generator[SeriesSelector, None, None]:
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute(self._config.list_query)

        for (series_name,) in cursor:
            yield SeriesSelector(selector.source, series_name)

    def __search_metadata(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        connection = self.connect()
        cursor = connection.cursor()
        dictionary_cursor = None
        if self._config.dictionary_query is not None:
            dictionary_cursor = self.connect().cursor()

        cursor.execute(self._config.list_query)
        series_name_index = None
        for i, name in enumerate(self._config.list_columns):
            if name == "series name":
                series_name_index = i
        if series_name_index is None:
            raise InvalidMetadataError('column "series name" not found')
        for row in cursor:
            series_selector = SeriesSelector(selector.source, row[series_name_index])
            metadata = Metadata(series_selector)
            for i, name in enumerate(self._config.list_columns):
                if i == series_name_index:
                    continue
                value = row[i]
                if value is None:
                    continue
                if isinstance(value, str) and value == "":
                    continue
                try:
                    metadata.coerce_field(
                        name, self._metadata_value_mapper.from_source(name, value)
                    )
                except ValueError:
                    pass
            dictionary_name = metadata.get_field(fields.DictionaryName)
            if dictionary_name is not None and dictionary_cursor is not None:
                metadata.set_field(
                    fields.Dictionary,
                    self.__query_dictionary(dictionary_cursor, dictionary_name),
                )
            yield metadata

    def __query_dictionary(self, cursor, dictionary_name: str) -> Optional[Dictionary]:
        if self._config.dictionary_query is None:
            return None
        mapping: dict[int, str] = {}

        query = self._config.dictionary_query
        params = [dictionary_name]
        if self._config.query_string_parameters:
            query = query.format(*params)
            params = []
        cursor.execute(query, params)
        for row in cursor.fetchall():
            mapping[int(row[0])] = row[1]
        if len(mapping) == 0:
            return None
        return Dictionary(mapping)

    def __format_date(self, date):
        if self._config.data_query_timezone:
            date = date.astimezone(self._config.data_query_timezone).replace(
                tzinfo=None
            )
        if self._config.data_query_datetime_format is not None:
            return date.strftime(self._config.data_query_datetime_format)
        return date

    @abstractmethod
    def connect(self):
        """Create a new PEP-249 connection."""
        ...
