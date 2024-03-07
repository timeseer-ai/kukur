"""Base classes for connections to SQL-like sources from Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import date, datetime, timezone, tzinfo
from decimal import Decimal
from typing import Dict, Generator, List, Optional, Tuple, Union

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


class InvalidConfigurationError(KukurException):
    """Raised when the source configuration is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid configuration: {message}")


@dataclass
class SQLConfig:  # pylint: disable=too-many-instance-attributes
    """Configuration settings for a SQL connection."""

    connection_string: Optional[str]
    tag_columns: List[str]
    query_string_parameters: bool = False
    list_query: Optional[str] = None
    list_columns: List[str] = field(default_factory=list)
    field_columns: Optional[List[str]] = None
    metadata_query: Optional[str] = None
    metadata_columns: List[str] = field(default_factory=list)
    dictionary_query: Optional[str] = None
    data_query: Optional[str] = None
    data_query_datetime_format: Optional[str] = None
    data_timezone: Optional[tzinfo] = None
    data_query_timezone: Optional[tzinfo] = None
    enable_trace_logging: bool = False
    query_timeout_seconds: Optional[int] = None
    type_checking_row_limit: int = 300

    @classmethod
    def from_dict(cls, data):
        """Create a new SQL data source from a configuration dict."""
        connection_string = None
        if "connection_string" in data:
            connection_string = data["connection_string"]
        elif "connection_string_path" in data:
            with open(data["connection_string_path"], encoding="utf-8") as f:
                connection_string = f.read().strip()

        config = SQLConfig(connection_string, data.get("tag_columns", ["series name"]))

        config.list_query = data.get("list_query")
        if config.list_query is None and "list_query_path" in data:
            with open(data["list_query_path"], encoding="utf-8") as f:
                config.list_query = f.read()
        config.list_columns = data.get("list_columns", [])
        config.field_columns = data.get("field_columns")
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
        if data.get("query_timeout_enable", True):
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
        params = [selector.tags[tag_name] for tag_name in self._config.tag_columns]

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

    def get_data(  # noqa: PLR0912, PLR0915
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data using the specified DB-API query."""
        if self._config.data_query is None:
            return pa.Table.from_pydict({"ts": [], "value": [], "quality": []})
        connection = self.connect()
        cursor = connection.cursor()

        query, params = self.__prepare_data_query(selector, start_date, end_date)
        cursor.execute(query, params)

        timestamps = []
        values: list[Union[float, str]] = []
        qualities = []
        type_count: dict[str, int] = defaultdict(int)
        detected_type = None
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
            elif isinstance(row[0], date):
                ts = datetime(
                    row[0].year, row[0].month, row[0].day, tzinfo=timezone.utc
                )
            else:
                ts = dateutil.parser.parse(row[0])
            if self._config.data_timezone:
                ts = ts.replace(tzinfo=self._config.data_timezone)
            ts = ts.astimezone(timezone.utc)
            value = row[1]
            if isinstance(value, str):
                type_count["str"] += 1
            if type(value) in [int, float, Decimal]:
                type_count["number"] += 1
            if isinstance(value, datetime):
                type_count["datetime"] += 1

            if len(values) == self._config.type_checking_row_limit:
                detected_type = _get_main_type(type_count)
                _coerce_types(values, detected_type)

            if detected_type == "number":
                if value is None or type(value) not in [int, float, Decimal]:
                    value = float("nan")
            if detected_type == "str":
                if value is not None and type(value) in [int, float, Decimal]:
                    value = str(value)

            if isinstance(value, bytes):
                continue
            if isinstance(value, (datetime, date)):
                value = value.isoformat()
            elif isinstance(value, Decimal):
                value = float(value)
            if self._quality_mapper.is_present():
                quality = self._quality_mapper.from_source(row[2])
                qualities.append(quality)
            timestamps.append(ts)
            values.append(value)

        if len(values) < self._config.type_checking_row_limit:
            detected_type = _get_main_type(type_count)
            _coerce_types(values, detected_type)

        if self._quality_mapper.is_present():
            return pa.Table.from_pydict(
                {"ts": timestamps, "value": values, "quality": qualities}
            )
        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def __prepare_data_query(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> Tuple[str, List]:
        assert self._config.data_query is not None

        try:
            query = self._config.data_query.format(field=selector.field)
        except (TypeError, IndexError):
            query = self._config.data_query
        params = [selector.tags[tag_name] for tag_name in self._config.tag_columns]
        params.extend(
            [
                self.__format_date(start_date),
                self.__format_date(end_date),
            ]
        )
        if self._config.query_string_parameters:
            query = query.format(*params)
            params = []
        return query, params

    def __search_names(
        self, selector: SeriesSearch
    ) -> Generator[SeriesSelector, None, None]:
        connection = self.connect()
        cursor = connection.cursor()
        cursor.execute(self._config.list_query)

        for tag_values in cursor:
            if len(tag_values) != len(self._config.tag_columns):
                raise InvalidConfigurationError(
                    "number of tag_columns does not match result of list_query"
                )
            tags = dict(zip(self._config.tag_columns, tag_values))
            if self._config.field_columns is not None:
                for field_name in self._config.field_columns:
                    yield SeriesSelector(selector.source, tags, field_name)
            else:
                yield SeriesSelector(selector.source, tags)

    def __search_metadata(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        for tag_name in self._config.tag_columns:
            if tag_name not in self._config.list_columns:
                raise InvalidMetadataError(f'tag column "{tag_name}" not found')

        connection = self.connect()
        cursor = connection.cursor()
        dictionary_cursor = None
        if self._config.dictionary_query is not None:
            dictionary_cursor = self.connect().cursor()

        cursor.execute(self._config.list_query)
        tag_column_indices = []

        for row in cursor:
            tags = {}
            for i, name in enumerate(self._config.list_columns):
                if name in self._config.tag_columns:
                    if i not in tag_column_indices:
                        tag_column_indices.append(i)
                    tags[name] = row[i]

            if self._config.field_columns is not None:
                for field_name in self._config.field_columns:
                    series_selector = SeriesSelector(selector.source, tags, field_name)
                    yield self.__get_metadata(
                        row, series_selector, tag_column_indices, dictionary_cursor
                    )
            else:
                series_selector = SeriesSelector(selector.source, tags)
                yield self.__get_metadata(
                    row, series_selector, tag_column_indices, dictionary_cursor
                )

    def __get_metadata(
        self,
        row,
        selector: SeriesSelector,
        tag_column_indices: List[int],
        dictionary_cursor,
    ) -> Metadata:
        metadata = Metadata(selector)
        for i, name in enumerate(self._config.list_columns):
            if i in tag_column_indices:
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
        return metadata

    def __query_dictionary(self, cursor, dictionary_name: str) -> Optional[Dictionary]:
        if self._config.dictionary_query is None:
            return None
        mapping: Dict[int, str] = {}

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


def _get_main_type(type_count: dict[str, int]) -> str:
    if len(type_count) > 0:
        detected_type = sorted(type_count, reverse=True)[0]
        if type_count[detected_type] > (sum(type_count.values()) * 0.9):
            return detected_type
        return "str"
    return "number"


def _coerce_types(values: list[Union[float, str]], detected_type: str):
    for i, value in enumerate(values):
        if detected_type == "str":
            if value is not None and type(value) in [str, Decimal, float]:
                values[i] = str(value)
        elif detected_type == "number":
            if value is None or type(value) not in [Decimal, float]:
                values[i] = float("nan")
