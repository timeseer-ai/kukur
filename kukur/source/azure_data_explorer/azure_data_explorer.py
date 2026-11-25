"""Connections to azure data explorer data sources from Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

import pyarrow as pa

from kukur.source.azure_data_explorer.connection import (
    HAS_AZURE_IDENTITY,
    HAS_KUSTO,
    DataExplorerConnection,
    DataExplorerConnectionConfiguration,
)
from kukur.source.token_cache import TokenCache

try:
    from azure.kusto.data import ClientRequestProperties
    from azure.kusto.data._models import KustoResultRow
except ImportError:
    pass


from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import KukurException, MissingModuleException
from kukur.source.metadata import MetadataMapper, MetadataValueMapper

logger = logging.getLogger(__name__)


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        super().__init__(self, f"Connection error: {message}")


@dataclass
class DataExplorerConfiguration:
    """Data Explorer source configuration."""

    connection: DataExplorerConnectionConfiguration
    table: str | None
    timestamp_column: str
    tag_columns: list[str]
    field_columns: list[str]
    metadata_columns: list[str]
    query_timeout_seconds: int | None
    list_query: str | None
    list_columns: list[str] | None
    data_query: str | None
    data_query_named_parameters: dict[str, str] | None


def from_config(
    config: dict[str, Any],
    metadata_mapper: MetadataMapper,
    metadata_value_mapper: MetadataValueMapper,
    token_cache: TokenCache,
):
    """Create a new Azure Data Explorer data source."""
    table = config.get("table")
    timestamp_column = config.get("timestamp_column", "ts")
    tag_columns = config.get("tag_columns", [])
    field_columns = config.get("field_columns", [])
    metadata_columns = config.get("metadata_columns", [])
    return DataExplorerSource(
        DataExplorerConfiguration(
            DataExplorerConnectionConfiguration.from_config(config),
            table,
            timestamp_column,
            tag_columns,
            field_columns,
            metadata_columns,
            config.get("query_timeout_seconds"),
            config.get("list_query"),
            config.get("list_columns"),
            config.get("data_query"),
            config.get("data_query_named_parameters"),
        ),
        metadata_mapper,
        metadata_value_mapper,
        token_cache,
    )


class _Sleeper:
    def sleep(self, sleep_seconds: int):
        time.sleep(sleep_seconds)


class DataExplorerSource:  # pylint: disable=too-many-instance-attributes
    """An Azure Data Explorer data source."""

    def __init__(
        self,
        config: DataExplorerConfiguration,
        metadata_mapper: MetadataMapper,
        metadata_value_mapper: MetadataValueMapper,
        token_cache: TokenCache,
    ):
        if not HAS_AZURE_IDENTITY:
            raise MissingModuleException("azure-identity")

        if not HAS_KUSTO:
            raise MissingModuleException("azure-kusto-data", "data_explorer")

        self.__metadata_mapper = metadata_mapper
        self.__metadata_value_mapper = metadata_value_mapper
        self.__config = config
        self.__token_cache = token_cache
        self._sleeper = _Sleeper()

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        if len(self.__config.tag_columns) == 0:
            raise KukurException("Define tags to support listing time series")

        if self.__config.list_query is not None:
            yield from self._search_query(selector, self.__config.list_query)
        elif len(self.__config.metadata_columns) == 0:
            yield from self._search_distinct(selector)
        else:
            yield from self._search_metadata(selector)

    def _search_query(
        self, selector: SeriesSearch, list_query: str
    ) -> Generator[Metadata, None, None]:
        for row in self._query(list_query):
            tags = {}
            for tag in self.__config.tag_columns:
                tags[tag] = row[tag]
            for field in self.__config.field_columns:
                metadata = Metadata(SeriesSelector(selector.source, tags, field))
                for metadata_field in self.__config.metadata_columns:
                    if row[metadata_field] is None:
                        continue
                    metadata.coerce_field(
                        metadata_field,
                        self.__metadata_value_mapper.from_source(
                            metadata_field, row[metadata_field]
                        ),
                    )
                yield metadata

    def _search_distinct(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        query = f"['{self.__config.table}'] | distinct {', '.join(self.__config.tag_columns)}"
        for row in self._query(query):
            tags = {}
            for tag in self.__config.tag_columns:
                tags[tag] = row[tag]
            for field in self.__config.field_columns:
                yield Metadata(SeriesSelector(selector.source, tags, field))

    def _search_metadata(
        self, selector: SeriesSearch
    ) -> Generator[Metadata, None, None]:
        summaries = [
            f"['{name}']=arg_max(['{self.__config.timestamp_column}'], ['{name}'])"
            for name in self.__config.metadata_columns
        ]
        renames = [f"['{name}']=['{name}1']" for name in self.__config.metadata_columns]
        query = f"""['{self.__config.table}']
            | summarize {", ".join(summaries)} by {", ".join(_add_square_brackets(self.__config.tag_columns))}
            | project-away {", ".join(_add_square_brackets(self.__config.metadata_columns))}
            | project-rename {", ".join(renames)}
        """
        for row in self._query(query):
            tags = {}
            for tag in self.__config.tag_columns:
                tags[tag] = row[tag]
            for field in self.__config.field_columns:
                series_selector = SeriesSelector(selector.source, tags, field)
                metadata = Metadata(series_selector)
                for column_name in self.__config.metadata_columns:
                    metadata.coerce_field(
                        self.__metadata_mapper.from_source(column_name),
                        self.__metadata_value_mapper.from_source(
                            self.__metadata_mapper.from_source(column_name),
                            row[column_name],
                        ),
                    )
                yield metadata

    # pylint: disable=no-self-use
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        if selector.field not in self.__config.field_columns:
            raise KukurException(f"Unknown field: {selector.field}")

        query, props = self._prepare_data_query(selector, start_date, end_date)

        ts = self.__config.timestamp_column
        timestamps = []
        values = []
        for row in self._query(query, props):
            timestamps.append(row[ts])
            values.append(row[selector.field])

        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def _prepare_data_query(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ):
        if self.__config.data_query is not None:
            return self._prepare_custom_data_query(selector, start_date, end_date)
        return self._prepare_simple_data_query(selector, start_date, end_date)

    def _prepare_simple_data_query(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ):
        params = ["startDate: string", "endDate: string"]
        props = ClientRequestProperties()
        props.set_parameter("startDate", start_date.isoformat())
        props.set_parameter("endDate", end_date.isoformat())
        for i, tag_column in enumerate(self.__config.tag_columns):
            params.append(f"tag_{i}: string")
            props.set_parameter(f"tag_{i}", selector.tags[tag_column])

        ts = self.__config.timestamp_column

        query = f"""declare query_parameters ({", ".join(params)});
        ['{self.__config.table}']
            | where ['{ts}'] >= todatetime(startDate)
            | where ['{ts}'] <= todatetime(endDate)
        """

        for i, tag_column in enumerate(self.__config.tag_columns):
            query += f" | where ['{tag_column}']==tag_{i}"

        query = f"{query} | project ['{ts}'], ['{selector.field}']"
        query = f"{query} | sort by ['{ts}'] asc"

        return query, props

    def _prepare_custom_data_query(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ):
        named_params = self.__config.data_query_named_parameters
        if named_params is None:
            named_params = {}

        props = ClientRequestProperties()
        props.set_parameter("startDate", start_date.isoformat())
        props.set_parameter("endDate", end_date.isoformat())

        for param_name, tag_key in named_params.items():
            props.set_parameter(param_name, selector.tags[tag_key])

        return self.__config.data_query, props

    def _query(
        self, query: str, props: "ClientRequestProperties | None" = None
    ) -> Generator["KustoResultRow", None, None]:
        if props is None:
            props = ClientRequestProperties()
        props.set_option("notruncation", True)
        if self.__config.query_timeout_seconds is not None:
            props.set_option(
                ClientRequestProperties.request_timeout_option_name,
                timedelta(seconds=self.__config.query_timeout_seconds),
            )
        with self._connect() as connection:
            response = connection.execute_streaming(query, props)
            result = next(response.iter_primary_results(), None)
            if result is not None:
                yield from result

    def _connect(self) -> DataExplorerConnection:
        return DataExplorerConnection(
            self.__config.connection, self.__token_cache, self._sleeper.sleep
        )


def _add_square_brackets(columns: list[str]) -> list[str]:
    return [f"['{column}']" for column in columns]
