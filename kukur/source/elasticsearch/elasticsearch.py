"""Connections to Elasticsearch data sources from Timeseer."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import pyarrow as pa

from kukur.source.metadata import MetadataMapper

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.exceptions import KukurException


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"Connection error: {message}")


class InvalidMetadataError(KukurException):
    """Raised when the metadata is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid metadata: {message}")


def from_config(config: Dict[str, Any], metadata_mapper: MetadataMapper):
    """Create a new Influx data source."""
    host = config.get("host", "localhost")
    port = config.get("port", 9200)

    username = config.get("username", "")
    password = config.get("password", "")
    configuration = ElasticsearchSourceConfiguration(host, port, username, password)

    options = ElasticsearchSourceOptions(
        config["list_query"],
        config["metadata_query"],
        config["data_query"],
        config.get("tag_columns", ["series name"]),
        config.get("field_columns", ["value"]),
        config["metadata_columns"],
        config.get("max_number_of_rows", 10000),
        config.get("metadata_field_column"),
        config.get("timestamp_column"),
    )

    return ElasticsearchSource(configuration, options, metadata_mapper)


@dataclass
class ElasticsearchSourceConfiguration:
    """Options for Elasticsearch sources."""

    host: str
    port: int
    username: str
    password: str


@dataclass
class ElasticsearchSourceOptions:
    """Options for Elasticsearch sources."""

    list_query: str
    metadata_query: str
    data_query: str
    tag_columns: List[str]
    field_columns: List[str]
    metadata_columns: List[str]
    max_number_of_rows: int
    metadata_field_column: Optional[str] = None
    timestamp_column: Optional[str] = None


class ElasticsearchSource:
    """An Elasticsearch data source."""

    def __init__(  # noqa: PLR0913
        self,
        configuration: ElasticsearchSourceConfiguration,
        options: ElasticsearchSourceOptions,
        metadata_mapper: MetadataMapper,
    ):
        self.__configuration = configuration
        self.__options = options
        self.__metadata_mapper = metadata_mapper

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        table = self._search_esql(selector)

        for row in table.to_pylist():
            tags = {
                self.__metadata_mapper.from_source(tag_name): row[tag_name]
                for tag_name in self.__options.tag_columns
            }
            fields = self.__options.field_columns
            if self.__options.metadata_field_column is not None:
                fields = [row[self.__options.metadata_field_column]]
            for field_column in fields:
                series = SeriesSelector(selector.source, tags, field_column)
                metadata = Metadata(series)
                if len(row) == len(tags):
                    yield metadata
                else:
                    for k, v in row.items():
                        if k in self.__options.tag_columns:
                            continue
                        if v is None:
                            continue
                        metadata.coerce_field(self.__metadata_mapper.from_source(k), v)
                    yield metadata

    def _search_esql(self, selector: SeriesSearch) -> pa.Table:

        query = self.__options.list_query
        if selector.tags is not None and len(selector.tags) > 0:
            for tag_key, tag_value in selector.tags.items():
                query += f' | where {_escape(self.__metadata_mapper.from_kukur(tag_key))} == "{_escape(tag_value)}"'

        if selector.field is not None:
            column_name = self._get_field_column_name()
            query += f' | where {column_name} == "{_escape(selector.field)}"'

        response = requests.post(
            f"http://{self.__configuration.host}:{self.__configuration.port}/_query",
            auth=(self.__configuration.username, self.__configuration.password),
            headers={"Content-Type": "application/json"},
            json={"query": query, "columnar": True},
        )
        response.raise_for_status()
        content = json.loads(response.content)
        columns: Dict[str, List] = {}
        for index, column in enumerate(content["columns"]):
            if (
                column["name"] in self.__options.tag_columns
                or column["name"] in self.__options.metadata_columns
                or column["name"] in self.__options.field_columns
                or column["name"] == self.__options.metadata_field_column
            ):
                if column["name"] in columns:
                    columns[column["name"]].extend(content["values"][index])
                else:
                    columns[column["name"]] = content["values"][index]

        return pa.Table.from_pydict(columns)

    def _get_field_column_name(self):
        column_name = "value"
        if self.__options.metadata_field_column is not None:
            column_name = self.__metadata_mapper.from_kukur(
                self.__options.metadata_field_column
            )

        return column_name

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata, taking any configured metadata mapping into account."""
        table = self._metadata_esql(selector)

        metadata = Metadata(selector)
        rows = table.to_pylist()
        if len(rows) == 0:
            return metadata
        row = rows[0]

        field_names = [field for field, _ in metadata.iter_names()]

        for field_name in field_names:
            column_name = self.__metadata_mapper.from_kukur(field_name)
            if column_name in row:
                value = row[column_name]
                try:
                    metadata.coerce_field(
                        field_name,
                        value,
                    )
                except ValueError:
                    pass
        return metadata

    def _metadata_esql(self, selector: SeriesSelector) -> pa.Table:
        query = self.__options.metadata_query
        column_name = self._get_field_column_name()
        query += f' | where {column_name} == "{_escape(selector.field)}"'
        if len(selector.tags) > 0:
            for tag_key, tag_value in selector.tags.items():
                query += f' | where {_escape(self.__metadata_mapper.from_kukur(tag_key))} == "{_escape(tag_value)}"'

        response = requests.post(
            f"http://{self.__configuration.host}:{self.__configuration.port}/_query",
            auth=(self.__configuration.username, self.__configuration.password),
            headers={"Content-Type": "application/json"},
            json={"query": query, "columnar": True},
        )
        response.raise_for_status()
        content = json.loads(response.content)
        columns = {}
        for index, column in enumerate(content["columns"]):
            if (
                column["name"] in self.__options.tag_columns
                or column["name"] in self.__options.metadata_columns
                or column["name"] in self.__options.field_columns
                or column["name"] == self.__options.metadata_field_column
            ):
                columns[column["name"]] = content["values"][index]

        return pa.Table.from_pydict(columns)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        timestamps = []
        values = []
        while True:
            query = self.__options.data_query
            split_query = self.__options.data_query.split("|", 1)
            query = split_query[0]
            params = [start_date.isoformat(), end_date.isoformat()]
            if len(selector.tags) > 0:
                for tag_key, tag_value in selector.tags.items():
                    query += f' | where {_escape(self.__metadata_mapper.from_kukur(tag_key))} == "{_escape(tag_value)}"'

            if len(split_query) > 1:
                query += f" | {split_query[1]}"

            query += f" | keep {self.__options.timestamp_column}, {selector.field}"
            response = requests.post(
                f"http://{self.__configuration.host}:{self.__configuration.port}/_query",
                auth=(self.__configuration.username, self.__configuration.password),
                headers={"Content-Type": "application/json"},
                json={"query": query, "params": params, "columnar": True},
            )
            response.raise_for_status()
            content = json.loads(response.content)

            for index, column in enumerate(content["columns"]):
                if column["name"] == self.__options.timestamp_column:
                    timestamps.extend(content["values"][index])
                if column["name"] == selector.field:
                    values.extend(content["values"][index])

            if len(content["values"][0]) != self.__options.max_number_of_rows:
                break

            start_date = timestamps[-1]
            while timestamps[-1] == start_date:
                timestamps.pop()
                values.pop()

        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        """Return the available tag keys, tag value and tag fields."""
        return None


def _escape(context: Optional[str]) -> str:
    if context is None:
        context = "value"
    if "'" in context:
        context = context.replace("'", "")
    if '"' in context:
        context = context.replace('"', "")
    if "|" in context:
        context = context.replace("|", "")
    if ";" in context:
        context = context.replace(";", "")
    return context
