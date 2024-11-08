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

    index = config["index"]
    options = ElasticsearchSourceOptions(
        index,
        config.get("metadata_index", index),
        config.get("tag_columns", ["series name"]),
        config.get("timestamp_column", "ts"),
        config.get("metadata_field_column"),
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

    index: str
    metadata_index: str
    tags: List[str]
    timestamp_column: str
    metadata_field_column: Optional[str] = None


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
        query = f"FROM {self.__options.metadata_index}"
        if len(selector.tags) > 0:
            for tag_key, tag_value in selector.tags.items():
                query += f' | where {_escape(self.__metadata_mapper.from_kukur(tag_key))} == "{_escape(tag_value)}"'

        response = requests.post(
            f"http://{self.__configuration.host}:{self.__configuration.port}/_query",
            auth=(self.__configuration.username, self.__configuration.password),
            headers={"Content-Type": "application/json"},
            json={"query": query},
        )
        response.raise_for_status()
        content = json.loads(response.content)
        columns = {}
        for index, column in enumerate(content["columns"]):
            columns[column["name"]] = index

        for values in content["values"]:
            tags = {}
            for tag in self.__options.tags:
                column_name = self.__metadata_mapper.from_kukur(tag)
                if column_name not in columns:
                    raise InvalidMetadataError(f'column "{column_name}" not found')
                tags[tag] = values[columns[column_name]]

            field = None
            if self.__options.metadata_field_column is not None:
                column_name = self.__metadata_mapper.from_kukur(
                    self.__options.metadata_field_column
                )
                if column_name not in columns:
                    raise InvalidMetadataError(f'column "{column_name}" not found')
                field = values[columns[column_name]]

            metadata = Metadata(SeriesSelector.from_tags(selector.source, tags, field))

            field_names = [field for field, _ in metadata.iter_names()]
            for field in field_names:
                column_name = self.__metadata_mapper.from_kukur(field)
                if column_name in columns:
                    value = values[columns[column_name]]
                    try:
                        metadata.coerce_field(
                            field,
                            value,
                        )
                    except ValueError:
                        pass
            yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata, taking any configured metadata mapping into account."""
        column_name = "value"
        if self.__options.metadata_field_column is not None:
            column_name = self.__metadata_mapper.from_kukur(
                self.__options.metadata_field_column
            )
        query = f'FROM {self.__options.metadata_index} | where {column_name} == "{_escape(selector.field)}"'
        if len(selector.tags) > 0:
            for tag_key, tag_value in selector.tags.items():
                query += f' | where {_escape(self.__metadata_mapper.from_kukur(tag_key))} == "{_escape(tag_value)}"'
        response = requests.post(
            f"http://{self.__configuration.host}:{self.__configuration.port}/_query",
            auth=(self.__configuration.username, self.__configuration.password),
            headers={"Content-Type": "application/json"},
            json={"query": query},
        )
        response.raise_for_status()
        content = json.loads(response.content)

        columns = {}
        for index, column in enumerate(content["columns"]):
            columns[column["name"]] = index

        metadata = Metadata(selector)
        if len(content["values"]) == 0:
            return metadata
        if len(content["values"]) > 1:
            InvalidMetadataError('column "{column_name}" not found')
        values = content["values"][0]

        field_names = [field for field, _ in metadata.iter_names()]

        for field in field_names:
            column_name = self.__metadata_mapper.from_kukur(field)
            if column_name in columns:
                value = values[columns[column_name]]
                try:
                    metadata.coerce_field(
                        field,
                        value,
                    )
                except ValueError:
                    pass
        return metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        query = f"FROM {self.__options.index}"
        if len(selector.tags) > 0:
            for tag_key, tag_value in selector.tags.items():
                query += f' | where {_escape(self.__metadata_mapper.from_kukur(tag_key))} == "{_escape(tag_value)}"'

        query += f' | where ts >= "{start_date.isoformat()}" and ts <= "{end_date.isoformat()}"'
        query += f" | keep {self.__options.timestamp_column}, {_escape(selector.field)}"
        query += f" | sort {self.__options.timestamp_column} asc"

        response = requests.post(
            f"http://{self.__configuration.host}:{self.__configuration.port}/_query",
            auth=(self.__configuration.username, self.__configuration.password),
            headers={"Content-Type": "application/json"},
            json={"query": query},
        )
        response.raise_for_status()
        content = json.loads(response.content)
        timestamps = []
        values = []
        for row in content["values"]:
            timestamps.append(row[0])
            values.append(row[1])
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
