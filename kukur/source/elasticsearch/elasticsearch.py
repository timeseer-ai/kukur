"""Connections to Elasticsearch data sources from Timeseer."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import http
import itertools
import json
import logging
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

from kukur.source.metadata import MetadataMapper, MetadataValueMapper

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.exceptions import KukurException, MissingModuleException

logger = logging.getLogger(__name__)


class InvalidConfigurationException(KukurException):
    """Raised when the source configuration is invalid."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"invalid configuration: {message}")


def from_config(
    config: dict[str, Any],
    metadata_mapper: MetadataMapper,
    metadata_value_mapper: MetadataValueMapper,
):
    """Create a new Elasticsearch data source."""
    if not HAS_REQUESTS:
        raise MissingModuleException("requests")
    credentials = config.get("credentials")
    username = None
    password = None
    api_key = None
    if credentials is not None:
        username = credentials.get("username")
        password = credentials.get("password")
        api_key = credentials.get("api_key")

    configuration = ElasticsearchSourceConfiguration(
        config.get("scheme", "http"),
        config.get("host", "localhost"),
        config.get("port"),
        username,
        password,
        api_key,
        config.get("query_timeout_seconds", 60),
        config.get("query_page_size", 10_000),
    )

    index = config.get("index")
    list_query = config.get("list_query")
    metadata_query = config.get("metadata_query")
    if index is None and list_query is None and metadata_query is None:
        raise InvalidConfigurationException(
            "No `index`, `list_query` or `metadata_query` is defined"
        )
    options = ElasticsearchSourceOptions(
        index,
        config.get("metadata_index", index),
        config.get("tag_columns", ["series name"]),
        config.get("field_columns", ["value"]),
        config.get("metadata_columns", []),
        config.get("timestamp_column", "ts"),
        config.get("metadata_field_column"),
        list_query,
        metadata_query,
        config.get("metadata_index_filter"),
    )

    return ElasticsearchSource(
        configuration, options, metadata_mapper, metadata_value_mapper
    )


@dataclass
class ElasticsearchSourceConfiguration:
    """Options for Elasticsearch sources."""

    scheme: str
    host: str
    port: int | None
    username: str | None
    password: str | None
    api_key: str | None
    query_timeout_seconds: int
    query_page_size: int


@dataclass
class ElasticsearchSourceOptions:
    """Options for Elasticsearch sources."""

    index: str | None
    metadata_index: str | None
    tag_columns: list[str]
    field_columns: list[str]
    metadata_columns: list[str]
    timestamp_column: str
    metadata_field_column: str | None = None
    list_query: str | None = None
    metadata_query: str | None = None
    metadata_index_filter: dict | None = None


class ElasticsearchSource:
    """An Elasticsearch data source."""

    def __init__(  # noqa: PLR0913
        self,
        configuration: ElasticsearchSourceConfiguration,
        options: ElasticsearchSourceOptions,
        metadata_mapper: MetadataMapper,
        metadata_value_mapper: MetadataValueMapper,
    ):
        self.__configuration = configuration
        self.__options = options
        self.__metadata_mapper = metadata_mapper
        self.__metadata_value_mapper = metadata_value_mapper

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        if self.__options.list_query is not None:
            list_query = {
                "query": self.__options.list_query,
                "columnar": True,
            }
            table = self._search_sql(list_query)
            for row in table.to_pylist():
                for metadata in self._get_metadata(selector.source, row):
                    yield metadata
        else:
            rows = self._list_query_dsl(
                self.__options.metadata_index_filter or {}, [{"_doc": "asc"}]
            )
            for row in rows:
                for metadata in self._get_metadata(selector.source, row["_source"]):
                    yield metadata

    def _get_metadata(
        self, source_name: str, row: dict
    ) -> Generator[Metadata, None, None]:
        tags = {
            self.__metadata_mapper.from_source(tag_name): _dot_lookup(row, tag_name)
            for tag_name in self.__options.tag_columns
        }
        all_tags = _flatten_lists(tags)
        fields = self.__options.field_columns
        if self.__options.metadata_field_column is not None:
            fields = [_dot_lookup(row, self.__options.metadata_field_column)]
        for tags in all_tags:
            for field_column in fields:
                series = SeriesSelector(source_name, tags, field_column)
                metadata = Metadata(series)
                for metadata_column_name in self.__options.metadata_columns:
                    v = _dot_lookup(row, metadata_column_name, strict=False)
                    if v is None:
                        continue
                    name = self.__metadata_mapper.from_source(metadata_column_name)
                    metadata.coerce_field(
                        name,
                        self.__metadata_value_mapper.from_source(name, v),
                    )
                yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata, taking any configured metadata mapping into account."""
        metadata = Metadata(selector)
        if self.__options.metadata_query is not None:
            params = [
                selector.tags[self.__metadata_mapper.from_source(tag_name)]
                for tag_name in self.__options.tag_columns
            ]
            if self.__options.metadata_field_column is not None:
                params.append(selector.field)
            list_query = {
                "query": self.__options.metadata_query,
                "params": params,
                "columnar": True,
            }
            table = self._search_sql(list_query)

            if table.num_rows == 0:
                return metadata
            row = table.to_pylist()[0]
        else:
            fields = []
            if self.__options.metadata_field_column is not None:
                fields = [
                    {"term": {self.__options.metadata_field_column: selector.field}}
                ]
            query = {
                "bool": {
                    "must": [
                        {
                            "term": {
                                self.__metadata_mapper.from_kukur(tag_name): tag_value
                            }
                        }
                        for tag_name, tag_value in selector.tags.items()
                    ]
                    + fields
                }
            }
            rows = self._list_query_dsl(query, [{"_score": "asc"}])

            if len(rows) == 0:
                return metadata
            row = rows[0]["_source"]

        field_names = [field for field, _ in metadata.iter_names()]

        for field_name in field_names:
            column_name = self.__metadata_mapper.from_kukur(field_name)
            if column_name in row:
                value = self.__metadata_value_mapper.from_source(
                    field_name, row[column_name]
                )
                try:
                    metadata.coerce_field(
                        field_name,
                        value,
                    )
                except ValueError:
                    pass
        return metadata

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        if self.__options.index is None:
            raise KukurException("Define an `index` to fetch data.")
        query = {
            "bool": {
                "must": [
                    {"term": {self.__metadata_mapper.from_kukur(tag_name): tag_value}}
                    for tag_name, tag_value in selector.tags.items()
                ],
                "filter": {
                    "range": {
                        self.__options.timestamp_column: {
                            "gte": start_date.isoformat(),
                            "lte": end_date.isoformat(),
                        }
                    },
                },
            }
        }
        data_query = {
            "query": query,
            "fields": [self.__options.timestamp_column, selector.field],
            "sort": [{self.__options.timestamp_column: "asc"}],
            "_source": False,
        }

        with ElasticSearchConnection(self.__configuration) as connection:
            timestamps = []
            values = []
            search_after = None
            while True:
                if search_after is not None:
                    data_query = {
                        "query": query,
                        "fields": [self.__options.timestamp_column, selector.field],
                        "search_after": search_after,
                        "sort": [{self.__options.timestamp_column: "asc"}],
                    }
                data = connection.send_query(
                    data_query, f"{self.__options.index}/_search"
                )
                for row in data["hits"]["hits"]:
                    fields = row["fields"]
                    timestamps.extend(fields[self.__options.timestamp_column])
                    values.extend(fields[selector.field])
                    search_after = row["sort"]
                if len(timestamps) >= data["hits"]["total"]["value"]:
                    break

            return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def get_source_structure(self, _: SeriesSelector) -> SourceStructure | None:
        """Return the available tag keys, tag value and tag fields."""
        return None

    def _list_query_dsl(self, list_query: dict, sort: list) -> list:
        if self.__options.metadata_index is None:
            raise KukurException("Define a `metadata_index` to search time series.")

        with ElasticSearchConnection(self.__configuration) as connection:
            table = []
            search_after = None
            query: dict = {"size": self.__configuration.query_page_size}
            if len(list_query) != 0:
                query["query"] = list_query
            query["sort"] = sort
            while True:
                if search_after is not None:
                    query = {"size": self.__configuration.query_page_size}
                    if len(list_query) != 0:
                        query["query"] = list_query
                    query["search_after"] = search_after
                    query["sort"] = sort
                rows = connection.send_query(
                    query, f"{self.__options.metadata_index}/_search"
                )
                new_hits = rows["hits"]["hits"]
                table.extend(new_hits)
                if len(new_hits) < self.__configuration.query_page_size:
                    break
                search_after = new_hits[-1]["sort"]
            return table

    def _search_sql(self, query: dict) -> pa.Table:
        with ElasticSearchConnection(self.__configuration) as connection:
            columns = {}
            while True:
                content = connection.send_query(query, "_sql")
                if "columns" in content:
                    for index, column in enumerate(content["columns"]):
                        if (
                            column["name"] in self.__options.tag_columns
                            or column["name"] in self.__options.metadata_columns
                            or column["name"] in self.__options.field_columns
                            or column["name"] == self.__options.metadata_field_column
                        ):
                            columns[column["name"]] = content["values"][index]
                else:
                    for index, column in enumerate(columns.keys()):
                        columns[column].extend(content["values"][index])
                if "cursor" not in content:
                    break
                query = {
                    "cursor": content["cursor"],
                    "columnar": True,
                }
            return pa.Table.from_pydict(columns)


class ElasticSearchConnection:
    """Stateful connection to ElasticSearch.

    Should be used as a context manager.
    """

    def __init__(self, config: ElasticsearchSourceConfiguration):
        self._config = config

    def __enter__(self) -> "ElasticSearchConnection":
        self.session = requests.Session()
        self.session.headers["X-Requested-With"] = "Kukur"
        if self._config.api_key is not None:
            self.session.headers["Authorization"] = f"ApiKey {self._config.api_key}"
        elif self._config.username is not None and self._config.password is not None:
            self.session.auth = (self._config.username, self._config.password)
        return self

    def __exit__(self, exc_type, exc, tb):
        if self.session is not None:
            self.session.close()

    def send_query(self, query: dict, path: str) -> dict:
        """Query ElasticSearch."""
        headers = {"Content-Type": "application/json"}

        url = f"{self._config.scheme}://{self._config.host}:{self._config.port}/{path}"
        if self._config.port is None:
            url = f"{self._config.scheme}://{self._config.host}/{path}"
        response = self.session.post(
            url,
            headers=headers,
            json=query,
            timeout=self._config.query_timeout_seconds,
        )
        if response.status_code >= http.HTTPStatus.BAD_REQUEST:
            logger.error("error for query '%s': %s", json.dumps(query), response.text)
        response.raise_for_status()
        return json.loads(response.content)


def _dot_lookup(doc: dict, key: str, *, strict: bool = True):
    if key in doc:
        return doc[key]
    if "." not in key:
        if strict:
            raise AttributeError(name=key)
        return None
    parts = key.split(".")
    for part in parts:
        if part not in doc:
            if strict:
                raise AttributeError(name=part)
            return None
        doc = doc[part]
    return doc


def _flatten_lists(tags: dict) -> list[dict]:
    """Split up tag values that are lists."""
    simple_tags = {}
    list_tags = []
    for k, v in tags.items():
        if isinstance(v, list):
            pairs = [(k, vv) for vv in v]
            list_tags.append(pairs)
        else:
            simple_tags[k] = v
    if len(list_tags) == 0:
        return [simple_tags]
    all_tags = []
    for group in itertools.product(*list_tags):
        base = simple_tags.copy()
        for k, v in group:
            base[k] = v
        all_tags.append(base)

    return all_tags
