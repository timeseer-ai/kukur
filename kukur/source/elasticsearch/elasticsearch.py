"""Connections to Elasticsearch data sources from Timeseer."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import pyarrow as pa
import pyarrow.compute as pc

from kukur.source.metadata import MetadataMapper, MetadataValueMapper

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


def from_config(
    config: Dict[str, Any],
    metadata_mapper: MetadataMapper,
    metadata_value_mapper: MetadataValueMapper,
):
    """Create a new Influx data source."""
    host = config.get("host", "localhost")
    port = config.get("port", 9200)

    credentials = config.get("credentials")
    username = ""
    password = ""
    api_key = None
    if credentials is not None:
        username = credentials.get("username", "")
        password = credentials.get("password", "")
        api_key = credentials.get("api_key")
    configuration = ElasticsearchSourceConfiguration(
        host, port, username, password, api_key, config.get("query_timeout", 60)
    )
    index = config["index"]
    list_index = config.get("list_index", index)

    options = ElasticsearchSourceOptions(
        index,
        list_index,
        config.get("tag_columns", ["series name"]),
        config.get("field_columns", ["value"]),
        config.get("metadata_columns", []),
        config.get("timestamp_column", "ts"),
        config.get("metadata_field_column"),
        config.get("search_query"),
    )

    return ElasticsearchSource(
        configuration, options, metadata_mapper, metadata_value_mapper
    )


@dataclass
class ElasticsearchSourceConfiguration:
    """Options for Elasticsearch sources."""

    host: str
    port: int
    username: str
    password: str
    api_key: Optional[str]
    query_timeout: int


@dataclass
class ElasticsearchSourceOptions:
    """Options for Elasticsearch sources."""

    index: str
    list_index: str
    tag_columns: List[str]
    field_columns: List[str]
    metadata_columns: List[str]
    timestamp_column: str
    metadata_field_column: Optional[str] = None
    search_query: Optional[str] = None


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
        if self.__options.search_query is not None:
            search_query = {
                "query": self.__options.search_query,
                "columnar": True,
            }
            result = self._search_sql(search_query)
            for row in result.to_pylist():
                for metadata in self._get_metadata(selector.source, row):
                    yield metadata
        else:
            table = self._search_query_dsl({}, [{"_doc": "asc"}])
            for one_row in table:
                row = one_row["_source"]
                for metadata in self._get_metadata(selector.source, row):
                    yield metadata

    def _search_query_dsl(self, search_query: Dict, sort: List) -> List:
        table = []
        search_after = None
        query: Dict = {}
        if len(search_query) != 0:
            query["query"] = search_query
        query["sort"] = sort
        while True:
            if search_after is not None:
                query = {}
                if len(search_query) != 0:
                    query["query"] = search_query
                query["search_after"] = search_after
                query["sort"] = sort

            rows = self._send_query(query, f"{self.__options.list_index}/_search")
            table.extend(rows["hits"]["hits"])
            if len(table) >= rows["hits"]["total"]["value"]:
                break
            search_after = rows["hits"]["hits"][-1]["sort"]
        return table

    def _search_sql(self, query: Dict) -> pa.Table:
        columns = {}
        while True:
            content = self._send_query(query, "_sql")
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

    def _get_metadata(
        self, source_name: str, row: Dict
    ) -> Generator[Metadata, None, None]:
        tags = {
            self.__metadata_mapper.from_source(tag_name): row[tag_name]
            for tag_name in self.__options.tag_columns
        }
        fields = self.__options.field_columns
        if self.__options.metadata_field_column is not None:
            fields = [row[self.__options.metadata_field_column]]
        for field_column in fields:
            series = SeriesSelector(source_name, tags, field_column)
            metadata = Metadata(series)
            if len(row) == len(tags):
                yield metadata
            else:
                for k, v in row.items():
                    if k in self.__options.tag_columns:
                        continue
                    if v is None:
                        continue
                    name = self.__metadata_mapper.from_source(k)
                    metadata.coerce_field(
                        name,
                        self.__metadata_value_mapper.from_source(name, v),
                    )
                yield metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Read metadata, taking any configured metadata mapping into account."""
        metadata = Metadata(selector)
        if self.__options.search_query is not None:
            search_query = {
                "query": self.__options.search_query,
                "columnar": True,
            }
            table = self._search_sql(search_query)
            table = table.filter(
                pc.field(self.__options.metadata_field_column) == selector.field
            )
            for tag_name, tag_value in selector.tags.items():
                table = table.filter(
                    pc.field(self.__metadata_mapper.from_kukur(tag_name)) == tag_value
                )

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
            rows = self._search_query_dsl(query, [{"_score": "asc"}])

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
            data = self._send_query(data_query, f"{self.__options.index}/_search")
            for row in data["hits"]["hits"]:
                fields = row["fields"]
                timestamps.extend(fields[self.__options.timestamp_column])
                values.extend(fields[selector.field])
                search_after = row["sort"]
            if len(timestamps) >= data["hits"]["total"]["value"]:
                break

        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        """Return the available tag keys, tag value and tag fields."""
        return None

    def _send_query(self, query: Dict, path: str) -> Dict:
        headers = {"Content-Type": "application/json"}
        auth = None
        if self.__configuration.api_key is not None:
            headers["Authorization"] = f"ApiKey {self.__configuration.api_key}"
        else:
            auth = (self.__configuration.username, self.__configuration.password)
        response = requests.post(
            f"http://{self.__configuration.host}:{self.__configuration.port}/{path}",
            auth=auth,
            headers=headers,
            json=query,
            timeout=self.__configuration.query_timeout,
        )
        response.raise_for_status()
        return json.loads(response.content)
