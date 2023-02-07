"""Connections to azure data explorer data sources from Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json
from dataclasses import dataclass
from datetime import datetime
from typing import Any, Dict, Generator, List, Optional

import pyarrow as pa

try:
    from azure.identity import DefaultAzureCredential

    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False

try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    HAS_KUSTO = True
except ImportError:
    HAS_KUSTO = False


from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.exceptions import (
    KukurException,
    MissingModuleException,
)
from kukur.source.metadata import MetadataMapper, MetadataValueMapper


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        super().__init__(self, f"Connection error: {message}")


@dataclass
class DataExplorerConfiguration:
    """Data Explorer source configuration."""

    connection_string: str
    database: str
    table: str
    timestamp_column: str
    tags: List[str]
    metadata_columns: List[str]
    ignored_columns: List[str]


def from_config(
    config: Dict[str, Any],
    metadata_mapper: MetadataMapper,
    metadata_value_mapper: MetadataValueMapper,
):
    """Create a new Azure Data Explorer data source."""
    connection_string = config["connection_string"]
    database = config["database"]
    table = config["table"]
    timestamp_column = config.get("timestamp_column", "ts")
    tags = config.get("tag_columns", [])
    metadata_columns = config.get("metadata_columns", [])
    ignored_columns = config.get("ignored_columns", [])
    return DataExplorerSource(
        DataExplorerConfiguration(
            connection_string,
            database,
            table,
            timestamp_column,
            tags,
            metadata_columns,
            ignored_columns,
        ),
        metadata_mapper,
        metadata_value_mapper,
    )


class DataExplorerSource:  # pylint: disable=too-many-instance-attributes
    """An Azure Data Explorer data source."""

    __database: str
    __table: str
    __timestamp_column: str
    __tags: List[str]
    __metadata_columns: List[str]
    __ignored_columns: List[str]
    __metadata_mapper: MetadataMapper
    __metadata_value_mapper: MetadataValueMapper

    if HAS_KUSTO:
        __client: KustoClient

    def __init__(
        self,
        config: DataExplorerConfiguration,
        metadata_mapper: MetadataMapper,
        metadata_value_mapper: MetadataValueMapper,
    ):
        if not HAS_AZURE_IDENTITY:
            raise MissingModuleException("azure-identity")

        if not HAS_KUSTO:
            raise MissingModuleException("azure-kusto-data", "data_explorer")

        self.__metadata_mapper = metadata_mapper
        self.__metadata_value_mapper = metadata_value_mapper
        self.__database = _escape(config.database)
        self.__table = _escape(config.table)
        self.__timestamp_column = _escape(config.timestamp_column)
        self.__tags = [_escape(tag) for tag in config.tags]
        self.__metadata_columns = [
            _escape(metadata_column) for metadata_column in config.metadata_columns
        ]
        self.__ignored_columns = [
            _escape(ignored_column) for ignored_column in config.ignored_columns
        ]

        azure_credential = DefaultAzureCredential()

        def _get_token():
            return azure_credential.get_token(config.connection_string + "//.default")[
                0
            ]

        kcsb = KustoConnectionStringBuilder.with_token_provider(
            config.connection_string, _get_token
        )
        self.__client = KustoClient(kcsb)

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        if len(self.__tags) == 0:
            raise KukurException("Define tags to support listing time series")

        all_columns = {_escape(column) for column in self._get_table_columns()}
        fields = (
            all_columns
            - {self.__timestamp_column}
            - set(self.__tags)
            - set(self.__metadata_columns)
            - set(self.__ignored_columns)
        )

        if len(self.__metadata_columns) == 0:
            query = f"['{self.__table}'] | distinct {', '.join(self.__tags)}"
            result = self.__client.execute(self.__database, query)
            if result is None or len(result.primary_results) == 0:
                return

            for row in result.primary_results[0]:
                tags = {}
                for tag in self.__tags:
                    tags[tag] = row[tag]
                for field in fields:
                    yield Metadata(SeriesSelector(selector.source, tags, field))
        else:
            summaries = [
                f"['{name}']=arg_max(['{self.__timestamp_column}'], ['{name}'])"
                for name in self.__metadata_columns
            ]
            renames = [f"['{name}']=['{name}1']" for name in self.__metadata_columns]
            query = f"""['{self.__table}']
                | summarize {', '.join(summaries)} by {', '.join(_add_square_brackets(self.__tags))}
                | project-away {', '.join(_add_square_brackets(self.__metadata_columns))}
                | project-rename {', '.join(renames)}
            """
            result = self.__client.execute(self.__database, query)
            if result is None or len(result.primary_results) == 0:
                return

            for row in result.primary_results[0]:
                tags = {}
                for tag in self.__tags:
                    tags[tag] = row[tag]
                for field in fields:
                    series_selector = SeriesSelector(selector.source, tags, field)
                    metadata = Metadata(series_selector)
                    for column_name in self.__metadata_columns:
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
        query = f"""['{self.__table}']
            | where ['{self.__timestamp_column}'] >= todatetime('{start_date}')
            | where ['{self.__timestamp_column}'] <= todatetime('{end_date}')
        """

        for tag_key, tag_value in selector.tags.items():
            query += f" | where ['{_escape(tag_key)}']=='{_escape(tag_value)}'"

        query = f"{query} | sort by ['{self.__timestamp_column}'] asc"

        result = self.__client.execute(self.__database, query)
        timestamps = []
        values = []

        if result is not None and len(result.primary_results) > 0:
            for row in result.primary_results[0]:
                timestamps.append(row[self.__timestamp_column])
                values.append(row[selector.field])

        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        """Return the available tag keys, tag values and tag fields."""
        all_columns = {_escape(column) for column in self._get_table_columns()}
        fields = (
            all_columns
            - {self.__timestamp_column}
            - set(self.__tags)
            - set(self.__metadata_columns)
            - set(self.__ignored_columns)
        )
        tag_keys = set(self.__tags)
        tag_values = []

        for tag_key in tag_keys:
            query = f"""['{self.__table}']
                    | distinct ['{tag_key}']
            """
            result = self.__client.execute(self.__database, query)
            if result is not None and len(result.primary_results) > 0:
                tag_values.extend(
                    [
                        {"key": tag_key, "value": row[0]}
                        for row in result.primary_results[0]
                    ]
                )

        return SourceStructure(list(fields), list(tag_keys), tag_values)

    def _get_table_columns(self) -> List[str]:
        query = f".show table ['{self.__table}'] schema as json"
        result = self.__client.execute(self.__database, query)
        if result is None or len(result.primary_results) == 0:
            raise KukurException("Failed to get table schema")
        for row in result.primary_results[0]:
            schema = json.loads(row["Schema"])
            return [column["Name"] for column in schema.get("OrderedColumns", [])]
        raise KukurException("Table schema is empty")


def _add_square_brackets(columns: List[str]) -> List[str]:
    return [f"['{column}']" for column in columns]


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
