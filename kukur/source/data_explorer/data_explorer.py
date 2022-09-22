"""Connections to azure data explorer data sources from Timeseer."""
# SPDX-FileCopyrightText: 2022 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

import os

from datetime import datetime
from typing import Any, Dict, Generator, List, Optional, Tuple

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


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"Connection error: {message}")


def from_config(config: Dict[str, Any]):
    """Create a new Influx data source"""
    connection_string = config["connection_string"]
    database = config["database"]
    table = config["table"]
    return DataExplorerSource(connection_string, database, table)


class DataExplorerSource:
    """An InfluxDB data source."""

    __database: str
    __table: str
    __connection_string: str
    __azure_credential: DefaultAzureCredential

    if HAS_KUSTO:
        __client: KustoClient

    def __init__(self, connection_string: str, database: str, table: str):
        if not HAS_AZURE_IDENTITY:
            raise MissingModuleException("azure-identity")

        if not HAS_KUSTO:
            raise MissingModuleException("data_explorer", "azure-kusto-data")

        self.__database = database
        self.__table = table
        self.__connection_string = connection_string
        self.__azure_credential = DefaultAzureCredential()

        kcsb = KustoConnectionStringBuilder.with_token_provider(
            connection_string, self._get_auth_token
        )
        self.__client = KustoClient(kcsb)

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        raise NotImplementedError()

    # pylint: disable=no-self-use
    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Data explorer currently always returns empty metadata."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""

        query = f"""['{self.__table}']
            | where ts >= todatetime('{start_date}')
            | where ts <= todatetime('{end_date}')
        """

        for (tag_key, tag_value) in selector.tags.items():
            query += f" | where {_escape(tag_key)}=='{_escape(tag_value)}'"

        result = self.__client.execute(self.__database, query)
        timestamps = []
        values = []

        if result is not None and len(result.primary_results) > 0:
            for row in result.primary_results[0]:
                timestamps.append(row["ts"])
                values.append(row["value"])

        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def get_source_structure(self, _: SeriesSelector) -> Optional[SourceStructure]:
        """Return the available tag keys, tag value and tag fields."""
        query_tag_keys = ".show database schema"
        result = self.__client.execute(self.__database, query_tag_keys)
        tag_keys = [
            row["ColumnName"]
            for row in result.primary_results[0]
            if row["ColumnName"] is not None
            and row["ColumnName"] != "ts"
            and row["ColumnName"] != "value"
        ]

        tag_values: List[dict] = []
        for key in tag_keys:
            query_tag_values = f"['{self.__table}'] | project {key} | distinct {key}"
            result = self.__client.execute(self.__database, query_tag_values)
            tag_values.append({key: [row[key] for row in result.primary_results[0]]})
        return SourceStructure([], tag_keys, tag_values)

    def _get_auth_token(self) -> str:
        """Uses azure default authentication to get an auth token for the configured cluster."""
        return self.__azure_credential.get_token(self.__connection_string + '//.default')[0]


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
