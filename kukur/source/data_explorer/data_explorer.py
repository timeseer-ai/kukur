"""Connections to azure data explorer data sources from Timeseer."""
# SPDX-FileCopyrightText: 2022 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0


from datetime import datetime
from typing import Any, Dict, Generator, Optional

import pyarrow as pa

try:
    from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

    HAS_KUSTO = True
except ImportError:
    HAS_KUSTO = False

from kukur import Metadata, SeriesSearch, SeriesSelector, SourceStructure
from kukur.exceptions import KukurException, MissingModuleException


class InvalidClientConnection(KukurException):
    """Raised when an error occured when making the connection."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"Connection error: {message}")


def from_config(config: Dict[str, Any]):
    """Create a new Influx data source"""
    if not HAS_KUSTO:
        raise MissingModuleException("data_explorer", "azure-kusto-data")
    cluster = config["cluster"]
    database = config["database"]
    table = config["table"]
    return DataExplorerSource(cluster, database, table)


class DataExplorerSource:
    """An InfluxDB data source."""

    __database: str
    __table: str

    if HAS_KUSTO:
        __client: KustoClient

    def __init__(
        self,
        cluster: str,
        database: str,
        table: str
    ):
        if not HAS_KUSTO:
            raise MissingModuleException("data_explorer", "azure-kusto-data")
        self.__database = database
        self.__table = table

        kcsb = KustoConnectionStringBuilder.with_az_cli_authentication(cluster)
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

        query = f"""['{self.__table}']"""

        for (tag_key, tag_value) in selector.tags.items():
            query += f" | where {tag_key}=='{tag_value}'"

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
        raise NotImplementedError()
