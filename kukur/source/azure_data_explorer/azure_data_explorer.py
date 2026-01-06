"""Connections to azure data explorer data sources from Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import pyarrow as pa

try:
    from azure.identity import DefaultAzureCredential

    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False

try:
    from azure.kusto.data import (
        ClientRequestProperties,
        KustoClient,
        KustoConnectionStringBuilder,
    )
    from azure.kusto.data.exceptions import KustoMultiApiError

    HAS_KUSTO = True
except ImportError:
    HAS_KUSTO = False


from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import (
    KukurException,
    MissingModuleException,
)
from kukur.source.metadata import MetadataMapper, MetadataValueMapper

MAX_ITEMS_PER_CALL = 500_000


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
    tag_columns: list[str]
    field_columns: list[str]
    metadata_columns: list[str]
    max_items_per_call: int


def from_config(
    config: dict[str, Any],
    metadata_mapper: MetadataMapper,
    metadata_value_mapper: MetadataValueMapper,
):
    """Create a new Azure Data Explorer data source."""
    connection_string = config["connection_string"]
    database = config["database"]
    table = config["table"]
    timestamp_column = config.get("timestamp_column", "ts")
    tag_columns = config.get("tag_columns", [])
    field_columns = config.get("field_columns", [])
    metadata_columns = config.get("metadata_columns", [])
    return DataExplorerSource(
        DataExplorerConfiguration(
            connection_string,
            database,
            table,
            timestamp_column,
            tag_columns,
            field_columns,
            metadata_columns,
            config.get("max_items_per_call", MAX_ITEMS_PER_CALL),
        ),
        metadata_mapper,
        metadata_value_mapper,
    )


class DataExplorerSource:  # pylint: disable=too-many-instance-attributes
    """An Azure Data Explorer data source."""

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
        self.__config = config

    def search(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Search for series matching the given selector."""
        if len(self.__config.tag_columns) == 0:
            raise KukurException("Define tags to support listing time series")

        if len(self.__config.metadata_columns) == 0:
            query = f"['{self.__config.table}'] | distinct {', '.join(self.__config.tag_columns)}"
            with self._get_client() as client:
                result = client.execute(self.__config.database, query)
            if result is None or len(result.primary_results) == 0:
                return

            for row in result.primary_results[0]:
                tags = {}
                for tag in self.__config.tag_columns:
                    tags[tag] = row[tag]
                for field in self.__config.field_columns:
                    yield Metadata(SeriesSelector(selector.source, tags, field))
        else:
            summaries = [
                f"['{name}']=arg_max(['{self.__config.timestamp_column}'], ['{name}'])"
                for name in self.__config.metadata_columns
            ]
            renames = [
                f"['{name}']=['{name}1']" for name in self.__config.metadata_columns
            ]
            query = f"""['{self.__config.table}']
                | summarize {", ".join(summaries)} by {", ".join(_add_square_brackets(self.__config.tag_columns))}
                | project-away {", ".join(_add_square_brackets(self.__config.metadata_columns))}
                | project-rename {", ".join(renames)}
            """
            with self._get_client() as client:
                result = client.execute(self.__config.database, query)
            if result is None or len(result.primary_results) == 0:
                return

            for row in result.primary_results[0]:
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

        max_items_per_call = self.__config.max_items_per_call

        timestamps = []
        values = []
        offset = 0
        with self._get_client() as client:
            while True:
                try:
                    paginated_query = f"""{query}
                    | serialize
                    | where row_number() > {offset}
                    | take {max_items_per_call}"""
                    result = client.execute(
                        self.__config.database, paginated_query, props
                    )
                    if result is None or len(result.primary_results) == 0:
                        break

                    for row in result.primary_results[0]:
                        timestamps.append(row[ts])
                        values.append(row[selector.field])
                    if len(result.primary_results[0]) < max_items_per_call:
                        break
                    offset += max_items_per_call
                except KustoMultiApiError as e:
                    if _is_result_set_too_large(e):
                        max_items_per_call = max_items_per_call // 2
                        if max_items_per_call == 0:
                            raise e
                    else:
                        raise e

        return pa.Table.from_pydict({"ts": timestamps, "value": values})

    def _get_client(self):
        """Return a Kusto client.

        The client should be closed after use.
        """
        azure_credential = DefaultAzureCredential()

        def _get_token():
            return azure_credential.get_token(
                self.__config.connection_string + "//.default"
            )[0]

        kcsb = KustoConnectionStringBuilder.with_token_provider(
            self.__config.connection_string, _get_token
        )
        return KustoClient(kcsb)


def _is_result_set_too_large(err: KustoMultiApiError) -> bool:
    for api_error in err.get_api_errors():
        if (
            api_error.description is not None
            and "E_QUERY_RESULT_SET_TOO_LARGE" in api_error.description
        ):
            return True
    return False


def _add_square_brackets(columns: list[str]) -> list[str]:
    return [f"['{column}']" for column in columns]
