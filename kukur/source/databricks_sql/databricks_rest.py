"""Kukur source for Databricks SQL Warehouse using the statement execution API.

https://docs.databricks.com/api/workspace/statementexecution
"""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
from collections.abc import Generator
from dataclasses import dataclass
from datetime import datetime
from http import HTTPStatus
from urllib.parse import urljoin

import pyarrow as pa
from pyarrow import ipc

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import (
    InvalidSourceException,
    KukurException,
    MissingModuleException,
)
from kukur.source.arrow import empty_table
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper

try:
    import requests

    HAS_REQUESTS = True
except ImportError:
    HAS_REQUESTS = False


logger = logging.getLogger(__name__)


class UnexpectedResponseError(KukurException):
    """Raised when a response from the API is unexpected."""


class DatabricksError(KukurException):
    """Raised when databricks generates an error."""


@dataclass
class ConnectionConfiguration:
    """Databricks Statement Execution API connection properties."""

    host: str
    warehouse_id: str
    password: str
    proxy: str | None
    verify_ssl: bool
    timeout_seconds: int
    user_agent: str

    @classmethod
    def from_data(cls, data: dict) -> "ConnectionConfiguration":
        """Create from a data dictionary."""
        return cls(
            data["host"],
            data["warehouse_id"],
            data["password"],
            data.get("proxy"),
            data.get("verify_ssl", True),
            data.get("timeout_seconds", 60),
            data.get("user_agent", "Timeseer.AI+Kukur"),
        )


@dataclass
class StatementExecutionConfiguration:
    """Databricks statement execution API source configuration."""

    connection: ConnectionConfiguration
    list_query: str | None
    list_columns: list[str] | None
    tag_columns: list[str]
    metadata_columns: list[str] | None
    field_columns: list[str]
    data_query: str | None

    @classmethod
    def from_data(cls, data: dict) -> "StatementExecutionConfiguration":
        """Create from a data dictionary."""
        return cls(
            ConnectionConfiguration.from_data(data["connection"]),
            data.get("list_query"),
            data.get("list_columns"),
            data.get("tag_columns", ["series name"]),
            data.get("metadata_columns"),
            data.get("field_columns", ["value"]),
            data.get("data_query"),
        )


class DatabricksStatementExecutionSource:
    """Kukur source for the Databricks Statement Execution API."""

    def __init__(
        self,
        config: StatementExecutionConfiguration,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        if not HAS_REQUESTS:
            raise MissingModuleException("requests", "databricks-sql")
        self._config = config
        self.__metadata_value_mapper = metadata_value_mapper
        self.__quality_mapper = quality_mapper

    def search(
        self, selector: SeriesSearch
    ) -> Generator[SeriesSelector | Metadata, None, None]:
        """List all time series.

        Request data as a JSON array of arrays.
        """
        if self._config.list_query is None:
            raise InvalidSourceException("no `list_query` defined")
        if self._config.list_columns is None:
            raise InvalidSourceException("no `list_columns` defined")

        with requests.Session() as session:
            if self._config.connection.proxy:
                session.proxies.update({"https": self._config.connection.proxy})
            if not self._config.connection.verify_ssl:
                session.verify = False
            session.headers["User-Agent"] = self._config.connection.user_agent

            headers = {"Authorization": "Bearer " + self._config.connection.password}
            query = {
                "warehouse_id": self._config.connection.warehouse_id,
                "statement": self._config.list_query,
                "disposition": "EXTERNAL_LINKS",
                "format": "JSON_ARRAY",
                "wait_timeout": "50s",
            }
            data_links = _query_data_links(
                session,
                self._config.connection,
                query,
                headers,
            )

            for data_link in data_links:
                response = session.get(
                    data_link[1], timeout=self._config.connection.timeout_seconds
                )
                _log_error(response, "Failed to GET data link")
                response.raise_for_status()
                for row in response.json():
                    tags = {}
                    metadata = {}
                    for name, value in zip(self._config.list_columns, row, strict=True):
                        if name in self._config.tag_columns:
                            tags[name] = value
                        else:
                            metadata[name] = self.__metadata_value_mapper.from_source(
                                name, value
                            )
                    for field_name in self._config.field_columns:
                        series_selector = SeriesSelector(
                            selector.source, tags, field_name
                        )
                        series_metadata = Metadata(series_selector)
                        for k, v in metadata.items():
                            series_metadata.coerce_field(k, v)
                        yield series_metadata

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Not implemented."""
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data."""
        if self._config.data_query is None:
            raise InvalidSourceException("no `data_query` defined")

        with requests.Session() as session:
            if self._config.connection.proxy:
                session.proxies.update({"https": self._config.connection.proxy})
            if not self._config.connection.verify_ssl:
                session.verify = False
            session.headers["User-Agent"] = self._config.connection.user_agent

            headers = {"Authorization": "Bearer " + self._config.connection.password}
            query = {
                "warehouse_id": self._config.connection.warehouse_id,
                "statement": self._config.data_query,
                "disposition": "EXTERNAL_LINKS",
                "format": "ARROW_STREAM",
                "wait_timeout": "50s",
                "parameters": [
                    {
                        "name": _generate_named_parameter_marker(tag_name),
                        "value": selector.tags[tag_name],
                    }
                    for tag_name in self._config.tag_columns
                ]
                + [
                    {
                        "name": "start_date",
                        "type": "TIMESTAMP",
                        "value": start_date.isoformat(),
                    },
                    {
                        "name": "end_date",
                        "type": "TIMESTAMP",
                        "value": end_date.isoformat(),
                    },
                ],
            }
            data_links = _query_data_links(
                session,
                self._config.connection,
                query,
                headers,
            )

            tables = []
            for data_link in data_links:
                response = session.get(
                    data_link[1], timeout=self._config.connection.timeout_seconds
                )
                _log_error(response, "Failed to GET data link")
                response.raise_for_status()
                stream = ipc.open_stream(response.content)
                table = stream.read_all()
                if table.num_columns == 2:  # noqa: PLR2004
                    table = table.rename_columns(["ts", "value"])
                if table.num_columns == 3:  # noqa: PLR2004
                    table = table.rename_columns(["ts", "value", "quality"])
                    table = table.set_column(
                        2, "quality", self.__quality_mapper.map_array(table["quality"])
                    )
                tables.append(table)
            if len(tables) == 0:
                return empty_table(include_quality=self.__quality_mapper.is_present())
            return pa.concat_tables(tables)


def _query_data_links(
    session, config: ConnectionConfiguration, query: dict, headers: dict
) -> list[tuple[int, str]]:
    response = session.post(
        urljoin(f"https://{config.host}", "/api/2.0/sql/statements"),
        json=query,
        headers=headers,
        timeout=60,  # calls take at most 50 seconds due to "wait_timeout"
    )
    _log_error(response, "Failed to execute statement")
    response.raise_for_status()

    body = response.json()

    # Wait until the response is complete.
    while body["status"]["state"] in ("PENDING", "RUNNING"):
        statement_id = body["statement_id"]
        response = session.get(
            urljoin(
                f"https://{config.host}", f"/api/2.0/sql/statements/{statement_id}"
            ),
            headers=headers,
            timeout=config.timeout_seconds,
        )
        _log_error(response, f"Failed to query status of statement {statement_id}")
        response.raise_for_status()
        body = response.json()

        # TODO: cancel when taking longer than a certain time

    if body["status"]["state"] == "FAILED":
        raise DatabricksError(body["status"]["error"])

    # List all responses
    chunk_body = body["result"]
    data_links: list[tuple[int, str]] = []

    if "external_links" not in chunk_body:
        return data_links

    if len(chunk_body["external_links"]) != 1:
        raise UnexpectedResponseError("not exactly 1 external link")

    data_links.append(
        (
            chunk_body["external_links"][0]["chunk_index"],
            chunk_body["external_links"][0]["external_link"],
        )
    )
    while "next_chunk_internal_link" in chunk_body["external_links"][0]:
        response = session.get(
            urljoin(
                f"https://{config.host}",
                chunk_body["external_links"][0]["next_chunk_internal_link"],
            ),
            headers=headers,
            timeout=config.timeout_seconds,
        )
        _log_error(response, "Failed to query chunk")
        response.raise_for_status()
        chunk_body = response.json()
        if len(chunk_body["external_links"]) != 1:
            raise UnexpectedResponseError("not exactly 1 external link")
        data_links.append(
            (
                chunk_body["external_links"][0]["chunk_index"],
                chunk_body["external_links"][0]["external_link"],
            )
        )

    return data_links


def _generate_named_parameter_marker(value: str) -> str:
    return value.replace(" ", "_")


def _log_error(response, message: str):
    if response.status_code >= HTTPStatus.BAD_REQUEST:
        logger.error("%s - %s:\n%s", message, response.status_code, response.text)
