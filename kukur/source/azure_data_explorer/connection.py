"""Connections to Azure Data Explorer."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, TypeVar

from kukur.exceptions import MissingModuleException
from kukur.source.token_cache import Token, TokenCache

try:
    from azure.identity import ClientSecretCredential, DefaultAzureCredential

    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False

try:
    from azure.kusto.data import (
        ClientRequestProperties,
        KustoClient,
        KustoConnectionStringBuilder,
    )
    from azure.kusto.data.exceptions import KustoThrottlingError
    from azure.kusto.data.response import (
        KustoResponseDataSet,
        KustoStreamingResponseDataSet,
    )

    HAS_KUSTO = True
except ImportError:
    HAS_KUSTO = False

MAX_THROTTLE_COUNT = 8  # 4.5 minutes

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class DataExplorerConnectionConfiguration:
    """How to connect to an Azure Data Explorer database."""

    connection_string: str
    database: str
    client_id: str | None
    client_secret: str | None
    tenant_id: str | None
    throttle_backoff_count: int

    @classmethod
    def from_config(
        cls, config: dict[str, Any]
    ) -> "DataExplorerConnectionConfiguration":
        """Read the connection settings of a source configuration."""
        return cls(
            config["connection_string"],
            config["database"],
            config.get("client_id"),
            config.get("client_secret"),
            config.get("tenant_id"),
            config.get("throttle_backoff_count", MAX_THROTTLE_COUNT),
        )


class DataExplorerConnection:
    """A connection to an Azure Data Explorer database.

    Throttled queries are retried with an exponential backoff.
    Use it as a context manager: the connection is closed on exit.
    """

    def __init__(
        self,
        config: DataExplorerConnectionConfiguration,
        token_cache: TokenCache,
        sleep: Callable[[int], None] = time.sleep,
    ):
        if not HAS_AZURE_IDENTITY:
            raise MissingModuleException("azure-identity")

        if not HAS_KUSTO:
            raise MissingModuleException("azure-kusto-data", "data_explorer")

        self.__config = config
        self.__sleep = sleep
        self.__client = KustoClient(
            KustoConnectionStringBuilder.with_token_provider(
                config.connection_string,
                lambda: token_cache.get_token(self._get_azure_token),
            )
        )

    def __enter__(self) -> "DataExplorerConnection":
        return self

    def __exit__(self, *_args) -> None:
        self.__client.close()

    def execute(
        self, query: str, properties: "ClientRequestProperties | None" = None
    ) -> "KustoResponseDataSet":
        """Run a query and read its complete result."""
        return self._retry_throttled(
            lambda: self.__client.execute(self.__config.database, query, properties)
        )

    def execute_streaming(
        self, query: str, properties: "ClientRequestProperties | None" = None
    ) -> "KustoStreamingResponseDataSet":
        """Run a query and stream its result."""
        return self._retry_throttled(
            lambda: self.__client.execute_streaming_query(
                self.__config.database, query, properties=properties
            )
        )

    def _retry_throttled(self, call: Callable[[], T]) -> T:
        throttle_count = 1
        while True:
            try:
                return call()
            except KustoThrottlingError:
                logger.info(
                    "request throttled, sleeping for %s seconds", 2**throttle_count
                )
                self.__sleep(2**throttle_count)
                throttle_count = throttle_count + 1
                if throttle_count > self.__config.throttle_backoff_count:
                    raise

    def _get_azure_token(self, _refresh_token: str | None) -> Token:
        if (
            self.__config.client_id is not None
            and self.__config.client_secret is not None
            and self.__config.tenant_id is not None
        ):
            azure_credential: Any = ClientSecretCredential(
                client_id=self.__config.client_id,
                client_secret=self.__config.client_secret,
                tenant_id=self.__config.tenant_id,
            )
        else:
            azure_credential = DefaultAzureCredential()
        token = azure_credential.get_token(
            self.__config.connection_string + "//.default"
        )
        return Token(token.token, datetime.fromtimestamp(token.expires_on, tz=UTC))
