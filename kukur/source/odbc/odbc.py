"""Connections to ODBC data sources from Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

try:
    import pyodbc

    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False

from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
):
    """Create a new ODBC data source from a configuration dict."""
    if not HAS_ODBC:
        raise MissingModuleException("pyodbc", "odbc")

    config = SQLConfig.from_dict(data)

    return ODBCSource(config, metadata_value_mapper, quality_mapper)


class ODBCSource(BaseSQLSource):
    """An ODBC data source."""

    def __init__(
        self,
        config: SQLConfig,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        super().__init__(config, metadata_value_mapper, quality_mapper)
        if not HAS_ODBC:
            raise MissingModuleException("pyodbc", "odbc")

    def connect(self):
        """Return a pyodbc connection."""
        if self._config.connection_string is None:
            raise InvalidSourceException(
                "'connection_string' is required for source with type 'odbc'"
            )
        autocommit = False
        if self._config.autocommit is not None:
            autocommit = self._config.autocommit
        connection = pyodbc.connect(
            self._config.connection_string, autocommit=autocommit
        )
        if self._config.query_timeout_seconds is not None:
            connection.timeout = self._config.query_timeout_seconds
        return connection
