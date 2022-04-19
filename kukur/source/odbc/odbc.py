"""Connections to ODBC data sources from Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

try:
    import pyodbc

    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False

from kukur.exceptions import MissingModuleException
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
        connection = pyodbc.connect(self._config.connection_string)
        connection.timeout = self._config.query_timeout_seconds
        return connection
