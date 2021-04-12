"""Connections to ODBC data sources from Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

try:
    import pyodbc

    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False

from kukur.source.metadata import MetadataValueMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


class ODBCNotInstalledError(Exception):
    """Raised when the pyodbc module is not available."""

    def __init__(self):
        Exception.__init__(self, "the pyodbc modules is not available. Install pyodbc")


def from_config(data, metadata_value_mapper: MetadataValueMapper):
    """Create a new ODBC data source from a configuration dict."""
    if not HAS_ODBC:
        raise ODBCNotInstalledError()

    config = SQLConfig.from_dict(data)

    return ODBCSource(config, metadata_value_mapper)


class ODBCSource(BaseSQLSource):
    """An ODBC data source."""

    def __init__(self, config: SQLConfig, metadata_value_mapper: MetadataValueMapper):
        super().__init__(config, metadata_value_mapper)
        if not HAS_ODBC:
            raise ODBCNotInstalledError()

    def connect(self):
        return pyodbc.connect(self._config.connection_string)
