"""Connections to Databricks SQL data sources from Timeseer."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0

try:
    import pyodbc  # noqa: F401

    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False

from kukur.exceptions import MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.odbc.odbc import ODBCSource
from kukur.source.quality import QualityMapper
from kukur.source.sql import SQLConfig


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
):
    """Create a new Databricks SQL data source from a configuration dict."""
    if not HAS_ODBC:
        raise MissingModuleException("pyodbc", "odbc")

    if "connection_string" not in data and "connection" in data:
        connection_string = build_connection_string(data["connection"])
        data["connection_string"] = connection_string
    config = SQLConfig.from_dict(data)

    return DatabricksSQLSource(config, metadata_value_mapper, quality_mapper)


class DatabricksSQLSource(ODBCSource):
    """A Databricks SQL data source."""

    def __init__(
        self,
        config: SQLConfig,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        super().__init__(config, metadata_value_mapper, quality_mapper)


def build_connection_string(data: dict) -> str:
    """Build the connection string from configuration."""
    connection_string = f"""
        Driver={data['driver']};
        Host={data['host']};
        Port={data.get("port", 433)};
        SSL=1;
        ThriftTransport=2;
        HTTPPath={data['http_path']};
        """
    if data.get("oauth_client_id") is not None:
        connection_string = f"""
            {connection_string}
            AuthMech=11;
            Auth_Flow=1;
            Auth_Client_ID={data['oauth_client_id']};
            Auth_Client_Secret={data['oauth_secret']};
        """
    else:
        connection_string = f"""
            {connection_string}
            Azure_workspace_resource_id={data['azure_workspace_resource_id']};
            AuthMech=11;
            Auth_Flow=3;
        """
    return connection_string
