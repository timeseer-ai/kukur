"""Connections to Databricks SQL Warehouse data sources from Timeseer."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
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


class DatabricksSQLSource(ODBCSource):
    """A Databricks SQL data source."""

    def __init__(
        self,
        config: SQLConfig,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        if not HAS_ODBC:
            raise MissingModuleException("pyodbc", "odbc")
        if config.autocommit is None:
            config.autocommit = True
        super().__init__(config, metadata_value_mapper, quality_mapper)


def build_connection_string(data: dict) -> str:
    """Build the connection string from configuration."""
    connection_string = f"""
Driver={data.get("driver", "/opt/simba/spark/lib/64/libsparkodbc_sb64.so")};
Host={data["host"]};
Port={data.get("port", 443)};
SSL=1;
ThriftTransport=2;
HTTPPath={data["http_path"]};
        """
    if data.get("oauth_client_id") is not None:
        connection_string = f"""
{connection_string}
AuthMech=11;
Auth_Flow=1;
Auth_Client_ID={data["oauth_client_id"]};
Auth_Client_Secret={data["oauth_secret"]};
        """
    elif data.get("password") is not None:
        connection_string = f"""
{connection_string}
AuthMech=3;
UID=token;
PWD={data["password"]};
"""
    else:
        connection_string = f"""
{connection_string}
Azure_workspace_resource_id={data["azure_workspace_resource_id"]};
AuthMech=11;
Auth_Flow=3;
        """
    return connection_string
