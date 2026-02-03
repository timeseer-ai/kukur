"""Connections to Databricks SQL data sources from Timeseer."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper

from .databricks_rest import (
    DatabricksStatementExecutionSource,
    StatementExecutionConfiguration,
)
from .databricks_sql import DatabricksSQLSource, SQLConfig, build_connection_string


def from_config(
    data: dict,
    metadata_value_mapper: MetadataValueMapper,
    quality_mapper: QualityMapper,
):
    """Create a new Databricks SQL data source from a configuration dict.

    Two different source providers are available:
    * REST (the default)
    * ODBC (requires the Databricks Simbaspark ODBC driver)
    """
    if "provider" not in data or data["provider"] == "REST":
        return DatabricksStatementExecutionSource(
            StatementExecutionConfiguration.from_data(data),
            metadata_value_mapper,
            quality_mapper,
        )

    if "connection_string" not in data and "connection" in data:
        connection_string = build_connection_string(data["connection"])
        data["connection_string"] = connection_string
    config = SQLConfig.from_dict(data)

    return DatabricksSQLSource(config, metadata_value_mapper, quality_mapper)


__all__ = ["from_config"]
