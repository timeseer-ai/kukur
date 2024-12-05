"""Inspect databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Generator, List, Optional

import pyarrow as pa

from kukur.inspect import (
    Connection,
    InspectedPath,
    InspectOptions,
)
from kukur.inspect.databricks_sql import (
    inspect_databricks_sql_database,
    preview_databricks_sql_database,
    read_databricks_sql_database,
)
from kukur.inspect.odbc import (
    inspect_odbc_database,
    preview_odbc_database,
    read_odbc_database,
)
from kukur.inspect.postgres import get_connection


def inspect_database(
    config: Connection, path: Optional[str] = None
) -> List[InspectedPath]:
    """Inspect a database."""
    if config.connection_type == "postgresql":
        connection = get_connection(config)
        return connection.inspect_database(path)

    if config.connection_type == "databricks-sql":
        return inspect_databricks_sql_database(config, path)

    if config.connection_type == "odbc":
        return inspect_odbc_database(config, path)

    return []


def preview_database(
    config: Connection,
    path: str,
    num_rows: int = 5000,
    options: Optional[InspectOptions] = None,
) -> Optional[pa.Table]:
    """Preview the contents of a database."""
    if config.connection_type == "postgresql":
        connection = get_connection(config)
        return connection.preview_database(path, num_rows, options)

    if config.connection_type == "databricks-sql":
        return preview_databricks_sql_database(config, path, num_rows, options)

    if config.connection_type == "odbc":
        return preview_odbc_database(config, path, num_rows, options)
    return None


def read_database(
    config: Connection, path: str, options: Optional[InspectOptions] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over the RecordBatches at the given Connection."""
    if config.connection_type == "postgresql":
        connection = get_connection(config)
        yield from connection.read_database(path, options)
    elif config.connection_type == "databricks-sql":
        yield from read_databricks_sql_database(config, path, options)
    elif config.connection_type == "odbc":
        yield from read_odbc_database(config, path, options)
