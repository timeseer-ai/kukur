"""Inspect Databricks SQL Warehouse databases.

This uses the functions in the ODBC inspect.
"""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Generator, List, Optional

import pyarrow as pa

from kukur.inspect import (
    Connection,
    DataOptions,
    InspectedPath,
)
from kukur.inspect.odbc import (
    inspect_odbc_database,
    preview_odbc_database,
    read_odbc_database,
)
from kukur.source.databricks_sql.databricks_sql import build_connection_string


def inspect_databricks_sql_database(
    config: Connection, path: Optional[str] = None
) -> List[InspectedPath]:
    """Inspect a database."""
    if config.connection_string is None and config.connection_options is not None:
        config.connection_string = build_connection_string(config.connection_options)
    return inspect_odbc_database(config, path)


def preview_databricks_sql_database(
    config: Connection,
    path: str,
    num_rows: int = 5000,
    options: Optional[DataOptions] = None,
) -> Optional[pa.Table]:
    """Preview the contents of a database."""
    if config.connection_string is None and config.connection_options is not None:
        config.connection_string = build_connection_string(config.connection_options)
    return preview_odbc_database(config, path, num_rows, options)


def read_databricks_sql_database(
    config: Connection, path: str, options: Optional[DataOptions] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over the RecordBatches at the given Connection."""
    if config.connection_string is None and config.connection_options is not None:
        config.connection_string = build_connection_string(config.connection_options)
    yield from read_odbc_database(config, path, options)
