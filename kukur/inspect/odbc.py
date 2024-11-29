"""Inspect ODBC databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from typing import Generator, List, Optional

import pyarrow as pa

from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.inspect import (
    Connection,
    InspectedPath,
    InspectOptions,
    InvalidInspectURI,
    ResourceType,
)
from kukur.source.databricks_sql.databricks_sql import build_connection_string

try:
    import pyodbc

    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False


def inspect_odbc_database(
    config: Connection, path: Optional[str] = None
) -> List[InspectedPath]:
    """Inspect a database."""
    connection = _get_connection(config)
    cursor = connection.cursor()
    where = ""
    column = "table_schema"
    params = []
    if path is not None:
        params.append(path)
        column = "table_name"
        where = "where table_schema = ?"
    cursor.execute(
        f"""
        select distinct {column} from {_escape(config.database)}.information_schema.tables {where}
        """,
        params,
    )
    results = []
    for (name,) in cursor:
        full_path = name
        resource_type = ResourceType.DIRECTORY
        if path is not None:
            full_path = path + f"/{name}"
            resource_type = ResourceType.TABLE
        results.append(InspectedPath(resource_type, full_path))
    connection.close()
    return results


def preview_odbc_database(
    config: Connection,
    path: str,
    num_rows: int = 5000,
    options: Optional[InspectOptions] = None,
) -> Optional[pa.Table]:
    """Preview the contents of a database."""
    connection = _get_connection(config)
    cursor = connection.cursor()
    split_path = path.split("/")
    if len(split_path) == 1:
        InvalidInspectURI("No schema or table provided.")
    if options is not None and options.column_names is not None:
        column_names = options.column_names
    else:
        cursor.execute(
            f"""
            select column_name from {_escape(config.database)}.information_schema.columns
            where table_schema = ?
                and table_name = ?
            """,
            split_path,
        )
        column_names = [name for name, in cursor]

    columns = [_escape(column_name) for column_name in column_names]
    if config.limit_specification == "limit":
        query = f"""
            select {', '.join(columns)}
            from {_escape(config.database)}.{_escape(split_path[0])}.{_escape(split_path[1])} limit ?
        """
        cursor.execute(query, [num_rows])
    else:
        query = f"""
            select top {num_rows} {', '.join(columns)}
            from {_escape(config.database)}.{_escape(split_path[0])}.{_escape(split_path[1])}
        """
        cursor.execute(query)

    results = defaultdict(list)
    for row in cursor:
        for index in range(len(row)):
            results[column_names[index]].append(row[index])
    connection.close()
    return pa.Table.from_pydict(results)


def read_odbc_database(
    config: Connection, path: str, options: Optional[InspectOptions] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over the RecordBatches at the given Connection."""
    connection = _get_connection(config)
    cursor = connection.cursor()
    split_path = path.split("/")
    if len(split_path) == 1:
        raise InvalidInspectURI("No schema or table provided.")
    if options is not None and options.column_names is not None:
        column_names = options.column_names
    else:
        cursor.execute(
            f"""
            select column_name from {_escape(config.database)}.information_schema.columns
            where table_schema = ?
                and table_name = ?
            """,
            split_path,
        )
        column_names = [name for name, in cursor]
    params = column_names
    columns = ", ".join(["?"] * len(column_names))
    query = f"select {columns} from {_escape(config.database)}.{_escape(split_path[0])}.{_escape(split_path[1])}"

    cursor.execute(query, params)
    results = defaultdict(list)
    for row in cursor:
        for index in range(len(row)):
            results[column_names[index]].append(row[index])
    yield pa.RecordBatch.from_pydict(results)
    connection.close()


def _get_connection(config: Connection):
    """Return an ODBC connection."""
    if not HAS_ODBC:
        raise MissingModuleException("pyodbc")
    connection_string = config.connection_string
    if config.connection_string is None:
        if config.connection_options is None:
            raise InvalidSourceException("Missing `connection_string` in configuration")
        connection_string = build_connection_string(config.connection_options)
    return pyodbc.connect(connection_string, autocommit=True)


def _escape(context: Optional[str]) -> str:
    if context is None:
        context = "value"
    if '"' in context:
        context = context.replace('"', "")
    if ";" in context:
        context = context.replace(";", "")
    return context
