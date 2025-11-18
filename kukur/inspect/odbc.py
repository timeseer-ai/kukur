"""Inspect ODBC databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from collections.abc import Generator

import pyarrow as pa

from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.inspect import (
    Connection,
    DataOptions,
    InspectedPath,
    InvalidInspectURI,
    ResourceType,
)

try:
    import pyodbc

    HAS_ODBC = True
except ImportError:
    HAS_ODBC = False


def inspect_odbc_database(
    config: Connection, path: str | None = None
) -> list[InspectedPath]:
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

    catalog = ""
    if config.catalog is not None:
        catalog = f"{_escape(config.catalog)}."

    cursor.execute(
        f"""
        select distinct {column} from {catalog}information_schema.tables {where}
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
    options: DataOptions | None = None,
) -> pa.Table | None:
    """Preview the contents of a database."""
    connection = _get_connection(config)
    cursor = connection.cursor()
    split_path = path.split("/")
    if len(split_path) == 1:
        InvalidInspectURI("No schema or table provided.")

    catalog = ""
    if config.catalog is not None:
        catalog = f"{_escape(config.catalog)}."

    if options is not None and options.column_names is not None:
        column_names = options.column_names
    else:
        cursor.execute(
            f"""
            select column_name from {catalog}information_schema.columns
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
            from {catalog}{_escape(split_path[0])}.{_escape(split_path[1])} limit ?
        """
        cursor.execute(query, [num_rows])
    else:
        query = f"""
            select top {num_rows} {', '.join(columns)}
            from {catalog}{_escape(split_path[0])}.{_escape(split_path[1])}
        """
        cursor.execute(query)

    results = defaultdict(list)
    for row in cursor:
        for index in range(len(row)):
            results[column_names[index]].append(row[index])
    connection.close()
    return pa.Table.from_pydict(results)


def read_odbc_database(
    config: Connection, path: str, options: DataOptions | None = None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over the RecordBatches at the given Connection."""
    connection = _get_connection(config)
    cursor = connection.cursor()
    split_path = path.split("/")
    if len(split_path) == 1:
        raise InvalidInspectURI("No schema or table provided.")

    catalog = ""
    if config.catalog is not None:
        catalog = f"{_escape(config.catalog)}."

    if options is not None and options.column_names is not None:
        column_names = options.column_names
    else:
        cursor.execute(
            f"""
            select column_name from {catalog}information_schema.columns
            where table_schema = ?
                and table_name = ?
            """,
            split_path,
        )
        column_names = [name for name, in cursor]

    columns = [_escape(column_name) for column_name in column_names]
    query = f"""
        select {', '.join(columns)}
        from {catalog}{_escape(split_path[0])}.{_escape(split_path[1])}
    """

    cursor.execute(query)
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
    if config.connection_string is None:
        raise InvalidSourceException("Missing `connection_string` in configuration")
    return pyodbc.connect(config.connection_string, autocommit=True)


def _escape(context: str | None) -> str:
    if context is None:
        context = "value"
    if '"' in context:
        context = context.replace('"', "")
    if ";" in context:
        context = context.replace(";", "")
    return f'"{context}"'
