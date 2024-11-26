"""Inspect databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from typing import Generator, List, Optional

import psycopg
import pyarrow as pa

from kukur.inspect import (
    Connection,
    InspectedPath,
    InspectOptions,
    InvalidInspectURI,
    ResourceType,
)


def inspect_database(config: Connection) -> List[InspectedPath]:
    """Inspect a database."""
    resource_type = ResourceType.TABLE
    results = []
    if config.connection_type == "postgres":
        connection = psycopg.connect(config.connection_string)
        cursor = connection.cursor()
        where = ""
        column = "table_schema"
        params = []
        if config.path is not None:
            params.append(config.path)
            column = "table_name"
            where = "where table_schema = %s"
        cursor.execute(
            f"""
            select distinct {column} from information_schema.tables {where}
            """,
            params,
        )
        for (name,) in cursor:
            path = name
            if config.path is not None:
                path = config.path + f"/{name}"
            results.append(InspectedPath(resource_type, path))
    return results


def preview_database(
    config: Connection, num_rows: int = 5000, options: Optional[InspectOptions] = None
) -> Optional[pa.Table]:
    """Preview the contents of a database."""
    if config.connection_type == "postgres":
        connection = psycopg.connect(config.connection_string)
        cursor = connection.cursor()
        if config.path is None:
            raise InvalidInspectURI("No schema and table provided.")
        split_path = config.path.split("/")
        if len(split_path) == 1:
            InvalidInspectURI("No schema or table provided.")

        if options is not None and options.column_names is not None:
            column_names = options.column_names
        else:
            cursor.execute(
                """
                select column_name from information_schema.columns
                  where table_schema = %s
                    and table_name = %s
                """,
                split_path,
            )
            column_names = [name for name, in cursor]

        query = psycopg.sql.SQL("select {column_names} from {schema}.{table} limit %s").format(
            column_names=psycopg.sql.SQL(",").join([
                psycopg.sql.Identifier(column_name)
            for column_name in column_names]),
            schema=psycopg.sql.Identifier(split_path[0]),
            table=psycopg.sql.Identifier(split_path[1])
        )
        cursor.execute(
            query,
            [num_rows]
        )

        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        return pa.Table.from_pydict(results)
    return None


def read_database(
    config: Connection, options: Optional[InspectOptions] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over the RecordBatches at the given Connection."""
    if config.connection_type == "postgres":
        connection = psycopg.connect(config.connection_string)
        cursor = connection.cursor()
        if config.path is None:
            raise InvalidInspectURI("No schema and table provided.")
        split_path = config.path.split("/")
        if len(split_path) == 1:
            raise InvalidInspectURI("No schema or table provided.")

        if options is not None and options.column_names is not None:
            column_names = options.column_names
        else:
            cursor.execute(
                """
                select column_name from information_schema.columns
                    where table_schema = %s
                    and table_name = %s
                """,
                split_path,
            )
            column_names = [name for name, in cursor]

        query = psycopg.sql.SQL("select {column_names} from {schema}.{table}").format(
            column_names=psycopg.sql.SQL(",").join([
                psycopg.sql.Identifier(column_name)
            for column_name in column_names]),
            schema=psycopg.sql.Identifier(split_path[0]),
            table=psycopg.sql.Identifier(split_path[1])
        )
        cursor.execute(
            query,
        )

        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        yield pa.RecordBatch.from_pydict(results)
