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
        if config.path is not None:
            schema = config.path
            column = "table_name"
            where = f"where table_schema = '{schema}'"
        cursor.execute(
            f"""
            select distinct {column} from information_schema.tables {where}
            """
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
        schema = split_path[0]
        table = split_path[1]

        if options is not None and options.column_names is not None:
            column_names = options.column_names
        else:
            cursor.execute(
                f"""
                select column_name from information_schema.columns
                    where table_schema = '{schema}'
                    and table_name = '{table}'
                """
            )
            column_names = [name for name, in cursor]

        cursor.execute(
            f"""
            select {", ".join(column_names)} from {schema}.{table} limit {num_rows}
            """
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
        schema = split_path[0]
        table = split_path[1]

        if options is not None and options.column_names is not None:
            column_names = options.column_names
        else:
            cursor.execute(
                f"""
                select column_name from information_schema.columns
                    where table_schema = '{schema}'
                    and table_name = '{table}'
                """
            )
            column_names = [name for name, in cursor]

        cursor.execute(
            f"""
            select {", ".join(column_names)} from {schema}.{table}
            """
        )

        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        yield pa.RecordBatch.from_pydict(results)
