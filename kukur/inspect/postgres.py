"""Inspect postgres databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from typing import Generator, List, Optional

from kukur.exceptions import InvalidSourceException, MissingModuleException

try:
    import pg8000.dbapi

    HAS_POSTGRES_8000 = True
except ImportError:
    HAS_POSTGRES_8000 = False

try:
    import psycopg

    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

import pyarrow as pa

from kukur.inspect import (
    Connection,
    DataOptions,
    InspectedPath,
    InvalidInspectURI,
    ResourceType,
)


class PostgresPg8000:
    """Inspect class for PG8000 postgres connections."""

    def __init__(self, connection_options: dict):
        self.__connection_options = connection_options

    def _connect(self):
        return pg8000.dbapi.connect(**self.__connection_options)

    def inspect_database(self, path: Optional[str] = None) -> List[InspectedPath]:
        """Inspect a database."""
        connection = self._connect()
        cursor = connection.cursor()
        where = ""
        column = "table_schema"
        params = []
        if path is not None:
            params.append(path)
            column = "table_name"
            where = "where table_schema = %s"
        cursor.execute(
            f"""
            select distinct {column} from information_schema.tables {where}
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
        return results

    def preview_database(
        self,
        path: str,
        num_rows: int = 5000,
        options: Optional[DataOptions] = None,
    ) -> Optional[pa.Table]:
        """Preview the contents of a database."""
        connection = self._connect()
        cursor = connection.cursor()
        split_path = path.split("/")
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
        columns = [_escape(column) for column in column_names]
        params = [num_rows]
        query = f"select {', '.join(columns)} from {_escape(split_path[0])}.{_escape(split_path[1])} limit %s"

        cursor.execute(query, params)

        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        return pa.Table.from_pydict(results)

    def read_database(
        self, path: str, options: Optional[DataOptions] = None
    ) -> Generator[pa.RecordBatch, None, None]:
        """Iterate over the RecordBatches at the given Connection."""
        connection = self._connect()
        cursor = connection.cursor()
        split_path = path.split("/")
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
        columns = [_escape(column) for column in column_names]
        query = f"select {', '.join(columns)} from {_escape(split_path[0])}.{_escape(split_path[1])}"
        cursor.execute(query)
        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        yield pa.RecordBatch.from_pydict(results)


class PostgresPsycopg:
    """Inspect class for psycopg postgres connections."""

    def __init__(self, connection_string: str):
        self.__connection_string = connection_string

    def _connect(self):
        return psycopg.connect(self.__connection_string)

    def inspect_database(self, path: Optional[str] = None) -> List[InspectedPath]:
        """Inspect a database."""
        connection = self._connect()
        cursor = connection.cursor()
        where = ""
        column = "table_schema"
        params = []
        if path is not None:
            params.append(path)
            column = "table_name"
            where = "where table_schema = %s"
        cursor.execute(
            f"""
            select distinct {column} from information_schema.tables {where}
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
        return results

    def preview_database(
        self,
        path: str,
        num_rows: int = 5000,
        options: Optional[DataOptions] = None,
    ) -> Optional[pa.Table]:
        """Preview the contents of a database."""
        connection = self._connect()
        cursor = connection.cursor()
        split_path = path.split("/")
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

        query = psycopg.sql.SQL(
            "select {column_names} from {schema}.{table} limit %s"
        ).format(
            column_names=psycopg.sql.SQL(",").join(
                [psycopg.sql.Identifier(column_name) for column_name in column_names]
            ),
            schema=psycopg.sql.Identifier(split_path[0]),
            table=psycopg.sql.Identifier(split_path[1]),
        )

        cursor.execute(query, [num_rows])
        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        return pa.Table.from_pydict(results)

    def read_database(
        self, path: str, options: Optional[DataOptions] = None
    ) -> Generator[pa.RecordBatch, None, None]:
        """Iterate over the RecordBatches at the given Connection."""
        connection = self._connect()
        cursor = connection.cursor()
        split_path = path.split("/")
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
            column_names=psycopg.sql.SQL(",").join(
                [psycopg.sql.Identifier(column_name) for column_name in column_names]
            ),
            schema=psycopg.sql.Identifier(split_path[0]),
            table=psycopg.sql.Identifier(split_path[1]),
        )
        cursor.execute(query)
        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        yield pa.RecordBatch.from_pydict(results)


def get_connection(config: Connection):
    """Return a postgres connection."""
    if config.connection_options is not None:
        if not HAS_POSTGRES_8000:
            raise MissingModuleException("pg8000")
        return PostgresPg8000(config.connection_options)
    if config.connection_string is not None:
        if not HAS_POSTGRES:
            raise MissingModuleException("psycopg")
        return PostgresPsycopg(config.connection_string)
    raise InvalidSourceException("Missing `connection_options` or `connection_string`")


def _escape(context: Optional[str]) -> str:
    if context is None:
        context = "value"
    if '"' in context:
        context = context.replace('"', "")
    if ";" in context:
        context = context.replace(";", "")
    return context
