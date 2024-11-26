"""Inspect postgres databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections import defaultdict
from typing import Generator, List, Optional, Union

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
    InspectedPath,
    InspectOptions,
    InvalidInspectURI,
    ResourceType,
)


class PostgresPg8000:
    """Inspect class for PG8000 postgres connections."""

    def __init__(self, connection_options: dict):
        self.__connection_options = connection_options

    def _connect(self) -> pg8000.dbapi.Connection:
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
            if path is not None:
                full_path = path + f"/{name}"
            results.append(InspectedPath(ResourceType.TABLE, full_path))
        return results

    def preview_database(
        self,
        path: str,
        num_rows: int = 5000,
        options: Optional[InspectOptions] = None,
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
        params = column_names + [num_rows]
        columns = ", ".join(["%s"] * len(column_names))
        query = f"select {columns} from {_escape(split_path[0])}.{_escape(split_path[1])} limit %s"

        cursor.execute(query, params)

        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        return pa.Table.from_pydict(results)

    def read_database(
        self, path: str, options: Optional[InspectOptions] = None
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
        params = column_names
        columns = ", ".join(["%s"] * len(column_names))
        query = (
            f"select {columns} from {_escape(split_path[0])}.{_escape(split_path[1])}"
        )

        cursor.execute(query, params)
        results = defaultdict(list)
        for row in cursor:
            for index in range(len(row)):
                results[column_names[index]].append(row[index])
        yield pa.RecordBatch.from_pydict(results)


class PostgresPsycopg:
    """Inspect class for psycopg postgres connections."""

    def __init__(self, connection_string: str):
        self.__connection_string = connection_string

    def _connect(self) -> psycopg.Connection:
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
            if path is not None:
                full_path = path + f"/{name}"
            results.append(InspectedPath(ResourceType.TABLE, full_path))
        return results

    def preview_database(
        self,
        path: str,
        num_rows: int = 5000,
        options: Optional[InspectOptions] = None,
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
        self, path: str, options: Optional[InspectOptions] = None
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


def get_connection(config: Connection) -> Union[PostgresPg8000, PostgresPsycopg]:
    """Return a postgres connection."""
    connection: Union[PostgresPg8000, PostgresPsycopg]
    if config.connection_options is not None:
        if not HAS_POSTGRES_8000:
            MissingModuleException("pg8000")
        connection = PostgresPg8000(config.connection_options)
    elif config.connection_string is not None:
        if not HAS_POSTGRES:
            MissingModuleException("psycopg")
        connection = PostgresPsycopg(config.connection_string)
    else:
        InvalidSourceException("Missing `connection_options` or `connection_string`")
    return connection


def _escape(context: Optional[str]) -> str:
    if context is None:
        context = "value"
    if '"' in context:
        context = context.replace('"', "")
    if ";" in context:
        context = context.replace(";", "")
    return context
