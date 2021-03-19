"""Common parts for all repositories."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import abc
import contextlib
import sqlite3

from typing import List
from dateutil.parser import parse as parse_date


class BaseRepository:  # pylint: disable=too-few-public-methods
    """Base repository to open a fully-featured connection to sqlite."""

    __connection_string: str

    def __init__(self, connection_string: str):
        self.__connection_string = connection_string

    def _get_connection_string(self) -> str:
        return self.__connection_string

    def _open_db(self):
        return _open_db(self._get_connection_string())


class Migration(abc.ABC):  # pylint: disable=too-few-public-methods
    """A database migration makes changes to the schema of the database."""

    __connection_string: str

    def __init__(self, connection_string: str):
        self.__connection_string = connection_string

    @abc.abstractmethod
    def migrate(self):
        """Run a migration in the database given by the cursor."""
        ...

    def _open_db(self):
        return _open_db(self.__connection_string)


class MigrationRunner:
    """MigrationRunner keeps track of registered database migrations and runs them."""

    __migrations: List[Migration]

    def __init__(self):
        self.__migrations = []

    def register(self, migration: Migration):
        """Register a new migration with the migration runner."""
        self.__migrations.append(migration)

    def migrate(self):
        """Run all registered migrations."""
        for migration in self.__migrations:
            migration.migrate()


def _open_db(connection_string: str):
    sqlite3.register_converter("datetime", parse_date)
    connection = sqlite3.connect(
        connection_string,
        uri=True,
        timeout=60.0,
        detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
    )
    connection.execute("pragma foreign_keys = ON")
    return contextlib.closing(connection)
