"""Persistence for api keys"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from typing import List, Tuple, Optional
from datetime import datetime
from kukur.repository.base import BaseRepository, Migration

from kukur.api_key import ApiKey


class ApiKeyMigration(Migration):  # pylint: disable=too-few-public-methods
    """Create database tables for api keys."""

    def migrate(self):
        with self._open_db() as db:
            cursor = db.cursor()
            cursor.executescript(
                """
                create table if not exists ApiKey (
                    id integer primary key autoincrement,
                    name text not null unique,
                    api_key blob not null,
                    salt blob not null,
                    creation_date datetime not null
                );
            """
            )
            db.commit()


class ApiKeyRepository(BaseRepository):
    """ApiKeyRepository stores and retrieves api keys."""

    def migrations(self):
        """Return the database migrations for the api key tables."""
        return ApiKeyMigration(self._get_connection_string())

    def store(self, name: str, api_key: bytes, salt: bytes, creation_date: datetime):
        """Create a new api key with the given name.

        It returns the newly created api_key."""
        with self._open_db() as db:
            cursor = db.cursor()
            cursor.execute(
                """
                insert into ApiKey (name, api_key, salt, creation_date)
                values (?, ?, ?, ?)
            """,
                [name, api_key, salt, creation_date],
            )
            db.commit()

    def list(self) -> List[ApiKey]:
        """List all api keys."""
        with self._open_db() as db:
            cursor = db.cursor()
            cursor.execute(
                """
            select name, creation_date
            from ApiKey
            """
            )
            results = cursor.fetchall()
            return [ApiKey(name, creation_date) for name, creation_date in results]

    def get(self, name) -> Tuple[Optional[bytes], Optional[bytes]]:
        """Get an api key by name."""
        with self._open_db() as db:
            cursor = db.cursor()
            cursor.execute(
                """
            select api_key, salt
            from ApiKey
            where name = ?
            """,
                [name],
            )
            result = cursor.fetchone()
            if result is None:
                return None, None
            api_key, salt = result
            return api_key, salt

    def has_api_key(self, name: str) -> bool:
        """Check if api key exists."""
        with self._open_db() as db:
            cursor = db.cursor()
            cursor.execute(
                """
            select count(*)
            from ApiKey
            where name = ?
            """,
                [name],
            )
            return cursor.fetchone()[0] == 1

    def revoke(self, name: str):
        """Delete an api key."""
        with self._open_db() as db:
            cursor = db.cursor()
            cursor.execute(
                """
                delete from ApiKey
                where name = ?
            """,
                [name],
            )
            db.commit()
