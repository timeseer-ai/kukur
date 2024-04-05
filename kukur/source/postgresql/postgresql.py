"""PostgreSQL connection for Kukur.

Two providers can be used:
 - The BSD 3-clause pg8000 library.
 - The LGPL psycopg library.
"""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

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

from typing import Dict

from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


class PostgresSource(BaseSQLSource):
    """Kukur source for PostgreSQL."""

    def __init__(
        self,
        data: Dict,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        self.__provider = data.get("provider", "pg8000")
        if self.__provider == "psycopg" and not HAS_POSTGRES:
            raise MissingModuleException("postgresql", "psycopg")
        if self.__provider == "pg8000" and not HAS_POSTGRES_8000:
            raise MissingModuleException("postgresql", "pg8000")

        config = SQLConfig.from_dict(data)

        self.__connection_options = None
        if self.__provider == "pg8000":
            if "connection" not in data:
                raise InvalidSourceException(
                    "'postgresql' sources with a 'pg8000' provider require a 'connection' dictionary."
                )
            self.__connection_options = data["connection"]
        super().__init__(config, metadata_value_mapper, quality_mapper)

    def connect(self):
        """Create a connection to PostgreSQL."""
        if self.__provider == "psycopg":
            if self._config.connection_string is None:
                raise InvalidSourceException(
                    "'connection_string' is required for a source with type 'postgresql' and provider 'psycopg'."
                )
            return psycopg.connect(self._config.connection_string)
        if self.__connection_options is None:
            raise InvalidSourceException(
                "'postgresql' sources with a 'pg8000' provider require a 'connection' dictionary."
            )
        return pg8000.dbapi.connect(**self.__connection_options)


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
) -> PostgresSource:
    """Create a new PostgresSource source from a configuration dictionay."""
    if not HAS_POSTGRES and not HAS_POSTGRES_8000:
        raise MissingModuleException("postgresql", "pg8000 or psycopg")
    return PostgresSource(data, metadata_value_mapper, quality_mapper)
