"""PostgreSQL connection for Kukur.

This uses the LGPL psycopg library.
"""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


try:
    import psycopg

    HAS_POSTGRES = True
except ImportError:
    HAS_POSTGRES = False

from kukur.exceptions import InvalidSourceException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


class PostgresSource(BaseSQLSource):
    """Kukur source for PostgreSQL."""

    def connect(self):
        """Create a connection to PostgreSQL."""
        if self._config.connection_string is None:
            raise InvalidSourceException(
                "'connection_string' is required for source with type 'postgresql'"
            )
        return psycopg.connect(self._config.connection_string)


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
) -> PostgresSource:
    """Create a new PostgresSource source from a configuration dictionay."""
    config = SQLConfig.from_dict(data)
    return PostgresSource(config, metadata_value_mapper, quality_mapper)
