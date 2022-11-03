"""Kukur connection to SQLite."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import re
import sqlite3

from dateutil.parser import parse as parse_date

from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


sqlite3.register_converter("datetime", parse_date)


def _match(pattern, value):
    match = re.search(pattern, value)
    if match is None:
        return None
    return match.group(1)


class SQLiteSource(BaseSQLSource):
    """Kukur source for SQLite."""

    def connect(self):
        uri = False
        if self._config.connection_string.startswith("file:"):
            uri = True
        if self._config.query_timeout_seconds == 0:
            connection = sqlite3.connect(
                self._config.connection_string,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                uri=uri,
            )
        else:
            connection = sqlite3.connect(
                self._config.connection_string,
                timeout=self._config.query_timeout_seconds,
                detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
                uri=uri,
            )

        connection.create_function("match", 2, _match)
        return connection


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
) -> SQLiteSource:
    """Create a new SQLite source from a configuration dictionay."""

    config = SQLConfig.from_dict(data)
    return SQLiteSource(config, metadata_value_mapper, quality_mapper)
