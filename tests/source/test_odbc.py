"""Unit tests for the SQL source, using an in-memory SQLite database."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import sqlite3

from typing import Any, List

from kukur import Metadata, SeriesSelector
from kukur.source.metadata import MetadataValueMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


class DummySQLSource(BaseSQLSource):
    """In-memory SQLite database."""

    def __init__(self, config: SQLConfig, mapper: MetadataValueMapper):
        super().__init__(config, mapper)
        self.db = sqlite3.connect(config.connection_string)

    def connect(self):
        return self.db


def test_metadata_value():
    config = SQLConfig(
        ":memory:",
        metadata_query="select dictionary_name from Metadata where series_name = ?",
        metadata_columns=["dictionary name"],
    )
    source = DummySQLSource(config, MetadataValueMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            dictionary_name text
        );

        insert into Metadata (series_name, dictionary_name) values ('random', 'prisma');
    """
    )
    metadata = source.get_metadata(SeriesSelector("dummy", "random"))
    assert metadata.dictionary_name == "prisma"


def test_metadata_none_value_on_empty_return():
    config = SQLConfig(
        ":memory:",
        metadata_query="select dictionary_name from Metadata where series_name = ?",
        metadata_columns=["dictionary name"],
    )
    source = DummySQLSource(config, MetadataValueMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            dictionary_name text
        );

        insert into Metadata (series_name, dictionary_name) values ('random', '');
    """
    )
    metadata = source.get_metadata(SeriesSelector("dummy", "random"))
    assert metadata.dictionary_name is None


def test_list_none_value_on_empty_return():
    config = SQLConfig(
        ":memory:",
        list_query="select series_name, dictionary_name from Metadata",
        list_columns=["series name", "dictionary name"],
    )
    source = DummySQLSource(config, MetadataValueMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            dictionary_name text
        );

        insert into Metadata (series_name, dictionary_name) values ('random', '');
    """
    )
    for metadata in source.search(SeriesSelector("dummy")):
        assert metadata.series.name == "random"
        assert metadata.dictionary_name is None
