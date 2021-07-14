"""Unit tests for the SQL source, using an in-memory SQLite database."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import math
import sqlite3

from datetime import datetime
from typing import Any, List

from dateutil.parser import parse as parse_date

from kukur import Metadata, SeriesSelector
from kukur.source.metadata import MetadataValueMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


class DummySQLSource(BaseSQLSource):
    """In-memory SQLite database."""

    def __init__(self, config: SQLConfig, mapper: MetadataValueMapper):
        super().__init__(config, mapper)
        sqlite3.register_converter("datetime", parse_date)
        self.db = sqlite3.connect(
            config.connection_string,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )

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


def test_blob_values():
    config = SQLConfig(
        ":memory:",
        data_query="select ts, value from Data where series_name = ? and ts between ? and ?",
    )
    source = DummySQLSource(config, MetadataValueMapper())
    source.db.execute(
        """
        create table Data (
            ts datetime,
            series_name text,
            value blob
        );
        """
    )
    source.db.execute(
        "insert into Data (ts, series_name, value) values (?, ?, ?)",
        [
            datetime.fromisoformat("2021-01-02T00:00:00+00:00"),
            "blob-series",
            b"hello",
        ],
    )
    source.db.execute(
        "insert into Data (ts, series_name, value) values (?, ?, ?)",
        [
            datetime.fromisoformat("2021-01-02T00:00:00+00:00"),
            "blob-series",
            None,
        ],
    )

    data = source.get_data(
        SeriesSelector("dummy", "blob-series"),
        datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2021-02-01T00:00:00+00:00"),
    )

    assert len(data) == 1
    assert math.isnan(data["value"][0].as_py())


def test_datetime_values():
    config = SQLConfig(
        ":memory:",
        data_query="select ts, value from Data where series_name = ? and ts between ? and ?",
    )
    source = DummySQLSource(config, MetadataValueMapper())
    source.db.execute(
        """
        create table Data (
            ts datetime,
            series_name text,
            value datetime
        );
        """
    )
    source.db.execute(
        "insert into Data (ts, series_name, value) values (?, ?, ?)",
        [
            datetime.fromisoformat("2021-01-02T00:00:00+00:00"),
            "datetime-series",
            datetime.fromisoformat("2021-01-02T12:34:56+00:00"),
        ],
    )

    data = source.get_data(
        SeriesSelector("dummy", "datetime-series"),
        datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2021-02-01T00:00:00+00:00"),
    )

    assert len(data) == 1
    assert (
        data["value"][0].as_py()
        == datetime.fromisoformat("2021-01-02T12:34:56+00:00").isoformat()
    )
