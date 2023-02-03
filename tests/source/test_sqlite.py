"""Tests for SQLite source."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import sqlite3
from datetime import datetime

from pytest import approx

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.metadata import fields
from kukur.source import SourceFactory


def test_search() -> None:
    db_uri = "file:search?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                name text not null,
                description text,
                accuracy real
            );

            insert into Metadata (name, description, accuracy)
            values ('test-tag-1', 'test', 0.5)
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "list_query": "select name, description, accuracy from Metadata",
                    "list_columns": ["series name", "description", "accuracy"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        all_series = list(source.search(SeriesSearch("sqlite")))
        assert len(all_series) == 1
        assert isinstance(all_series[0], Metadata)
        assert all_series[0].get_field(fields.Accuracy) == approx(0.5)
        assert all_series[0].get_field(fields.Description) == "test"


def test_metadata_match() -> None:
    db_uri = "file:metadata_match?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                name text not null
            );

            insert into Metadata (name)
            values ('test-tag-1,location=Antwerp,unit=A');

            create table Sensors (
                id integer primary key autoincrement,
                location text not null,
                vendor text not null
            );

            insert into Sensors (location, vendor)
            values ('Hasselt', 'foo');

            insert into Sensors (location, vendor)
            values ('Antwerp', 'bar');
        """
        )
        connection.commit()

        config = {
            "metadata": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "metadata_query": "select vendor from Sensors where location = (? match 'location=([^,]+)')",
                    "metadata_columns": ["sensorVendor"],
                },
            },
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "list_query": "select name from Metadata",
                    "list_columns": ["series name"],
                    "metadata_sources": ["sqlite"],
                },
            },
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        all_series = list(source.search(SeriesSearch("sqlite")))
        assert len(all_series) == 1
        assert isinstance(all_series[0], Metadata)
        assert all_series[0].get_field_by_name("sensorVendor") == "bar"


def test_data_datetime_conversion() -> None:
    db_uri = "file:data_datetime?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Data (
                id integer primary key autoincrement,
                name text not null,
                ts datetime,
                value real
            );

            insert into Data (name, ts, value)
            values ('test-tag-1', '2022-01-01T00:00:00Z', 0.5);

            insert into Data (name, ts, value)
            values ('test-tag-1', '2022-01-02T00:00:00Z', 1.5);

            insert into Data (name, ts, value)
            values ('test-tag-1', '2022-01-03T00:00:00Z', 2);

            insert into Data (name, ts, value)
            values ('test-tag-1', '2022-01-04T00:00:00Z', 3);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "data_query": "select ts, value from Data where name = ? and ts >= ? and ts < ?",
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        data = source.get_data(
            SeriesSelector("sqlite", "test-tag-1"),
            datetime.fromisoformat("2022-01-01T00:00:00+00:00"),
            datetime.fromisoformat("2022-01-04T00:00:00+00:00"),
        )
        assert len(data) == 3
        assert data["ts"][0].as_py() == datetime.fromisoformat(
            "2022-01-01T00:00:00+00:00"
        )
        assert data["ts"][1].as_py() == datetime.fromisoformat(
            "2022-01-02T00:00:00+00:00"
        )
        assert data["ts"][2].as_py() == datetime.fromisoformat(
            "2022-01-03T00:00:00+00:00"
        )
        assert data["value"][0].as_py() == approx(0.5)
        assert data["value"][1].as_py() == approx(1.5)
        assert data["value"][2].as_py() == approx(2)
