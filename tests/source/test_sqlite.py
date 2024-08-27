"""Tests for SQLite source."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import sqlite3
from datetime import datetime

import pytest
from pytest import approx

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.metadata import fields
from kukur.source import SourceFactory
from kukur.source.sql import InvalidConfigurationError


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


def test_search_with_tags() -> None:
    db_uri = "file:search_tags?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                location text not null,
                plant text not null,
                description text,
                accuracy real
            );

            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '01', 'test', 0.5);
            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '02', 'test', 1);
            insert into Metadata (location, plant, description, accuracy)
            values ('Barcelona', '02', 'test', 1.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "list_query": "select location, plant, description, accuracy from Metadata",
                    "list_columns": ["location", "plant", "description", "accuracy"],
                    "tag_columns": ["location", "plant"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        all_series = list(source.search(SeriesSearch("sqlite")))
        assert len(all_series) == 3
        assert isinstance(all_series[0], Metadata)
        assert all_series[0].series.tags == {"location": "Antwerp", "plant": "01"}
        assert all_series[0].get_field(fields.Accuracy) == approx(0.5)
        assert all_series[0].get_field(fields.Description) == "test"
        assert isinstance(all_series[1], Metadata)
        assert all_series[1].series.tags == {"location": "Antwerp", "plant": "02"}
        assert all_series[1].get_field(fields.Accuracy) == approx(1)
        assert all_series[1].get_field(fields.Description) == "test"
        assert isinstance(all_series[2], Metadata)
        assert all_series[2].series.tags == {"location": "Barcelona", "plant": "02"}
        assert all_series[2].get_field(fields.Accuracy) == approx(1.5)
        assert all_series[2].get_field(fields.Description) == "test"


def test_search_with_tags_and_fields() -> None:
    db_uri = "file:search_tag_fields?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                location text not null,
                plant text not null,
                description text,
                accuracy real
            );

            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '01', 'test', 0.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "list_query": "select location, plant, description, accuracy from Metadata",
                    "list_columns": ["location", "plant", "description", "accuracy"],
                    "tag_columns": ["location", "plant"],
                    "field_columns": ["temperature", "pressure"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        all_series = list(source.search(SeriesSearch("sqlite")))
        assert len(all_series) == 2
        assert isinstance(all_series[0], Metadata)
        assert all_series[0].series.tags == {"location": "Antwerp", "plant": "01"}
        assert all_series[0].series.field == "temperature"
        assert all_series[0].get_field(fields.Accuracy) == approx(0.5)
        assert all_series[0].get_field(fields.Description) == "test"
        assert isinstance(all_series[1], Metadata)
        assert all_series[1].series.tags == {"location": "Antwerp", "plant": "01"}
        assert all_series[1].series.field == "pressure"
        assert all_series[1].get_field(fields.Accuracy) == approx(0.5)
        assert all_series[1].get_field(fields.Description) == "test"


def test_search_names_with_tags() -> None:
    db_uri = "file:search_name_tag?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                location text not null,
                plant text not null,
                description text,
                accuracy real
            );

            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '01', 'test', 0.5);
            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '02', 'test', 1);
            insert into Metadata (location, plant, description, accuracy)
            values ('Barcelona', '02', 'test', 1.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "list_query": "select location, plant from Metadata",
                    "tag_columns": ["location", "plant"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        all_series = list(source.search(SeriesSearch("sqlite")))
        assert len(all_series) == 3
        assert isinstance(all_series[0], SeriesSelector)
        assert all_series[0].tags == {"location": "Antwerp", "plant": "01"}
        assert isinstance(all_series[1], SeriesSelector)
        assert all_series[1].tags == {"location": "Antwerp", "plant": "02"}
        assert isinstance(all_series[2], SeriesSelector)
        assert all_series[2].tags == {"location": "Barcelona", "plant": "02"}


def test_search_names_with_tags_and_fields() -> None:
    db_uri = "file:search_name_field?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                location text not null,
                plant text not null,
                description text,
                accuracy real
            );

            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '01', 'test', 0.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "list_query": "select location, plant from Metadata",
                    "tag_columns": ["location", "plant"],
                    "field_columns": ["temperature", "pressure"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        all_series = list(source.search(SeriesSearch("sqlite")))
        assert len(all_series) == 2
        assert isinstance(all_series[0], SeriesSelector)
        assert all_series[0].tags == {"location": "Antwerp", "plant": "01"}
        assert all_series[0].field == "temperature"
        assert isinstance(all_series[1], SeriesSelector)
        assert all_series[1].tags == {"location": "Antwerp", "plant": "01"}
        assert all_series[1].field == "pressure"


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


def test_data_tags() -> None:
    db_uri = "file:data_tags?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Data (
                id integer primary key autoincrement,
                ts datetime,
                location text not null,
                plant text not null,
                value real
            );

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:00:00Z', 'Antwerp', '01', 0.5);

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:00:00Z', 'Antwerp', '02', 10);

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:01:00Z', 'Antwerp', '01', 1);

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:01:00Z', 'Antwerp', '02', 10.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "data_query": "select ts, value from Data where location = ? and plant = ? and ts >= ? and ts < ?",
                    "list_columns": ["ts", "value", "location", "plant", "value"],
                    "tag_columns": ["location", "plant"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        data = source.get_data(
            SeriesSelector("sqlite", {"location": "Antwerp", "plant": "02"}),
            datetime.fromisoformat("2022-01-01T00:00:00+00:00"),
            datetime.fromisoformat("2022-01-04T00:00:00+00:00"),
        )
        assert len(data) == 2
        assert data["ts"][0].as_py() == datetime.fromisoformat(
            "2022-01-01T00:00:00+00:00"
        )
        assert data["ts"][1].as_py() == datetime.fromisoformat(
            "2022-01-01T00:01:00+00:00"
        )
        assert data["value"][0].as_py() == approx(10)
        assert data["value"][1].as_py() == approx(10.5)


def test_data_fields() -> None:
    db_uri = "file:data_fields?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Data (
                id integer primary key autoincrement,
                name text not null,
                ts datetime,
                pressure real
            );

            insert into Data (name, ts, pressure)
            values ('test-tag-1', '2022-01-01T00:00:00Z', 0.5);

            insert into Data (name, ts, pressure)
            values ('test-tag-1', '2022-01-02T00:00:00Z', 1.5);

            insert into Data (name, ts, pressure)
            values ('test-tag-1', '2022-01-03T00:00:00Z', 2);

            insert into Data (name, ts, pressure)
            values ('test-tag-1', '2022-01-04T00:00:00Z', 3);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "data_query": "select ts, {field} from Data where name = ? and ts >= ? and ts < ?",
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        data = source.get_data(
            SeriesSelector("sqlite", "test-tag-1", "pressure"),
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


def test_data_tags_fields() -> None:
    db_uri = "file:data_tags_fields?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Data (
                id integer primary key autoincrement,
                ts datetime,
                location text not null,
                plant text not null,
                pressure real
            );

            insert into Data (ts, location, plant, pressure)
            values ('2022-01-01T00:00:00Z', 'Antwerp', '01', 0.5);

            insert into Data (ts, location, plant, pressure)
            values ('2022-01-01T00:00:00Z', 'Antwerp', '02', 10);

            insert into Data (ts, location, plant, pressure)
            values ('2022-01-01T00:01:00Z', 'Antwerp', '01', 1);

            insert into Data (ts, location, plant, pressure)
            values ('2022-01-01T00:01:00Z', 'Antwerp', '02', 10.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "data_query": """
select ts, {field} from Data where location = ? and plant = ? and ts >= ? and ts < ?
""",
                    "list_columns": ["ts", "value", "location", "plant", "value"],
                    "tag_columns": ["location", "plant"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        data = source.get_data(
            SeriesSelector(
                "sqlite", {"location": "Antwerp", "plant": "02"}, "pressure"
            ),
            datetime.fromisoformat("2022-01-01T00:00:00+00:00"),
            datetime.fromisoformat("2022-01-04T00:00:00+00:00"),
        )
        assert len(data) == 2
        assert data["ts"][0].as_py() == datetime.fromisoformat(
            "2022-01-01T00:00:00+00:00"
        )
        assert data["ts"][1].as_py() == datetime.fromisoformat(
            "2022-01-01T00:01:00+00:00"
        )
        assert data["value"][0].as_py() == approx(10)
        assert data["value"][1].as_py() == approx(10.5)


def test_data_query_tags() -> None:
    db_uri = "file:data_query_tags?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Data (
                id integer primary key autoincrement,
                ts datetime,
                location text not null,
                plant text not null,
                value real
            );

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:00:00Z', 'Antwerp', '01', 0.5);

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:00:00Z', 'Antwerp', '02', 10);

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:01:00Z', 'Antwerp', '01', 1);

            insert into Data (ts, location, plant, value)
            values ('2022-01-01T00:01:00Z', 'Antwerp', '02', 10.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "data_query": "select ts, value from Data where location = ? and plant = ? and ts >= ? and ts < ?",
                    "tag_columns": ["location", "plant", "sensor"],
                    "data_query_tags": ["location", "plant"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        data = source.get_data(
            SeriesSelector(
                "sqlite", {"location": "Antwerp", "plant": "02", "sensor": "01"}
            ),
            datetime.fromisoformat("2022-01-01T00:00:00+00:00"),
            datetime.fromisoformat("2022-01-04T00:00:00+00:00"),
        )
        assert len(data) == 2
        assert data["ts"][0].as_py() == datetime.fromisoformat(
            "2022-01-01T00:00:00+00:00"
        )
        assert data["ts"][1].as_py() == datetime.fromisoformat(
            "2022-01-01T00:01:00+00:00"
        )
        assert data["value"][0].as_py() == approx(10)
        assert data["value"][1].as_py() == approx(10.5)


def test_get_metadata() -> None:
    db_uri = "file:get_metadata?mode=memory&cache=shared"
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
                    "metadata_query": "select description, accuracy from Metadata where name = ?",
                    "metadata_columns": ["description", "accuracy"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        metadata = source.get_metadata(SeriesSelector("sqlite", "test-tag-1"))
        assert isinstance(metadata, Metadata)
        assert metadata.get_field(fields.Accuracy) == approx(0.5)
        assert metadata.get_field(fields.Description) == "test"


def test_get_metadata_tags() -> None:
    db_uri = "file:get_metadata_tags?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                location text not null,
                plant text not null,
                description text,
                accuracy real
            );

            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '01', 'test', 0.5);

            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '02', 'test', 1);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "tag_columns": ["location", "plant"],
                    "metadata_query": "select description, accuracy from Metadata where location = ? and plant = ?",
                    "metadata_columns": ["description", "accuracy"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None
        metadata = source.get_metadata(
            SeriesSelector("sqlite", {"location": "Antwerp", "plant": "01"})
        )
        assert isinstance(metadata, Metadata)
        assert metadata.get_field(fields.Accuracy) == approx(0.5)
        assert metadata.get_field(fields.Description) == "test"


def test_search_names_mismatched_tags_list_query() -> None:
    db_uri = "file:search_name_tag_mismatch?mode=memory&cache=shared"
    with sqlite3.connect(db_uri, uri=True) as connection:
        cursor = connection.cursor()
        cursor.executescript(
            """
            create table Metadata (
                id integer primary key autoincrement,
                location text not null,
                plant text not null,
                description text,
                accuracy real
            );

            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '01', 'test', 0.5);
            insert into Metadata (location, plant, description, accuracy)
            values ('Antwerp', '02', 'test', 1);
            insert into Metadata (location, plant, description, accuracy)
            values ('Barcelona', '02', 'test', 1.5);
        """
        )
        connection.commit()

        config = {
            "source": {
                "sqlite": {
                    "type": "sqlite",
                    "connection_string": db_uri,
                    "list_query": "select location from Metadata",
                    "tag_columns": ["location", "plant"],
                }
            }
        }
        source = SourceFactory(config).get_source("sqlite")
        assert source is not None

        with pytest.raises(InvalidConfigurationError):
            list(source.search(SeriesSearch("sqlite")))
