"""Unit tests for the SQL source, using an in-memory SQLite database."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import math
import sqlite3
from datetime import datetime

from dateutil.parser import parse as parse_date

from kukur import InterpolationType, Metadata, SeriesSelector
from kukur.metadata import fields
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


class DummySQLSource(BaseSQLSource):
    """In-memory SQLite database."""

    def __init__(
        self,
        config: SQLConfig,
        mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        super().__init__(config, mapper, quality_mapper)
        sqlite3.register_converter("datetime", parse_date)
        self.db = sqlite3.connect(
            config.connection_string,
            detect_types=sqlite3.PARSE_DECLTYPES | sqlite3.PARSE_COLNAMES,
        )

    def connect(self):
        return self.db


class MockSQLIterator:
    def __next__(self):
        raise StopIteration()


class MockSQLCursor:
    def __init__(self, execute_fn):
        self.execute_fn = execute_fn

    def execute(self, query, params):
        self.execute_fn(query, params)

    def __iter__(self):
        return MockSQLIterator()


class MockSQLConnection:
    def __init__(self, execute_fn):
        self.execute_fn = execute_fn

    def cursor(self) -> MockSQLCursor:
        return MockSQLCursor(self.execute_fn)


class MockSQLSource(BaseSQLSource):
    """SQL source that mocks the connection."""

    def __init__(
        self,
        config: SQLConfig,
        mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
        execute_fn,
    ):
        super().__init__(config, mapper, quality_mapper)
        self.execute_fn = execute_fn

    def connect(self) -> MockSQLConnection:
        return MockSQLConnection(self.execute_fn)


def test_metadata_value():
    config = SQLConfig(
        ":memory:",
        metadata_query="select dictionary_name from Metadata where series_name = ?",
        metadata_columns=["dictionary name"],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            dictionary_name text
        );

        insert into Metadata (series_name, dictionary_name) values ('random', 'prisma');
    """
    )
    metadata = source.get_metadata(
        SeriesSelector.from_tags("dummy", {"series name": "random"})
    )
    assert metadata.get_field(fields.DictionaryName) == "prisma"


def test_metadata_none_value_on_empty_return():
    config = SQLConfig(
        ":memory:",
        metadata_query="select dictionary_name from Metadata where series_name = ?",
        metadata_columns=["dictionary name"],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            dictionary_name text
        );

        insert into Metadata (series_name, dictionary_name) values ('random', '');
    """
    )
    metadata = source.get_metadata(
        SeriesSelector.from_tags("dummy", {"series name": "random"})
    )
    assert metadata.get_field(fields.DictionaryName) is None


def test_list_none_value_on_empty_return():
    config = SQLConfig(
        ":memory:",
        list_query="select series_name, dictionary_name from Metadata",
        list_columns=["series name", "dictionary name"],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
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
        assert metadata.series.tags["series name"] == "random"
        assert metadata.get_field(fields.DictionaryName) is None


def test_custom_fields() -> None:
    config = SQLConfig(
        ":memory:",
        metadata_query="select interpolation_type, process_type from Metadata where series_name = ?",
        metadata_columns=["interpolation type", "process type"],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            interpolation_type text,
            process_type text
        );

        insert into Metadata (series_name, interpolation_type, process_type) values ('random', 'LINEAR', 'batch');
        """
    )
    metadata = source.get_metadata(
        SeriesSelector.from_tags("dummy", {"series name": "random"})
    )
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR
    assert metadata.get_field_by_name("process type") == "batch"


def test_custom_fields_search() -> None:
    config = SQLConfig(
        ":memory:",
        list_query="select series_name, interpolation_type, process_type from Metadata",
        list_columns=["series name", "interpolation type", "process type"],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            interpolation_type text,
            process_type text
        );

        insert into Metadata (series_name, interpolation_type, process_type) values ('random', 'LINEAR', 'batch');
        """
    )
    all_metadata = list(source.search(SeriesSelector("dummy")))
    assert len(all_metadata) == 1
    metadata = all_metadata[0]
    assert isinstance(metadata, Metadata)
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR
    assert metadata.get_field_by_name("process type") == "batch"


def test_accuracy_percentage_metadata() -> None:
    config = SQLConfig(
        ":memory:",
        metadata_query="select accuracy_percentage, limit_low, limit_high from Metadata where series_name = ?",
        metadata_columns=[
            "accuracy percentage",
            "physical lower limit",
            "physical upper limit",
        ],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            accuracy_percentage integer,
            limit_low integer,
            limit_high integer
        );

        insert into Metadata (series_name, accuracy_percentage, limit_low, limit_high) values ('random', 2, 0, 10);
        """
    )
    metadata = source.get_metadata(
        SeriesSelector.from_tags("dummy", {"series name": "random"})
    )
    assert metadata.get_field(fields.AccuracyPercentage) == 2
    assert metadata.get_field(fields.LimitLowPhysical) == 0
    assert metadata.get_field(fields.LimitHighPhysical) == 10
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_accuracy_percentage_metadata_string() -> None:
    config = SQLConfig(
        ":memory:",
        metadata_query="select accuracy_percentage, limit_low, limit_high from Metadata where series_name = ?",
        metadata_columns=[
            "accuracy percentage",
            "physical lower limit",
            "physical upper limit",
        ],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            accuracy_percentage integer,
            limit_low integer,
            limit_high integer
        );

        insert into Metadata (series_name, accuracy_percentage, limit_low, limit_high) values ('random', 'two', 0, 10);
        """
    )
    metadata = source.get_metadata(
        SeriesSelector.from_tags("dummy", {"series name": "random"})
    )
    assert metadata.get_field(fields.AccuracyPercentage) is None
    assert metadata.get_field(fields.LimitLowPhysical) == 0
    assert metadata.get_field(fields.LimitHighPhysical) == 10
    assert metadata.get_field(fields.Accuracy) is None


def test_accuracy_percentage_search() -> None:
    config = SQLConfig(
        ":memory:",
        list_query="select series_name, accuracy_percentage, limit_low, limit_high from Metadata",
        list_columns=[
            "series name",
            "accuracy percentage",
            "physical lower limit",
            "physical upper limit",
        ],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            accuracy_percentage integer,
            limit_low integer,
            limit_high integer
        );

        insert into Metadata (series_name, accuracy_percentage, limit_low, limit_high) values ('random', 2, 0, 10);
        """
    )
    all_metadata = list(source.search(SeriesSelector("dummy")))
    assert len(all_metadata) == 1
    metadata = all_metadata[0]
    assert isinstance(metadata, Metadata)
    assert metadata.get_field(fields.AccuracyPercentage) == 2
    assert metadata.get_field(fields.LimitLowPhysical) == 0
    assert metadata.get_field(fields.LimitHighPhysical) == 10
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_accuracy_percentage_search_with_wrong_string_value() -> None:
    config = SQLConfig(
        ":memory:",
        list_query="select series_name, accuracy_percentage, limit_low, limit_high from Metadata",
        list_columns=[
            "series name",
            "accuracy percentage",
            "physical lower limit",
            "physical upper limit",
        ],
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
    source.db.executescript(
        """
        create table Metadata (
            series_name text,
            accuracy_percentage integer,
            limit_low integer,
            limit_high integer
        );

        insert into Metadata (series_name, accuracy_percentage, limit_low, limit_high) values ('random', '', 0, 10);
        """
    )
    all_metadata = list(source.search(SeriesSelector("dummy")))
    assert len(all_metadata) == 1
    metadata = all_metadata[0]
    assert isinstance(metadata, Metadata)
    assert metadata.get_field(fields.AccuracyPercentage) is None
    assert metadata.get_field(fields.LimitLowPhysical) == 0
    assert metadata.get_field(fields.LimitHighPhysical) == 10
    assert metadata.get_field(fields.Accuracy) is None


def test_blob_values():
    config = SQLConfig(
        ":memory:",
        data_query="select ts, value from Data where series_name = ? and ts between ? and ?",
    )
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
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
        SeriesSelector.from_tags("dummy", {"series name": "blob-series"}),
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
    source = DummySQLSource(config, MetadataValueMapper(), QualityMapper())
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
        SeriesSelector.from_tags("dummy", {"series name": "datetime-series"}),
        datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2021-02-01T00:00:00+00:00"),
    )

    assert len(data) == 1
    assert (
        data["value"][0].as_py()
        == datetime.fromisoformat("2021-01-02T12:34:56+00:00").isoformat()
    )


def test_timezone_on_queries():
    config = SQLConfig.from_dict(
        dict(
            connection_string=":memory:",
            data_query="select ts, value from Data where series_name = ? and ts between ? and ?",
            data_query_timezone="EST",
        )
    )

    start_date = None

    def execute_fn(query, params):
        nonlocal start_date
        start_date = params[1]

    source = MockSQLSource(config, MetadataValueMapper(), QualityMapper(), execute_fn)
    source.get_data(
        SeriesSelector.from_tags("dummy", {"series name": "series"}),
        datetime.fromisoformat("2021-08-01T00:00:00+00:00"),
        datetime.fromisoformat("2021-08-02T00:00:00+00:00"),
    )
    assert start_date == datetime.fromisoformat("2021-07-31T19:00:00")


def test_string_in_timezone_on_queries():
    config = SQLConfig.from_dict(
        dict(
            connection_string=":memory:",
            data_query="select ts, value from Data where series_name = ? and ts between ? and ?",
            data_query_datetime_format="%Y-%m-%d %H:%M:%S",
            data_query_timezone="EST",
        )
    )

    start_date = None

    def execute_fn(query, params):
        nonlocal start_date
        start_date = params[1]

    source = MockSQLSource(config, MetadataValueMapper(), QualityMapper(), execute_fn)
    source.get_data(
        SeriesSelector.from_tags("dummy", {"series name": "series"}),
        datetime.fromisoformat("2021-08-01T00:00:00+00:00"),
        datetime.fromisoformat("2021-08-02T00:00:00+00:00"),
    )
    assert start_date == "2021-07-31 19:00:00"


def test_quality_flag():
    config = SQLConfig(
        ":memory:",
        data_query="select ts, value, quality from Data where series_name = ? and ts between ? and ?",
    )
    quality_mapper = QualityMapper()
    quality_mapper.add_mapping(192)
    source = DummySQLSource(config, MetadataValueMapper(), quality_mapper)
    source.db.execute(
        """
        create table Data (
            ts datetime,
            series_name text,
            value text,
            quality int
        );
        """
    )
    source.db.execute(
        "insert into Data (ts, series_name, value, quality) values (?, ?, ?, ?)",
        [
            datetime.fromisoformat("2021-01-02T00:00:00+00:00"),
            "quality-series",
            "good-quality",
            192,
        ],
    )
    source.db.execute(
        "insert into Data (ts, series_name, value, quality) values (?, ?, ?, ?)",
        [
            datetime.fromisoformat("2021-01-02T00:00:00+00:00"),
            "quality-series",
            "bad-quality",
            1,
        ],
    )

    data = source.get_data(
        SeriesSelector.from_tags("dummy", {"series name": "quality-series"}),
        datetime.fromisoformat("2021-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2021-02-01T00:00:00+00:00"),
    )

    assert len(data) == 2
    assert data["value"][0].as_py() == "good-quality"
    assert data["quality"][0].as_py() == 1
    assert data["value"][1].as_py() == "bad-quality"
    assert data["quality"][1].as_py() == 0
