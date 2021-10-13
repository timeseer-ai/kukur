"""Test the CSV time series source."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dateutil.parser import parse as parse_date

import kukur.config

from kukur import SeriesSelector, DataType, Dictionary, InterpolationType, Source
from kukur.source import SourceFactory


def get_source(source_name: str) -> Source:
    source = SourceFactory(
        kukur.config.from_toml("tests/test_data/Kukur.toml")
    ).get_source(source_name)
    assert source is not None
    return source


def make_series(source: str, name: str = "test-tag-1") -> SeriesSelector:
    return SeriesSelector(source, name)


START_DATE = parse_date("2020-01-01T00:00:00Z")
END_DATE = parse_date("2020-11-01T00:00:00Z")


def test_dir():
    table = get_source("dir").get_data(
        make_series("dir", "test-tag-1"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_quality():
    table = get_source("dir-quality").get_data(
        make_series("dir-quality", "test-tag-1"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1
    assert table["quality"][2].as_py() == 0
    assert table["quality"][3].as_py() == 1


def test_row():
    table = get_source("row").get_data(make_series("row"), START_DATE, END_DATE)
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_quality():
    table = get_source("row_quality").get_data(
        make_series("row_quality"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1
    assert table["quality"][2].as_py() == 0


def test_pivot():
    table = get_source("pivot").get_data(make_series("pivot"), START_DATE, END_DATE)
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_metadata():
    series = make_series("row")
    metadata = get_source("row").get_metadata(series)
    assert metadata.series == series
    assert isinstance(metadata.description, str)
    assert isinstance(metadata.unit, str)
    assert isinstance(metadata.limit_low, float)
    assert isinstance(metadata.limit_high, float)
    assert isinstance(metadata.accuracy, float)


def test_row_metadata_dictionary():
    metadata = get_source("row").get_metadata(SeriesSelector("row", "test-tag-6"))
    assert metadata.series == SeriesSelector("row", "test-tag-6")
    assert metadata.data_type == DataType.DICTIONARY
    assert metadata.dictionary_name == "Active"
    assert isinstance(metadata.dictionary, Dictionary)


def test_metadata_mapping():
    metadata = get_source("mapping").get_metadata(make_series("mapping"))
    assert metadata.series == SeriesSelector("mapping", "test-tag-1")
    assert metadata.unit == "kg"
    assert metadata.limit_low == 1
    assert metadata.interpolation_type == InterpolationType.LINEAR


def test_metadata_mapping_multiple():
    metadata = get_source("mapping").get_metadata(make_series("mapping", "test-tag-1"))
    assert metadata.data_type == DataType.FLOAT64
    metadata = get_source("mapping").get_metadata(make_series("mapping", "test-tag-4"))
    assert metadata.data_type == DataType.FLOAT64
