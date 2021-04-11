"""Test the CSV time series source."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import pytest

from dateutil.parser import parse as parse_date

from kukur import SeriesSelector, DataType, Dictionary
from kukur.source.csv import from_config


@pytest.fixture
def make_series():
    def make_selector(source: str = "fake", name: str = "test-tag-1"):
        return SeriesSelector(source, name)

    return make_selector


START_DATE = parse_date("2020-01-01T00:00:00Z")
END_DATE = parse_date("2020-11-01T00:00:00Z")


def test_dir(make_series):
    source = from_config({"path": "tests/test_data/csv/dir", "format": "dir"})
    table = source.get_data(make_series(), START_DATE, END_DATE)
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row(make_series):
    source = from_config({"path": "tests/test_data/csv/row.csv", "format": "row"})
    table = source.get_data(make_series(), START_DATE, END_DATE)
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_pivot(make_series):
    source = from_config({"path": "tests/test_data/csv/pivot.csv", "format": "pivot"})
    table = source.get_data(make_series(), START_DATE, END_DATE)
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_metadata(make_series):
    source = from_config({"metadata": "tests/test_data/csv/row-metadata.csv"})
    series = make_series()
    metadata = source.get_metadata(series)
    assert metadata.series == series
    assert isinstance(metadata.description, str)
    assert isinstance(metadata.unit, str)
    assert isinstance(metadata.limit_low, float)
    assert isinstance(metadata.limit_high, float)
    assert isinstance(metadata.accuracy, float)


def test_row_metadata_dictionary():
    source = from_config(
        {
            "metadata": "tests/test_data/csv/row-metadata.csv",
            "dictionary_dir": "tests/test_data/csv/dictionary",
        }
    )
    metadata = source.get_metadata(SeriesSelector("fake", "test-tag-6"))
    assert metadata.series == SeriesSelector("fake", "test-tag-6")
    assert metadata.data_type == DataType.DICTIONARY
    assert metadata.dictionary_name == "Active"
    assert isinstance(metadata.dictionary, Dictionary)
