"""Test the Parquet time series source."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dateutil.parser import parse as parse_date

import pytest

from kukur import SeriesSelector
from kukur.source.parquet import from_config


START_DATE = parse_date("2020-01-01T00:00:00Z")
END_DATE = parse_date("2020-11-01T00:00:00Z")


@pytest.fixture
def make_series():
    def make_selector(source: str = "fake", name: str = "test-tag-1"):
        return SeriesSelector(source, name)

    return make_selector


def test_dir(make_series):
    source = from_config({"path": "tests/test_data/parquet/dir", "format": "dir"})
    table = source.get_data(make_series(), START_DATE, END_DATE)
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_string(make_series):
    source = from_config({"path": "tests/test_data/parquet/dir", "format": "dir"})
    table = source.get_data(make_series(name="test-tag-5"), START_DATE, END_DATE)
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"


def test_row(make_series):
    source = from_config(
        {"path": "tests/test_data/parquet/row.parquet", "format": "row"}
    )
    table = source.get_data(make_series(), START_DATE, END_DATE)
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_pivot(make_series):
    source = from_config(
        {"path": "tests/test_data/parquet/pivot.parquet", "format": "pivot"}
    )
    table = source.get_data(make_series(), START_DATE, END_DATE)
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_pivot_string(make_series):
    source = from_config(
        {"path": "tests/test_data/parquet/pivot.parquet", "format": "pivot"}
    )
    table = source.get_data(make_series(name="test-tag-5"), START_DATE, END_DATE)
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"
