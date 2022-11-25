"""Test the Feather time series source."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict

from dateutil.parser import parse as parse_date

import kukur.config

from kukur import SeriesSearch, SeriesSelector, Source
from kukur.source import SourceFactory


START_DATE = parse_date("2020-01-01T00:00:00Z")
END_DATE = parse_date("2020-11-01T00:00:00Z")


def get_source(source_name: str) -> Source:
    source = SourceFactory(
        kukur.config.from_toml("tests/test_data/Kukur.toml")
    ).get_source(source_name)
    assert source is not None
    return source


def make_series(
    source: str, tags: Dict[str, str] = {"series name": "test-tag-1"}
) -> SeriesSelector:
    return SeriesSelector.from_tags(source, tags)


def test_dir():
    table = get_source("dir-feather").get_data(
        make_series("dir-feather"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_string():
    table = get_source("dir-feather").get_data(
        make_series("dir-feather", {"series name": "test-tag-5"}), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"


def test_dir_quality():
    table = get_source("dir-feather-quality").get_data(
        make_series("dir-feather-quality"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1


def test_search_row():
    series = list(get_source("row-feather").search(SeriesSearch("row-feather")))
    assert len(series) == 3
    assert SeriesSelector("row-feather", "test-tag-1") in series


def test_row():
    table = get_source("row-feather").get_data(
        make_series("row-feather"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_quality():
    table = get_source("row-feather-quality").get_data(
        make_series("row-feather-quality"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1


def test_search_pivot():
    series = list(get_source("pivot-feather").search(SeriesSearch("pivot-feather")))
    assert len(series) == 3
    assert SeriesSelector("pivot-feather", "test-tag-1") in series


def test_pivot():
    table = get_source("pivot-feather").get_data(
        make_series("pivot-feather"), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_pivot_string():
    table = get_source("pivot-feather").get_data(
        make_series("pivot-feather", {"series name": "test-tag-5"}),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"
