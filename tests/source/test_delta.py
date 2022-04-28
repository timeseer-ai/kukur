"""Test the Delta Lake time series source."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dateutil.parser import parse as parse_date

import kukur.config

from kukur import ComplexSeriesSelector, Source
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
    source: str, tags: dict[str, str] = {"series name": "test-tag-1"}
) -> ComplexSeriesSelector:
    return ComplexSeriesSelector(source, tags)


def test_row():
    table = get_source("row-delta").get_data(
        make_series("row-delta"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_quality():
    table = get_source("row-delta-quality").get_data(
        make_series("row-delta-quality"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1


def test_pivot():
    table = get_source("pivot-delta").get_data(
        make_series("pivot-delta"), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_pivot_string():
    table = get_source("pivot-delta").get_data(
        make_series("pivot-delta", {"series name": "test-tag-5"}), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"


def test_unordered():
    table = get_source("unordered-delta").get_data(
        make_series("unordered-delta"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["ts"][0].as_py() < table["ts"][1].as_py()
    assert table["ts"][1].as_py() < table["ts"][2].as_py()
    assert table["ts"][2].as_py() < table["ts"][3].as_py()
    assert table["ts"][3].as_py() < table["ts"][4].as_py()
    assert table["value"][0].as_py() == 1.0
