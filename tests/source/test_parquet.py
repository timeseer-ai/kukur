"""Test the Parquet time series source."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Optional

import pytest
import pytz
from dateutil.parser import parse as parse_date

import kukur.config
from kukur import SeriesSearch, SeriesSelector, Source
from kukur.exceptions import InvalidPathException
from kukur.source import SourceFactory

START_DATE = parse_date("2020-01-01T00:00:00Z")
END_DATE = parse_date("2020-11-01T00:00:00Z")


def get_source(source_name: str) -> Source:
    source = SourceFactory(
        kukur.config.from_toml("tests/test_data/Kukur.toml")
    ).get_source(source_name)
    assert source is not None
    return source


def make_series(source: str, tags: Optional[Dict[str, str]] = None) -> SeriesSelector:
    if tags is None:
        tags = {"series name": "test-tag-1"}
    return SeriesSelector.from_tags(source, tags)


def test_dir():
    table = get_source("dir-parquet").get_data(
        make_series("dir-parquet"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_quality():
    table = get_source("dir-parquet-quality").get_data(
        make_series("dir-parquet"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1


def test_dir_string():
    table = get_source("dir-parquet").get_data(
        make_series("dir-parquet", {"series name": "test-tag-5"}), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"


def test_search_row():
    series = list(get_source("row-parquet").search(SeriesSearch("row-parquet")))
    assert len(series) == 3
    assert SeriesSelector("row-parquet", "test-tag-1") in series


def test_row():
    table = get_source("row-parquet").get_data(
        make_series("row-parquet"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_row_quality():
    table = get_source("row-parquet-quality").get_data(
        make_series("row-parquet-quality"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value", "quality"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
    assert table["quality"][0].as_py() == 1


def test_search_pivot():
    series = list(get_source("pivot-parquet").search(SeriesSearch("pivot-parquet")))
    assert len(series) == 3
    assert SeriesSelector("pivot-parquet", "test-tag-1") in series


def test_pivot():
    table = get_source("pivot-parquet").get_data(
        make_series("pivot-parquet"), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_pivot_string():
    table = get_source("pivot-parquet").get_data(
        make_series("pivot-parquet", {"series name": "test-tag-5"}),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"


def test_pivot_column_mapping() -> None:
    table = get_source("pivot-parquet-column-mapping").get_data(
        make_series("pivot-parquet-column-mapping"), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_mapping():
    table = get_source("dir-parquet-mapping").get_data(
        make_series("dir-parquet-mapping"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_data_datetime_format() -> None:
    table = get_source("dir-parquet-datetime").get_data(
        make_series("dir-parquet-datetime"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_dir_data_timezone() -> None:
    table = get_source("dir-parquet-datetime-naive").get_data(
        make_series("dir-parquet-datetime-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_pivot_data_datetime_format() -> None:
    table = get_source("pivot-parquet-datetime").get_data(
        make_series("pivot-parquet-datetime"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_pivot_data_timezone() -> None:
    table = get_source("pivot-parquet-datetime-naive").get_data(
        make_series("pivot-parquet-datetime-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_data_datetime_format():
    table = get_source("row-parquet-datetime").get_data(
        make_series("row-parquet-datetime"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_data_timezone():
    table = get_source("row-parquet-datetime-naive").get_data(
        make_series("row-parquet-datetime-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_timestamp():
    table = get_source("row-parquet-timestamp").get_data(
        make_series("row-parquet-timestamp"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_data_timezone_timestamp_naive():
    table = get_source("row-parquet-timestamp-naive").get_data(
        make_series("row-parquet-timestamp-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_partitions() -> None:
    table = get_source("partitioned-parquet").get_data(
        make_series(
            "partitioned-parquet",
            {"location": "Antwerp", "plant": "PlantA", "series name": "test-tag-1"},
        ),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_partitions_string():
    table = get_source("partitioned-parquet").get_data(
        make_series(
            "partitioned-parquet",
            {"location": "Barcelona", "plant": "PlantB", "series name": "test-tag-5"},
        ),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == "A"


def test_partitions_traversal() -> None:
    with pytest.raises(InvalidPathException):
        get_source("partitioned-parquet").get_data(
            make_series(
                "partitioned-parquet",
                {
                    "location": "Antwerp",
                    "plant": "PlantA",
                    "series name": "../../../dir/test-tag-5",
                },
            ),
            START_DATE,
            END_DATE,
        )


def test_row_tags_search():
    series = list(
        get_source("row-parquet-tags").search(SeriesSearch("row-parquet-tags"))
    )
    assert len(series) == 8
    assert (
        SeriesSelector("row-parquet-tags", {"location": "Antwerp", "plant": "P1"})
        in series
    )
    assert (
        SeriesSelector(
            "row-parquet-tags", {"location": "Antwerp", "plant": "P1"}, "product"
        )
        in series
    )


def test_row_tags_value():
    table = get_source("row-parquet-tags").get_data(
        make_series("row-parquet-tags", {"location": "Antwerp", "plant": "P1"}),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 3
    assert table["value"][0].as_py() == 1.0
    assert table["value"][1].as_py() == 2.0
    assert table["value"][2].as_py() == 1.0


def test_row_tags_second_field():
    table = get_source("row-parquet-tags").get_data(
        SeriesSelector(
            "row-parquet-tags", {"location": "Barcelona", "plant": "P1"}, "product"
        ),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 3
    assert table["value"][0].as_py() == "A"
    assert table["value"][1].as_py() == "A"
    assert table["value"][2].as_py() == "B"
