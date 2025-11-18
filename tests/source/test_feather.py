"""Test the Feather time series source."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


import pytz
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


def make_series(source: str, tags: dict[str, str] | None = None) -> SeriesSelector:
    if tags is None:
        tags = {"series name": "test-tag-1"}
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


def test_row_map_columns():
    table = get_source("row-feather-map-columns").get_data(
        make_series("row-feather-map-columns"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]


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


def test_pivot_column_mapping() -> None:
    table = get_source("pivot-feather-column-mapping").get_data(
        make_series("pivot-feather-column-mapping"), START_DATE, END_DATE
    )
    assert len(table) == 7
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_map_columns():
    table = get_source("dir-feather-mapping").get_data(
        make_series("dir-feather-mapping"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0


def test_dir_data_datetime_format() -> None:
    table = get_source("dir-feather-datetime").get_data(
        make_series("dir-feather-datetime"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_dir_data_timezone() -> None:
    table = get_source("dir-feather-datetime-naive").get_data(
        make_series("dir-feather-datetime-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_pivot_data_datetime_format() -> None:
    table = get_source("pivot-feather-datetime").get_data(
        make_series("pivot-feather-datetime"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_pivot_data_timezone() -> None:
    table = get_source("pivot-feather-datetime-naive").get_data(
        make_series("pivot-feather-datetime-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_data_datetime_format():
    table = get_source("row-feather-datetime").get_data(
        make_series("row-feather-datetime"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_data_timezone():
    table = get_source("row-feather-datetime-naive").get_data(
        make_series("row-feather-datetime-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_timestamp():
    table = get_source("row-feather-timestamp").get_data(
        make_series("row-feather-timestamp"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_data_timezone_timestamp_naive():
    table = get_source("row-feather-timestamp-naive").get_data(
        make_series("row-feather-timestamp-naive"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_data_timezone_timestamp_naive_string_microseconds():
    table = get_source("row-feather-timestamp-string-naive-us").get_data(
        make_series("row-feather-timestamp-string-naive-us"), START_DATE, END_DATE
    )
    assert len(table) == 5
    start_date = table["ts"][0].as_py()
    assert start_date == START_DATE
    assert start_date.tzinfo == pytz.UTC


def test_row_tags_search():
    series = list(
        get_source("row-feather-tags").search(SeriesSearch("row-feather-tags"))
    )
    assert len(series) == 8
    assert (
        SeriesSelector("row-feather-tags", {"location": "Antwerp", "plant": "P1"})
        in series
    )
    assert (
        SeriesSelector(
            "row-feather-tags", {"location": "Antwerp", "plant": "P1"}, "product"
        )
        in series
    )


def test_row_tags_value():
    table = get_source("row-feather-tags").get_data(
        make_series("row-feather-tags", {"location": "Antwerp", "plant": "P1"}),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 3
    assert table["value"][0].as_py() == 1.0
    assert table["value"][1].as_py() == 2.0
    assert table["value"][2].as_py() == 1.0


def test_row_tags_second_field():
    table = get_source("row-feather-tags").get_data(
        SeriesSelector(
            "row-feather-tags", {"location": "Barcelona", "plant": "P1"}, "product"
        ),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 3
    assert table["value"][0].as_py() == "A"
    assert table["value"][1].as_py() == "A"
    assert table["value"][2].as_py() == "B"


def test_row_no_mapping_search() -> None:
    series = list(
        get_source("row-feather-no-mapping").search(
            SeriesSearch("row-feather-no-mapping")
        )
    )
    assert len(series) == 2


def test_row_no_mapping_data() -> None:
    table = get_source("row-feather-no-mapping").get_data(
        SeriesSelector(
            "row-feather-no-mapping",
            {"name": "name", "location": "location"},
            "pressure",
        ),
        START_DATE,
        END_DATE,
    )

    assert len(table) == 1
    assert table["value"][0].as_py() == 42
