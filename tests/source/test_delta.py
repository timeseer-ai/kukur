"""Test the Delta Lake time series source."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Optional

from dateutil.parser import parse as parse_date

import kukur.config
from kukur import SeriesSelector, Source
from kukur.base import SeriesSearch
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


def test_row_tags_search():
    series = list(get_source("row-delta-tags").search(SeriesSearch("row-delta-tags")))
    assert len(series) == 8
    assert (
        SeriesSelector("row-delta-tags", {"location": "Antwerp", "plant": "P1"})
        in series
    )
    assert (
        SeriesSelector(
            "row-delta-tags", {"location": "Antwerp", "plant": "P1"}, "product"
        )
        in series
    )


def test_row_tags_value():
    table = get_source("row-delta-tags").get_data(
        make_series("row-delta-tags", {"location": "Antwerp", "plant": "P1"}),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 3
    assert table["value"][0].as_py() == 1.0
    assert table["value"][1].as_py() == 2.0
    assert table["value"][2].as_py() == 1.0


def test_row_tags_second_field():
    table = get_source("row-delta-tags").get_data(
        SeriesSelector(
            "row-delta-tags", {"location": "Barcelona", "plant": "P1"}, "product"
        ),
        START_DATE,
        END_DATE,
    )
    assert len(table) == 3
    assert table["value"][0].as_py() == "A"
    assert table["value"][1].as_py() == "A"
    assert table["value"][2].as_py() == "B"


def test_name_partition():
    start_date = parse_date("2023-01-01T00:00:00Z")
    end_date = parse_date("2023-03-20T00:00:00Z")
    table = get_source("partition-name").get_data(
        make_series("partition-name", {"series name": "test-tag-1"}),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]


def test_location_name_partition():
    start_date = parse_date("2023-01-01T00:00:00Z")
    end_date = parse_date("2023-03-20T00:00:00Z")
    table = get_source("partition-location-name").get_data(
        make_series(
            "partition-location-name",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]


def test_year_partition():
    start_date = parse_date("2021-01-01T00:00:00Z")
    end_date = parse_date("2022-12-31T23:59:00Z")
    table = get_source("partition-year").get_data(
        make_series(
            "partition-year",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]


def test_year_partition_format():
    start_date = parse_date("2021-01-01T00:00:00Z")
    end_date = parse_date("2022-12-31T23:59:00Z")
    table = get_source("partition-year-custom").get_data(
        make_series(
            "partition-year-custom",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]


def test_month_partition():
    start_date = parse_date("2020-11-01T00:00:00Z")
    end_date = parse_date("2021-01-31T23:59:00Z")
    table = get_source("partition-month").get_data(
        make_series(
            "partition-month",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 23
    assert table.column_names == ["ts", "value"]


def test_day_partition():
    start_date = parse_date("2020-01-02T00:00:00Z")
    end_date = parse_date("2020-01-03T23:59:00Z")
    table = get_source("partition-day").get_data(
        make_series(
            "partition-day",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]


def test_month_location_partition():
    start_date = parse_date("2020-08-01T00:00:00Z")
    end_date = parse_date("2020-10-01T00:00:00Z")
    table = get_source("partition-month-location").get_data(
        make_series(
            "partition-month-location",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 8
    assert table.column_names == ["ts", "value"]


def test_location_month_partition():
    start_date = parse_date("2020-08-01T00:00:00Z")
    end_date = parse_date("2020-11-01T00:00:00Z")
    table = get_source("partition-location-month").get_data(
        make_series(
            "partition-location-month",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]


def test_location_year_month_partition():
    start_date = parse_date("2020-08-01T00:00:00Z")
    end_date = parse_date("2020-11-01T00:00:00Z")
    table = get_source("partition-location-year-month").get_data(
        make_series(
            "partition-location-year-month",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]


def test_location_year_month_day_hour_partition():
    start_date = parse_date("2020-08-01T00:00:00Z")
    end_date = parse_date("2020-11-01T00:00:00Z")
    table = get_source("partition-location-year-month-day-hour").get_data(
        make_series(
            "partition-location-year-month-day-hour",
            {"series name": "test-tag-1", "location": "Antwerp"},
        ),
        start_date,
        end_date,
    )
    assert len(table) == 12
    assert table.column_names == ["ts", "value"]
