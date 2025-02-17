"""Test reading the Arrow IPC Streaming Format"""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict, Optional

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


def make_series(source: str, tags: Optional[Dict[str, str]] = None) -> SeriesSelector:
    if tags is None:
        tags = {"series name": "test-tag-1"}
    return SeriesSelector.from_tags(source, tags)


def test_search_row():
    series = list(get_source("row-arrows").search(SeriesSearch("row-arrows")))
    assert len(series) == 3
    assert SeriesSelector("row-arrows", "test-tag-1") in series


def test_row():
    table = get_source("row-arrows").get_data(
        make_series("row-arrows"), START_DATE, END_DATE
    )
    assert len(table) == 5
    assert table.column_names == ["ts", "value"]
    assert table["ts"][0].as_py() == START_DATE
    assert table["value"][0].as_py() == 1.0
