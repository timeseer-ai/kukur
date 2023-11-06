"""Tests for the binary plugin source."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import sys
from datetime import datetime

from kukur import Source
from kukur.base import SeriesSearch, SeriesSelector
from kukur.metadata import Metadata
from kukur.source import SourceFactory


def get_source() -> Source:
    source = SourceFactory(
        {
            "source": {
                "Plugin": {
                    "type": "plugin",
                    "cmd": [sys.executable, "tests/test_data/plugin/plugin.py"],
                    "quality_mapping": "plugin_quality",
                }
            },
            "quality_mapping": {
                "plugin_quality": {
                    "GOOD": ["GOOD"],
                }
            },
        }
    ).get_source("Plugin")
    assert source is not None
    return source


def test_search() -> None:
    source = get_source()
    search_results = list(source.search(SeriesSearch("Plugin")))
    assert len(search_results) == 2
    assert isinstance(search_results[0], Metadata)
    assert search_results[0].series.name == "test"
    assert search_results[0].get_field_by_name("description") == "Test series"
    assert isinstance(search_results[1], SeriesSelector)
    assert search_results[1].name == "test-2"


def test_metadata() -> None:
    source = get_source()
    metadata = source.get_metadata(SeriesSelector("Plugin", "test"))
    assert metadata.series.name == "test"
    assert metadata.get_field_by_name("description") == "Description of test (Plugin)"


def test_data() -> None:
    source = get_source()
    start_date = datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2022-01-02T00:00:00+00:00")
    data = source.get_data(SeriesSelector("Plugin", "test"), start_date, end_date)
    assert len(data) == 2
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 0
    assert data["quality"][0].as_py() == 0
    assert data["ts"][1].as_py() == end_date
    assert data["value"][1].as_py() == 42
    assert data["quality"][1].as_py() == 1
