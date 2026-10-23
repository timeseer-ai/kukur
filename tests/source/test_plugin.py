"""Tests for the binary plugin source."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import logging
import sys
from datetime import datetime

import pyarrow as pa
import pytest

from kukur import PlotSource, quality
from kukur.base import SeriesSearch, SeriesSelector
from kukur.metadata import Metadata
from kukur.source import SourceFactory
from kukur.source.plugin.plugin import PluginException


def get_source(**config) -> PlotSource:
    source = SourceFactory(
        {
            "source": {
                "Plugin": {
                    "type": "plugin",
                    "cmd": [sys.executable, "tests/test_data/plugin/plugin.py"],
                    "quality_mapping": "plugin_quality",
                    "features": {"plot": True},
                    **config,
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
    assert data["quality"][0].as_py() == "BAD"
    assert data["ts"][1].as_py() == end_date
    assert data["value"][1].as_py() == 42
    assert data["quality"][1].as_py() == "GOOD"
    assert data.schema.field("quality").type == pa.string()
    assert quality.get_mapping(data) == {"GOOD": ["GOOD"]}
    assert quality.simplify(data)["quality"].to_pylist() == [1, 0]


def test_plot_data() -> None:
    source = get_source()
    start_date = datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2022-01-02T00:00:00+00:00")
    data = source.get_plot_data(
        SeriesSelector("Plugin", "test"), start_date, end_date, 200
    )
    assert len(data) == 2
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 0
    assert data["quality"][0].as_py() == "BAD"
    assert data["ts"][1].as_py() == end_date
    assert data["value"][1].as_py() == 47
    assert data["quality"][1].as_py() == "GOOD"
    assert data.schema.field("quality").type == pa.string()
    assert quality.get_mapping(data) == {"GOOD": ["GOOD"]}
    assert quality.simplify(data)["quality"].to_pylist() == [1, 0]


def test_stderr_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    source = get_source(stderr="Something to note\n")
    with caplog.at_level(logging.WARNING):
        source.get_metadata(SeriesSelector("Plugin", "test"))
    assert [record.getMessage() for record in caplog.records] == [
        'Source "Plugin" logged: Something to note'
    ]


def test_empty_stderr_is_not_logged(caplog: pytest.LogCaptureFixture) -> None:
    source = get_source(stderr="\n")
    with caplog.at_level(logging.WARNING):
        source.get_metadata(SeriesSelector("Plugin", "test"))
    assert caplog.records == []


def test_invalid_utf8_stderr_is_logged(caplog: pytest.LogCaptureFixture) -> None:
    source = get_source(stderr="caf\u00e9", stderr_encoding="latin-1")
    with caplog.at_level(logging.WARNING):
        source.get_metadata(SeriesSelector("Plugin", "test"))
    assert [record.getMessage() for record in caplog.records] == [
        'Source "Plugin" logged: caf\ufffd'
    ]


def test_failure() -> None:
    source = get_source(stderr="Connection refused", exit_code=3)
    with pytest.raises(
        PluginException,
        match='source "Plugin" exited with status 3: Connection refused',
    ):
        source.get_metadata(SeriesSelector("Plugin", "test"))


def test_failure_with_invalid_utf8_stderr() -> None:
    source = get_source(stderr="caf\u00e9", stderr_encoding="latin-1", exit_code=1)
    with pytest.raises(PluginException, match="caf\ufffd"):
        source.get_metadata(SeriesSelector("Plugin", "test"))


def test_failure_without_stderr() -> None:
    source = get_source(exit_code=1)
    with pytest.raises(PluginException, match='source "Plugin" exited with status 1$'):
        source.get_metadata(SeriesSelector("Plugin", "test"))
