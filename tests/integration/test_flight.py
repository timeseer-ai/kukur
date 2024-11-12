"""Integration tests require a running Kukur instance.

They use the client to request data.
"""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

import pytest

from kukur import Client, Metadata, SeriesSelector
from kukur.base import SeriesSearch
from kukur.metadata import fields

pytestmark = pytest.mark.kukur


@pytest.fixture
def client() -> Client:
    kukur_client = Client()
    kukur_client._get_client().wait_for_available(timeout=10)
    return kukur_client


def test_search(client: Client):
    many_series = list(client.search(SeriesSelector("row")))
    assert len(many_series) == 5
    dictionary_series = [
        series
        for series in many_series
        if series.series.tags["series name"] == "test-tag-6"
    ][0]
    assert isinstance(dictionary_series, Metadata)
    assert dictionary_series.get_field(fields.Description) == "Valve X"
    assert dictionary_series.get_field(fields.DictionaryName) == "Active"
    dictionary = dictionary_series.get_field(fields.Dictionary)
    assert dictionary is not None
    assert len(dictionary.mapping) == 2
    assert dictionary.mapping[0] == "OFF"
    assert dictionary.mapping[1] == "ON"


def test_search_custom_metadata(client: Client):
    all_metadata = list(client.search(SeriesSelector("custom-fields")))
    assert len(all_metadata) == 1
    metadata = all_metadata[0]
    assert isinstance(metadata, Metadata)
    assert metadata.get_field(fields.Description) == "Test for custom metadata fields"
    assert metadata.get_field_by_name("process type") == "BATCH"
    assert metadata.get_field_by_name("location") == "Antwerp"


def test_metadata(client: Client):
    dictionary_series = client.get_metadata(SeriesSelector("row", "test-tag-6"))
    assert dictionary_series.get_field(fields.Description) == "Valve X"
    assert dictionary_series.get_field(fields.DictionaryName) == "Active"
    dictionary = dictionary_series.get_field(fields.Dictionary)
    assert dictionary is not None
    assert len(dictionary.mapping) == 2
    assert dictionary.mapping[0] == "OFF"
    assert dictionary.mapping[1] == "ON"


def test_custom_metadata(client: Client):
    metadata = client.get_metadata(SeriesSelector("custom-fields", "test-tag-custom"))
    assert metadata.get_field(fields.Description) == "Test for custom metadata fields"
    assert metadata.get_field_by_name("process type") == "BATCH"
    assert metadata.get_field_by_name("location") == "Antwerp"


def test_data(client: Client):
    start_date = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    data = client.get_data(SeriesSelector("row", "test-tag-6"), start_date, end_date)
    assert len(data) == 7
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 1.0
    assert data["ts"][6].as_py() == datetime.fromisoformat("2020-07-01T00:00:00+00:00")
    assert data["value"][6].as_py() == 1.0


def test_data_with_quality(client: Client):
    start_date = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    data = client.get_data(
        SeriesSelector("row_quality", "test-tag-1"),
        start_date,
        end_date,
    )
    assert len(data) == 5
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 1.0
    assert data["quality"][0].as_py() == 1
    assert data["ts"][2].as_py() == datetime.fromisoformat("2020-03-01T00:00:00+00:00")
    assert data["value"][2].as_py() == 2.0
    assert data["quality"][2].as_py() == 0


def test_plot_data_fallback(client: Client):
    start_date = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    data = client.get_plot_data(
        SeriesSelector("row", "test-tag-6"), start_date, end_date, 200
    )
    assert len(data) == 7
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 1.0
    assert data["ts"][6].as_py() == datetime.fromisoformat("2020-07-01T00:00:00+00:00")
    assert data["value"][6].as_py() == 1.0


def test_sources(client: Client):
    data = client.list_sources()
    assert len(data) == 125

    assert "sql" in data
    assert "row" in data
    assert "noaa" in data


def test_metadata_backwards_compatibility(client: Client):
    dictionary_series = client.get_metadata(SeriesSelector("row", "test-tag-6"))
    assert dictionary_series.get_field(fields.Description) == "Valve X"
    assert dictionary_series.get_field(fields.DictionaryName) == "Active"
    dictionary = dictionary_series.get_field(fields.Dictionary)
    assert dictionary is not None
    assert len(dictionary.mapping) == 2
    assert dictionary.mapping[0] == "OFF"
    assert dictionary.mapping[1] == "ON"


def test_search_series_without_series_name(client: Client):
    series = list(client.search(SeriesSearch("integration-test")))
    assert len(series) == 3
    assert isinstance(series[0], SeriesSelector)
    assert series[0].tags == {"tag1": "value1", "tag2": "value2"}
    assert series[0].field == "pressure"
    assert isinstance(series[1], Metadata)
    assert series[1].series.tags == {"tag1": "value1a", "tag2": "value2a"}
    assert series[1].series.field == "temperature"
    assert series[1].get_field_by_name("description") == "integration test temperature"


def test_series_search_without_series_name_and_extra_metadata(client: Client):
    series = list(client.search(SeriesSearch("integration-test-extra-metadata")))
    assert len(series) == 3
    assert isinstance(series[1], Metadata)
    assert series[1].series.tags == {"tag1": "value1a", "tag2": "value2a"}
    assert series[1].series.field == "temperature"
    assert series[1].get_field_by_name("unit") == "c"


def test_series_metadata_without_series_name(client: Client):
    metadata = client.get_metadata(
        SeriesSelector(
            "integration-test", {"tag1": "value1", "tag2": "value2"}, "pressure"
        )
    )
    assert metadata.get_field_by_name("description") == "integration test pressure"


def test_search_returns_metadata_if_extra_metadata_fails(client: Client):
    search_results = client.search(SeriesSelector("integration-test-extra-metadata"))

    metadata = next(
        selector_or_metadata
        for selector_or_metadata in search_results
        if isinstance(selector_or_metadata, Metadata)
        and selector_or_metadata.series.tags["tag1"] == "value1b"
    )
    assert isinstance(metadata, Metadata)


def test_get_metadata_for_series_without_metadata(client: Client):
    with pytest.raises(Exception) as error:
        client.get_metadata(
            SeriesSelector(
                "integration-test", {"tag1": "value1b", "tag2": "value2b"}, "pH"
            ),
        )
    assert error.match("Metadata not found")
