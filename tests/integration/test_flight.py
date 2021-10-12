"""Integration tests require a running Kukur instance.

They use the client to request data."""

from datetime import datetime

import pytest

from kukur import Client, SeriesSelector


@pytest.fixture
def client() -> Client:
    kukur_client = Client()
    kukur_client._get_client().wait_for_available(timeout=10)
    return kukur_client


def test_search(client: Client):
    many_series = list(client.search(SeriesSelector("row")))
    assert len(many_series) == 5
    dictionary_series = [
        series for series in many_series if series.series.name == "test-tag-6"
    ][0]
    assert dictionary_series.description == "Valve X"
    assert dictionary_series.dictionary_name == "Active"
    assert dictionary_series.dictionary is not None
    assert len(dictionary_series.dictionary.mapping) == 2
    assert dictionary_series.dictionary.mapping[0] == "OFF"
    assert dictionary_series.dictionary.mapping[1] == "ON"


def test_metadata(client: Client):
    dictionary_series = client.get_metadata(SeriesSelector("row", "test-tag-6"))
    assert dictionary_series.description == "Valve X"
    assert dictionary_series.dictionary_name == "Active"
    assert dictionary_series.dictionary is not None
    assert len(dictionary_series.dictionary.mapping) == 2
    assert dictionary_series.dictionary.mapping[0] == "OFF"
    assert dictionary_series.dictionary.mapping[1] == "ON"


def test_data(client: Client):
    start_date = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    data = client.get_data(SeriesSelector("row", "test-tag-6"), start_date, end_date)
    assert len(data) == 7
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 1.0
    assert data["ts"][6].as_py() == datetime.fromisoformat("2020-07-01T00:00:00+00:00")
    assert data["value"][6].as_py() == 1.0


def test_sources(client: Client):
    data = client.list_sources()
    assert len(data) == 27
    assert "sql" in data
    assert "row" in data
    assert "noaa" in data
