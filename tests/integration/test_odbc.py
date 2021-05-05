"""Integration tests require a running Kukur instance.

They use the client to request data."""

import os

from datetime import datetime

import pytest

from kukur import Client, InterpolationType, SeriesSelector


@pytest.fixture
def client() -> Client:
    kukur_client = Client()
    kukur_client._get_client().wait_for_available(timeout=10)
    return kukur_client


@pytest.fixture
def suffix_source():
    def _suffix_source(source_name: str) -> str:
        if "KUKUR_INTEGRATION_TARGET" in os.environ:
            target = os.environ["KUKUR_INTEGRATION_TARGET"]
            return f"{source_name}-{target}"
        return source_name  # works in docker container

    return _suffix_source


def test_search(client: Client, suffix_source):
    many_series = list(client.search(SeriesSelector(suffix_source("sql-list"))))
    assert len(many_series) == 4
    dictionary_series = [
        series for series in many_series if series.series.name == "test-tag-6"
    ][0]
    assert dictionary_series.description == "A dictionary series"
    assert dictionary_series.interpolation_type == InterpolationType.STEPPED
    assert dictionary_series.dictionary_name == "Active"
    assert dictionary_series.dictionary is not None
    assert len(dictionary_series.dictionary.mapping) == 2
    assert dictionary_series.dictionary.mapping[0] == "OFF"
    assert dictionary_series.dictionary.mapping[1] == "ON"


def test_interpolation_type_mapping(client: Client, suffix_source):
    many_series = list(client.search(SeriesSelector(suffix_source("sql-list"))))
    interpolation_types = [
        (metadata.series.name, metadata.interpolation_type) for metadata in many_series
    ]
    assert ("test-tag-1", InterpolationType.LINEAR) in interpolation_types
    assert ("test-tag-4", InterpolationType.STEPPED) in interpolation_types


def test_metadata(client: Client, suffix_source):
    dictionary_series = client.get_metadata(
        SeriesSelector(suffix_source("sql"), "test-tag-6")
    )
    assert dictionary_series.description == "A dictionary series"
    assert dictionary_series.interpolation_type == InterpolationType.STEPPED
    assert dictionary_series.dictionary_name == "Active"
    assert dictionary_series.dictionary is not None
    assert len(dictionary_series.dictionary.mapping) == 2
    assert dictionary_series.dictionary.mapping[0] == "OFF"
    assert dictionary_series.dictionary.mapping[1] == "ON"


def test_data(client: Client, suffix_source):
    start_date = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    data = client.get_data(
        SeriesSelector(suffix_source("sql-list"), "test-tag-6"), start_date, end_date
    )
    assert len(data) == 5
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 1.0
    assert data["ts"][4].as_py() == datetime.fromisoformat("2020-05-01T00:00:00+00:00")
    assert data["value"][4].as_py() == 1.0


def test_metadata_string_query(client: Client, suffix_source):
    dictionary_series = client.get_metadata(
        SeriesSelector(suffix_source("sql-string"), "test-tag-6")
    )
    assert dictionary_series.description == "A dictionary series"
    assert dictionary_series.interpolation_type == InterpolationType.STEPPED
    assert dictionary_series.dictionary_name == "Active"
    assert dictionary_series.dictionary is not None
    assert len(dictionary_series.dictionary.mapping) == 2
    assert dictionary_series.dictionary.mapping[0] == "OFF"
    assert dictionary_series.dictionary.mapping[1] == "ON"


def test_data_string_query(client: Client, suffix_source):
    start_date = datetime.fromisoformat("2020-01-01T00:00:00+00:00")
    end_date = datetime.fromisoformat("2021-01-01T00:00:00+00:00")
    data = client.get_data(
        SeriesSelector(suffix_source("sql-string"), "test-tag-6"), start_date, end_date
    )
    assert len(data) == 5
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 1.0
    assert data["ts"][4].as_py() == datetime.fromisoformat("2020-05-01T00:00:00+00:00")
    assert data["value"][4].as_py() == 1.0


def test_metadata_no_dictionary_query(client: Client, suffix_source):
    dictionary_series = client.get_metadata(
        SeriesSelector(suffix_source("sql-no-dictionary-query"), "test-tag-6")
    )
    assert dictionary_series.description == "A dictionary series"
    assert dictionary_series.interpolation_type == InterpolationType.STEPPED
    assert dictionary_series.dictionary_name == "Active"
    assert dictionary_series.dictionary is None
