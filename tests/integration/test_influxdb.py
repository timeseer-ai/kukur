"""Integration tests require a running Kukur instance.

They use the client to request data."""

import os

from datetime import datetime

import pytest

from kukur import Client, Metadata, SeriesSelector
from kukur.metadata import fields


@pytest.fixture
def client() -> Client:
    kukur_client = Client()
    kukur_client._get_client().wait_for_available(timeout=10)
    return kukur_client


def suffix_source(source_name: str) -> str:
    if "KUKUR_INTEGRATION_TARGET" in os.environ:
        target = os.environ["KUKUR_INTEGRATION_TARGET"]
        return f"{source_name}-{target}"
    return source_name  # works in docker container


def test_search(client: Client):
    many_series = list(client.search(SeriesSelector(suffix_source("noaa"))))
    assert len(many_series) == 16
    series = [
        series
        for series in many_series
        if series.series.tags["series name"] == "h2o_feet"
        and series.series.tags["location"] == "coyote_creek"
        and series.series.field == "water_level"
    ][0]
    assert isinstance(series, Metadata)
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_search_with_tags(client: Client):
    many_series = list(
        client.search(
            SeriesSelector(
                suffix_source("noaa"),
                {"series name": "h2o_feet", "location": "coyote_creek"},
            )
        )
    )
    assert len(many_series) == 2
    series = [series for series in many_series if series.series.field == "water_level"][
        0
    ]
    assert isinstance(series, Metadata)
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_search_with_fields(client: Client):
    many_series = list(
        client.search(
            SeriesSelector(
                suffix_source("noaa"),
                {"series name": "h2o_feet", "location": "coyote_creek"},
                "water_level",
            )
        )
    )
    assert len(many_series) == 1
    series = many_series[0]
    assert isinstance(series, Metadata)
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_metadata(client: Client):
    tags = {
        "series name": "h2o_feet",
        "location": "coyote_creek",
    }
    series = client.get_metadata(
        SeriesSelector.from_tags(suffix_source("noaa"), tags, "water_level")
    )
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_data(client: Client):
    tags = {
        "series name": "h2o_feet",
        "location": "coyote_creek",
    }
    start_date = datetime.fromisoformat("2019-09-17T00:00:00+00:00")
    end_date = datetime.fromisoformat("2019-09-17T16:24:00+00:00")
    data = client.get_data(
        SeriesSelector.from_tags(suffix_source("noaa"), tags, "water_level"),
        start_date,
        end_date,
    )
    assert len(data) == 165
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 8.412
    assert data["ts"][164].as_py() == end_date
    assert data["value"][164].as_py() == 3.235


def test_get_source_structure(client: Client):
    source_structure = client.get_source_structure(
        SeriesSelector(suffix_source("noaa")),
    )
    assert len(source_structure.tag_keys) == 2
    assert "location" in source_structure.tag_keys
    assert len(source_structure.tag_values) == 5
    assert {"key": "location", "value": "coyote_creek"} in source_structure.tag_values
    assert len(source_structure.fields) == 6
    assert "degrees" in source_structure.fields
