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
        if series.series.name == "h2o_feet,location=coyote_creek::water_level"
    ][0]
    assert isinstance(series, Metadata)
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_metadata(client: Client):
    series = client.get_metadata(
        SeriesSelector(
            suffix_source("noaa"), "h2o_feet,location=coyote_creek::water_level"
        )
    )
    assert series.get_field(fields.LimitLowFunctional) == 6
    assert series.get_field(fields.LimitHighFunctional) == 9


def test_data(client: Client):
    start_date = datetime.fromisoformat("2019-09-17T00:00:00+00:00")
    end_date = datetime.fromisoformat("2019-09-17T16:24:00+00:00")
    data = client.get_data(
        SeriesSelector(
            suffix_source("noaa"), "h2o_feet,location=coyote_creek::water_level"
        ),
        start_date,
        end_date,
    )
    assert len(data) == 165
    assert data["ts"][0].as_py() == start_date
    assert data["value"][0].as_py() == 8.412
    assert data["ts"][164].as_py() == end_date
    assert data["value"][164].as_py() == 3.235
