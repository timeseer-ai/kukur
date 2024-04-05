"""Integration tests require a running Kukur instance.

They use the client to request data.
"""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os
from datetime import datetime

import pytest

from kukur import Client, Metadata, SeriesSelector

pytestmark = pytest.mark.postgresql


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
    many_series = list(client.search(SeriesSelector(suffix_source("postgres"))))
    assert len(many_series) == 2
    assert isinstance(many_series[0], Metadata)
    assert many_series[0].series.tags["series name"] == "test-tag-1"
    assert isinstance(many_series[1], Metadata)
    assert many_series[1].series.tags["series name"] == "test-tag-2"


def test_data(client: Client):
    data = client.get_data(
        SeriesSelector(suffix_source("postgres"), "test-tag-1"),
        datetime.fromisoformat("2020-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2020-03-01T00:00:00+00:00"),
    )
    assert len(data) == 2


def test_search_psycopg(client: Client):
    many_series = list(client.search(SeriesSelector(suffix_source("postgres-psycopg"))))
    assert len(many_series) == 2
    assert isinstance(many_series[0], Metadata)
    assert many_series[0].series.tags["series name"] == "test-tag-1"
    assert isinstance(many_series[1], Metadata)
    assert many_series[1].series.tags["series name"] == "test-tag-2"


def test_data_psycopg(client: Client):
    data = client.get_data(
        SeriesSelector(suffix_source("postgres-psycopg"), "test-tag-1"),
        datetime.fromisoformat("2020-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2020-03-01T00:00:00+00:00"),
    )
    assert len(data) == 2
