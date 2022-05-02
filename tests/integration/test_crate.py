"""Integration tests require a running Kukur instance.

They use the client to request data."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os

from datetime import datetime
from typing import Generator

import pytest

from crate import client as crate_client

from kukur import Client, SeriesSelector
from kukur.metadata import fields


@pytest.fixture
def client() -> Client:
    kukur_client = Client()
    kukur_client._get_client().wait_for_available(timeout=10)
    return kukur_client


@pytest.fixture(autouse=True)
def insert_sample_data() -> Generator[None, None, None]:
    db = crate_client.connect("localhost:4200")
    cursor = db.cursor()
    cursor.execute(
        """
        drop table if exists Data
    """
    )
    cursor.execute(
        """
        create table Data (
            timestamp timestamp with time zone,
            name text,
            value double precision,
            unit text
        )
    """
    )
    cursor.executemany(
        """
        insert into Data (timestamp, name, value, unit) values (?, ?, ?, ?)
    """,
        [
            ("2022-01-01T00:00:00+00:00", "test-tag-1", 42, "Pa"),
            ("2022-01-02T00:00:00+00:00", "test-tag-1", 43, "Pa"),
            ("2022-01-03T00:00:00+00:00", "test-tag-1", 44, "Pa"),
        ],
    )
    while True:  # wait for eventual consistency
        cursor.execute("select count(*) from Data")
        (count,) = cursor.fetchone()
        if count == 3:
            break
    yield


def suffix_source(source_name: str) -> str:
    if "KUKUR_INTEGRATION_TARGET" in os.environ:
        target = os.environ["KUKUR_INTEGRATION_TARGET"]
        return f"{source_name}-{target}"
    return source_name  # works in docker container


def test_search(client: Client):
    many_series = list(client.search(SeriesSelector(suffix_source("crate"))))
    assert len(many_series) == 1
    assert many_series[0].tags["series name"] == "test-tag-1"


def test_metadata(client: Client):
    metadata = client.get_metadata(
        SeriesSelector(suffix_source("crate"), {"series name": "test-tag-1"})
    )
    assert metadata.get_field(fields.Unit) == "Pa"


def test_data(client: Client):
    data = client.get_data(
        SeriesSelector(suffix_source("crate"), {"series name": "test-tag-1"}),
        datetime.fromisoformat("2022-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2022-01-03T00:00:00+00:00"),
    )
    assert len(data) == 2
    assert data["ts"][0].as_py() == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert data["value"][0].as_py() == 42
    assert data["ts"][1].as_py() == datetime.fromisoformat("2022-01-02T00:00:00+00:00")
    assert data["value"][1].as_py() == 43


def test_metadata_backwards_compatibility(client: Client):
    metadata = client.get_metadata(SeriesSelector(suffix_source("crate"), "test-tag-1"))
    assert metadata.get_field(fields.Unit) == "Pa"


def test_data_backwards_compatibility(client: Client):
    data = client.get_data(
        SeriesSelector(suffix_source("crate"), "test-tag-1"),
        datetime.fromisoformat("2022-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2022-01-03T00:00:00+00:00"),
    )
    assert len(data) == 2
    assert data["ts"][0].as_py() == datetime.fromisoformat("2022-01-01T00:00:00+00:00")
    assert data["value"][0].as_py() == 42
    assert data["ts"][1].as_py() == datetime.fromisoformat("2022-01-02T00:00:00+00:00")
    assert data["value"][1].as_py() == 43
