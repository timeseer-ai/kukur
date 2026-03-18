"""Integration tests require a running Kukur instance.

They use the client to request data.
"""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os
from datetime import datetime

import pytest

from kukur import Client, SeriesSearch, SeriesSelector

pytestmark = pytest.mark.tdengine


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
    all_metadata = list(client.search(SeriesSearch(suffix_source("tdengine"))))
    assert len(all_metadata) == 2


def test_data(client: Client):
    data = client.get_data(
        SeriesSelector(
            suffix_source("tdengine"), {"name": "test-tag-1", "location": "Antwerp"}
        ),
        datetime.fromisoformat("2020-01-01T00:00:00+00:00"),
        datetime.fromisoformat("2020-01-03T00:00:00+00:00"),
    )
    assert len(data) == 2
