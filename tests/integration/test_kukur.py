"""Integration test for Kukur sources."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import pytest

from kukur import Client, SeriesSelector

pytestmark = pytest.mark.kukur


@pytest.fixture
def client() -> Client:
    kukur_client = Client()
    kukur_client._get_client().wait_for_available(timeout=10)
    return kukur_client


def test_series_metadata_without_series_name(client: Client):
    metadata = client.get_metadata(
        SeriesSelector(
            "kukur-integration-test", {"tag1": "value1", "tag2": "value2"}, "pressure"
        )
    )
    assert metadata.get_field_by_name("description") == "integration test pressure"
