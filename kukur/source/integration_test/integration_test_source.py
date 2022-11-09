"""Return fake data for integration tests."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from typing import Generator, Union

from pyarrow import Table

from kukur import Metadata, SeriesSearch, SeriesSelector
from kukur.exceptions import InvalidDataError
from kukur.metadata.fields import Description, Unit


class IntegrationTestSource:
    """Source that returns fake data for Flight integration testing."""

    def search(  # pylint: disable=no-self-use
        self, selector: SeriesSearch
    ) -> Generator[Union[Metadata, SeriesSelector], None, None]:
        """Search time series using the Flight service."""
        yield SeriesSelector(
            selector.source, {"tag1": "value1", "tag2": "value2"}, "pressure"
        )
        yield Metadata(
            SeriesSelector(
                selector.source, {"tag1": "value1a", "tag2": "value2a"}, "temperature"
            ),
            {Description: "integration test temperature"},
        )
        yield Metadata(
            SeriesSelector(
                selector.source, {"tag1": "value1b", "tag2": "value2b"}, "pH"
            ),
            {Description: "integration test pH"},
        )

    def get_metadata(  # pylint: disable=no-self-use
        self, selector: SeriesSelector
    ) -> Metadata:
        """Get metadata from the Flight service."""
        if selector == SeriesSelector(
            selector.source, {"tag1": "value1", "tag2": "value2"}, "pressure"
        ):
            return Metadata(selector, {Description: "integration test pressure"})
        if selector == SeriesSelector(
            selector.source, {"tag1": "value1a", "tag2": "value2a"}, "temperature"
        ):
            return Metadata(selector, {Unit: "c"})
        if selector == SeriesSelector(
            selector.source, {"tag1": "value1b", "tag2": "value2b"}, "pH"
        ):
            raise InvalidDataError("Metadata not found")
        return Metadata(selector)

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> Table:
        """Get data from the Flight service."""


def from_config() -> IntegrationTestSource:
    """Create a new integration test source."""
    return IntegrationTestSource()
