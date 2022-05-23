"""Kukur makes time series data and metadata available to the Apache Arrow ecosystem."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import typing

from datetime import datetime
from typing import Generator, Optional, Protocol, Union

import pyarrow as pa

from .base import (
    DataType,
    Dictionary,
    InterpolationType,
    SeriesSearch,
    SeriesSelector,
    SourceStructure,
)
from .exceptions import KukurException  # noqa
from .metadata import Metadata
from .client import Client


@typing.runtime_checkable
class Source(Protocol):
    """Source is the interface that Kukur data sources need to implement."""

    def search(
        self, selector: SeriesSearch
    ) -> Generator[Union[SeriesSelector, Metadata], None, None]:
        """Return all time series or even the metadata of them in this source matching the selector."""
        ...

    def get_metadata(self, selector: SeriesSelector) -> Metadata:
        """Return metadata for the given time series."""
        ...

    def get_data(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Return data for the given time series in the given time period."""
        ...


@typing.runtime_checkable
class TagSource(Source, Protocol):
    """TagSource is the interface that Kukur data sources that support tags and fields need to implement."""

    def get_source_structure(
        self, selector: SeriesSelector
    ) -> Optional[SourceStructure]:
        """Return the available tag keys and tag values and fields of a source."""
        ...


__all__ = [
    "Client",
    "DataType",
    "Dictionary",
    "InterpolationType",
    "Metadata",
    "SeriesSearch",
    "SeriesSelector",
]
