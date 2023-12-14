"""Kukur makes time series data and metadata available to the Apache Arrow ecosystem."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
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
from .client import Client, TLSOptions  # noqa


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


@typing.runtime_checkable
class PlotSource(Source, Protocol):
    """PlotSource is the interface that Kukur data sources that support getting plot data need to implement."""

    def get_plot_data(
        self,
        selector: SeriesSelector,
        start_date: datetime,
        end_date: datetime,
        interval_count: int,
    ) -> pa.Table:
        """Return plotting data for the given time series in the given time period."""
        ...


@typing.runtime_checkable
class SignalGenerator(Protocol):
    """Protocol for generating signals."""

    def generate(
        self, selector: SeriesSelector, start_date: datetime, end_date: datetime
    ) -> pa.Table:
        """Generate data based on a selector, start date and end date."""
        ...

    def list_series(self, selector: SeriesSearch) -> Generator[Metadata, None, None]:
        """Yield all possible metadata combinations using the signal configuration and the provided selector."""
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
