"""The main objects in Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass, field as data_field
from enum import Enum
from typing import Any, Union


@dataclass
class Dictionary:
    """A Dictionary maps numbers to labels.

    Time series can contain integer values that have a meaning. For example
    the number 0 could be 'OFF' and 1 'ON'.

    The ordering of the labels can also be significant while presenting the
    series to a user.

    In Python 3.8+, iteration over a dict keeps the insert ordering."""

    mapping: dict[int, str]


@dataclass
class ComplexSeriesSelector:
    """SeriesSelector identifies a group of time series matching the given pattern."""

    source: str
    tags: dict[str, str] = data_field(default_factory=dict)
    field: str = "value"

    @classmethod
    def from_data(cls, data: dict[str, Any]):
        """Create a Series from a dictionary."""
        tags = data.get("tags", {})
        if "name" in data and "tags" not in data:
            tags["series name"] = data["name"]
        return cls(data["source"], tags, data["field"])

    def to_data(self) -> dict[str, Any]:
        """Convert to JSON object."""
        return dict(source=self.source, tags=self.tags, field=self.field)

    def get_series_name(self) -> str:
        """Get the series name with tags and fields included.

        For sources that cannot handle tags and fiels yet."""
        series_string = ""
        for tag_key, tag_value in self.tags.items():
            if tag_key == "series name":
                series_string = series_string + tag_value
                continue
            series_string = series_string + "," + f"{tag_key}={tag_value}"
        if self.field == "value":
            return f"{series_string}"
        return f"{series_string}::{self.field}"


@dataclass
class SeriesSelector(ComplexSeriesSelector):
    """SeriesSelector identifies a group of time series matching the given pattern."""

    def __init__(
        self, source: str, tags: Union[str, dict[str, str]] = None, field: str = "value"
    ):
        tags_dict = {}
        if isinstance(tags, str):
            tags_dict["series name"] = tags
        if isinstance(tags, dict):
            tags_dict = tags
        super().__init__(source, tags_dict, field)


class InterpolationType(Enum):
    """InterpolationType describes how the value of a series evolves between data points."""

    LINEAR = "LINEAR"
    STEPPED = "STEPPED"


class DataType(Enum):
    """DataType represents the data type of the values in a time series.

    FLOAT32 is a 32 bit floating point number
    FLOAT64 is a 64 bit floating point number
    STRING is a variable length utf-8 character array
    DICTIONARY is an enumeration of ordered discrete values with a corresponding mapping
    CATEGORICAL is an enumeration of ordered discrete values
    """

    FLOAT32 = "FLOAT32"
    FLOAT64 = "FLOAT64"
    STRING = "STRING"
    DICTIONARY = "DICTIONARY"
    CATEGORICAL = "CATEGORICAL"
