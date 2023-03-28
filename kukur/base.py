"""The main objects in Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from dataclasses import field as data_field
from enum import Enum
from typing import Any, Dict, List, Optional, Union


@dataclass
class Dictionary:
    """A Dictionary maps numbers to labels.

    Time series can contain integer values that have a meaning. For example
    the number 0 could be 'OFF' and 1 'ON'.

    The ordering of the labels can also be significant while presenting the
    series to a user.

    In Python 3.8+, iteration over a dict keeps the insert ordering.
    """

    mapping: Dict[int, str]


@dataclass
class SeriesSearch:
    """SeriesSearch is the series selector to search series.

    The field can be optional to allow searching for anything.
    """

    source: str
    tags: Dict[str, str] = data_field(default_factory=dict)
    field: Optional[str] = None

    def __init__(
        self,
        source: str,
        tags: Optional[Union[str, Dict[str, str]]] = None,
        field: Optional[str] = None,
    ):
        tags_dict = {}
        if isinstance(tags, str):
            tags_dict["series name"] = tags
        if isinstance(tags, dict):
            tags_dict = tags
        self.source = source
        self.tags = tags_dict
        self.field = field

    @property
    def name(self) -> str:
        """Get the series name with tags and fields included.

        For sources that cannot handle tags and fields yet.
        """
        series_tags: List[str] = []
        for tag_key, tag_value in self.tags.items():
            if tag_key == "series name":
                series_tags.insert(0, tag_value)
                continue
            series_tags.append(f"{tag_key}={tag_value}")
        series_string = ",".join(series_tags)
        if self.field is None:
            return f"{series_string}"
        return f"{series_string}::{self.field}"

    def to_data(self) -> Dict[str, Any]:
        """Convert to JSON object."""
        return dict(source=self.source, tags=self.tags, field=self.field)


@dataclass
class SeriesSelector(SeriesSearch):
    """SeriesSelector identifies a group of time series matching the given pattern."""

    field: str = "value"

    def __init__(
        self,
        source: str,
        tags: Optional[Union[str, Dict[str, str]]] = None,
        field: str = "value",
    ):
        super().__init__(source, tags)
        self.field = field

    @classmethod
    def from_tags(cls, source: str, tags: Dict[str, str], field: Optional[str] = None):
        """Create the SeriesSelector from tags."""
        if field is None:
            field = "value"
        return cls(source, tags, field)

    @classmethod
    def from_data(cls, data: Dict[str, Any]):
        """Create a SeriesSelector from a dictionary."""
        tags = data.get("tags", {})
        if "name" in data and "tags" not in data:
            tags["series name"] = data["name"]
        return cls(data["source"], tags, data.get("field", "value"))

    @classmethod
    def from_name(cls, source: str, name: str):
        """Create a SeriesSelector from a name."""
        field_parts = name.strip().rsplit("::", maxsplit=1)
        field = "value"
        if len(field_parts) > 1:
            field = field_parts[1]
        tags = {}
        for tag_part in field_parts[0].split(","):
            parts = tag_part.split("=", maxsplit=1)
            if len(parts) == 1:
                tags["series name"] = parts[0]
            else:
                tags[parts[0]] = parts[1]

        return cls(source, tags, field)

    def to_data(self) -> Dict[str, Any]:
        """Convert to JSON object."""
        return dict(source=self.source, tags=self.tags, field=self.field)

    @property
    def name(self) -> str:
        """Get the series name with tags and fields included.

        For sources that cannot handle tags and fields yet.
        """
        series_tags: List[str] = []
        for tag_key, tag_value in self.tags.items():
            if tag_key == "series name":
                series_tags.insert(0, tag_value)
                continue
            series_tags.append(f"{tag_key}={tag_value}")
        series_string = ",".join(series_tags)
        if self.field == "value":
            return f"{series_string}"
        return f"{series_string}::{self.field}"


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


@dataclass
class SourceStructure:
    """The SourceStructure defines the fields and tags that are present in the source."""

    fields: List[str]
    tag_keys: List[str]
    tag_values: List[dict]

    @classmethod
    def from_data(cls, data: Dict[str, Any]) -> "SourceStructure":
        """Create a SourceStructure from a dictionary."""
        return cls(data["fields"], data["tagKeys"], data["tagValues"])

    def to_data(self):
        """Convert to JSON object."""
        return dict(
            fields=self.fields, tagKeys=self.tag_keys, tagValues=self.tag_values
        )
