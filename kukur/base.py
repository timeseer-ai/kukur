"""The main objects in Kukur."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import typing

from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, Optional, Union


@dataclass
class Dictionary:
    """A Dictionary maps numbers to labels.

    Time series can contain integer values that have a meaning. For example
    the number 0 could be 'OFF' and 1 'ON'.

    The ordering of the labels can also be significant while presenting the
    series to a user.

    In Python 3.8+, iteration over a dict keeps the insert ordering."""

    mapping: Dict[int, str]


@dataclass
class SeriesSelector:
    """SeriesSelector identifies a group of time series matching the given pattern."""

    source: str
    name: Optional[str] = None


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


class ProcessType(Enum):
    """ProcessType represents the process type of an analysis"""

    CONTINUOUS = "CONTINUOUS"
    REGIME = "REGIME"
    BATCH = "BATCH"


@dataclass
class Metadata:  # pylint: disable=too-many-instance-attributes
    """Metadata contains metadata fields known by Kukur."""

    series: SeriesSelector
    description: str = ""
    unit: str = ""
    limit_low: Optional[float] = None
    limit_high: Optional[float] = None
    accuracy: Optional[float] = None
    interpolation_type: Optional[InterpolationType] = None
    data_type: Optional[DataType] = None
    dictionary_name: Optional[str] = None
    dictionary: Optional[Dictionary] = None
    process_type: Optional[ProcessType] = None

    __json_mapping = {
        "limit_low": "limitLow",
        "limit_high": "limitHigh",
        "interpolation_type": "interpolationType",
        "data_type": "dataType",
        "dictionary_name": "dictionaryName",
        "dictionary": "dictionary",
        "process_type": "processType",
    }

    __human_mapping = {
        "lower limit": "limit_low",
        "upper limit": "limit_high",
        "interpolation type": "interpolation_type",
        "data type": "data_type",
        "dictionary name": "dictionary_name",
        "process type": "process_type",
    }

    __hidden = [
        "dictionary name",
        "dictionary",
    ]

    def __iter__(self):
        reverse_mapping = {v: k for k, v in self.__human_mapping.items()}
        for k, v in vars(self).items():
            if k == "series":
                continue
            yield (reverse_mapping.get(k, k), self._serialize_value(v))

    def camelcase(self):
        """Converts this Metadata instance to a dict that follows the camelCase naming convention.

        This dict is suitable for JSON."""
        return {
            self.__json_mapping.get(k, k): self._to_json(v)
            for k, v in vars(self).items()
        }

    def set_field(self, name: str, value: Union[Optional[float], str, Dictionary]):
        """Set a metadata field from the human friendly or camelcase field key representation."""
        mapped_name = self.__human_mapping.get(name, name)
        if mapped_name not in vars(self):
            camelcase_mapping = {v: k for k, v in self.__json_mapping.items()}
            mapped_name = camelcase_mapping.get(name, name)
            if mapped_name not in vars(self):
                raise AttributeError()

        hints = typing.get_type_hints(self.__class__)
        options = typing.get_args(hints[mapped_name])
        if isinstance(value, Dictionary):

            def convert_fn(value):
                return value

        elif len(options) == 0:
            convert_fn = hints[mapped_name]
        else:
            none_class = None.__class__
            convert_fn = [clazz for clazz in options if clazz != none_class][0]

        vars(self)[mapped_name] = convert_fn(value)

    def get_field(self, name: str) -> Union[Optional[float], str]:
        """Get the value of the metadata field with the given human friendly name."""
        mapped_name = self.__human_mapping.get(name, name)
        return vars(self)[mapped_name]

    def is_shown(self, name: str) -> bool:
        """Determine if the field with the given human friendly name should be displayed.

        The 'dictionary' will only be shown when it contains actual values for example.
        """
        return name not in self.__hidden or (
            self.get_field(name) is not None and self.get_field(name) != ""
        )

    def _to_json(self, v: Any):
        if isinstance(v, Dictionary):
            return list(v.mapping.items())
        return self._serialize_value(v)

    def _serialize_value(self, v: Any):  # pylint: disable=no-self-use
        if isinstance(v, Enum):
            return v.value
        return v
