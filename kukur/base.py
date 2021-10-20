"""The main objects in Kukur."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from dataclasses import dataclass
from enum import Enum
from typing import Dict, Optional


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
