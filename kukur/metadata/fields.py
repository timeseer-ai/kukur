"""Time series metadata fields common to all time series."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from collections.abc import Callable
from typing import Any, Generic, TypeVar

from kukur.base import DataType as KukurDataType
from kukur.base import Dictionary as KukurDictionary
from kukur.base import InterpolationType as KukurInterpolationType

T = TypeVar("T")


class MetadataField(Generic[T]):
    """A typed metadata field.

    This class is generic over the actual type of the field value.

    JSON-ready data dictionary conversions should be provided for all non-trivial implementations.
    """

    def __init__(  # noqa: PLR0913
        self,
        name: str,
        *,
        default: T,
        serialized_name: str,
        serialize: Callable[[T], Any] | None = None,
        deserialize: Callable[[Any], T] | None = None,
        calculate: Callable[[Any, Any], T] | None = None,
    ):
        self.__name = name
        self.__default = default
        self.__serialized_name = serialized_name
        self.__serialize = serialize
        self.__deserialize = deserialize
        self.__calculate = calculate

    def __repr__(self) -> str:
        return f'MetadataField("{self.__name}")'

    def __hash__(self):
        return hash(self.__name)

    def name(self) -> str:
        """Return the name of the metadata field."""
        return self.__name

    def default(self) -> T:
        """Return the default (empty) value for this field."""
        return self.__default

    def serialized_name(self) -> str:
        """Return the name of the field in a JSON dictionary."""
        return self.__serialized_name

    def serialize(self, value: T) -> Any:
        """Convert a field of this type to JSON."""
        if self.__serialize is None:
            return value
        return self.__serialize(value)

    def deserialize(self, value: Any) -> T:
        """Convert the result of a serialization to a proper value."""
        if self.__deserialize is None:
            return value
        return self.__deserialize(value)

    def calculated_value(self, metadata, value: Any) -> T:
        """Convert the value based on the calculate function."""
        if self.__calculate is None:
            return value
        return self.__calculate(metadata, value)


Description = MetadataField[str](
    "description", default="", serialized_name="description"
)

Unit = MetadataField[str]("unit", default="", serialized_name="unit")


def _parse_float(number: Any | None) -> float | None:
    if number is None:
        return None
    if _is_empty_string(number):
        return None
    return float(number)


LimitLowPhysical = MetadataField[float | None](
    "physical lower limit",
    default=None,
    serialized_name="limitLowPhysical",
    deserialize=_parse_float,
)


LimitHighPhysical = MetadataField[float | None](
    "physical upper limit",
    default=None,
    serialized_name="limitHighPhysical",
    deserialize=_parse_float,
)


LimitLowFunctional = MetadataField[float | None](
    "functional lower limit",
    default=None,
    serialized_name="limitLowFunctional",
    deserialize=_parse_float,
)


LimitHighFunctional = MetadataField[float | None](
    "functional upper limit",
    default=None,
    serialized_name="limitHighFunctional",
    deserialize=_parse_float,
)


def _calculate_accuracy(metadata, accuracy: float | None) -> float | None:
    """Calculate the accuracy based on the accuracy percentage."""
    if accuracy is not None:
        return accuracy
    accuracy_percentage = metadata.get_field(AccuracyPercentage)
    if (
        accuracy_percentage is None
        or accuracy_percentage < 0
        or accuracy_percentage > 100  # noqa: PLR2004
    ):
        return None
    low_limit = metadata.get_field(LimitLowPhysical)
    if low_limit is None:
        low_limit = metadata.get_field(LimitLowFunctional)
        if low_limit is None:
            return None
    high_limit = metadata.get_field(LimitHighPhysical)
    if high_limit is None:
        high_limit = metadata.get_field(LimitHighFunctional)
        if high_limit is None:
            return None
    return (high_limit - low_limit) * float(accuracy_percentage) / 100


Accuracy = MetadataField[float | None](
    "accuracy",
    default=None,
    serialized_name="accuracy",
    deserialize=_parse_float,
    calculate=_calculate_accuracy,
)


def _parse_accuracy_percentage_float(number: Any | None) -> float | None:
    if number is None:
        return None
    if _is_empty_string(number):
        return None
    return float(number)


AccuracyPercentage = MetadataField[float | None](
    "accuracy percentage",
    default=None,
    serialized_name="accuracyPercentage",
    deserialize=_parse_accuracy_percentage_float,
)


def _interpolation_type_to_json(
    interpolation_type: KukurInterpolationType | None,
) -> str | None:
    if interpolation_type is None:
        return None
    return interpolation_type.value


def _interpolation_type_from_json(
    interpolation_type: str | None,
) -> KukurInterpolationType | None:
    if interpolation_type is None:
        return None
    if _is_empty_string(interpolation_type):
        return None
    return KukurInterpolationType(interpolation_type)


InterpolationType = MetadataField[KukurInterpolationType | None](
    "interpolation type",
    default=None,
    serialized_name="interpolationType",
    serialize=_interpolation_type_to_json,
    deserialize=_interpolation_type_from_json,
)


def _data_type_to_json(data_type: KukurDataType | None) -> str | None:
    if data_type is None:
        return None
    return data_type.value


def _data_type_from_json(data_type: str | None) -> KukurDataType | None:
    if data_type is None:
        return None
    if _is_empty_string(data_type):
        return None
    return KukurDataType(data_type)


DataType = MetadataField[KukurDataType | None](
    "data type",
    default=None,
    serialized_name="dataType",
    serialize=_data_type_to_json,
    deserialize=_data_type_from_json,
)


DictionaryName = MetadataField[str | None](
    "dictionary name", default=None, serialized_name="dictionaryName"
)


def _dictionary_to_json(
    dictionary: KukurDictionary | None,
) -> list[tuple[int, str]] | None:
    if dictionary is None:
        return None
    return list(dictionary.mapping.items())


def _dictionary_from_json(
    dictionary: list[tuple[int, str]] | None,
) -> KukurDictionary | None:
    if dictionary is None:
        return None
    return KukurDictionary(dict(dictionary))


Dictionary = MetadataField[KukurDictionary | None](
    "dictionary",
    default=None,
    serialized_name="dictionary",
    serialize=_dictionary_to_json,
    deserialize=_dictionary_from_json,
)


def _is_empty_string(value: Any) -> bool:
    return isinstance(value, str) and value.strip() == ""


def register_default_fields(cls) -> None:
    """Register all common metadata fields to the Metadata class."""
    cls.register_field(Description)
    cls.register_field(Unit)
    cls.register_field(LimitLowPhysical)
    cls.register_field(LimitHighPhysical)
    cls.register_field(LimitLowFunctional)
    cls.register_field(LimitHighFunctional)
    cls.register_field(Accuracy)
    cls.register_field(AccuracyPercentage)
    cls.register_field(InterpolationType)
    cls.register_field(DataType)
    cls.register_field(DictionaryName)
    cls.register_field(Dictionary)
