"""Time series metadata fields common to all time series."""

from typing import Any, Optional

from kukur.base import (
    DataType as KukurDataType,
    Dictionary as KukurDictionary,
    InterpolationType as KukurInterpolationType,
    ProcessType as KukurProcessType,
)

from . import Metadata, MetadataField


Description = MetadataField[str](
    "description", default="", serialized_name="description"
)
Metadata.register_field(Description)

Unit = MetadataField[str]("unit", default="", serialized_name="unit")
Metadata.register_field(Unit)


def _parse_float(number: Optional[Any]) -> Optional[float]:
    if number is None:
        return None
    return float(number)


LimitLow = MetadataField[Optional[float]](
    "lower limit",
    default=None,
    serialized_name="limitLow",
    deserialize=_parse_float,
)
Metadata.register_field(LimitLow)


LimitHigh = MetadataField[Optional[float]](
    "upper limit",
    default=None,
    serialized_name="limitHigh",
    deserialize=_parse_float,
)
Metadata.register_field(LimitHigh)


Accuracy = MetadataField[Optional[float]](
    "accuracy",
    default=None,
    serialized_name="accuracy",
    deserialize=_parse_float,
)
Metadata.register_field(Accuracy)


def _interpolation_type_to_json(
    interpolation_type: Optional[KukurInterpolationType],
) -> Optional[str]:
    if interpolation_type is None:
        return None
    return interpolation_type.value


def _interpolation_type_from_json(
    interpolation_type: Optional[str],
) -> Optional[KukurInterpolationType]:
    if interpolation_type is None:
        return None
    return KukurInterpolationType(interpolation_type)


InterpolationType = MetadataField[Optional[KukurInterpolationType]](
    "interpolation type",
    default=None,
    serialized_name="interpolationType",
    serialize=_interpolation_type_to_json,
    deserialize=_interpolation_type_from_json,
)
Metadata.register_field(InterpolationType)


def _data_type_to_json(data_type: Optional[KukurDataType]) -> Optional[str]:
    if data_type is None:
        return None
    return data_type.value


def _data_type_from_json(data_type: Optional[str]) -> Optional[KukurDataType]:
    if data_type is None:
        return None
    return KukurDataType(data_type)


DataType = MetadataField[Optional[KukurDataType]](
    "data type",
    default=None,
    serialized_name="dataType",
    serialize=_data_type_to_json,
    deserialize=_data_type_from_json,
)
Metadata.register_field(DataType)


DictionaryName = MetadataField[Optional[str]](
    "dictionary name", default=None, serialized_name="dictionaryName"
)
Metadata.register_field(DictionaryName)


def _dictionary_to_json(
    dictionary: Optional[KukurDictionary],
) -> Optional[list[tuple[int, str]]]:
    if dictionary is None:
        return None
    return list(dictionary.mapping.items())


def _dictionary_from_json(
    dictionary: Optional[list[tuple[int, str]]]
) -> Optional[KukurDictionary]:
    if dictionary is None:
        return None
    return KukurDictionary(dict(dictionary))


Dictionary = MetadataField[Optional[KukurDictionary]](
    "dictionary",
    default=None,
    serialized_name="dictionary",
    serialize=_dictionary_to_json,
    deserialize=_dictionary_from_json,
)
Metadata.register_field(Dictionary)


def _process_type_to_json(process_type: Optional[KukurProcessType]) -> Optional[str]:
    if process_type is None:
        return None
    return process_type.value


def _process_type_from_json(process_type: Optional[str]) -> Optional[KukurProcessType]:
    if process_type is None:
        return None
    return KukurProcessType(process_type)


ProcessType = MetadataField[Optional[KukurProcessType]](
    "process type",
    default=None,
    serialized_name="processType",
    serialize=_process_type_to_json,
    deserialize=_process_type_from_json,
)
Metadata.register_field(ProcessType)
