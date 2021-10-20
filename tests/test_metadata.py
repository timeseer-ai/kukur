"""Test flexible Metadata"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from kukur import (
    DataType,
    Dictionary,
    InterpolationType,
    SeriesSelector,
)
from kukur.metadata import Metadata, fields

SERIES = SeriesSelector("test", "test-tag-1")


def test_series_json():
    metadata = Metadata(SERIES)
    data = metadata.to_data()
    assert data["series"]["source"] == "test"
    assert data["series"]["name"] == "test-tag-1"

    new_metadata = Metadata.from_data(data)
    assert new_metadata.series == SERIES


def test_description():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Description) == ""

    metadata.set_field(fields.Description, "a test tag")
    assert metadata.get_field(fields.Description) == "a test tag"


def test_description_json():
    metadata = Metadata(SERIES, {fields.Description: "a test tag"})
    data = metadata.to_data()
    assert data["description"] == "a test tag"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Description) == "a test tag"


def test_unit():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Unit) == ""

    metadata.set_field(fields.Unit, "kg")
    assert metadata.get_field(fields.Unit) == "kg"


def test_unit_json():
    metadata = Metadata(SERIES, {fields.Unit: "kg"})
    data = metadata.to_data()
    assert data["unit"] == "kg"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Unit) == "kg"


def test_limit_low():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.LimitLow) is None

    metadata.set_field(fields.LimitLow, 0)
    assert metadata.get_field(fields.LimitLow) == 0


def test_limit_low_json():
    metadata = Metadata(SERIES, {fields.LimitLow: 0})
    data = metadata.to_data()
    assert data["limitLow"] == 0

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.LimitLow) == 0


def test_limit_low_coerce():
    metadata = Metadata(SERIES)
    metadata.coerce_field("lower limit", "0")
    assert metadata.get_field(fields.LimitLow) == 0


def test_limit_high():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.LimitHigh) is None

    metadata.set_field(fields.LimitHigh, 0)
    assert metadata.get_field(fields.LimitHigh) == 0


def test_limit_high_json():
    metadata = Metadata(SERIES, {fields.LimitHigh: 0})
    data = metadata.to_data()
    assert data["limitHigh"] == 0

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.LimitHigh) == 0


def test_limit_high_coerce():
    metadata = Metadata(SERIES)
    metadata.coerce_field("upper limit", "0")
    assert metadata.get_field(fields.LimitHigh) == 0


def test_accuracy():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Accuracy) is None

    metadata.set_field(fields.Accuracy, 0.2)
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_accuracy_json():
    metadata = Metadata(SERIES, {fields.Accuracy: 0.2})
    data = metadata.to_data()
    assert data["accuracy"] == 0.2

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Accuracy) == 0.2


def test_accuracy_coerce():
    metadata = Metadata(SERIES)
    metadata.coerce_field("accuracy", "0.2")
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_interpolation_type():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.InterpolationType) is None

    metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR


def test_interpolation_type_json():
    metadata = Metadata(SERIES, {fields.InterpolationType: InterpolationType.LINEAR})
    data = metadata.to_data()
    assert data["interpolationType"] == "LINEAR"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR


def test_interpolation_type_coerce():
    metadata = Metadata(SERIES)
    metadata.coerce_field("interpolation type", "STEPPED")
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.STEPPED


def test_data_type():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.DataType) is None

    metadata.set_field(fields.DataType, DataType.STRING)
    assert metadata.get_field(fields.DataType) == DataType.STRING


def test_data_type_json():
    metadata = Metadata(SERIES, {fields.DataType: DataType.STRING})
    data = metadata.to_data()
    assert data["dataType"] == "STRING"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.DataType) == DataType.STRING


def test_data_type_coerce():
    metadata = Metadata(SERIES)
    metadata.coerce_field("data type", "DICTIONARY")
    assert metadata.get_field(fields.DataType) == DataType.DICTIONARY


def test_dictionary_name():
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.DictionaryName) is None

    metadata.set_field(fields.DictionaryName, "onoff")
    assert metadata.get_field(fields.DictionaryName) == "onoff"


def test_dictionary_name_json():
    metadata = Metadata(SERIES, {fields.DictionaryName: "onoff"})
    data = metadata.to_data()
    assert data["dictionaryName"] == "onoff"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.DictionaryName) == "onoff"


def test_dictionary():
    dictionary = Dictionary({0: "OFF", 1: "ON"})

    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Dictionary) is None

    metadata.set_field(fields.Dictionary, dictionary)
    assert metadata.get_field(fields.Dictionary) == dictionary


def test_dictionary_json():
    dictionary = Dictionary({0: "OFF", 1: "ON"})

    metadata = Metadata(SERIES, {fields.Dictionary: dictionary})
    data = metadata.to_data()
    assert data["dictionary"] == [(0, "OFF"), (1, "ON")]

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Dictionary) == dictionary


def test_dictionary_coerce():
    metadata = Metadata(SERIES)
    metadata.coerce_field("dictionary", [(0, "OFF"), (1, "ON")])
    assert metadata.get_field(fields.Dictionary) == Dictionary({0: "OFF", 1: "ON"})
