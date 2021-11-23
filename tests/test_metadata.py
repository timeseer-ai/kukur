"""Test flexible Metadata"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from kukur import (
    DataType,
    Dictionary,
    InterpolationType,
    SeriesSelector,
)
from kukur.metadata import Metadata, MetadataField, fields

SERIES = SeriesSelector("test", "test-tag-1")


def test_series_json() -> None:
    metadata = Metadata(SERIES)
    data = metadata.to_data()
    assert data["series"]["source"] == "test"
    assert data["series"]["name"] == "test-tag-1"

    new_metadata = Metadata.from_data(data)
    assert new_metadata.series == SERIES


def test_repr() -> None:
    metadata = Metadata(SERIES)
    metadata.set_field(fields.Description, "a test tag")
    metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)
    metadata.set_field_by_name("custom", "value")
    metadata.set_field_by_name("custom2", "value2")

    assert "SeriesSelector(source='test', name='test-tag-1')" in repr(metadata)
    assert "'description': 'a test tag'" in repr(metadata)
    assert "'interpolation type': <InterpolationType.LINEAR: 'LINEAR'>" in repr(
        metadata
    )
    assert "'custom': 'value'" in repr(metadata)
    assert "'custom2': 'value2'" in repr(metadata)


def test_typed_field() -> None:
    CustomField = MetadataField[str]("custom", default="", serialized_name="custom")
    Metadata.register_field(CustomField, after_field=fields.Description)

    metadata = Metadata(SERIES)
    metadata.set_field(CustomField, "test")
    assert list(metadata.iter_names())[1] == ("custom", "test")


def test_description() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Description) == ""

    metadata.set_field(fields.Description, "a test tag")
    assert metadata.get_field(fields.Description) == "a test tag"


def test_description_json() -> None:
    metadata = Metadata(SERIES, {fields.Description: "a test tag"})
    data = metadata.to_data()
    assert data["description"] == "a test tag"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Description) == "a test tag"


def test_unit() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Unit) == ""

    metadata.set_field(fields.Unit, "kg")
    assert metadata.get_field(fields.Unit) == "kg"


def test_unit_json() -> None:
    metadata = Metadata(SERIES, {fields.Unit: "kg"})
    data = metadata.to_data()
    assert data["unit"] == "kg"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Unit) == "kg"


def test_physical_limit_low() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.LimitLowPhysical) is None

    metadata.set_field(fields.LimitLowPhysical, 0)
    assert metadata.get_field(fields.LimitLowPhysical) == 0


def test_physical_limit_low_json() -> None:
    metadata = Metadata(SERIES, {fields.LimitLowPhysical: 0})
    data = metadata.to_data()
    assert data["limitLowPhysical"] == 0

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.LimitLowPhysical) == 0


def test_physical_limit_low_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("physical lower limit", "0")
    assert metadata.get_field(fields.LimitLowPhysical) == 0


def test_physical_limit_high() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.LimitHighPhysical) is None

    metadata.set_field(fields.LimitHighPhysical, 0)
    assert metadata.get_field(fields.LimitHighPhysical) == 0


def test_physical_limit_high_json() -> None:
    metadata = Metadata(SERIES, {fields.LimitHighPhysical: 0})
    data = metadata.to_data()
    assert data["limitHighPhysical"] == 0

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.LimitHighPhysical) == 0


def test_physical_limit_high_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("physical upper limit", "0")
    assert metadata.get_field(fields.LimitHighPhysical) == 0


def test_functional_limit_low() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.LimitLowFunctional) is None

    metadata.set_field(fields.LimitLowFunctional, 0)
    assert metadata.get_field(fields.LimitLowFunctional) == 0


def test_functional_limit_low_json() -> None:
    metadata = Metadata(SERIES, {fields.LimitLowFunctional: 0})
    data = metadata.to_data()
    assert data["limitLowFunctional"] == 0

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.LimitLowFunctional) == 0


def test_functional_limit_low_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("functional lower limit", "0")
    assert metadata.get_field(fields.LimitLowFunctional) == 0


def test_functional_limit_high() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.LimitHighFunctional) is None

    metadata.set_field(fields.LimitHighFunctional, 0)
    assert metadata.get_field(fields.LimitHighFunctional) == 0


def test_functional_limit_high_json() -> None:
    metadata = Metadata(SERIES, {fields.LimitHighFunctional: 0})
    data = metadata.to_data()
    assert data["limitHighFunctional"] == 0

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.LimitHighFunctional) == 0


def test_functional_limit_high_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("functional upper limit", "0")
    assert metadata.get_field(fields.LimitHighFunctional) == 0


def test_accuracy() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Accuracy) is None

    metadata.set_field(fields.Accuracy, 0.2)
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_accuracy_json() -> None:
    metadata = Metadata(SERIES, {fields.Accuracy: 0.2})
    data = metadata.to_data()
    assert data["accuracy"] == 0.2

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Accuracy) == 0.2


def test_accuracy_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("accuracy", "0.2")
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_accuracy_percentage() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Accuracy) is None

    metadata.set_field(fields.AccuracyPercentage, 2)
    metadata.set_field(fields.LimitLowPhysical, 0)
    metadata.set_field(fields.LimitHighPhysical, 10)
    assert metadata.get_field(fields.Accuracy) == 0.2
    assert metadata.get_field(fields.AccuracyPercentage) == 2


def test_accuracy_percentage_outside_range() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Accuracy) is None

    metadata.set_field(fields.AccuracyPercentage, 200)
    metadata.set_field(fields.LimitLowPhysical, 0)
    metadata.set_field(fields.LimitHighPhysical, 10)
    assert metadata.get_field(fields.Accuracy) == None
    assert metadata.get_field(fields.AccuracyPercentage) == 200


def test_accuracy_percentage_with_accuracy() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Accuracy) is None

    metadata.set_field(fields.Accuracy, 1)
    metadata.set_field(fields.AccuracyPercentage, 2)
    metadata.set_field(fields.LimitLowPhysical, 0)
    metadata.set_field(fields.LimitHighPhysical, 10)
    assert metadata.get_field(fields.Accuracy) == 1
    assert metadata.get_field(fields.AccuracyPercentage) == 2


def test_accuracy_percentage_no_physical_limits() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Accuracy) is None

    metadata.set_field(fields.AccuracyPercentage, 2)
    metadata.set_field(fields.LimitLowFunctional, 0)
    metadata.set_field(fields.LimitHighFunctional, 10)
    assert metadata.get_field(fields.Accuracy) == 0.2
    assert metadata.get_field(fields.AccuracyPercentage) == 2


def test_accuracy_percentage_no_limits() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Accuracy) is None

    metadata.set_field(fields.AccuracyPercentage, 2)
    assert metadata.get_field(fields.Accuracy) == None
    assert metadata.get_field(fields.AccuracyPercentage) == 2


def test_percentage_accuracy_json() -> None:
    metadata = Metadata(
        SERIES,
        {
            fields.AccuracyPercentage: 2,
            fields.LimitLowPhysical: 0,
            fields.LimitHighPhysical: 10,
        },
    )
    data = metadata.to_data()
    assert data["accuracyPercentage"] == 2

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Accuracy) == 0.2
    assert new_metadata.get_field(fields.AccuracyPercentage) == 2


def test_percentage_accuracy_json_outside_range() -> None:
    metadata = Metadata(
        SERIES,
        {
            fields.AccuracyPercentage: 200,
            fields.LimitLowPhysical: 0,
            fields.LimitHighPhysical: 10,
        },
    )
    data = metadata.to_data()
    assert data["accuracyPercentage"] == 200

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Accuracy) == None
    assert new_metadata.get_field(fields.AccuracyPercentage) == None


def test_percentage_accuracy_json_with_accuracy() -> None:
    metadata = Metadata(
        SERIES,
        {
            fields.Accuracy: 1,
            fields.AccuracyPercentage: 2,
            fields.LimitLowPhysical: 0,
            fields.LimitHighPhysical: 10,
        },
    )
    data = metadata.to_data()
    assert data["accuracyPercentage"] == 2
    assert data["accuracy"] == 1

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Accuracy) == 1
    assert metadata.get_field(fields.AccuracyPercentage) == 2


def test_percentage_accuracy_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("accuracyPercentage", "2")
    metadata.coerce_field("physical lower limit", "0")
    metadata.coerce_field("physical upper limit", "10")
    assert metadata.get_field(fields.Accuracy) == 0.2


def test_percentage_accuracy_coerce_outside_range() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("accuracyPercentage", "200")
    metadata.coerce_field("physical lower limit", "0")
    metadata.coerce_field("physical upper limit", "10")
    assert metadata.get_field(fields.Accuracy) == None
    assert metadata.get_field(fields.AccuracyPercentage) == None


def test_percentage_accuracy_coerce_with_accuracy() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("accuracy", "1")
    metadata.coerce_field("accuracyPercentage", "2")
    metadata.coerce_field("physical lower limit", "0")
    metadata.coerce_field("physical upper limit", "10")
    assert metadata.get_field(fields.Accuracy) == 1
    assert metadata.get_field(fields.AccuracyPercentage) == 2


def test_interpolation_type() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.InterpolationType) is None

    metadata.set_field(fields.InterpolationType, InterpolationType.LINEAR)
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR


def test_interpolation_type_json() -> None:
    metadata = Metadata(SERIES, {fields.InterpolationType: InterpolationType.LINEAR})
    data = metadata.to_data()
    assert data["interpolationType"] == "LINEAR"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.InterpolationType) == InterpolationType.LINEAR


def test_interpolation_type_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("interpolation type", "STEPPED")
    assert metadata.get_field(fields.InterpolationType) == InterpolationType.STEPPED


def test_data_type() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.DataType) is None

    metadata.set_field(fields.DataType, DataType.STRING)
    assert metadata.get_field(fields.DataType) == DataType.STRING


def test_data_type_json() -> None:
    metadata = Metadata(SERIES, {fields.DataType: DataType.STRING})
    data = metadata.to_data()
    assert data["dataType"] == "STRING"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.DataType) == DataType.STRING


def test_data_type_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("data type", "DICTIONARY")
    assert metadata.get_field(fields.DataType) == DataType.DICTIONARY


def test_dictionary_name() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.DictionaryName) is None

    metadata.set_field(fields.DictionaryName, "onoff")
    assert metadata.get_field(fields.DictionaryName) == "onoff"


def test_dictionary_name_json() -> None:
    metadata = Metadata(SERIES, {fields.DictionaryName: "onoff"})
    data = metadata.to_data()
    assert data["dictionaryName"] == "onoff"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.DictionaryName) == "onoff"


def test_dictionary() -> None:
    dictionary = Dictionary({0: "OFF", 1: "ON"})

    metadata = Metadata(SERIES)
    assert metadata.get_field(fields.Dictionary) is None

    metadata.set_field(fields.Dictionary, dictionary)
    assert metadata.get_field(fields.Dictionary) == dictionary


def test_dictionary_json() -> None:
    dictionary = Dictionary({0: "OFF", 1: "ON"})

    metadata = Metadata(SERIES, {fields.Dictionary: dictionary})
    data = metadata.to_data()
    assert data["dictionary"] == [(0, "OFF"), (1, "ON")]

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field(fields.Dictionary) == dictionary


def test_dictionary_coerce() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("dictionary", [(0, "OFF"), (1, "ON")])
    assert metadata.get_field(fields.Dictionary) == Dictionary({0: "OFF", 1: "ON"})


def test_unknown() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("process type", "BATCH")

    assert metadata.get_field_by_name("process type") == "BATCH"


def test_unknown_iter() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("process type", "BATCH")

    assert dict(metadata.iter_serialized())["process type"] == "BATCH"


def test_unknown_json() -> None:
    metadata = Metadata(SERIES)
    metadata.coerce_field("process type", "BATCH")

    data = metadata.to_data()
    assert data["process type"] == "BATCH"

    new_metadata = Metadata.from_data(data, SERIES)
    assert new_metadata.get_field_by_name("process type") == "BATCH"


def test_non_existent_unknown() -> None:
    metadata = Metadata(SERIES)
    assert metadata.get_field_by_name("process type") is None
