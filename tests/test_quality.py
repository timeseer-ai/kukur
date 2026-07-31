"""Test the quality mapping."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pytest

from kukur import quality
from kukur.exceptions import InvalidDataError
from kukur.quality import Quality, QualityMapper


def _table(quality_values, quality_mapping=None) -> pa.Table:
    table = pa.table(
        {
            "ts": pa.array([None] * len(quality_values), pa.timestamp("us", "UTC")),
            "value": pa.array([1.0] * len(quality_values)),
            "quality": quality_values,
        }
    )
    if quality_mapping is None:
        return table
    return quality.set_mapping(table, quality_mapping)


def test_good_is_zero() -> None:
    """Kukur follows the convention of Unix exit codes."""
    assert Quality.GOOD.value == 0
    assert Quality.BAD.value == 1


def test_empty_mapper() -> None:
    mapper = QualityMapper()
    assert not mapper.is_present()
    assert mapper.good_values() == []


def test_from_config() -> None:
    mapper = QualityMapper.from_config({"GOOD": [192, 194]})
    assert mapper.is_present()
    assert mapper.good_values() == [192, 194]


def test_from_config_ranges() -> None:
    mapper = QualityMapper.from_config({"GOOD": [[192], [194, 198]]})
    assert mapper.good_values() == [192, 194, 195, 196, 197, 198]


def test_from_config_strings() -> None:
    mapper = QualityMapper.from_config({"GOOD": ["GoodQuality", "Decent"]})
    assert mapper.good_values() == ["GoodQuality", "Decent"]


def test_from_config_ignores_other_keys() -> None:
    mapper = QualityMapper.from_config({"BAD": [3], "GOOD": [192]})
    assert mapper.good_values() == [192]


def test_from_config_display() -> None:
    """Quality values are keyed by their string representation, also when they are digits."""
    mapper = QualityMapper.from_config(
        {"GOOD": [192], "display": {"192": "good", "194": "very good"}}
    )
    assert mapper.good_values() == [192]
    assert mapper.display_values() == {"192": "good", "194": "very good"}


def test_from_config_display_only() -> None:
    """A source can describe its quality values without declaring which are good."""
    mapper = QualityMapper.from_config({"display": {"192": "good"}})
    assert mapper.is_present()
    assert mapper.good_values() == []


def test_metadata_round_trip() -> None:
    """Ranges stay ranges, to keep the embedded mapping compact."""
    mapper = QualityMapper.from_config({"GOOD": [[192], [194, 198]]})
    assert mapper.to_metadata() == {"GOOD": [192, [194, 198]]}
    assert QualityMapper.from_metadata(mapper.to_metadata()).good_values() == (
        mapper.good_values()
    )


def test_metadata_round_trip_strings() -> None:
    mapper = QualityMapper.from_config({"GOOD": ["GoodQuality"]})
    assert mapper.to_metadata() == {"GOOD": ["GoodQuality"]}
    assert QualityMapper.from_metadata(mapper.to_metadata()).good_values() == [
        "GoodQuality"
    ]


def test_metadata_round_trip_display() -> None:
    mapper = QualityMapper.from_config({"GOOD": [192], "display": {"192": "good"}})
    assert mapper.to_metadata() == {"GOOD": [192], "display": {"192": "good"}}
    assert QualityMapper.from_metadata(mapper.to_metadata()).display_values() == (
        mapper.display_values()
    )


def test_metadata_omits_empty_display() -> None:
    mapper = QualityMapper.from_config({"GOOD": [192]})
    assert mapper.to_metadata() == {"GOOD": [192]}


def test_normalize_numbers() -> None:
    assert quality.normalize_array(pa.array([192, 3], pa.int64())).type == pa.int16()


def test_normalize_keeps_strings() -> None:
    assert quality.normalize_array(pa.array(["GoodQuality"])).type == pa.string()


def test_normalize_booleans() -> None:
    """A source can provide quality as a flag."""
    quality_array = quality.normalize_array(pa.array([True, False]))
    assert quality_array.type == pa.int16()
    assert quality_array.to_pylist() == [1, 0]


def test_normalize_rejects_other_types() -> None:
    with pytest.raises(InvalidDataError):
        quality.normalize_array(pa.array([[1], [2]], pa.list_(pa.int8())))


def test_get_mapping_defaults() -> None:
    """Tables without an embedded mapping use the mapping of Kukur itself."""
    table = _table(pa.array([1, 0], pa.int16()))
    assert not quality.has_mapping(table)
    assert quality.get_mapping(table) == quality.DEFAULT_MAPPING


def test_set_mapping_keeps_other_metadata() -> None:
    table = _table(pa.array([1], pa.int16())).replace_schema_metadata(
        {"kukur.statistics": "{}"}
    )
    table = quality.set_mapping(table, {"GOOD": [192]})
    assert table.schema.metadata[b"kukur.statistics"] == b"{}"
    assert table.schema.metadata[quality.METADATA_KEY.encode()] == b'{"GOOD": [192]}'


def test_simplify_numbers() -> None:
    table = _table(pa.array([192, 3, 197], pa.int16()), {"GOOD": [192, [194, 198]]})
    simplified = quality.simplify(table)
    assert simplified.schema.field("quality").type == pa.int8()
    assert simplified["quality"].to_pylist() == [0, 1, 0]


def test_simplify_strings() -> None:
    table = _table(
        pa.array(["GoodQuality", "Bad", "Decent"]),
        {"GOOD": ["GoodQuality", "Decent"]},
    )
    assert quality.simplify(table)["quality"].to_pylist() == [0, 1, 0]


def test_simplify_default_mapping() -> None:
    """A quality column that declares no mapping already uses 0 for good."""
    table = _table(pa.array([0, 1], pa.int16()))
    assert quality.simplify(table)["quality"].to_pylist() == [0, 1]


def test_simplify_nulls_are_bad() -> None:
    table = _table(pa.array([192, None], pa.int16()), {"GOOD": [192]})
    assert quality.simplify(table)["quality"].to_pylist() == [0, 1]


def test_simplify_numerical_mapping_on_string_column() -> None:
    """The quality values of a source are not necessarily of the mapped type."""
    table = _table(pa.array(["192", "3"]), {"GOOD": [192]})
    assert quality.simplify(table)["quality"].to_pylist() == [0, 1]


def test_simplify_mismatched_mapping() -> None:
    table = _table(pa.array([192, 3], pa.int16()), {"GOOD": ["GoodQuality"]})
    with pytest.raises(InvalidDataError):
        quality.simplify(table)


def test_simplify_embeds_simplified_mapping() -> None:
    table = _table(pa.array([192], pa.int16()), {"GOOD": [192]})
    assert quality.get_mapping(quality.simplify(table)) == {"GOOD": [0]}


def test_simplify_is_idempotent() -> None:
    table = _table(pa.array([192, 3], pa.int16()), {"GOOD": [192]})
    once = quality.simplify(table)
    assert quality.simplify(once)["quality"].to_pylist() == (
        once["quality"].to_pylist()
    )


def test_simplify_without_quality_column() -> None:
    table = pa.table({"ts": [], "value": []})
    assert quality.simplify(table) is table


def test_simplify_empty_table() -> None:
    table = _table(pa.array([], pa.int16()), {"GOOD": [192]})
    simplified = quality.simplify(table)
    assert len(simplified) == 0
    assert simplified.schema.field("quality").type == pa.int8()


def test_simplify_keeps_column_order() -> None:
    table = pa.table(
        {
            "ts": pa.array([None], pa.timestamp("us", "UTC")),
            "quality": pa.array([192], pa.int16()),
            "value": pa.array([1.0]),
        }
    )
    assert quality.simplify(table).column_names == ["ts", "quality", "value"]


def test_normalize_keeps_simplified_quality() -> None:
    """An already simplified quality column stays simplified."""
    assert quality.normalize_array(pa.array([0, 1], pa.int8())).type == pa.int8()


def test_describe_numbers() -> None:
    table = _table(
        pa.array([192, 194], pa.int16()),
        {"GOOD": [192, 194], "display": {"192": "good", "194": "very good"}},
    )
    described = quality.describe(table)
    assert described.schema.field("quality").type == pa.string()
    assert described["quality"].to_pylist() == ["good", "very good"]


def test_describe_strings() -> None:
    table = _table(
        pa.array(["GoodQuality", "BadQuality"]),
        {
            "GOOD": ["GoodQuality"],
            "display": {"GoodQuality": "good", "BadQuality": "bad"},
        },
    )
    assert quality.describe(table)["quality"].to_pylist() == ["good", "bad"]


def test_describe_falls_back_to_the_quality_value() -> None:
    """Not every quality value of a source necessarily has a display value."""
    table = _table(
        pa.array([192, 3], pa.int16()), {"GOOD": [192], "display": {"192": "good"}}
    )
    assert quality.describe(table)["quality"].to_pylist() == ["good", "3"]


def test_describe_keeps_nulls() -> None:
    table = _table(
        pa.array([192, None], pa.int16()), {"GOOD": [192], "display": {"192": "good"}}
    )
    assert quality.describe(table)["quality"].to_pylist() == ["good", None]


def test_describe_without_display_mapping() -> None:
    """A quality value without display value is rendered as the value itself."""
    table = _table(pa.array([192, 3], pa.int16()), {"GOOD": [192]})
    assert quality.describe(table)["quality"].to_pylist() == ["192", "3"]


def test_describe_numerical_mapping_on_string_column() -> None:
    """The quality values of a source are not necessarily of the mapped type."""
    table = _table(pa.array(["192", "3"]), {"GOOD": [192], "display": {"192": "good"}})
    assert quality.describe(table)["quality"].to_pylist() == ["good", "3"]


def test_describe_translates_the_mapping() -> None:
    """Ranges are expanded, since a range of display values is not a range."""
    table = _table(
        pa.array([192, 195], pa.int16()),
        {"GOOD": [192, [194, 195]], "display": {"192": "good", "194": "very good"}},
    )
    assert quality.get_mapping(quality.describe(table)) == {
        "GOOD": ["good", "very good", "195"],
        "display": {"192": "good", "194": "very good"},
    }


def test_describe_can_still_be_simplified() -> None:
    table = _table(
        pa.array([192, 3, 195], pa.int16()),
        {"GOOD": [192, [194, 195]], "display": {"192": "good", "194": "very good"}},
    )
    assert quality.simplify(quality.describe(table))["quality"].to_pylist() == (
        quality.simplify(table)["quality"].to_pylist()
    )


def test_describe_without_quality_column() -> None:
    table = pa.table({"ts": [], "value": []})
    assert quality.describe(table) is table


def test_describe_empty_table() -> None:
    table = _table(
        pa.array([], pa.int16()), {"GOOD": [192], "display": {"192": "good"}}
    )
    described = quality.describe(table)
    assert len(described) == 0
    assert described.schema.field("quality").type == pa.string()
