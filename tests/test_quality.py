"""Test the quality mapping."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pytest

from kukur.exceptions import InvalidDataError
from kukur.quality import (
    DEFAULT_QUALITY_MAPPING,
    QUALITY_METADATA_KEY,
    Quality,
    QualityMapper,
    get_quality_mapping,
    has_quality_mapping,
    normalize_quality_array,
    set_quality_mapping,
    simplify_quality,
)


def _table(quality, quality_mapping=None) -> pa.Table:
    table = pa.table(
        {
            "ts": pa.array([None] * len(quality), pa.timestamp("us", "UTC")),
            "value": pa.array([1.0] * len(quality)),
            "quality": quality,
        }
    )
    if quality_mapping is None:
        return table
    return set_quality_mapping(table, quality_mapping)


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


def test_normalize_numbers() -> None:
    assert normalize_quality_array(pa.array([192, 3], pa.int64())).type == pa.int16()


def test_normalize_keeps_strings() -> None:
    assert normalize_quality_array(pa.array(["GoodQuality"])).type == pa.string()


def test_normalize_booleans() -> None:
    """A source can provide quality as a flag."""
    quality = normalize_quality_array(pa.array([True, False]))
    assert quality.type == pa.int16()
    assert quality.to_pylist() == [1, 0]


def test_normalize_rejects_other_types() -> None:
    with pytest.raises(InvalidDataError):
        normalize_quality_array(pa.array([[1], [2]], pa.list_(pa.int8())))


def test_get_quality_mapping_defaults() -> None:
    """Tables without an embedded mapping use the mapping of Kukur itself."""
    table = _table(pa.array([1, 0], pa.int16()))
    assert not has_quality_mapping(table)
    assert get_quality_mapping(table) == DEFAULT_QUALITY_MAPPING


def test_set_quality_mapping_keeps_other_metadata() -> None:
    table = _table(pa.array([1], pa.int16())).replace_schema_metadata(
        {"kukur.statistics": "{}"}
    )
    table = set_quality_mapping(table, {"GOOD": [192]})
    assert table.schema.metadata[b"kukur.statistics"] == b"{}"
    assert table.schema.metadata[QUALITY_METADATA_KEY.encode()] == b'{"GOOD": [192]}'


def test_simplify_numbers() -> None:
    table = _table(pa.array([192, 3, 197], pa.int16()), {"GOOD": [192, [194, 198]]})
    simplified = simplify_quality(table)
    assert simplified.schema.field("quality").type == pa.int8()
    assert simplified["quality"].to_pylist() == [0, 1, 0]


def test_simplify_strings() -> None:
    table = _table(
        pa.array(["GoodQuality", "Bad", "Decent"]),
        {"GOOD": ["GoodQuality", "Decent"]},
    )
    assert simplify_quality(table)["quality"].to_pylist() == [0, 1, 0]


def test_simplify_default_mapping() -> None:
    """A quality column that declares no mapping already uses 0 for good."""
    table = _table(pa.array([0, 1], pa.int16()))
    assert simplify_quality(table)["quality"].to_pylist() == [0, 1]


def test_simplify_nulls_are_bad() -> None:
    table = _table(pa.array([192, None], pa.int16()), {"GOOD": [192]})
    assert simplify_quality(table)["quality"].to_pylist() == [0, 1]


def test_simplify_numerical_mapping_on_string_column() -> None:
    """The quality values of a source are not necessarily of the mapped type."""
    table = _table(pa.array(["192", "3"]), {"GOOD": [192]})
    assert simplify_quality(table)["quality"].to_pylist() == [0, 1]


def test_simplify_mismatched_mapping() -> None:
    table = _table(pa.array([192, 3], pa.int16()), {"GOOD": ["GoodQuality"]})
    with pytest.raises(InvalidDataError):
        simplify_quality(table)


def test_simplify_embeds_simplified_mapping() -> None:
    table = _table(pa.array([192], pa.int16()), {"GOOD": [192]})
    assert get_quality_mapping(simplify_quality(table)) == {"GOOD": [0]}


def test_simplify_is_idempotent() -> None:
    table = _table(pa.array([192, 3], pa.int16()), {"GOOD": [192]})
    once = simplify_quality(table)
    assert simplify_quality(once)["quality"].to_pylist() == (
        once["quality"].to_pylist()
    )


def test_simplify_without_quality_column() -> None:
    table = pa.table({"ts": [], "value": []})
    assert simplify_quality(table) is table


def test_simplify_empty_table() -> None:
    table = _table(pa.array([], pa.int16()), {"GOOD": [192]})
    simplified = simplify_quality(table)
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
    assert simplify_quality(table).column_names == ["ts", "quality", "value"]


def test_normalize_keeps_simplified_quality() -> None:
    """An already simplified quality column stays simplified."""
    assert normalize_quality_array(pa.array([0, 1], pa.int8())).type == pa.int8()
