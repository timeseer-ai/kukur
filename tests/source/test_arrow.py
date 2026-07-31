"""Test the common logic for the pyarrow file formats."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa

from kukur.source.arrow import conform_to_schema, empty_table


def _table(quality=None) -> pa.Table:
    data = {
        "ts": pa.array([None], pa.timestamp("us", "UTC")),
        "value": pa.array([1.0]),
    }
    if quality is not None:
        data["quality"] = quality
    return pa.table(data)


def test_conform_without_quality() -> None:
    assert conform_to_schema(_table()).column_names == ["ts", "value"]


def test_conform_keeps_quality_of_the_data() -> None:
    """A source can provide quality without a configured quality mapping."""
    table = conform_to_schema(_table(pa.array([192], pa.int64())))
    assert table.column_names == ["ts", "value", "quality"]
    assert table.schema.field("quality").type == pa.int16()


def test_conform_keeps_string_quality() -> None:
    table = conform_to_schema(_table(pa.array(["GoodQuality"])))
    assert table.schema.field("quality").type == pa.string()


def test_conform_keeps_simplified_quality() -> None:
    table = conform_to_schema(_table(pa.array([0], pa.int8())))
    assert table.schema.field("quality").type == pa.int8()


def test_empty_table() -> None:
    assert empty_table(include_quality=False).column_names == ["ts", "value"]
    table = empty_table(include_quality=True)
    assert table.schema.field("quality").type == pa.int16()
    table = empty_table(include_quality=True, quality_type=pa.string())
    assert table.schema.field("quality").type == pa.string()
