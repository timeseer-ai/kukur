"""Test the common logic for the pyarrow file formats."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import UTC, datetime

import pyarrow as pa

from kukur.arrow import concat_tables, conform_to_schema, empty_table

TIMESTAMP = datetime(2021, 1, 2, 12, 34, 56, tzinfo=UTC)


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


def _values(values: pa.Array) -> pa.Table:
    return pa.table(
        {
            "ts": pa.array([TIMESTAMP] * len(values), pa.timestamp("us", "UTC")),
            "value": values,
        }
    )


def test_conform_keeps_timestamp_values() -> None:
    table = conform_to_schema(_values(pa.array([TIMESTAMP], pa.timestamp("s"))))
    assert table.schema.field("value").type == pa.timestamp("us", "UTC")
    assert table["value"].to_pylist() == [TIMESTAMP]


def test_concat_keeps_timestamp_values() -> None:
    table = concat_tables(
        [
            _values(pa.array([TIMESTAMP], pa.timestamp("us", "UTC"))),
            _values(pa.array([TIMESTAMP], pa.timestamp("ms", "Europe/Brussels"))),
        ]
    )
    assert table.schema.field("value").type == pa.timestamp("us", "UTC")
    assert table["value"].to_pylist() == [TIMESTAMP, TIMESTAMP]


def test_concat_timestamp_and_number_values_become_strings() -> None:
    table = concat_tables(
        [
            _values(pa.array([TIMESTAMP], pa.timestamp("us", "UTC"))),
            _values(pa.array([42.5])),
        ]
    )
    assert table["value"].to_pylist() == ["2021-01-02T12:34:56.000000+00:00", "42.5"]


def test_concat_timestamp_and_string_values_become_strings() -> None:
    table = concat_tables(
        [
            _values(pa.array(["on"])),
            _values(pa.array([TIMESTAMP], pa.timestamp("us", "UTC"))),
        ]
    )
    assert table["value"].to_pylist() == ["on", "2021-01-02T12:34:56.000000+00:00"]
