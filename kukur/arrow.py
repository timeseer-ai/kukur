"""Create and combine the Arrow tables that contain time series data in Kukur."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.types

from kukur import quality

DATETIME_VALUE_TYPE = pa.timestamp("us", "UTC")


def get_value_schema_type(data: pa.Table):
    """Get the type of the value column."""
    value_type = pa.float64()
    if len(data) > 0:
        if pa.types.is_string(data["value"][0].type):
            value_type = pa.string()
        elif pa.types.is_timestamp(data["value"][0].type):
            value_type = DATETIME_VALUE_TYPE
    return value_type


def assume_value_timezone(table: pa.Table, data_timezone: str | None) -> pa.Table:
    """Interpret naive timestamps outside of the ts column in the given timezone."""
    if data_timezone is None:
        return table
    for index, name in enumerate(table.schema.names):
        field_type = table.schema.field(name).type
        if (
            name != "ts"
            and pyarrow.types.is_timestamp(field_type)
            and field_type.tz is None
        ):
            table = table.set_column(
                index, name, pc.assume_timezone(table[index], data_timezone)
            )
    return table


def conform_to_schema(table: pa.Table) -> pa.Table:
    """Conform the table to the schema expected in Kukur.

    A quality column is kept when the data contains one, whether or not a
    quality mapping is configured for the source.
    """
    schema = pa.schema(
        [
            ("ts", pa.timestamp("us", "UTC")),
            ("value", get_value_schema_type(table)),
        ]
    )
    if "quality" in table.column_names:
        quality_array = quality.normalize_array(table["quality"])
        schema = schema.append(pa.field("quality", quality_array.type))
        table = table.set_column(2, "quality", quality_array)

    return table.cast(schema)


def empty_table(
    *, include_quality: bool, quality_type: pa.DataType | None = None
) -> pa.Table:
    """Create a new empty table, optionally including a quality column."""
    data: dict = {"ts": [], "value": []}
    fields = [
        ("ts", pa.timestamp("us", "UTC")),
        ("value", pa.float64()),
    ]
    if include_quality:
        data["quality"] = []
        fields.append(("quality", quality_type or pa.int16()))
    return pa.Table.from_pydict(data, schema=pa.schema(fields))


def concat_tables(tables: list[pa.Table]) -> pa.Table:
    """Safely concatenate multiple pyarrow.Table's.

    If any of the given tables contains strings, the result will contain a
    string value. Timestamps remain timestamps unless they are combined with
    numbers, in which case the result contains strings as well. If any of the
    given tables contains a floating point number, the value will be a double.
    """
    tables = [table for table in tables if len(table) > 0]
    if len(tables) == 0:
        return empty_table(include_quality=True)

    schema = pa.schema(
        [
            ("ts", pa.timestamp("us", "UTC")),
            ("value", _get_value_type(tables)),
        ]
    )

    if _has_quality_data_flag(tables):
        schema = schema.append(pa.field("quality", _get_quality_type(tables)))

    schema = schema.with_metadata(_get_metadata(tables))

    return pa.concat_tables([_cast(table, schema) for table in tables])


def _get_value_type(tables: list[pa.Table]) -> pa.DataType:
    value_types = [table.schema.field("value").type for table in tables]
    if any(pyarrow.types.is_string(value_type) for value_type in value_types):
        return pa.string()
    if any(pyarrow.types.is_timestamp(value_type) for value_type in value_types):
        if all(pyarrow.types.is_timestamp(value_type) for value_type in value_types):
            return DATETIME_VALUE_TYPE
        return pa.string()
    if all(pyarrow.types.is_integer(value_type) for value_type in value_types):
        return pa.int64()
    return pa.float64()


def _cast(table: pa.Table, schema: pa.Schema) -> pa.Table:
    value_index = table.schema.get_field_index("value")
    if pyarrow.types.is_string(
        schema.field("value").type
    ) and pyarrow.types.is_timestamp(table.schema.field(value_index).type):
        table = table.set_column(
            value_index, "value", _format_timestamps(table["value"])
        )
    return table.cast(schema)


def _format_timestamps(values: pa.Array) -> pa.Array:
    return pc.strftime(values.cast(DATETIME_VALUE_TYPE), "%Y-%m-%dT%H:%M:%S+00:00")


def _has_quality_data_flag(tables: list[pa.Table]) -> bool:
    quality_table = [table for table in tables if "quality" in table.column_names]
    return len(quality_table) > 0


def _get_metadata(tables: list[pa.Table]) -> dict[bytes, bytes]:
    """Return the schema metadata of the concatenated tables.

    Casting a table to a schema replaces its metadata by the metadata of that
    schema. The chunks of one query all describe the same data, so the metadata
    of the first chunk that has any describes all of them.
    """
    for table in tables:
        if table.schema.metadata:
            return quality.encode_metadata(table)
    return {}


def _get_quality_type(tables: list[pa.Table]) -> pa.DataType:
    """Return the type of the quality column.

    Quality values of a source are either strings or numerical status codes.
    Sources that provide quality flags instead of status codes return them
    simplified, which keeps them narrower.
    """
    quality_types = [
        table.schema.field("quality").type
        for table in tables
        if "quality" in table.column_names
    ]
    if any(pyarrow.types.is_string(quality_type) for quality_type in quality_types):
        return pa.string()
    if quality_types and all(
        quality_type == pa.int8() for quality_type in quality_types
    ):
        return pa.int8()
    return pa.int16()
