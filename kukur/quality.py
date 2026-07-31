"""Quality mapper for Kukur data sources."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json
from enum import Enum
from typing import Any

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.types

from kukur.exceptions import InvalidDataError


class Quality(Enum):
    """Enumeration of the values of a simplified Kukur quality column.

    Following the convention of Unix exit codes, GOOD is 0 and anything that is
    not good is non-zero.

    Note that these values do not occur in the quality column returned by a
    source: that column contains the quality codes of the source itself. Use
    :func:`simplify_quality` to convert such a column to these values.
    """

    GOOD = 0
    BAD = 1


# The key of the quality mapping in the metadata of an Arrow schema.
QUALITY_METADATA_KEY = "kukur.quality"

# The quality mapping of a source that provides quality flags without
# configuring a quality mapping. These sources return 1 for good data points.
DEFAULT_QUALITY_MAPPING: dict[str, list] = {"GOOD": [1]}

# The quality mapping of a quality column that has been simplified by
# simplify_quality.
SIMPLIFIED_QUALITY_MAPPING: dict[str, list] = {"GOOD": [Quality.GOOD.value]}


class QualityMapper:
    """QualityMapper describes which quality values of a source are good.

    The mapping is not applied to the data of a source. Sources return their own
    quality values, the mapping travels along in the metadata of the Arrow
    schema. Use :func:`simplify_quality` to reduce a quality column to
    :class:`Quality` values.
    """

    __good_mapping: list[Any]

    @classmethod
    def from_config(cls, config: dict[str, Any]) -> "QualityMapper":
        """Create a new mapper from a dictionary that maps Kukur quality values to external quality values."""
        mapper = cls()
        for quality, quality_values in config.items():
            if quality != Quality.GOOD.name:
                continue
            for quality_value in quality_values:
                if isinstance(quality_value, list):
                    if len(quality_value) > 1:
                        mapper.add_mapping_range(
                            range(int(quality_value[0]), int(quality_value[1]) + 1)
                        )
                    else:
                        mapper.add_mapping(quality_value[0])
                    continue
                mapper.add_mapping(quality_value)
        return mapper

    @classmethod
    def from_metadata(cls, metadata: dict[str, Any]) -> "QualityMapper":
        """Create a new mapper from a quality mapping embedded in Arrow schema metadata."""
        return cls.from_config(metadata)

    def __init__(self):
        self.__good_mapping = []

    def add_mapping(self, quality_value: str | int):
        """Add a mapping."""
        self.__good_mapping.append(quality_value)

    def add_mapping_range(self, quality_values: range):
        """Add a mapping range.

        The range is stored as its inclusive bounds, not as the values it
        contains, to keep the resulting mapping compact.
        """
        self.__good_mapping.append([quality_values.start, quality_values.stop - 1])

    def to_metadata(self) -> dict[str, list]:
        """Return the mapping in the form that is embedded in Arrow schema metadata."""
        return {Quality.GOOD.name: list(self.__good_mapping)}

    def good_values(self) -> list[str | int]:
        """Return all quality values of the source that are considered good.

        Ranges in the mapping are expanded to the values they contain.
        """
        values: list[str | int] = []
        for entry in self.__good_mapping:
            if isinstance(entry, list):
                values.extend(range(int(entry[0]), int(entry[1]) + 1))
                continue
            values.append(entry)
        return values

    def is_present(self) -> bool:
        """Check if there is a quality mapping present."""
        return len(self.__good_mapping) > 0


def normalize_quality_array(array: pa.Array) -> pa.Array:
    """Normalize the quality column of a source to a type supported by Kukur.

    String quality columns are returned as ``string``, all others as ``int16``.
    The quality values themselves are not changed.
    """
    if pyarrow.types.is_dictionary(array.type):
        array = pc.cast(array, array.type.value_type)
    if pyarrow.types.is_string(array.type):
        return array
    if pyarrow.types.is_large_string(array.type):
        return pc.cast(array, pa.string())
    try:
        return pc.cast(array, pa.int16())
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as err:
        raise InvalidDataError(
            f'quality column of type "{array.type}" is not a status code'
        ) from err


def get_quality_mapping(table: pa.Table) -> dict[str, Any]:
    """Return the quality mapping embedded in the metadata of the table.

    Tables that do not contain a quality mapping are assumed to use the default
    mapping, where 1 means good.
    """
    metadata = table.schema.metadata
    if metadata is None:
        return DEFAULT_QUALITY_MAPPING
    quality_mapping = metadata.get(QUALITY_METADATA_KEY.encode())
    if quality_mapping is None:
        return DEFAULT_QUALITY_MAPPING
    return json.loads(quality_mapping)


def has_quality_mapping(table: pa.Table) -> bool:
    """Check if the table metadata contains a quality mapping."""
    metadata = table.schema.metadata
    if metadata is None:
        return False
    return QUALITY_METADATA_KEY.encode() in metadata


def set_quality_mapping(table: pa.Table, quality_mapping: dict[str, Any]) -> pa.Table:
    """Embed the given quality mapping in the metadata of the table."""
    metadata = encode_metadata(table)
    metadata[QUALITY_METADATA_KEY.encode()] = json.dumps(quality_mapping).encode()
    return table.replace_schema_metadata(metadata)


def encode_metadata(table: pa.Table) -> dict[bytes, bytes]:
    """Return the metadata of the schema of the table, keyed by bytes.

    Arrow stores schema metadata as bytes. Adding an entry keyed by a string to
    the metadata of a table that already contains that entry replaces nothing.
    """
    return {
        key if isinstance(key, bytes) else key.encode(): value
        for key, value in (table.schema.metadata or {}).items()
    }


def simplify_quality(table: pa.Table) -> pa.Table:
    """Simplify the quality column of a table to good and bad.

    The quality column of a table returned by Kukur contains the quality values
    of the source itself. This uses the quality mapping in the metadata of the
    table to replace them by ``Quality.GOOD`` (0) for good data points and
    ``Quality.BAD`` (1) for all others. Data points without quality value are
    considered bad.

    Tables that do not contain a quality column are returned unchanged.
    """
    if "quality" not in table.column_names:
        return table

    mapper = QualityMapper.from_metadata(get_quality_mapping(table))
    index = table.column_names.index("quality")
    quality = _simplify_quality_array(table.column(index), mapper)
    table = table.set_column(index, pa.field("quality", pa.int8()), quality)
    return set_quality_mapping(table, SIMPLIFIED_QUALITY_MAPPING)


def _simplify_quality_array(array: pa.Array, mapper: QualityMapper) -> pa.Array:
    good = pa.scalar(Quality.GOOD.value, pa.int8())
    bad = pa.scalar(Quality.BAD.value, pa.int8())
    value_set = _to_value_set(mapper.good_values(), array.type)
    # pylint: disable=no-member
    return pc.if_else(pc.is_in(array, value_set), good, bad)


def _to_value_set(values: list[str | int], quality_type: pa.DataType) -> pa.Array:
    """Convert the good quality values to an array of the type of the quality column.

    The type of the values in the quality mapping does not necessarily match the
    type of the quality column. A source that provides numerical quality codes
    as strings is configured with a numerical quality mapping.
    """
    try:
        value_set = pa.array(values)
    except (pa.ArrowInvalid, pa.ArrowTypeError) as err:
        raise InvalidDataError(f"invalid quality mapping: {values}") from err
    if value_set.type == quality_type:
        return value_set
    try:
        return pc.cast(value_set, quality_type)
    except (pa.ArrowInvalid, pa.ArrowNotImplementedError) as err:
        raise InvalidDataError(
            f'quality mapping does not match quality column of type "{quality_type}"'
        ) from err
