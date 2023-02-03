"""Quality mapper for Kukur data sources."""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from enum import Enum
from typing import Any, Dict, List, Union

import pyarrow as pa
import pyarrow.compute as pc


class Quality(Enum):
    """Enumeration of possible Kukur quality flags."""

    BAD = 0
    GOOD = 1


class QualityMapper:
    """QualityMapper maps quality values used in a source to the quality values of Kukur.

    GOOD = 1 and BAD = 0.
    """

    __good_mapping: List[Union[str, int]]

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> "QualityMapper":
        """Create a new mapper from a dictionary that maps Kukur quality values to external quality values."""
        mapper = cls()
        for quality, quality_values in config.items():
            if quality == Quality.GOOD.name:
                for value_list in quality_values:
                    if isinstance(value_list, list):
                        if len(value_list) > 1:
                            mapper.add_mapping_range(
                                range(int(value_list[0]), int(value_list[1]) + 1)
                            )
                        mapper.add_mapping(value_list[0])
                        continue
                    mapper.add_mapping(value_list)
        return mapper

    def __init__(self):
        self.__good_mapping = []

    def add_mapping(self, quality_value: Union[str, int]):
        """Add a mapping."""
        self.__good_mapping.append(quality_value)

    def add_mapping_range(self, quality_values: range):
        """Add a mapping range."""
        self.__good_mapping.extend(quality_values)

    def from_source(self, source_quality_value: Union[str, int]) -> int:
        """Map a quality value of a source to the Kukur quality value."""
        if source_quality_value in self.__good_mapping:
            return Quality.GOOD.value
        return Quality.BAD.value

    def map_array(self, array: pa.Array) -> pa.Array:
        """Use the given quality mapping to convert the values in the array to GOOD or BAD."""
        good = pa.scalar(Quality.GOOD.value, pa.int8())
        bad = pa.scalar(Quality.BAD.value, pa.int8())
        # pylint: disable=no-member
        return pc.if_else(pc.is_in(array, pa.array(self.__good_mapping)), good, bad)

    def is_present(self) -> bool:
        """Check if there is a quality mapping present."""
        return len(self.__good_mapping) > 0
