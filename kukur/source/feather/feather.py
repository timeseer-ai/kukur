"""
feather contains the Feather data source for Timeseer.

Three formats are supported:
- row based, with many series in one file containing one row per data point
- directory based, with one file per series
- pivot, with many series as columns in one file
"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Dict

import pyarrow as pa
import pyarrow.feather as feather

from kukur.exceptions import InvalidSourceException
from kukur.loader import from_config as loader_from_config
from kukur.source.arrow import BaseArrowSource


def from_config(config: Dict[str, str]):
    """Create a new Feather data source from the given configuration dictionary."""
    data_format = config.get("format", "row")
    if "path" not in config:
        raise InvalidSourceException('Feather sources require a "path" entry')
    loader = loader_from_config(config, files_as_path=True)
    return FeatherSource(data_format, loader)


class FeatherSource(BaseArrowSource):
    """A Feather data source."""

    def read_file(self, file_like) -> pa.Table:
        """Read the file_like object as Feather."""
        return feather.read_table(file_like)

    def get_file_extension(self) -> str:
        """Return the default feather file extension."""
        return "feather"
