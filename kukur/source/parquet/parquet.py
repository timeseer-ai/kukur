"""parquet contains the Parquet data source for Timeseer.

Three formats are supported:
- row based, with many series in one file containing one row per data point
- directory based, with one file per series
- pivot, with many series as columns in one file

Parquet DataSets are not yet supported.
"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict

import pyarrow as pa
from pyarrow import parquet

from kukur.exceptions import InvalidSourceException
from kukur.loader import from_config as loader_from_config
from kukur.source.arrow import BaseArrowSource, BaseArrowSourceOptions
from kukur.source.quality import QualityMapper


def from_config(config: Dict[str, Any], quality_mapper: QualityMapper):
    """Create a new Parquet data source from the given configuration dictionary."""
    loader = loader_from_config(config, files_as_path=True)
    if "path" not in config:
        raise InvalidSourceException('Parquet sources require a "path" entry')
    options = BaseArrowSourceOptions.from_data(config)
    return ParquetSource(options, loader, quality_mapper)


class ParquetSource(BaseArrowSource):
    """A Parquet data source."""

    def read_file(self, file_like) -> pa.Table:
        """Read the file_like object as Parquet."""
        return parquet.read_table(file_like)

    def get_file_extension(self) -> str:
        """Return the default parquet file extension."""
        return "parquet"
