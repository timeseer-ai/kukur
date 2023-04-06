"""Kukur source for Arrow IPC streams."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Any, Dict

import pyarrow as pa
from pyarrow.ipc import open_stream

from kukur.exceptions import InvalidSourceException
from kukur.loader import from_config as loader_from_config
from kukur.source.arrow import BaseArrowSource, BaseArrowSourceOptions
from kukur.source.quality import QualityMapper


class ArrowIPCStreamSource(BaseArrowSource):
    """An Arrow IPC stream data source."""

    def read_file(self, file_like) -> pa.Table:
        """Read an Arrow table from the stream."""
        return open_stream(file_like).read_all()

    def get_file_extension(self) -> str:
        """Return the default .arrows stream file extension."""
        return "arrows"


def from_config(
    config: Dict[str, Any], quality_mapper: QualityMapper
) -> ArrowIPCStreamSource:
    """Create a new IPC Stream data source from the given configuration dictionary."""
    loader = loader_from_config(config, files_as_path=True)
    if "path" not in config:
        raise InvalidSourceException('arrows sources require a "path" entry')
    options = BaseArrowSourceOptions.from_data(config)
    return ArrowIPCStreamSource(
        options,
        loader,
        quality_mapper,
        sort_by_timestamp=config.get("sort_by_timestamp", False),
    )
