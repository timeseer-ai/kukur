"""Kukur connection to TDengine.

This uses the Websocket library.
"""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from kukur.exceptions import MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig

HAS_TAOSWS = False

try:
    import taosws

    HAS_TAOSWS = True
except ImportError:
    pass


class TDengineSource(BaseSQLSource):
    """Kukur source for TDengine."""

    def __init__(
        self,
        data: dict,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        if not HAS_TAOSWS:
            raise MissingModuleException("taos-ws-py", "tdengine")
        config = SQLConfig.from_dict(data)
        self._connection_options: dict = data.get("connection", {})
        super().__init__(config, metadata_value_mapper, quality_mapper)

    def connect(self):
        """Create a connection to TDengine."""
        return taosws.connect(
            self._config.connection_string, **self._connection_options
        )


def from_config(
    data: dict,
    metadata_value_mapper: MetadataValueMapper,
    quality_mapper: QualityMapper,
) -> TDengineSource:
    """Create a new TDengine source from a configuration dictionay."""
    if not HAS_TAOSWS:
        raise MissingModuleException("taos-ws-py", "tdengine")
    return TDengineSource(data, metadata_value_mapper, quality_mapper)
