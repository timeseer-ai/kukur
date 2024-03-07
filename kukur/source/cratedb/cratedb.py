"""Connections to CrateDB data sources from Kukur.

This requires the crate package
"""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

try:
    from crate import client

    HAS_CRATE = True
except ImportError:
    HAS_CRATE = False

from kukur.exceptions import InvalidSourceException, MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
):
    """Create a new CrateDB data source from a configuration dict.

    Raises ADODBNotInstalledError when the adodbapi module is not available.
    """
    if not HAS_CRATE:
        raise MissingModuleException("crate", "cratedb")

    config = SQLConfig.from_dict(data)

    return CrateDBSource(config, metadata_value_mapper, quality_mapper)


class CrateDBSource(BaseSQLSource):
    """A CrateDB data source."""

    def __init__(
        self,
        config: SQLConfig,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        super().__init__(config, metadata_value_mapper, quality_mapper)
        if not HAS_CRATE:
            raise MissingModuleException("crate", "adodb")

    def connect(self):
        """Create a cratedb connection."""
        if self._config.connection_string is None:
            raise InvalidSourceException(
                "'connection_string' is required for source with type 'cratedb'"
            )
        return client.connect(self._config.connection_string)
