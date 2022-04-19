"""Connections to ADODB data sources from Kukur.

This requires an installation of pywin32 (LGPL).
"""

# SPDX-FileCopyrightText: 2021 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

try:
    import adodbapi

    HAS_ADODB = True
except ImportError:
    HAS_ADODB = False

from kukur.exceptions import MissingModuleException
from kukur.source.metadata import MetadataValueMapper
from kukur.source.quality import QualityMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


def from_config(
    data, metadata_value_mapper: MetadataValueMapper, quality_mapper: QualityMapper
):
    """Create a new ADODB data source from a configuration dict.

    Raises ADODBNotInstalledError when the adodbapi module is not available."""
    if not HAS_ADODB:
        raise MissingModuleException("pywin32", "adodb")

    config = SQLConfig.from_dict(data)

    return ADODBSource(config, metadata_value_mapper, quality_mapper)


class ADODBSource(BaseSQLSource):
    """An ADODB data source."""

    def __init__(
        self,
        config: SQLConfig,
        metadata_value_mapper: MetadataValueMapper,
        quality_mapper: QualityMapper,
    ):
        super().__init__(config, metadata_value_mapper, quality_mapper)
        if not HAS_ADODB:
            raise MissingModuleException("pywin32", "adodb")

    def connect(self):
        return adodbapi.connect(
            self._config.connection_string,
            {"timeout": self._config.query_timeout_seconds},
        )
