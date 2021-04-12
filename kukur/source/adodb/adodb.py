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

from kukur.source.metadata import MetadataValueMapper
from kukur.source.sql import BaseSQLSource, SQLConfig


class ADODBNotInstalledError(Exception):
    """Raised when the adodbapi module of pywin32 is not available."""

    def __init__(self):
        Exception.__init__(
            self, "the adodbapi modules is not available. Install pywin32."
        )


def from_config(data, metadata_value_mapper: MetadataValueMapper):
    """Create a new ADODB data source from a configuration dict.

    Raises ADODBNotInstalledError when the adodbapi module is not available."""
    if not HAS_ADODB:
        raise ADODBNotInstalledError()

    config = SQLConfig.from_dict(data)

    return ADODBSource(config, metadata_value_mapper)


class ADODBSource(BaseSQLSource):
    """An ADODB data source."""

    def __init__(self, config: SQLConfig, metadata_value_mapper: MetadataValueMapper):
        super().__init__(config, metadata_value_mapper)
        if not HAS_ADODB:
            raise ADODBNotInstalledError()

    def connect(self):
        return adodbapi.connect(self._config.connection_string)
