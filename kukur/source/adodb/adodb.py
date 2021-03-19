"""Connections to ADODB data sources from Timeseer.

This requires an installation of pywin32 (LGPL).
"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
try:
    import adodbapi

    HAS_ADODB = True
except ImportError:
    HAS_ADODB = False

from kukur.sql import SQLConfig, SQLSource


class ADODBNotInstalledError(Exception):
    """Raised when the adodbapi module of pywin32 is not available."""

    def __init__(self):
        Exception.__init__(
            self, "the adodbapi modules is not available. Install pywin32."
        )


def from_config(data):
    """Create a new ADODB data source from a configuration dict.

    Raises ADODBNotInstalledError when the adodbapi module is not available."""
    if not HAS_ADODB:
        raise ADODBNotInstalledError()

    config = SQLConfig.from_dict(data)

    return ADODBSource(config)


class ADODBSource(SQLSource):
    """An ADODB data source."""

    def __init__(self, config: SQLConfig):
        super().__init__(config)
        if not HAS_ADODB:
            raise ADODBNotInstalledError()

    def connect(self):
        return adodbapi.connect(self._config.connection_string)
