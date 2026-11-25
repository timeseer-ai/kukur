"""Connections to azure data explorer data sources from Timeseer."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from .azure_data_explorer import from_config
from .connection import DataExplorerConnection, DataExplorerConnectionConfiguration

__all__ = [
    "DataExplorerConnection",
    "DataExplorerConnectionConfiguration",
    "from_config",
]
