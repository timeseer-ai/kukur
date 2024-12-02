"""Connections to Databricks SQL data sources from Timeseer."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from .databricks_sql import from_config

__all__ = ["from_config"]
