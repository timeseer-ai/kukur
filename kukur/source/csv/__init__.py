"""
csv contains the CSV data source for Timeseer.

Three formats are supported:
- row based, with many series in one file containing one line per data point
- directory based, with one file per series
- pivot, with many series as columns in one file
"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from .csv import from_config

__all__ = ["from_config"]
