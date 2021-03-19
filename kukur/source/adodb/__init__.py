"""Connections to ADODB data sources from Timeseer.

This requires an installation of pywin32 (LGPL).
"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from .adodb import from_config

__all__ = ["from_config"]
