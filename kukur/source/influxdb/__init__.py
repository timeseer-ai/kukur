"""Connections to InfluxDB data sources from Timeseer."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from .influxdb import from_config

__all__ = ["from_config"]
