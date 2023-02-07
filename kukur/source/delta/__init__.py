"""Optional support for Delta Lake using https://github.com/delta-io/delta-rs."""

# SPDX-FileCopyrightText: 2022 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from .delta_lake import from_config

__all__ = ["from_config"]
