"""PostgreSQL connection for Kukur."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from .postgresql import from_config

__all__ = ["from_config"]
