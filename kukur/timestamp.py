"""Parse timestamps returned by data sources."""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime

import dateutil.parser


def parse_timestamp(value: str) -> datetime:
    """Parse a timestamp, preferring the fast ISO 8601 parser over dateutil."""
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return dateutil.parser.parse(value)
