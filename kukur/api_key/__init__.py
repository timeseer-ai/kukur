"""Module for api keys."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ApiKey:
    """An api key contains the information of an api key"""

    name: str
    creation_date: datetime
