"""Module for api keys."""
from dataclasses import dataclass
from datetime import datetime


@dataclass
class ApiKey:
    """An api key contains the information of an api key"""

    name: str
    creation_date: datetime
