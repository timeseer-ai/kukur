"""Connections to ADODB data sources from Timeseer.

This requires an installation of pywin32 (LGPL).
"""

from .adodb import from_config

__all__ = ["from_config"]
