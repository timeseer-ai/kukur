"""parquet contains the Parquet data source for Timeseer.

Three formats are supported:
- row based, with many series in one file containing one row per data point
- directory based, with one file per series
- pivot, with many series as columns in one file

Parquet DataSets are not yet supported.
"""

from .parquet import from_config

__all__ = ["from_config"]
