"""Inspect databases."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from typing import Generator, List, Optional

import pyarrow as pa

from kukur.inspect import (
    Connection,
    InspectedPath,
    InspectOptions,
)
from kukur.inspect.postgres import get_connection


def inspect_database(
    config: Connection, path: Optional[str] = None
) -> List[InspectedPath]:
    """Inspect a database."""
    results = []
    if config.connection_type == "postgres":
        connection = get_connection(config)
        results = connection.inspect_database(path)

    return results


def preview_database(
    config: Connection,
    path: str,
    num_rows: int = 5000,
    options: Optional[InspectOptions] = None,
) -> Optional[pa.Table]:
    """Preview the contents of a database."""
    if config.connection_type == "postgres":
        connection = get_connection(config)
        return connection.preview_database(path, num_rows, options)
    return None


def read_database(
    config: Connection, path: str, options: Optional[InspectOptions] = None
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over the RecordBatches at the given Connection."""
    if config.connection_type == "postgres":
        connection = get_connection(config)
        yield from connection.read_database(path, options)
