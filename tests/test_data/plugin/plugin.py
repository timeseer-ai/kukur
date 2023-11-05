"""Executable sample for tests."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import json
import sys
from datetime import datetime

from pyarrow import Table, ipc


def _run() -> None:
    """Perform the action requested by the first argument."""
    if sys.argv[1] == "search":
        _search()
    if sys.argv[1] == "metadata":
        _metadata()
    if sys.argv[1] == "data":
        _data()
    sys.exit(0)


def _search() -> None:
    """List all time series (or metadata)."""
    query = json.load(sys.stdin)
    source_name = query["search"]["source"]
    data: dict = {
        "metadata": [
            {
                "series": {"source": source_name, "tags": {"series name": "test"}},
                "description": "Test series",
            }
        ],
        "series": [{"source": source_name, "tags": {"series name": "test-2"}}],
    }
    json.dump(data, sys.stdout)


def _metadata() -> None:
    """Return metadata for the time series received on stdin."""
    query = json.load(sys.stdin)
    source_name = query["metadata"]["series"]["source"]
    series_name = query["metadata"]["series"]["tags"]["series name"]
    data: dict = {"description": f"Description of {series_name} ({source_name})"}
    json.dump(data, sys.stdout)


def _data() -> None:
    """Return Arrow IPC with that for the time series received on stdin."""
    query = json.load(sys.stdin)
    start_date = datetime.fromisoformat(query["data"]["startDate"])
    end_date = datetime.fromisoformat(query["data"]["endDate"])
    table = Table.from_pydict(
        {"ts": [start_date, end_date], "value": [0, 42], "quality": ["BAD", "GOOD"]}
    )
    with ipc.new_stream(sys.stdout.buffer, table.schema) as writer:
        writer.write_table(table)


if __name__ == "__main__":
    _run()
