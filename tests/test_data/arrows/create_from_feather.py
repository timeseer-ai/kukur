"""Create Arrow IPC Streaming data from a Feather file."""

# SPDX-FileCopyrightText: 2023 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pyarrow import feather
from pyarrow.ipc import new_stream


def run() -> None:
    table = feather.read_table("../feather/row.feather")
    stream = new_stream("row.arrows", table.schema)
    stream.write_table(table, max_chunksize=1)


if __name__ == "__main__":
    run()
