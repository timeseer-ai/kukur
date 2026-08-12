import typing

from .lib import RecordBatch, Schema, Table


class RecordBatchStreamReader(typing.Iterator[RecordBatch]):

    schema: Schema

    def __enter__(self) -> "RecordBatchStreamReader":
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...

    def __iter__(self) -> "RecordBatchStreamReader":
        ...

    def __next__(self) -> RecordBatch:
        ...

    def close(self) -> None:
        ...

    def read_all(self) -> Table:
        ...

    def read_next_batch(self) -> RecordBatch:
        ...


def open_stream(source, *, options=None, memory_pool=None) -> RecordBatchStreamReader:
    ...
