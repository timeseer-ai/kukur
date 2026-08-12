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


class RecordBatchFileReader:

    schema: Schema
    num_record_batches: int

    def __enter__(self) -> "RecordBatchFileReader":
        ...

    def __exit__(self, exc_type, exc_value, traceback) -> None:
        ...

    def close(self) -> None:
        ...

    def get_batch(self, i: int) -> RecordBatch:
        ...

    def get_record_batch(self, i: int) -> RecordBatch:
        ...

    def read_all(self) -> Table:
        ...


def open_file(
    source, footer_offset=None, *, options=None, memory_pool=None
) -> RecordBatchFileReader:
    ...


def open_stream(source, *, options=None, memory_pool=None) -> RecordBatchStreamReader:
    ...
