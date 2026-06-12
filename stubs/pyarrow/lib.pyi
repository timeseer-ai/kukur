
import collections.abc
import typing

import pandas

class DataType:

    tz: str | None
    value_type: "DataType"


class Field:

    name: str
    type: DataType


class Scalar:

    def as_py(self) -> typing.Any:
        ...


class Schema:

    names: list[str]
    types: list[DataType]
    metadata: dict | None

    def append(self, field: Field) -> "Schema":
        ...

    def get_field_index(self, name: str) -> int:
        ...

    def field(self, name) -> Field:
        ...

    def with_metadata(self, metadata: dict) -> "Schema":
        ...


class Array(typing.Sequence):

    type: DataType
    chunks: list[Array]

    def __getitem__(self, index: int | slice):
        ...

    def __len__(self) -> int:
        ...

    def cast(self, data_type: DataType) -> "Array":
        ...

    def combine_chunks(self) -> "Array":
        ...

    def fill_null(self, fill_value) -> "Array":
        ...

    def filter(self, mask: "Array") -> "Array":
        ...

    def is_valid(self) -> "Array":
        ...

    def to_pylist(self) -> list:
        ...

    def slice(self, offset: int, length: int = ...) -> Array:
        ...

class ChunkedArray(Array):

    chunks: list["Array"]

    def combine_chunks(self) -> "Array":
        ...

    def cast(self, data_type: DataType) -> "ChunkedArray":
        ...

    def fill_null(self, fill_value) -> "ChunkedArray":
        ...

    def filter(self, mask: "Array") -> "ChunkedArray":
        ...

    def is_valid(self) -> "ChunkedArray":
        ...

    def slice(self, offset: int, length: int = ...) -> "ChunkedArray":
        ...

    def sort(self, order: str = "ascending") -> "ChunkedArray":
        ...

class TableGroupBy:

    def aggregate(self, aggregations: list) -> "Table":
        ...


class RecordBatch(typing.Sized):

    nbytes: int
    num_rows: int
    schema: Schema
    column_names: list[str]

    @classmethod
    def from_arrays(
        cls,
        arrays: list,
        names=None,
        schema=None,
        metadata=None
    ) -> "RecordBatch":
        ...

    def __getitem__(self, index: str) -> Array:
        ...

    def __len__(self) -> int:
        ...

    def cast(self, target_schema: Schema, safe=None, options=None) -> "RecordBatch":
        ...

    def set_column(self, i: int, field_, column) -> "RecordBatch":
        ...

    def drop_columns(self, columns) -> "RecordBatch":
        ...

    @classmethod
    def from_pydict(cls, pydict: dict, schema: Schema | None = None) -> "RecordBatch":
        ...


class SparseUnionType:

    type_codes: list[int]

    def __iter__(self) -> typing.Generator[Field, None, None]:
        ...


class UnionArray:

    @classmethod
    def from_sparse(
        cls,
        types: Array,
        children: list,
        field_names: list | None = None,
        type_codes: list | None = None
    ) -> "UnionArray":
        ...

class Table(typing.Sized):

    column_names: list[str]
    num_rows: int
    schema: Schema

    @classmethod
    def from_pydict(cls, pydict: dict, schema: Schema | None = None) -> "Table":
        ...

    @classmethod
    def from_pylist(cls, mapping: list, schema: Schema | None = None) -> "Table":
        ...

    @classmethod
    def from_pandas(cls, df: pandas.DataFrame, preserve_index: bool | None = None) -> "Table":
        ...

    @classmethod
    def from_arrays(cls, arrays: list[Array], names: list[str]) -> "Table":
        ...

    @classmethod
    def from_batches(
        cls,
        batches: collections.abc.Iterator[RecordBatch] | list[RecordBatch],
        schema: Schema | None = None,
    ):
        ...

    def __getitem__(self, index: str | int) -> ChunkedArray:
        ...

    def __len__(self) -> int:
        ...

    def __arrow_c_stream__(self, requested_schema: object | None = None) -> object: ...

    def add_column(self, i: int, field: str| Field, column: Array | list) -> "Table":
        ...

    def append_column(self, field: str | Field, column: Array | list) -> "Table":
        ...

    def column(self, index_or_name: str | int) -> ChunkedArray:
        ...

    def combine_chunks(self) -> "Table":
        ...

    def drop_columns(self, columns: str | list[str]) -> "Table":
        ...

    def filter(self, mask: Array | list) -> "Table":
        ...

    def group_by(self, names: list[str] | str, use_threads: bool = True) -> TableGroupBy:
        ...

    def rename_columns(self, new_names: list[str] | dict[str, str]) -> "Table":
        ...

    def select(self, expression) -> "Table":
        ...

    def set_column(self, i: int, field: str | Field, column: Array | list) -> "Table":
        ...

    def slice(self, offset: int = 0, length: int = 0) -> "Table":
        ...

    def sort_by(self, column: str | list[str]) -> "Table":
        ...

    def to_batches(self) -> list[RecordBatch]:
        ...

    def to_pandas(self, *, ignore_metadata: bool, coerce_temporal_nanoseconds: bool) -> pandas.DataFrame:
        ...

    def to_pylist(self) -> list[dict]:
        ...

    def to_pydict(self) -> dict[str, list]:
        ...

    def cast(self, schema: Schema) -> Table:
        ...

    def replace_schema_metadata(self, metadata: dict | None) -> Table:
        ...

class ArrowException(Exception):
    ...

class ArrowTypeError(TypeError, ArrowException):
    ...

class ArrowInvalid(ArrowException):
    ...
