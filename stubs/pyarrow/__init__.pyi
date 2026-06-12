
import typing

from . import compute, csv, ipc
from .lib import Array as Array
from .lib import ArrowInvalid as ArrowInvalid
from .lib import ArrowTypeError as ArrowTypeError
from .lib import DataType as DataType
from .lib import Field as Field
from .lib import RecordBatch as RecordBatch
from .lib import Schema as Schema
from .lib import SparseUnionType as SparseUnionType
from .lib import Table as Table
from .lib import UnionArray as UnionArray

class Buffer:

    def to_pybytes(self) -> bytes:
        ...


class TimestampType(DataType):

    tz: str


class BufferOutputStream:
    def getvalue(self) -> Buffer:
        ...

def bool_() -> DataType:
    ...

def float64() -> DataType:
    ...

def int8() -> DataType:
    ...

def int64() -> DataType:
    ...

def timestamp(unit: str, tz: str | None=None) -> TimestampType:
    ...

def string() -> DataType:
    ...

def struct(fields) -> DataType:
    ...

def scalar(obj, type: DataType | None = None):
    ...

def schema(fields) -> Schema:
    ...

def sparse_union(child_fields, type_codes=None) -> SparseUnionType:
    ...

def field(name, type=None, nullable=None, metadata=None) -> Field:
    ...

class ChunkedArray(Array):
    ...


def array(obj: typing.Iterable, type: DataType | None = None) -> Array:
    ...


def table(data: dict | list[Array], names: list[str] | None = None) -> Table:
    ...

def concat_tables(
    tables: typing.Iterable[Table],
    promote_options: typing.Literal["none"] | typing.Literal["default"] | typing.Literal["permissive"] = "none"
) -> Table:
    ...


def concat_batches(obj: typing.Iterable[RecordBatch]) -> RecordBatch:
    ...


def set_timezone_db_path(path: str):
    ...


def concat_arrays(arrays: typing.Iterable[Array]) -> Array:
    ...
