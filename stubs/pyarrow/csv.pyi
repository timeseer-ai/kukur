import collections.abc
import typing

from .lib import DataType, Schema, Table


class ReadOptions:

    def __init__(
        self,
        use_threads: bool | None = None,
        *,
        block_size: int | None = None,
        skip_rows: int | None = None,
        skip_rows_after_names: int | None = None,
        column_names: list[str] | None = None,
        autogenerate_column_names: bool | None = None,
        encoding: str = "utf8",
    ):
        ...


class ParseOptions:

    def __init__(
        self,
        delimiter: str | None = None,
        *,
        quote_char: str | typing.Literal[False] | None = None,
        double_quote: bool | None = None,
        escape_char: str | typing.Literal[False] | None = None,
        newlines_in_values: bool | None = None,
        ignore_empty_lines: bool | None = None,
        invalid_row_handler: typing.Callable | None = None,
    ):
        ...


class ConvertOptions:

    def __init__(
        self,
        check_utf8: bool | None = None,
        *,
        column_types: collections.abc.Mapping[str, DataType] | Schema | None = None,
        default_column_type: DataType | None = None,
        null_values: list[str] | None = None,
        true_values: list[str] | None = None,
        false_values: list[str] | None = None,
        decimal_point: str | None = None,
        strings_can_be_null: bool | None = None,
        quoted_strings_can_be_null: bool | None = None,
        include_columns: list[str] | None = None,
        include_missing_columns: bool | None = None,
        auto_dict_encode: bool | None = None,
        auto_dict_max_cardinality: int | None = None,
        timestamp_parsers: list[str] | None = None,
    ):
        ...


def read_csv(
    input_file,
    read_options: ReadOptions | None = None,
    parse_options: ParseOptions | None = None,
    convert_options: ConvertOptions | None = None,
    memory_pool=None,
) -> Table:
    ...
