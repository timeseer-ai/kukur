from .lib import Table


class ParquetFile:

    num_row_groups: int

    def __init__(self, source) -> None:
        ...

    def read_row_group(
        self,
        i: int,
        columns: list[str] | None = None,
        use_threads: bool = True,
        use_pandas_metadata: bool = False,
    ) -> Table:
        ...


def read_table(
    source,
    *,
    columns: list[str] | None = None,
    use_threads: bool = True,
) -> Table:
    ...
