from .lib import Table


def read_table(
    source,
    columns: list[str] | list[int] | None = None,
    memory_map: bool = False,
    use_threads: bool = True,
) -> Table:
    ...


def write_feather(
    df,
    dest,
    compression: str | None = None,
    compression_level: int | None = None,
    chunksize: int | None = None,
    version: int = 2,
) -> None:
    ...
