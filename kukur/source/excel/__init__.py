"""Read Excel files to Apache Arrow tables."""

import pyarrow as pa

try:

    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:

    import openpyxl  # noqa: F401

    HAS_PYOPENXL = True

except ImportError:
    HAS_PYOPENXL = False


from kukur.exceptions import KukurException, MissingModuleException

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


class UnexpectedExcelException(KukurException):
    """Raised when the excel file is not as expected."""

    def __init__(self, message: str):
        super().__init__(message)


def parse_excel(readable) -> pa.Table:
    """Parse a excel file to a PyArrow Table."""
    if not HAS_PANDAS:
        raise MissingModuleException("pandas", "excel")
    if not HAS_PYOPENXL:
        raise MissingModuleException("openpyxl", "excel")

    excel_df = pd.read_excel(readable, dtype_backend="pyarrow", engine="openpyxl")

    numeric_columns = excel_df.select_dtypes(include=["int", "float"]).columns

    excel_df[numeric_columns] = excel_df[numeric_columns].apply(
        pd.to_numeric, errors="coerce"
    )

    return pa.Table.from_pandas(excel_df, preserve_index=False)
