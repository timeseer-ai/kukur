# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

"""Read Excel files to Apache Arrow tables."""

from typing import List, Optional

import pyarrow as pa

from kukur.inspect import DataOptions

try:

    import pandas as pd

    HAS_PANDAS = True
except ImportError:
    HAS_PANDAS = False

try:

    import openpyxl

    HAS_PYOPENXL = True

except ImportError:
    HAS_PYOPENXL = False


from kukur.exceptions import MissingModuleException


def parse_excel(readable, sheet_name: str, options: Optional[DataOptions]) -> pa.Table:
    """Parse a excel file to a PyArrow Table."""
    if not HAS_PANDAS:
        raise MissingModuleException("pandas", "excel")
    if not HAS_PYOPENXL:
        raise MissingModuleException("openpyxl", "excel")

    workbook = openpyxl.load_workbook(readable, data_only=True)
    worksheet = workbook[sheet_name or workbook.sheetnames[0]]

    data = list(worksheet.values)
    if options is not None and options.excel_header_row:
        headers = data[0]
        rows = data[1:]
        excel_df = pd.DataFrame(rows, columns=headers)
    else:
        excel_df = pd.DataFrame(data)
    return _to_pyarrow(excel_df)


def parse_table(readable, sheet_name: str, table_name: str) -> pa.Table:
    """Parse a table from a excel file to a PyArrow Table."""
    wb = openpyxl.load_workbook(readable)
    ws = wb[sheet_name or wb.sheetnames[0]]
    table = ws.tables[table_name]

    table_range = table.ref
    rows = openpyxl.utils.cell.range_boundaries(table_range)

    data = []
    for row in ws.iter_rows(
        rows,
        values_only=True,
    ):
        data.append(row)

    excel_df = pd.DataFrame(data[1:], columns=data[0])
    return _to_pyarrow(excel_df)


def list_sheets(readable) -> List[str]:
    """List the sheets in the Excel file."""
    if not HAS_PYOPENXL:
        raise MissingModuleException("openpyxl", "excel")

    workbook = openpyxl.load_workbook(readable)
    return workbook.sheetnames


def _to_pyarrow(excel_df: pd.DataFrame) -> pa.Table:
    """Convert a pandas DataFrame to a PyArrow Table."""
    for col in excel_df.columns:
        unique_types = set(map(type, excel_df[col].dropna()))
        if len(unique_types) > 1:
            excel_df[col] = excel_df[col].astype(str)

    excel_df = excel_df.convert_dtypes(dtype_backend="pyarrow")

    return pa.Table.from_pandas(excel_df, preserve_index=False)
