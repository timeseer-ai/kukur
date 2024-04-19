"""Inspect contents of a AWS S3 bucket."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from pathlib import PurePath
from typing import Generator, List, Optional
from urllib.parse import ParseResult

import pyarrow as pa
from pyarrow.dataset import Dataset
from pyarrow.fs import S3FileSystem

from kukur.exceptions import MissingModuleException
from kukur.inspect import InspectedPath, InspectOptions, InvalidInspectURI
from kukur.inspect.arrow import get_data_set
from kukur.inspect.arrow import inspect as inspect_s3

try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False


def inspect(blob_uri: ParseResult) -> List[InspectedPath]:
    """Inspect a path in an AWS S3 bucket."""
    blob_path = _get_blob_path(blob_uri)
    return inspect_s3(S3FileSystem(), blob_path)


def preview(
    blob_uri: ParseResult, num_rows: int, options: Optional[InspectOptions]
) -> Optional[pa.Table]:
    """Return the first nuw_rows of the blob."""
    data_set = _get_data_set(blob_uri, options)
    return data_set.head(num_rows)


def read(
    blob_uri: ParseResult, options: Optional[InspectOptions]
) -> Generator[pa.RecordBatch, None, None]:
    """Iterate over all RecordBatches at the given URI."""
    data_set = _get_data_set(blob_uri, options)
    column_names = None
    if options is not None:
        column_names = options.column_names
    for record_batch in data_set.to_batches(columns=column_names):
        yield record_batch


def _get_data_set(blob_uri: ParseResult, options: Optional[InspectOptions]) -> Dataset:
    filesystem = S3FileSystem()
    blob_path = _get_blob_path(blob_uri)
    data_set = get_data_set(filesystem, blob_path, options)
    if data_set is None:
        if not HAS_DELTA_LAKE:
            raise MissingModuleException("deltalake")
        data_set = DeltaTable(blob_uri.geturl()).to_pyarrow_dataset()
    return data_set


def _get_blob_path(blob_uri: ParseResult) -> PurePath:
    bucket_name = blob_uri.netloc
    if bucket_name is None:
        raise InvalidInspectURI("missing bucket name")
    return PurePath(bucket_name) / PurePath(blob_uri.path.lstrip("/"))
