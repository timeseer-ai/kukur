"""Inspect contents of a Azure Data Lake Storage Gen 2."""

# SPDX-FileCopyrightText: 2024 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import os.path
from typing import List, Optional
from urllib.parse import ParseResult

import pyarrow as pa
from pyarrow.dataset import dataset

from kukur.exceptions import MissingModuleException
from kukur.inspect import InspectedPath, InvalidInspectURI, ResourceType

try:
    from adlfs import AzureBlobFileSystem

    HAS_ADLFS = True
except ImportError:
    HAS_ADLFS = False

try:
    from azure.identity import DefaultAzureCredential

    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False

try:
    from azure.storage.filedatalake import DataLakeServiceClient

    HAS_AZURE_STORAGE_FILE_DATALAKE = True
except ImportError:
    HAS_AZURE_STORAGE_FILE_DATALAKE = False

try:
    from deltalake import DeltaTable

    HAS_DELTA_LAKE = True
except ImportError:
    HAS_DELTA_LAKE = False


def inspect(blob_uri: ParseResult) -> List[InspectedPath]:
    """Inspect a path in an Azure storage container."""
    if not HAS_AZURE_IDENTITY:
        raise MissingModuleException("azure.identity")
    if not HAS_AZURE_STORAGE_FILE_DATALAKE:
        raise MissingModuleException("azure.storage.filedatalake")

    account_uri = blob_uri.hostname
    if account_uri is None:
        raise InvalidInspectURI("missing storage container")
    container_name = blob_uri.username
    if container_name is None:
        raise InvalidInspectURI("missing container name")

    client = DataLakeServiceClient(account_uri, credential=DefaultAzureCredential())

    with client.get_file_system_client(container_name) as fs_client:
        paths = []
        for path in fs_client.get_paths(blob_uri.path, recursive=False):
            path_uri = blob_uri._replace(path=os.path.join(blob_uri.path, path["name"]))
            resource_type = None
            if path["is_directory"]:
                dir_client = fs_client.get_directory_client(path)
                if dir_client.get_file_client("_delta_log").exists():
                    resource_type = ResourceType.DELTA
                else:
                    resource_type = ResourceType.DIRECTORY
            else:
                path_parts = path["name"].split("/")
                if "." not in path_parts[-1]:
                    continue
                extension = path_parts[-1].lower().split(".")[-1]

                if extension == "parquet":
                    resource_type = ResourceType.PARQUET
                elif extension in ["arrow", "feather"]:
                    resource_type = ResourceType.ARROW
                elif extension in ["arrows"]:
                    resource_type = ResourceType.ARROWS
                elif extension in ["csv", "txt"]:
                    resource_type = ResourceType.CSV

            if resource_type is not None:
                paths.append(InspectedPath(resource_type, path_uri.geturl()))

        return paths


def preview(blob_uri: ParseResult, num_rows: int = 5000) -> Optional[pa.Table]:
    """Return the first nuw_rows of the blob."""
    if not HAS_ADLFS:
        raise MissingModuleException("adlfs")

    account_uri = blob_uri.hostname
    if account_uri is None:
        raise InvalidInspectURI("missing storage container")
    container_name = blob_uri.username
    if container_name is None:
        raise InvalidInspectURI("missing container name")

    fs = AzureBlobFileSystem(account_name=account_uri.split(".")[0], anon=False)
    blob_path = os.path.join(container_name, blob_uri.path.lstrip("/"))
    resource_type = _get_resource_type(fs, blob_path)
    if resource_type in [ResourceType.ARROW, ResourceType.PARQUET, ResourceType.CSV]:
        data_set = dataset(blob_path, format=resource_type.value, filesystem=fs)
        return data_set.head(num_rows)
    if resource_type == ResourceType.DELTA:
        if not HAS_DELTA_LAKE:
            raise MissingModuleException("deltalake")
        table = DeltaTable(blob_uri.geturl())
        data_set = table.to_pyarrow_dataset()
        return data_set.head(num_rows)
    return None


def _get_resource_type(fs, path: str) -> Optional[ResourceType]:  # noqa: PLR0911
    if fs.isdir(path):
        if fs.exists(os.path.join(path, "_delta_log")):
            return ResourceType.DELTA
        return ResourceType.DIRECTORY
    path_parts = path.split("/")
    if "." not in path_parts[-1]:
        return None
    extension = path_parts[-1].lower().split(".")[-1]

    if extension == "parquet":
        return ResourceType.PARQUET
    if extension in ["arrow", "feather"]:
        return ResourceType.ARROW
    if extension in ["arrows"]:
        return ResourceType.ARROWS
    if extension in ["csv", "txt"]:
        return ResourceType.CSV

    return None
