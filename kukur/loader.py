"""Generic data loaders for file based columnar data."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import io

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, Optional, Protocol, Union

try:
    from azure.identity import DefaultAzureCredential

    HAS_AZURE_IDENTITY = True
except ImportError:
    HAS_AZURE_IDENTITY = False

try:
    from azure.storage import blob

    HAS_AZURE_STORAGE_BLOB = True
except ImportError:
    HAS_AZURE_STORAGE_BLOB = False

from kukur.exceptions import InvalidDataError, KukurException, MissingModuleException


class UnknownLoaderError(KukurException):
    """Raised when the specified loader is unknown."""

    def __init__(self, message: str):
        KukurException.__init__(self, f"unknown loader: {message}")


class Loader(Protocol):
    """Loader opens file-like objects containing data."""

    def open(self):
        """Open a file-like object from the loading source."""
        ...

    def has_child(self, subpath: str) -> bool:
        """Test if a file-like object exists at a subpath."""

    def open_child(self, subpath: str):
        """Open a file-like object at a subpath."""
        ...


class FileLoader:
    """Load data from files"""

    __path: Path
    __mode: str
    __files_as_path: bool
    __encoding: str

    def __init__(self, path: Path, mode: str, files_as_path: bool, encoding: str):
        self.__path = path
        self.__mode = mode
        self.__files_as_path = files_as_path
        self.__encoding = encoding

    def open(self):
        """Open the file at path for reading.

        If files_as_path is True, return the path."""
        if not self.__path.exists():
            raise InvalidDataError(f"'{self.__path}' does not exist")
        if self.__files_as_path:
            return self.__path
        return self.__path.open(mode=self.__mode, encoding=self.__encoding)

    def has_child(self, name: str) -> bool:
        """Test if the file <name> is in the directory pointed to by <path>."""
        if not self.__path.is_dir():
            raise InvalidDataError(f'"{self.__path}" is not a directory')
        path = self.__path / name
        return path.exists()

    def open_child(self, name: str):
        """Open the file <name> that is in the directory path for reading.

        If files_as_path is True, return the path of the child."""
        if not self.__path.is_dir():
            raise InvalidDataError(f'"{self.__path}" is not a directory')
        path = self.__path / name
        if not path.exists():
            raise InvalidDataError(f"'{path}' does not exist")
        if self.__files_as_path:
            return path
        return path.open(mode=self.__mode, encoding=self.__encoding)


@dataclass
class AzureBlobConfiguration:
    """Connection details for an Azure blob inside a container.

    When the connection string does not include authentication tokens, provide an identity."""

    connection_string: str
    container: str
    identity: Optional[str] = None


class AzureBlobLoader:
    """Load data from Azure blobs"""

    __mode: str
    __config: AzureBlobConfiguration
    __path: str

    def __init__(
        self,
        mode: str,
        config: AzureBlobConfiguration,
        path,
    ):
        self.__mode = mode
        self.__config = config
        self.__path = path
        if not HAS_AZURE_IDENTITY:
            raise MissingModuleException("azure-identity")
        if not HAS_AZURE_STORAGE_BLOB:
            raise MissingModuleException("azure-storage-blob")

    def open(self):
        """Read the contents of the Blob given by path in a BytesIO/StringIO buffer."""
        container_client = self._get_container_client()
        downloader = container_client.download_blob(self.__path)
        if "b" in self.__mode:
            buffer = io.BytesIO()
            buffer.write(downloader.content_as_bytes())
        else:
            buffer = io.StringIO()
            buffer.write(downloader.content_as_bytes().decode())
        buffer.seek(0)
        return buffer

    def has_child(self, name: str) -> bool:
        """Test if the given child blob exists."""
        container_client = self._get_container_client()
        blob_client = container_client.get_blob_client(self.__path + "/" + name)
        return blob_client.exists()

    def open_child(self, name: str):
        """Read the contents of the Blob given by path/name in a BytesIO buffer."""
        container_client = self._get_container_client()
        downloader = container_client.download_blob(self.__path + "/" + name)
        buffer: Union[io.BytesIO, io.StringIO]
        if "b" in self.__mode:
            buffer = io.BytesIO()
            buffer.write(downloader.content_as_bytes())
        else:
            buffer = io.StringIO()
            buffer.write(downloader.content_as_bytes().decode())
        buffer.seek(0)
        return buffer

    def _get_container_client(self):
        if self.__config.identity is not None and self.__config.identity == "default":
            credential = DefaultAzureCredential()
            client = blob.BlobServiceClient.from_connection_string(
                self.__config.connection_string, credential
            )
        else:
            client = blob.BlobServiceClient.from_connection_string(
                self.__config.connection_string
            )

        return client.get_container_client(self.__config.container)


def from_config(
    config: Dict[str, str], key="path", mode="rb", files_as_path=False
) -> Loader:
    """Create a loader from a configuration object.

    The main path to the data file that will be loaded is found by key in the config.
    The file will be opened with the given mode if supported by the loader.

    Some sources allow more efficient reading of files using memory mapping. To
    support this, set files_as_path to True, and a loader for local files will
    return the Paths instead of the opened files.
    """
    loader_type = config.get("loader", "file")
    if loader_type == "file":
        return FileLoader(
            Path(config[key]), mode, files_as_path, config.get("file_encoding", "UTF-8")
        )
    if loader_type == "azure-blob":
        if not HAS_AZURE_IDENTITY:
            raise MissingModuleException("azure-identity")
        if not HAS_AZURE_STORAGE_BLOB:
            raise MissingModuleException("azure-storage-blob")
        azure_config = AzureBlobConfiguration(
            config["azure_connection_string"],
            config["azure_container"],
            config.get("azure_identity"),
        )

        path = config[key]
        return AzureBlobLoader(mode, azure_config, path)

    raise UnknownLoaderError(loader_type)
