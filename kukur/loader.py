"""Generic data loaders for file based columnar data."""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import io

from pathlib import Path
from typing import Dict, Protocol, Union

try:
    import azure.storage.blob as blob

    HAS_AZURE = True
except ImportError:
    HAS_AZURE = False

from kukur.exceptions import InvalidDataError


class UnknownLoaderError(Exception):
    """Raised when the specified loader is unknown."""

    def __init__(self, message: str):
        Exception.__init__(self, f"unknown loader: {message}")


class AzureNotInstalledError(Exception):
    """Raised when the blob module of azure is not available."""

    def __init__(self):
        Exception.__init__(self, "the blob modules is not available. Install azure.")


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

    def __init__(self, path: Path, mode: str, files_as_path: bool):
        self.__path = path
        self.__mode = mode
        self.__files_as_path = files_as_path

    def open(self):
        """Open the file at path for reading.

        If files_as_path is True, return the path."""
        if not self.__path.exists():
            raise InvalidDataError(f"'{self.__path}' does not exist")
        if self.__files_as_path:
            return self.__path
        return self.__path.open(mode=self.__mode)

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
        return path.open(mode=self.__mode)


class AzureBlobLoader:
    """Load data from Azure blobs"""

    __mode: str
    __connection_string: str
    __container: str
    __path: str

    def __init__(self, mode, connection_string, container, path):
        self.__mode = mode
        self.__connection_string = connection_string
        self.__container = container
        self.__path = path
        if not HAS_AZURE:
            raise AzureNotInstalledError()

    def open(self):
        """Read the contents of the Blob given by path in a BytesIO/StringIO buffer."""
        client = blob.BlobServiceClient.from_connection_string(self.__connection_string)
        container_client = client.get_container_client(self.__container)
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
        client = blob.BlobServiceClient.from_connection_string(self.__connection_string)
        container_client = client.get_container_client(self.__container)
        blob_client = container_client.get_blob_client(self.__path + "/" + name)
        return blob_client.exists()

    def open_child(self, name: str):
        """Read the contents of the Blob given by path/name in a BytesIO buffer."""
        client = blob.BlobServiceClient.from_connection_string(self.__connection_string)
        container_client = client.get_container_client(self.__container)
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
        return FileLoader(Path(config[key]), mode, files_as_path)
    if loader_type == "azure-blob":
        if not HAS_AZURE:
            raise AzureNotInstalledError()
        connection_string = config["azure_connection_string"]
        container = config["azure_container"]
        path = config[key]
        return AzureBlobLoader(mode, connection_string, container, path)

    raise UnknownLoaderError(loader_type)
