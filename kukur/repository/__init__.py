"""Persistence for Kukur"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
from pathlib import Path
from typing import Optional

from .api_key import ApiKeyRepository

from .base import MigrationRunner  # noqa: F401


class RepositoryRegistry:  # pylint: disable=too-few-public-methods
    """RepositoryRegistry provides access to the different repositories in Kukur."""

    __data_dir: Optional[Path]
    __connection_string: Optional[str] = None

    def __init__(self, *, data_dir: Path = None, connection_string: str = None):
        self.__data_dir = data_dir
        self.__connection_string = connection_string

    def api_key(self) -> ApiKeyRepository:
        """Return the repository used to store api keys."""
        return ApiKeyRepository(self._get_connection_string("api_key"))

    def _get_connection_string(self, name: str) -> str:
        if self.__data_dir is None:
            return f"file:{name}.sqlite"
        return "file:" + str(self.__data_dir / f"{name}.sqlite")
