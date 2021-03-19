"""Service layer for api keys"""
# SPDX-FileCopyrightText: 2021 Timeseer.AI
#
# SPDX-License-Identifier: Apache-2.0
import secrets
import os

from hashlib import scrypt
from datetime import datetime
from typing import List

from kukur.api_key import ApiKey
from kukur.repository import RepositoryRegistry


class ApiKeys:
    """Api keys for authentication"""

    __repository: RepositoryRegistry

    def __init__(self, repository_registry: RepositoryRegistry):
        self.__repository = repository_registry

    def create(self, name: str) -> str:
        """Create an api key"""
        api_key = _create_random_api_key()
        creation_date = datetime.now()
        salt = os.urandom(16)
        self.__repository.api_key().store(
            name, _hash_api_key(api_key, salt), salt, creation_date
        )
        return api_key

    def list(self) -> List[ApiKey]:
        """List all the api keys"""
        return self.__repository.api_key().list()

    def has_api_key(self, name: str):
        """Get the api key by name"""
        return self.__repository.api_key().has_api_key(name)

    def is_valid(self, name: str, api_key: str) -> bool:
        """Check if the supplied api key is a valid one"""
        stored_api_key, salt = self.__repository.api_key().get(name)
        if salt is None or stored_api_key is None:
            return False
        hashed_api_key = _hash_api_key(api_key, salt)
        return stored_api_key == hashed_api_key

    def revoke(self, name: str) -> ApiKey:
        """Revoke an api key"""
        return self.__repository.api_key().revoke(name)


def _create_random_api_key() -> str:
    return secrets.token_urlsafe(40)


def _hash_api_key(api_key: str, salt: bytes) -> bytes:
    hashed = scrypt(bytes(api_key, "UTF-8"), salt=salt, n=16384, r=8, p=1)
    return hashed
