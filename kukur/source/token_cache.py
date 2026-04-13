"""Cache authentications tokens.

Sources can optionally accept a `TokenCache`.
The `SourceFactory` can be configured with a specific implementation.

The default `TokenCache` keeps tokens in-memory only and is suitable for multithreaded use.
Other implementations can store tokens on-disk or in shared memory.
"""

# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Protocol


@dataclass
class Token:
    """A cached access token."""

    access_token: str
    expires: datetime | None = None
    refresh_token: str | None = None

    def is_expired(self) -> bool:
        """Return True when the token is (about ot be) expired."""
        now = datetime.now(tz=timezone.utc)
        if self.expires is None:
            return True
        return self.expires < now + timedelta(seconds=30)


class TokenCache(Protocol):
    """Cache access tokens."""

    def get_token(self, token_fn: Callable[[str | None], Token]) -> str:
        """Retrieve a token.

        Calls `token_fn` when the token is not cached or has expired.
        A refresh token is provided to the `token_fn` when one is available.
        """


class TokenCacheFactory(Protocol):
    """Create a `TokenCache` for a source."""

    def get_cache(self, name: str) -> TokenCache:
        """Return a `TokenCache` for the named source."""


class NullTokenCache:
    """Do not cache any tokens."""

    def get_token(self, token_fn: Callable[[str | None], Token]) -> str:
        """Retrieve a new token."""
        return token_fn(None).access_token


class InMemoryTokenCache:
    """Cache OAuth or other tokens for a source.

    This cache keeps tokens in memory and is thread safe.
    """

    def __init__(self, name: str, lock: threading.Lock, cache: dict[str, Token]):
        self._name = name
        self._cache = cache
        self._lock = lock

    def get_token(self, token_fn: Callable[[str | None], Token]) -> str:
        """Retrieve a token.

        Calls `token_fn` with a refresh token (if available) when the token expired.
        """
        with self._lock:
            token = self._cache.get(self._name)

            refresh_token = None
            if token is not None:
                refresh_token = token.refresh_token

            if token is None or token.is_expired():
                token = token_fn(refresh_token)
                self._cache[self._name] = token
            return token.access_token


class InMemoryTokenCacheFactory:
    """Create in-memory TokenCaches for sources."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._cache: dict[str, Token] = {}

    def get_cache(self, name: str) -> TokenCache:
        """Return a `TokenCache` for the named source."""
        return InMemoryTokenCache(name, self._lock, self._cache)
