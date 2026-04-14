# SPDX-FileCopyrightText: 2026 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0

from datetime import datetime
from unittest.mock import patch

from kukur.source.token_cache import (
    InMemoryTokenCacheFactory,
    Token,
)


def test_expires_none() -> None:
    token = Token("a")
    assert token.is_expired()


def test_expires_not() -> None:
    with patch("kukur.source.token_cache.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            "2026-01-02T00:00:00+00:00"
        )
        mock_datetime.side_effect = datetime

        token = Token("a", datetime.fromisoformat("2026-01-03T00:00:00+00:00"))
        assert not token.is_expired()


def test_expires_old() -> None:
    with patch("kukur.source.token_cache.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            "2026-01-02T00:00:00+00:00"
        )
        mock_datetime.side_effect = datetime

        token = Token("a", datetime.fromisoformat("2026-01-01T00:00:00+00:00"))
        assert token.is_expired()


def test_expires_early() -> None:
    with patch("kukur.source.token_cache.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            "2026-01-02T00:00:00+00:00"
        )
        mock_datetime.side_effect = datetime

        token = Token("a", datetime.fromisoformat("2026-01-02T00:00:30+00:00"))
        assert not token.is_expired()

        token = Token("a", datetime.fromisoformat("2026-01-02T00:00:29+00:00"))
        assert token.is_expired()


def test_cache_always() -> None:
    calls = {"count": 0}

    def _get_token(_) -> Token:
        calls["count"] += 1
        return Token("a")

    factory = InMemoryTokenCacheFactory()
    cache = factory.get_cache("test")
    assert cache.get_token(_get_token) == "a"
    assert cache.get_token(_get_token) == "a"
    assert calls["count"] == 2


def test_cache_token() -> None:
    calls = {"count": 0}

    def _get_token(_) -> Token:
        calls["count"] += 1
        return Token("a", datetime.fromisoformat("2026-01-03T00:00:00+00:00"))

    with patch("kukur.source.token_cache.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            "2026-01-02T00:00:00+00:00"
        )
        mock_datetime.side_effect = datetime

        factory = InMemoryTokenCacheFactory()
        cache = factory.get_cache("test")
        assert cache.get_token(_get_token) == "a"
        assert cache.get_token(_get_token) == "a"
        assert calls["count"] == 1


def test_cache_expires() -> None:
    calls = {"count": 0}

    def _get_token(r: str | None) -> Token:
        calls["count"] += 1
        if r is not None:
            calls["refresh"] = True
            assert r == "r"
        return Token("a", datetime.fromisoformat("2026-01-03T00:00:00+00:00"), "r")

    factory = InMemoryTokenCacheFactory()
    cache = factory.get_cache("test")

    with patch("kukur.source.token_cache.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            "2026-01-02T00:00:00+00:00"
        )
        mock_datetime.side_effect = datetime

        assert cache.get_token(_get_token) == "a"

    with patch("kukur.source.token_cache.datetime") as mock_datetime:
        mock_datetime.now.return_value = datetime.fromisoformat(
            "2026-01-03T00:00:00+00:00"
        )
        mock_datetime.side_effect = datetime

        assert cache.get_token(_get_token) == "a"

    assert calls["count"] == 2
    assert calls["refresh"]
