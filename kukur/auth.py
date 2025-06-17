"""Kukur module for authentication and authorization."""

# SPDX-FileCopyrightText: 2025 Timeseer.AI
# SPDX-License-Identifier: Apache-2.0


import time
from dataclasses import dataclass
from typing import Optional

from kukur.exceptions import MissingModuleException

try:
    import requests
    from requests.auth import AuthBase

    HAS_OIDC_AUTH = True
except ImportError:
    HAS_OIDC_AUTH = False

try:
    from requests_kerberos import HTTPKerberosAuth

    HAS_REQUESTS_KERBEROS = True
except ImportError:
    HAS_REQUESTS_KERBEROS = False


@dataclass
class OIDCConfig:
    """Configuration for OIDC authentication."""

    client_id: str
    client_secret: str
    oidc_token_url: str

    @classmethod
    def from_config(cls, config: dict) -> Optional["OIDCConfig"]:
        """Create OIDCConfig from a configuration dictionary."""
        if (
            (client_id := config.get("client_id"))
            and (client_secret := config.get("client_secret"))
            and (oidc_token_url := config.get("oidc_token_url"))
        ):
            return cls(
                client_id=client_id,
                client_secret=client_secret,
                oidc_token_url=oidc_token_url,
            )

        return None


def get_oidc_auth(config: OIDCConfig):  # noqa: ARG001
    """Return a requests authentication module for OpenID Connect."""
    raise MissingModuleException("requests")


def has_kerberos_auth() -> bool:
    """Check if requests-kerberos is available."""
    return HAS_REQUESTS_KERBEROS


def get_kerberos_auth() -> HTTPKerberosAuth:
    """Return a requests authentication module for Kerberos."""
    if not HAS_REQUESTS_KERBEROS:
        raise MissingModuleException("requests-kerberos")
    return HTTPKerberosAuth(
        mutual_authentication="REQUIRED", sanitize_mutual_error_response=False
    )


if HAS_OIDC_AUTH:

    class OIDCBearerAuth(AuthBase):
        """Handle OIDC bearer token authorization.

        Token is refreshed when expired.
        """

        # The OIDC specification allows omitting `expires_in` field.
        __FALLBACK_TOKEN_LIFETIME = 60 * 5

        def __init__(self, config: OIDCConfig):
            self.token_url = config.oidc_token_url
            self.client_id = config.client_id
            self.client_secret = config.client_secret
            self._access_token = None
            self._expires_at = 0

        def _refresh_token(self) -> None:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
                "scope": "openid",
            }
            response = requests.post(self.token_url, data=data)
            response.raise_for_status()
            token_data = response.json()
            self._access_token = token_data["access_token"]
            expires_in = token_data.get("expires_in", self.__FALLBACK_TOKEN_LIFETIME)
            self._expires_at = time.time() + expires_in - 10

        def __call__(self, r):
            """Define the authorization header."""
            if self._access_token is None or time.time() >= self._expires_at:
                self._refresh_token()
            r.headers["Authorization"] = f"Bearer {self._access_token}"
            return r

    def get_oidc_auth(config: OIDCConfig):
        """Return a requests authentication module for OpenID Connect."""
        return OIDCBearerAuth(config)
