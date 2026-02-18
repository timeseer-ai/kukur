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
    scope: str

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
                scope=config.get("scope", "openid"),
            )

        return None


def get_oidc_auth(config: OIDCConfig):  # noqa: ARG001
    """Return a requests authentication module for OpenID Connect."""
    raise MissingModuleException("requests")


def has_kerberos_auth() -> bool:
    """Check if requests-kerberos is available."""
    return HAS_REQUESTS_KERBEROS


def get_kerberos_auth(hostname_override: str | None = None):
    """Return a requests authentication module for Kerberos."""
    if not HAS_REQUESTS_KERBEROS:
        raise MissingModuleException("requests-kerberos")
    return HTTPKerberosAuth(
        mutual_authentication="REQUIRED",
        sanitize_mutual_error_response=False,
        hostname_override=hostname_override,
    )


if HAS_OIDC_AUTH:

    class OIDCBearerAuth(AuthBase):
        """Handle OIDC bearer token authorization.

        Token is refreshed when expired.
        """

        # The OIDC specification allows omitting `expires_in` field.
        __FALLBACK_TOKEN_LIFETIME = 60 * 5

        def __init__(self, config: OIDCConfig):
            self.config = config
            self._access_token = None
            self._expires_at = 0

        def _refresh_token(self) -> None:
            data = {
                "grant_type": "client_credentials",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "scope": self.config.scope,
            }
            response = requests.post(self.config.oidc_token_url, data=data)
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


@dataclass
class AuthenticationProperties:
    """Support different authentication options for requests."""

    basic_auth: tuple[str, str] | None
    oidc_auth: OIDCConfig | None
    kerberos_hostname: str | None

    def apply(self, session):
        """Apply the authentication properties to the requests Session."""
        if self.oidc_auth is not None:
            session.auth = get_oidc_auth(self.oidc_auth)
        elif self.basic_auth is not None:
            session.auth = self.basic_auth
        elif has_kerberos_auth():
            session.auth = get_kerberos_auth()

    @classmethod
    def from_data(cls, data: dict) -> "AuthenticationProperties":
        """Create from data configuration options."""
        basic_auth = None
        if "username" in data and "password" in data:
            basic_auth = (data["username"], data["password"])

        oidc_config = OIDCConfig.from_config(data)

        return cls(basic_auth, oidc_config, data.get("kerberos_hostname"))
