"""Kukur module for authentication and authorization."""

import time
from dataclasses import dataclass

import requests
from requests.auth import AuthBase


@dataclass
class OIDCConfig:
    """Configuration for OIDC authentication."""

    client_id: str
    client_secret: str
    oidc_token_url: str


class OIDCBearerAuth(AuthBase):
    """Handle OIDC bearer token authorization.

    Token is refreshed when expired.
    """

    def __init__(self, config: OIDCConfig):
        self.token_url = config.oidc_token_url
        self.client_id = config.client_id
        self.client_secret = config.client_secret
        self._access_token = None
        self._expires_at = 0

    def _refresh_token(self):
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
        expires_in = token_data.get("expires_in", 300)
        self._expires_at = time.time() + expires_in - 10

    def __call__(self, r):
        """Define the authorization header."""
        if self._access_token is None or time.time() >= self._expires_at:
            self._refresh_token()
        r.headers["Authorization"] = f"Bearer {self._access_token}"
        return r
