"""Kukur module for authentication and authorization."""

import requests


def get_oidc_token(client_id: str, client_secret: str, oidc_token_url: str):
    """Get an OIDC token using client credentials."""
    data = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": client_secret,
    }

    response = requests.post(oidc_token_url, data=data)
    response.raise_for_status()
    return response.json()["access_token"]
