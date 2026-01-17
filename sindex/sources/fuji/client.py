"""Low-level client for F-UJI FAIR evaluation API.

This module provides the core HTTP client functionality for making
requests to the F-UJI service to evaluate dataset FAIRness.
"""

from __future__ import annotations

from typing import Optional

import requests
from requests.auth import HTTPBasicAuth

from .constants import FUJI_EVALUATE_PATH, FUJI_TIMEOUT_SECS


def fuji_evaluate(
    object_identifier: str,
    *,
    session: requests.Session,
    base_url: str,
    username: str | None = None,
    password: str | None = None,
) -> dict:
    """Make a direct F-UJI evaluation API call.

    This is a low-level function that sends a POST request to the F-UJI
    evaluation endpoint. It requires a pre-configured requests session.

    Args:
        object_identifier: Resolvable URL to evaluate (can be a DOI URL or
                          any other resolvable URL)
        session: Pre-configured requests.Session for making HTTP requests
        base_url: Base URL of the F-UJI service (e.g., "http://fuji:1071")
        username: Optional Basic Auth username for F-UJI authentication
        password: Optional Basic Auth password for F-UJI authentication

    Returns:
        dict: Raw JSON response from F-UJI containing FAIR evaluation results

    Raises:
        requests.HTTPError: If the F-UJI API returns a non-2xx status code
    """
    # Construct the full evaluation endpoint URL
    evaluate_url = f"{base_url.rstrip('/')}{FUJI_EVALUATE_PATH}"

    # Set up Basic Auth if credentials are provided
    auth: Optional[HTTPBasicAuth] = None
    if username is not None and password is not None:
        auth = HTTPBasicAuth(username, password)

    # Make the POST request to F-UJI evaluation endpoint
    resp = session.post(
        evaluate_url,
        json={"object_identifier": object_identifier},
        auth=auth,
        timeout=FUJI_TIMEOUT_SECS,
    )
    resp.raise_for_status()
    return resp.json()
