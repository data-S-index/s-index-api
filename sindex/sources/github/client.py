"""GitHub API client functions.

This module provides low-level HTTP client functionality for interacting
with the GitHub API, including code search and repository metadata retrieval.
"""

import time
from typing import Any, Dict, List

import requests

from sindex.config.settings import get_github_token
from sindex.sources.github.constants import (
    ACCEPT_HEADER,
    DEFAULT_MAX_PAGES,
    GITHUB_API_VERSION,
    PAUSE_BETWEEN_CALLS,
    PER_PAGE,
    REQUEST_TIMEOUT,
    SEARCH_CODE_URL,
    USER_AGENT,
)


def build_headers(token: str | None = None) -> dict:
    """Build HTTP headers for GitHub API requests.

    Args:
        token: Optional GitHub personal access token. If None, uses
              the token from configuration.

    Returns:
        dict: HTTP headers dictionary with Authorization, Accept, API version,
              and User-Agent headers
    """
    tok = token or get_github_token()
    return {
        "Authorization": f"Bearer {tok}",
        "Accept": ACCEPT_HEADER,
        "X-GitHub-Api-Version": GITHUB_API_VERSION,
        "User-Agent": USER_AGENT,
    }


def _sleep_if_rate_limited(resp: requests.Response) -> bool:
    """Handle GitHub API rate limiting by sleeping until rate limit resets.

    Checks if the response indicates rate limiting (403 status) and if so,
    sleeps until the rate limit window resets. Adds a 2-second buffer to
    ensure the limit has fully reset.

    Args:
        resp: HTTP response from GitHub API

    Returns:
        bool: True if rate limited and slept (caller should retry),
              False otherwise
    """
    if resp.status_code != 403:
        return False

    # Get rate limit reset timestamp from response headers
    reset = resp.headers.get("X-RateLimit-Reset") or resp.headers.get(
        "X-Ratelimit-Reset"
    )
    if reset and reset.isdigit():
        # Calculate wait time: time until reset + 2 second buffer
        wait = max(0, int(reset) - int(time.time())) + 2
        print(f"[rate-limit] Sleeping {wait}s…")
        time.sleep(wait)
        return True
    return False


def search_code(
    query: str,
    *,
    max_pages: int = DEFAULT_MAX_PAGES,
    session: requests.Session | None = None,
    token: str | None = None,
) -> List[Dict[str, Any]]:
    """Run GitHub code search and return concatenated results.

    Performs a paginated search of GitHub code using the GitHub Search API.
    Automatically handles rate limiting and pagination. Stops when all results
    are retrieved or max_pages is reached.

    Args:
        query: GitHub search query string (e.g., '"10.1234/doi" in:file filename:README')
        max_pages: Maximum number of pages to fetch (defaults to DEFAULT_MAX_PAGES)
        session: Optional requests.Session for connection reuse
        token: Optional GitHub personal access token

    Returns:
        List of search result dictionaries from GitHub API

    Raises:
        requests.HTTPError: If the GitHub API returns an error status
    """
    headers = build_headers(token)
    s = session or requests.Session()

    items: list[dict] = []
    for page in range(1, max_pages + 1):
        resp = s.get(
            SEARCH_CODE_URL,
            headers=headers,
            params={"q": query, "per_page": PER_PAGE, "page": page},
            timeout=REQUEST_TIMEOUT,
        )

        # Handle rate limiting by sleeping and retrying
        if _sleep_if_rate_limited(resp):
            # retry same page after sleeping
            resp = s.get(
                SEARCH_CODE_URL,
                headers=headers,
                params={"q": query, "per_page": PER_PAGE, "page": page},
                timeout=REQUEST_TIMEOUT,
            )

        resp.raise_for_status()
        data = resp.json()
        batch = data.get("items", []) or []
        items.extend(batch)

        # Stop if we've retrieved all results or reached the total count
        if len(batch) < PER_PAGE or len(items) >= data.get("total_count", 0):
            break

        # Pause between API calls to be respectful
        time.sleep(PAUSE_BETWEEN_CALLS)

    return items


def get_repo_meta(
    full_name: str,
    *,
    session: requests.Session | None = None,
    token: str | None = None,
) -> dict:
    """Fetch repository metadata from GitHub API.

    Retrieves full repository information including creation date, fork status,
    and other metadata for a given repository.

    Args:
        full_name: Repository full name in format "owner/repo"
        session: Optional requests.Session for connection reuse
        token: Optional GitHub personal access token

    Returns:
        dict: Repository metadata JSON, or empty dict if request fails
    """
    headers = build_headers(token)
    s = session or requests.Session()

    url = f"https://api.github.com/repos/{full_name}"
    resp = s.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

    # Handle rate limiting
    if _sleep_if_rate_limited(resp):
        resp = s.get(url, headers=headers, timeout=REQUEST_TIMEOUT)

    if not resp.ok:
        return {}
    return resp.json()
