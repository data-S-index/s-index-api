from __future__ import annotations

import math
from typing import Optional

import requests

from sindex.core.http import make_session

from .constants import DEFAULT_OA_EMAIL, OA_BASE_URL, OA_TIMEOUT_SECS, USER_AGENT_OA


def make_openalex_session(
    *,
    api_key: Optional[str] = None,
    user_agent: str = USER_AGENT_OA,
    email: Optional[str] = DEFAULT_OA_EMAIL,
) -> requests.Session:
    """
    Create a requests.Session tuned for OpenAlex with retry/backoff.
    """
    s = make_session(
        user_agent=user_agent,
        allowed_methods=("GET",),
        status_forcelist=(429, 500, 502, 503, 504),
        pool_connections=20,
        pool_maxsize=20,
        total_retries=6,
        backoff=1.5,
    )

    # Decide which email to use
    effective_email = email or DEFAULT_OA_EMAIL

    if api_key or effective_email:
        s.params = getattr(s, "params", {})
        if api_key:
            s.params["api_key"] = api_key
        if effective_email:
            s.params["mailto"] = effective_email

    return s


def get_openalex_record(
    path: str,
    *,
    session: requests.Session,
    params: dict | None = None,
    timeout: int = OA_TIMEOUT_SECS,
) -> dict | None:
    """
    GET OpenAlex API endpoint and return the JSON response.

    This function makes a GET request to the OpenAlex API and returns the
    parsed JSON response. It handles 404 errors gracefully by returning None.

    Args:
        path: API endpoint path (e.g., "/works/W1234567890").
        session: Shared requests.Session for connection reuse.
        params: Optional query parameters to include in the request.
        timeout: Request timeout in seconds.

    Returns:
        Parsed JSON response as a dictionary, or None if the resource is not found (404).

    Raises:
        requests.HTTPError: If the API returns an HTTP error other than 404.
        RuntimeError: If the response is not valid JSON.

    Notes:
        - Does NOT call raise_for_status() automatically, but will raise for
          non-404 errors after attempting to parse JSON.
    """
    url = f"{OA_BASE_URL}{path}"
    r = session.get(url, params=params, timeout=timeout)
    if r.status_code == 404:
        return None

    try:
        r.raise_for_status()
    except requests.HTTPError as e:
        snippet = (r.text or "")[:300]
        raise requests.HTTPError(
            f"OpenAlex HTTP {r.status_code} for {r.url}. "
            f"Content-Type={r.headers.get('Content-Type')!r}. "
            f"Body starts: {snippet!r}",
            response=r,
        ) from e

    try:
        return r.json()
    except ValueError as e:
        snippet = (r.text or "")[:300]
        raise RuntimeError(
            f"OpenAlex returned non-JSON for {r.url}. "
            f"Content-Type={r.headers.get('Content-Type')!r}. "
            f"Body starts: {snippet!r}"
        ) from e


def fetch_openalex_topics_page(
    *,
    page: int = 1,
    per_page: int = 200,
    sort: str | None = None,
    session: requests.Session | None = None,
) -> dict:
    """
    Fetch one page of OpenAlex topics.

    Returns the full payload with keys: meta, results, group_by.
    """
    if page < 1:
        raise ValueError("page must be >= 1")
    if per_page < 1 or per_page > 200:
        raise ValueError("per_page must be between 1 and 200 (OpenAlex max is 200)")

    s = session or make_openalex_session()

    params: dict[str, object] = {"page": page, "per-page": per_page}
    if sort:
        params["sort"] = sort

    payload = get_openalex_record(
        "/topics",
        session=s,
        params=params,
        timeout=OA_TIMEOUT_SECS,
    )

    if payload is None:
        raise requests.HTTPError("OpenAlex /topics request returned 404")

    return payload


def fetch_all_openalex_topics(
    *,
    per_page: int = 200,
    sort: str | None = None,
    session: requests.Session | None = None,
    max_pages: int | None = None,
) -> list[dict]:
    """
    Fetch ALL OpenAlex topics by paging through /topics.

    Args:
        per_page: Number of results per page (OpenAlex max is 200).
        sort: Optional sort string, e.g. "cited_by_count:desc".
        session: Optional shared OpenAlex session (recommended).
        max_pages: Optional safety limit (useful during testing).

    Returns:
        List of topic records (each is a dict from payload["results"]).
    """
    s = session or make_openalex_session()

    first = fetch_openalex_topics_page(page=1, per_page=per_page, sort=sort, session=s)
    results = list(first.get("results", []) or [])

    meta = first.get("meta") or {}
    total_count = meta.get("count")
    if isinstance(total_count, int) and total_count >= 0:
        total_pages = math.ceil(total_count / per_page) if per_page else 1

    if max_pages is not None:
        total_pages = min(total_pages, max_pages)

    for page in range(2, total_pages + 1):
        payload = fetch_openalex_topics_page(
            page=page, per_page=per_page, sort=sort, session=s
        )
        page_results = payload.get("results", []) or []
        if not page_results:
            break
        results.extend(page_results)

    return results
