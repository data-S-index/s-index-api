"""High-level job functions for F-UJI FAIR evaluation.

This module provides convenient wrapper functions that combine discovery,
evaluation, and normalization steps into complete workflows.
"""

from __future__ import annotations

import requests

from .constants import DEFAULT_PASSWORD, DEFAULT_USERNAME, FUJI_DEFAULT_BASE_URL
from .discovery import fair_evaluate_doi_url
from .normalize import fuji_report_from_response


def fair_evaluation_report(
    doi_or_url: str,
    *,
    base_url: str = FUJI_DEFAULT_BASE_URL,
    username: str | None = DEFAULT_USERNAME,
    password: str | None = DEFAULT_PASSWORD,
    session: requests.Session | None = None,
) -> dict:
    """Generate a normalized FAIR evaluation report for a dataset.

    This is a high-level wrapper function that:
    1. Resolves the identifier (DOI or URL) to a canonical URL
    2. Calls the F-UJI evaluation API to get raw FAIR metrics
    3. Normalizes the response into a compact report format

    Args:
        doi_or_url: Dataset DOI, DOI URL, or resolvable URL to evaluate
        base_url: Base URL of the F-UJI service (defaults to FUJI_DEFAULT_BASE_URL)
        username: Basic Auth username (defaults to DEFAULT_USERNAME)
        password: Basic Auth password (defaults to DEFAULT_PASSWORD)
        session: Optional requests.Session for connection reuse

    Returns:
        dict: Normalized FAIR report with fair_score, evaluation_date,
              and F-UJI version information
    """
    print(f"[FUJI] fair_evaluation_report - Evaluating FAIR score for: {doi_or_url}")
    print(f"[FUJI] fair_evaluation_report - Using base URL: {base_url}")

    # Step 1: Get raw F-UJI evaluation response
    raw = fair_evaluate_doi_url(
        doi_or_url,
        base_url=base_url,
        username=username,
        password=password,
        session=session,
    )

    # Step 2: Normalize the response into a compact format
    print(
        "[FUJI] fair_evaluation_report - Raw evaluation received, processing response"
    )
    result = fuji_report_from_response(raw)

    # Log the FAIR score if available
    if result and "fair_score" in result:
        print(f"[FUJI] fair_evaluation_report - FAIR score: {result.get('fair_score')}")
    else:
        print("[FUJI] fair_evaluation_report - No FAIR score in result")
    return result
