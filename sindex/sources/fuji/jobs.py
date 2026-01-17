"""F-UJI job functions."""

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
    """
    Wrapper:
      - discovery.fair_evaluate_doi_url() -> raw F-UJI JSON
      - normalize.fuji_report_from_response() -> compact FAIR report
    """
    print(f"[FUJI] fair_evaluation_report - Evaluating FAIR score for: {doi_or_url}")
    print(f"[FUJI] fair_evaluation_report - Using base URL: {base_url}")
    raw = fair_evaluate_doi_url(
        doi_or_url,
        base_url=base_url,
        username=username,
        password=password,
        session=session,
    )
    print(
        f"[FUJI] fair_evaluation_report - Raw evaluation received, processing response"
    )
    result = fuji_report_from_response(raw)
    if result and "fair_score" in result:
        print(f"[FUJI] fair_evaluation_report - FAIR score: {result.get('fair_score')}")
    else:
        print(f"[FUJI] fair_evaluation_report - No FAIR score in result")
    return result
