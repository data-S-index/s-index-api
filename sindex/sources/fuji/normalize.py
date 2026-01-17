"""Normalization functions for F-UJI evaluation responses.

This module provides functions to extract and normalize data from
raw F-UJI API responses into a standardized format.
"""

from __future__ import annotations

from typing import Any

from sindex.core.dates import _norm_date_iso


def fuji_report_from_response(results: dict[str, Any]) -> dict[str, Any]:
    """Extract and normalize key information from a raw F-UJI response.

    This function processes the raw JSON response from F-UJI and extracts
    the essential FAIR evaluation metrics into a compact, standardized format.

    Args:
        results: Raw JSON response dictionary from F-UJI evaluation API

    Returns:
        dict: Normalized FAIR report containing:
            - fair_score: Overall FAIR score (0-100)
            - evaluation_date: ISO-formatted date when evaluation was performed
            - fuji_metric_version: Version of the FAIR metrics used
            - fuji_software_version: Version of F-UJI software used
    """
    # Extract summary section from F-UJI response
    summary = results.get("summary") or {}
    score_percent = summary.get("score_percent") or {}

    return {
        "fair_score": (score_percent.get("FAIR")),
        "evaluation_date": _norm_date_iso(results.get("end_timestamp")),
        "fuji_metric_version": results.get("metric_version"),
        "fuji_software_version": results.get("software_version"),
    }
