"""OpenAlex citation and topic jobs.

Fetches OpenAlex records by DOI, citing works, and primary topic; normalizes
to citation objects and topic dicts for the dataset index pipeline.
"""

from __future__ import annotations

from typing import Dict, List, Optional

import requests

from sindex.core.dates import _norm_date_iso, get_realistic_date

from .client import make_openalex_session
from .discovery import (
    extract_openalex_id,
    get_all_citing_works_oa,
    get_openalex_doi_record,
)
from .normalize import openalex_citing_works_to_citations


def find_citations_oa(
    doi: str,
    *,
    dataset_pub_date: str | None = None,
    email: Optional[str] = None,
    session: Optional[requests.Session] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """Find OpenAlex citations for a dataset DOI.

    Looks up the OpenAlex work by DOI, gets cited_by_count and OpenAlex ID,
    fetches all citing works, then normalizes to citation objects with
    citation_weight from dataset_pub_date and citation date.

    Args:
        doi: Dataset DOI (canonical or URL form).
        dataset_pub_date: Optional publication date for weight calculation.
        email: Optional mailto for polite OpenAlex requests.
        session: Optional requests session.
        api_key: Optional OpenAlex API key.

    Returns:
        List of citation dicts; empty if no record or no citations.
    """
    # Step 1: Normalize dataset_pub_date for weight calculation
    if dataset_pub_date:
        try:
            dataset_pub_date = _norm_date_iso(dataset_pub_date)
            dataset_pub_date = get_realistic_date(dataset_pub_date)
        except ValueError:
            dataset_pub_date = None

    s = session or make_openalex_session(api_key=api_key)

    # Step 2: Resolve DOI to OpenAlex work
    record = get_openalex_doi_record(doi, session=s, mailto=email)
    if not record:
        return []

    if not record.get("cited_by_count"):
        return []

    # Step 3: Get OpenAlex ID and fetch citing works
    openalex_id = extract_openalex_id(record)
    if not openalex_id:
        return []

    citing_records = get_all_citing_works_oa(openalex_id, session=s, mailto=email)

    # Step 4: Normalize citing works to citation objects (with citation_weight)
    return openalex_citing_works_to_citations(
        citing_records,
        dataset_id=doi,
        dataset_pub_date=dataset_pub_date,
    )


def get_primary_topic_for_doi(doi: str) -> dict | None:
    """Get the primary OpenAlex topic for a work by DOI.

    Fetches the OpenAlex work by DOI and returns a flat dict with
    topic_id, topic_name, topic_score, and hierarchy (subfield, field, domain)
    if primary_topic exists; otherwise None.

    Args:
        doi: Work DOI (canonical or URL form).

    Returns:
        Flat dict with topic + hierarchy info, or None.
    """
    work = get_openalex_doi_record(doi)
    if not work:
        return None

    pt = work.get("primary_topic")
    if not pt:
        return None

    return {
        "doi": doi,
        "work_id": work.get("id"),
        "topic_id": pt.get("id"),
        "topic_name": pt.get("display_name"),
        "topic_score": pt.get("score"),
        "subfield_name": pt.get("subfield", {}).get("display_name"),
        "field_name": pt.get("field", {}).get("display_name"),
        "domain_name": pt.get("domain", {}).get("display_name"),
    }
