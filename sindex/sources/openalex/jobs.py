"""OpenAlex job functions."""

# pipeline/external/openalex/jobs.py

from __future__ import annotations

from typing import Dict, List, Optional

import requests

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
    """Find all citations for a dataset DOI using OpenAlex.

    This function orchestrates the full pipeline:
    1. Resolves DOI to OpenAlex work record
    2. Extracts OpenAlex ID from the record
    3. Fetches all works that cite this dataset
    4. Normalizes citing works into citation objects

    Args:
        doi: Dataset DOI (canonical or URL format)
        dataset_pub_date: Optional publication date for citation weighting
        email: Optional contact email for OpenAlex API (polite usage)
        session: Optional shared requests.Session
        api_key: Optional OpenAlex API key

    Returns:
        List of citation dictionaries, each containing:
            - dataset_id: Original DOI
            - source: ["openalex"]
            - citation_link: URL to the citing work
            - citation_date: Publication date of citing work
            - citation_weight: Calculated weight based on time difference
    """
    print(f"[OPENALEX] find_citations_oa - Searching citations for DOI: {doi}")
    s = session or make_openalex_session(api_key=api_key)

    print(f"[OPENALEX] find_citations_oa - Fetching OpenAlex record for: {doi}")
    record = get_openalex_doi_record(doi, session=s, mailto=email)
    if not record:
        print(f"[OPENALEX] find_citations_oa - No OpenAlex record found for: {doi}")
        return []

    cited_by_count = record.get("cited_by_count")
    print(f"[OPENALEX] find_citations_oa - Cited by count: {cited_by_count}")
    if not cited_by_count:
        print(f"[OPENALEX] find_citations_oa - No citations found for: {doi}")
        return []

    openalex_id = extract_openalex_id(record)
    if not openalex_id:
        print(
            "[OPENALEX] find_citations_oa - Could not extract OpenAlex ID from record"
        )
        return []
    print(f"[OPENALEX] find_citations_oa - OpenAlex ID: {openalex_id}")

    print(f"[OPENALEX] find_citations_oa - Fetching citing works for: {openalex_id}")
    citing_records = get_all_citing_works_oa(openalex_id, session=s, mailto=email)
    print(f"[OPENALEX] find_citations_oa - Found {len(citing_records)} citing works")

    result = openalex_citing_works_to_citations(
        citing_records,
        dataset_id=doi,
        dataset_pub_date=dataset_pub_date,
    )
    print(f"[OPENALEX] find_citations_oa - Converted to {len(result)} citation objects")
    return result


def get_primary_topic_for_doi(doi: str) -> dict | None:
    """Get the primary OpenAlex topic classification for a dataset DOI.

    Fetches the OpenAlex work record for the given DOI and extracts
    the primary topic classification along with its hierarchy (subfield,
    field, domain).

    Args:
        doi: Dataset DOI (canonical or URL format)

    Returns:
        dict: Topic information containing:
            - doi: Original DOI
            - work_id: OpenAlex work ID
            - topic_id: OpenAlex topic ID
            - topic_name: Display name of the topic
            - topic_score: Confidence score for the topic assignment
            - subfield_name: Subfield display name
            - field_name: Field display name
            - domain_name: Domain display name
        Returns None if no OpenAlex record found or no primary topic exists
    """
    print(f"[OPENALEX] get_primary_topic_for_doi - Fetching topic for DOI: {doi}")
    work = get_openalex_doi_record(doi)
    if not work:
        print(
            f"[OPENALEX] get_primary_topic_for_doi - No OpenAlex record found for: {doi}"
        )
        return None

    pt = work.get("primary_topic")
    if not pt:
        print(
            f"[OPENALEX] get_primary_topic_for_doi - No primary topic found for: {doi}"
        )
        return None

    result = {
        "doi": doi,
        "work_id": work.get("id"),
        "topic_id": pt.get("id"),
        "topic_name": pt.get("display_name"),
        "topic_score": pt.get("score"),
        "subfield_name": pt.get("subfield", {}).get("display_name"),
        "field_name": pt.get("field", {}).get("display_name"),
        "domain_name": pt.get("domain", {}).get("display_name"),
    }
    print(
        f"[OPENALEX] get_primary_topic_for_doi - Found topic: {result.get('topic_id')} "
        f"(score: {result.get('topic_score')})"
    )
    return result
