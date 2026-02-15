# pipeline/external/openalex/jobs.py

from __future__ import annotations

from typing import Dict, List, Optional

import requests

from sindex.core.dates import is_realistic_integer_year
from sindex.core.ids import _norm_doi

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
    dataset_pubyear: int | None = None,
    email: Optional[str] = None,
    session: Optional[requests.Session] = None,
    api_key: Optional[str] = None,
) -> List[Dict[str, object]]:
    """
    Wrapper:
      - discovery: DOI (canonical or url) -> OA dataset record (raw)
      - discovery: OA id -> citing works (raw)
      - normalize: citing works -> citation objects
    """
    target_doi = _norm_doi(doi)
    if not target_doi:
        return []

    if not is_realistic_integer_year(dataset_pubyear):
        dataset_pubyear = None

    s = session or make_openalex_session(api_key=api_key)

    record = get_openalex_doi_record(target_doi, session=s, mailto=email)
    if not record:
        return []

    if not record.get("cited_by_count"):
        return []

    openalex_id = extract_openalex_id(record)
    if not openalex_id:
        return []

    citing_records = get_all_citing_works_oa(openalex_id, session=s, mailto=email)

    return openalex_citing_works_to_citations(
        citing_records,
        dataset_id=doi,
        dataset_pubyear=dataset_pubyear,
    )


def get_primary_topic_for_doi(doi: str) -> dict | None:
    """
    Given a DOI, fetch the OpenAlex record using get_openalex_doi_record
    and return the primary topic if it exists.

    Returns:
        Flat dict with topic + hierarchy info, or None.
    """
    print(
        f"[status] get_primary_topic_for_doi: fetching OpenAlex work record for DOI {doi!r}..."
    )
    work = get_openalex_doi_record(doi)
    if not work:
        print(
            f"[status] get_primary_topic_for_doi: OpenAlex record not found for {doi!r}"
        )
        return None

    print(
        f"[status] get_primary_topic_for_doi: record received, extracting primary topic..."
    )
    pt = work.get("primary_topic")
    if not pt:
        print(
            f"[status] get_primary_topic_for_doi: no primary_topic on work for {doi!r}"
        )
        return None

    print(f"[status] get_primary_topic_for_doi: primary topic found for {doi!r}")
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
