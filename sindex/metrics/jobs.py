"""Job functions for metrics processing.

This module contains job functions for generating dataset index series.
"""

import os

import duckdb


from sindex.core.dates import _norm_date_iso, _to_datetime_utc
from sindex.core.http import _is_reachable, is_url
from sindex.core.ids import _norm_dataset_id, _norm_doi, _norm_doi_url, is_working_doi
from sindex.metrics.citations import merge_citations_dicts
from sindex.metrics.datasetindex import dataset_index_timeseries
from sindex.metrics.mentions import merge_mentions_dicts
from sindex.metrics.normalization import get_topic_year_norm_factors
from sindex.sources.datacite.discovery import get_datacite_doi_record
from sindex.sources.datacite.jobs import find_citations_dc_from_citation_block
from sindex.sources.datacite.normalize import slim_datacite_record
from sindex.sources.fuji.jobs import fair_evaluation_report
from sindex.sources.github.discovery import find_github_mentions_for_dataset_id
from sindex.sources.mdc.jobs import find_citations_mdc_duckdb
from sindex.sources.openalex.jobs import find_citations_oa, get_primary_topic_for_doi


def default_mdc_db_path():
    """Get the default path to the MDC (Medical Data Commons) DuckDB database.

    Returns:
        str: Absolute path to the MDC DuckDB database file
    """
    print("[JOBS] Getting MDC DuckDB path")
    current_dir = os.getcwd()
    mdc_path = os.path.join(current_dir, "input", "mdc")
    db_path = os.path.join(mdc_path, "mdc_index.duckdb")
    print(f"[JOBS] Using MDC DuckDB file path: {db_path}")
    return db_path


def default_norm_db_path():
    """Get the default path to the normalization factors DuckDB database.

    Returns:
        str: Absolute path to the normalization DuckDB database file
    """
    print("[JOBS] Getting normalization DuckDB path")
    current_dir = os.getcwd()
    norm_path = os.path.join(current_dir, "input", "mock_norm")
    db_path = os.path.join(norm_path, "mock_norm.duckdb")
    print(f"[JOBS] Using normalization DuckDB file path: {db_path}")
    return db_path


def dataset_index_series_from_doi(doi):
    """Generate a complete dataset index series report from a DOI.

    This function orchestrates the full pipeline for processing a dataset DOI:
    - Validates and normalizes the DOI
    - Fetches metadata from DataCite
    - Retrieves OpenAlex topic classification
    - Evaluates FAIR scores using F-UJI
    - Collects citations from multiple sources (MDC, OpenAlex, DataCite)
    - Collects mentions from GitHub
    - Retrieves normalization factors
    - Calculates dataset index time series

    Args:
        doi: DOI string (e.g., "10.13026/kpb9-mt58") or DOI URL

    Returns:
        dict: Complete dataset report containing:
            - input_doi, norm_doi, norm_doi_url: Identifier information
            - metadata: Slimmed DataCite record
            - topic: OpenAlex topic classification
            - fair: FAIR evaluation report
            - citations: Merged citations from all sources
            - mentions: Merged mentions from GitHub
            - normalization_factors: Topic/year normalization factors
            - dataset_index_series: Time series of dataset index values

    Raises:
        ValueError: If DOI format is invalid or DOI does not resolve
    """
    print(f"[JOBS] dataset_index_series_from_doi - Starting processing for DOI: {doi}")
    # Validate DOI and normalize
    print(
        f"[JOBS] dataset_index_series_from_doi - Validating and normalizing DOI: {doi}"
    )
    norm_doi = _norm_doi(doi)
    if not norm_doi:
        print(
            f"[JOBS] dataset_index_series_from_doi - ERROR: Invalid DOI format: {doi}"
        )
        raise ValueError(f"Invalid DOI format: {doi}")

    norm_doi_url = _norm_doi_url(norm_doi)
    print(
        f"[JOBS] dataset_index_series_from_doi - Checking if DOI resolves: {norm_doi_url}"
    )
    if not is_working_doi(norm_doi_url, allow_blocked=True):
        print(
            f"[JOBS] dataset_index_series_from_doi - ERROR: DOI does not resolve: {norm_doi_url}"
        )
        raise ValueError(f"'{norm_doi_url}' does not appear to resolve.")

    dataset_report = {}
    dataset_report["input_doi"] = doi
    dataset_report["norm_doi"] = norm_doi
    dataset_report["norm_doi_url"] = norm_doi_url

    # Get metadata from DataCite and create slim version
    print(
        f"[JOBS] dataset_index_series_from_doi - Fetching DataCite record for: {norm_doi}"
    )
    rec = get_datacite_doi_record(norm_doi)

    if not rec:
        # Happens if DOI is valid but not found in DataCite (e.g. DOI of a manuscript)
        print(
            f"[JOBS] dataset_index_series_from_doi - DataCite record not found for: {norm_doi}"
        )
        slim = None
        pubdate = None
        pubyear = None
        citations_block = None
    else:
        print(
            "[JOBS] dataset_index_series_from_doi - DataCite record found, creating slim version"
        )
        slim = slim_datacite_record(rec)
        pubdate = slim.get("publication_date")
        pub_dt = _to_datetime_utc(pubdate)
        pubyear = pub_dt.year if pub_dt else None
        citations_block = slim.get("citations")
        print(
            f"[JOBS] dataset_index_series_from_doi - Publication date: {pubdate}, Year: {pubyear}"
        )

    dataset_report["metadata"] = slim

    # Get domain (OpenAlex topic)
    print(
        f"[JOBS] dataset_index_series_from_doi - Fetching OpenAlex topic for: {norm_doi}"
    )
    topic_result = get_primary_topic_for_doi(norm_doi)

    topic_id = None
    dataset_report["topic"] = None

    if topic_result and topic_result.get("topic_score", 0.0) > 0.5:
        dataset_report["topic"] = topic_result
        topic_id = topic_result.get("topic_id")
        print(
            f"[JOBS] dataset_index_series_from_doi - Topic found: {topic_id} (score: {topic_result.get('topic_score')})"
        )
    else:
        print("[JOBS] dataset_index_series_from_doi - No valid topic found")

    # Get F-UJI FAIR score
    print(
        f"[JOBS] dataset_index_series_from_doi - Fetching F-UJI FAIR evaluation for: {norm_doi_url}"
    )
    fair_report = fair_evaluation_report(norm_doi_url)
    # fair_report = None
    dataset_report["fair"] = fair_report

    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
            print(f"[JOBS] dataset_index_series_from_doi - FAIR score: {fair_score}")
        except Exception:
            fair_score = None
            print("[JOBS] dataset_index_series_from_doi - Could not parse FAIR score")
    else:
        print("[JOBS] dataset_index_series_from_doi - No FAIR score available")

    # Citations
    print("[JOBS] dataset_index_series_from_doi - Searching for citations")
    citations_list = []
    print("[JOBS] dataset_index_series_from_doi - Searching MDC citations")
    citations_mdc = find_citations_mdc_duckdb(
        doi, dataset_pub_date=pubdate, db_path=default_mdc_db_path()
    )
    if citations_mdc:
        print(
            f"[JOBS] dataset_index_series_from_doi - Found {len(citations_mdc)} MDC citations"
        )
        citations_list.append(citations_mdc)
    else:
        print("[JOBS] dataset_index_series_from_doi - No MDC citations found")

    print("[JOBS] dataset_index_series_from_doi - Searching OpenAlex citations")
    citations_oa = find_citations_oa(doi, dataset_pub_date=pubdate)
    if citations_oa:
        print(
            f"[JOBS] dataset_index_series_from_doi - Found {len(citations_oa)} OpenAlex citations"
        )
        citations_list.append(citations_oa)
    else:
        print("[JOBS] dataset_index_series_from_doi - No OpenAlex citations found")

    if citations_block:
        print(
            "[JOBS] dataset_index_series_from_doi - Processing DataCite citation block"
        )
        citations_dc = find_citations_dc_from_citation_block(
            doi, citations_block, dataset_pub_date=pubdate
        )
        if citations_dc:
            print(
                f"[JOBS] dataset_index_series_from_doi - Found {len(citations_dc)} DataCite citations"
            )
            citations_list.append(citations_dc)

    citations = merge_citations_dicts(citations_list) if citations_list else None
    if citations:
        print(
            f"[JOBS] dataset_index_series_from_doi - Total merged citations: {len(citations)}"
        )
    else:
        print("[JOBS] dataset_index_series_from_doi - No citations found")
    dataset_report["citations"] = citations

    # Mentions
    print("[JOBS] dataset_index_series_from_doi - Searching for mentions")
    mentions_list = []
    print("[JOBS] dataset_index_series_from_doi - Searching GitHub mentions")
    mentions_github = find_github_mentions_for_dataset_id(doi, dataset_pub_date=pubdate)
    if mentions_github:
        print(
            f"[JOBS] dataset_index_series_from_doi - Found {len(mentions_github)} GitHub mentions"
        )
        mentions_list.append(mentions_github)
    else:
        print("[JOBS] dataset_index_series_from_doi - No GitHub mentions found")

    mentions = merge_mentions_dicts(mentions_list)
    if mentions:
        print(
            f"[JOBS] dataset_index_series_from_doi - Total merged mentions: {len(mentions)}"
        )
        dataset_report["mentions"] = mentions
    else:
        print("[JOBS] dataset_index_series_from_doi - No mentions found")
        dataset_report["mentions"] = None

    # Normalization factors
    print(
        f"[JOBS] dataset_index_series_from_doi - Fetching normalization factors "
        f"(topic_id: {topic_id}, year: {pubyear})"
    )
    try:
        norm_db_path = default_norm_db_path()
        with duckdb.connect(norm_db_path) as con:
            norm = get_topic_year_norm_factors(
                con,
                topic_id=topic_id,
                year=pubyear,
                table="topic_norm_factors_mock",
            )
        print(f"[JOBS] dataset_index_series_from_doi - Normalization factors: {norm}")
    except KeyError:
        norm = None
        print(
            "[JOBS] dataset_index_series_from_doi - No normalization factors found (KeyError)"
        )

    dataset_report["normalization_factors"] = norm

    # Dataset Index series
    print("[JOBS] dataset_index_series_from_doi - Calculating dataset index series")
    Fi = (float(fair_score) / 100.0) if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 0.5
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0
    print(
        f"[JOBS] dataset_index_series_from_doi - Using factors: Fi={Fi}, FT={FT}, CTw={CTw}, MTw={MTw}"
    )

    dataset_index_series = dataset_index_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubdate=pubdate,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )

    if dataset_index_series:
        print(
            f"[JOBS] dataset_index_series_from_doi - Generated dataset index series "
            f"with {len(dataset_index_series)} data points"
        )
        dataset_report["dataset_index_series"] = dataset_index_series
    else:
        print(
            "[JOBS] dataset_index_series_from_doi - No dataset index series generated"
        )
        dataset_report["dataset_index_series"] = None

    print(f"[JOBS] dataset_index_series_from_doi - Completed processing for DOI: {doi}")
    return dataset_report


def dataset_index_series_from_url(
    url: str,
    *,
    identifier: str | None = None,
    pubdate: str | None = None,
    topic_id: str | None = None,
) -> dict:
    """
    Build a dataset_report starting from a URL (not a DOI).

    This function intentionally skips DOI-dependent sources:
      - get_datacite_doi_record
      - get_primary_topic_for_doi
      - find_citations_oa
      - find_citations_dc_from_citation_block

    Args:
        url: Required dataset landing page URL.
        identifier: Optional dataset identifier to use for MDC/GitHub searches
                    (if omitted, we only use the URL as the identifier).
        pubdate: Optional publication date in any reasonable format; normalized to ISO if provided.
        topic_id: Optional OpenAlex topic id (e.g. "https://openalex.org/T12345" or "T12345").

    Returns:
        dataset_report dict with citations/mentions + normalization + dataset_index_series.
    """
    print(f"[JOBS] dataset_index_series_from_url - Starting processing for URL: {url}")
    print(
        f"[JOBS] dataset_index_series_from_url - Parameters: identifier={identifier}, "
        f"pubdate={pubdate}, topic_id={topic_id}"
    )
    # Validate URL
    if not isinstance(url, str) or not url.strip():
        print(
            "[JOBS] dataset_index_series_from_url - ERROR: url must be a non-empty string"
        )
        raise ValueError("url must be a non-empty string")
    url = url.strip()

    if not url.startswith(("http://", "https://")):
        print(
            f"[JOBS] dataset_index_series_from_url - ERROR: Invalid URL format: {url}"
        )
        raise ValueError(
            f"Invalid URL format: {url} (must start with http:// or https://)"
        )
    if not is_url(url):
        print(
            f"[JOBS] dataset_index_series_from_url - ERROR: Invalid URL format: {url}"
        )
        raise ValueError(f"Invalid URL format: {url}")
    print(f"[JOBS] dataset_index_series_from_url - Checking if URL is reachable: {url}")
    if not _is_reachable(url):
        print(
            f"[JOBS] dataset_index_series_from_url - ERROR: URL is not reachable: {url}"
        )
        raise ValueError(f"'{url}' does not appear to be reachable.")

    print("[JOBS] dataset_index_series_from_url - Normalizing URL and identifier")
    norm_url = _norm_dataset_id(url)
    norm_identifier = _norm_dataset_id(identifier)
    print(
        f"[JOBS] dataset_index_series_from_url - Normalized URL: {norm_url}, "
        f"Normalized identifier: {norm_identifier}"
    )

    dataset_report = {}
    dataset_report["input_url"] = url
    dataset_report["norm_url"] = norm_url
    dataset_report["input_identifier"] = identifier
    dataset_report["norm_identifier"] = norm_identifier

    # Non metadata, resolve pubdate
    if pubdate:
        print(
            f"[JOBS] dataset_index_series_from_url - Normalizing publication date: {pubdate}"
        )
        try:
            pubdate = _norm_date_iso(pubdate)
            print(
                f"[JOBS] dataset_index_series_from_url - Normalized publication date: {pubdate}"
            )
        except ValueError as e:
            print(
                f"[JOBS] dataset_index_series_from_url - ERROR: Invalid pubdate '{pubdate}': {e}"
            )
            raise ValueError(f"Invalid pubdate '{pubdate}': {e}") from e
    pub_dt = _to_datetime_utc(pubdate)
    pubyear = pub_dt.year if pub_dt else None
    print(
        f"[JOBS] dataset_index_series_from_url - Publication date: {pubdate}, Year: {pubyear}"
    )

    dataset_report["metadata"] = None

    # Domain (OpenALex topic)
    dataset_report["topic"] = topic_id
    print(f"[JOBS] dataset_index_series_from_url - Using topic_id: {topic_id}")

    # Get F-UJI FAIR score
    print(
        f"[JOBS] dataset_index_series_from_url - Fetching F-UJI FAIR evaluation for: {url}"
    )
    fair_report = fair_evaluation_report(url)
    dataset_report["fair"] = fair_report

    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
            print(f"[JOBS] dataset_index_series_from_url - FAIR score: {fair_score}")
        except Exception:
            fair_score = None
            print("[JOBS] dataset_index_series_from_url - Could not parse FAIR score")
    else:
        print("[JOBS] dataset_index_series_from_url - No FAIR score available")

    # Citations (MDC only)
    print("[JOBS] dataset_index_series_from_url - Searching for citations (MDC only)")
    citations_list: list[list[dict]] = []

    print("[JOBS] dataset_index_series_from_url - Searching MDC citations")
    citations_mdc = find_citations_mdc_duckdb(
        url, dataset_pub_date=pubdate, db_path=default_mdc_db_path()
    )
    if citations_mdc:
        print(
            f"[JOBS] dataset_index_series_from_url - Found {len(citations_mdc)} MDC citations"
        )
        citations_list.append(citations_mdc)
    else:
        print("[JOBS] dataset_index_series_from_url - No MDC citations found")

    citations = merge_citations_dicts(citations_list) if citations_list else None
    if citations:
        print(
            f"[JOBS] dataset_index_series_from_url - Total merged citations: {len(citations)}"
        )
    else:
        print("[JOBS] dataset_index_series_from_url - No citations found")
    dataset_report["citations"] = citations

    # Mentions
    print("[JOBS] dataset_index_series_from_url - Searching for mentions")
    mentions_list: list[list[dict]] = []

    print("[JOBS] dataset_index_series_from_url - Searching GitHub mentions")
    mentions_github = find_github_mentions_for_dataset_id(
        url,
        dataset_pub_date=pubdate,
    )
    if mentions_github:
        print(
            f"[JOBS] dataset_index_series_from_url - Found {len(mentions_github)} GitHub mentions"
        )
        mentions_list.append(mentions_github)
    else:
        print("[JOBS] dataset_index_series_from_url - No GitHub mentions found")

    mentions = merge_mentions_dicts(mentions_list) if mentions_list else None
    if mentions:
        print(
            f"[JOBS] dataset_index_series_from_url - Total merged mentions: {len(mentions)}"
        )
        dataset_report["mentions"] = mentions
    else:
        print("[JOBS] dataset_index_series_from_url - No mentions found")
        dataset_report["mentions"] = mentions

    # 7) Normalization factors
    print(
        f"[JOBS] dataset_index_series_from_url - Fetching normalization factors "
        f"(topic_id: {topic_id}, year: {pubyear})"
    )
    try:
        norm_db_path = default_norm_db_path()
        with duckdb.connect(norm_db_path) as con:
            norm = get_topic_year_norm_factors(
                con,
                topic_id=topic_id,
                year=pubyear,
                table="topic_norm_factors_mock",
            )
        print(f"[JOBS] dataset_index_series_from_url - Normalization factors: {norm}")
    except KeyError:
        norm = None
        print(
            "[JOBS] dataset_index_series_from_url - No normalization factors found (KeyError)"
        )

    dataset_report["normalization_factors"] = norm

    # Dataset Index series
    print("[JOBS] dataset_index_series_from_url - Calculating dataset index series")
    Fi = (float(fair_score) / 100.0) if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 0.5
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0
    print(
        f"[JOBS] dataset_index_series_from_url - Using factors: "
        f"Fi={Fi}, FT={FT}, CTw={CTw}, MTw={MTw}"
    )

    dataset_index_series = dataset_index_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubdate=pubdate,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )

    if dataset_index_series:
        print(
            f"[JOBS] dataset_index_series_from_url - Generated dataset index series "
            f"with {len(dataset_index_series)} data points"
        )
        dataset_report["dataset_index_series"] = dataset_index_series
    else:
        print(
            "[JOBS] dataset_index_series_from_url - No dataset index series generated"
        )
        dataset_report["dataset_index_series"] = None
    print(f"[JOBS] dataset_index_series_from_url - Completed processing for URL: {url}")
    return dataset_report
