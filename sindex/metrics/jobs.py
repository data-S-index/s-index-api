"""Dataset index pipeline jobs.

Builds dataset reports (metadata, citations, mentions, FAIR scores, normalization)
from DOI or URL inputs and computes dataset index time series.
"""

import os
from concurrent.futures import ThreadPoolExecutor

import duckdb

from sindex.core.dates import _norm_date_iso, _to_datetime_utc, get_best_dataset_date
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

# Max worker threads for parallel I/O (topic, FAIR, citations, mentions).
DEFAULT_MAX_WORKERS = 6


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
    """Build a full dataset report and index series from a DOI.

    Fetches metadata (DataCite), topic (OpenAlex), FAIR score (F-UJI),
    citations (MDC, OpenAlex, DataCite), mentions (GitHub), normalization
    factors, and computes the dataset index time series.

    Args:
        doi: Dataset DOI (any format; will be normalized).

    Returns:
        dict: dataset_report with keys: input_doi, norm_doi, norm_doi_url,
              metadata, topic, fair, citations, mentions, normalization_factors,
              dataset_index_series.

    Raises:
        ValueError: If DOI is invalid or does not resolve.
    """
    print(f"[JOBS] Step 0: Building dataset report for DOI: {doi}")
    # --- Step 1: Validate DOI and normalize ---
    print("[JOBS] Step 1: Validating and normalizing DOI...")
    norm_doi = _norm_doi(doi)
    if not norm_doi:
        raise ValueError(f"Invalid DOI format: {doi}")

    norm_doi_url = _norm_doi_url(norm_doi)
    if not is_working_doi(norm_doi_url, allow_blocked=True):
        raise ValueError(f"'{norm_doi_url}' does not appear to resolve.")
    print(f"[JOBS] Step 1 done: norm_doi={norm_doi}, norm_doi_url={norm_doi_url}")

    dataset_report = {}
    dataset_report["input_doi"] = doi
    dataset_report["norm_doi"] = norm_doi
    dataset_report["norm_doi_url"] = norm_doi_url

    # --- Step 2: Get metadata from DataCite and create slim version ---
    print("[JOBS] Step 2: Fetching DataCite metadata...")
    rec = get_datacite_doi_record(norm_doi)

    if not rec:
        # Happens if DOI is valid but not found in DataCite (e.g. DOI of a manuscript)
        print("[JOBS] Step 2: No DataCite record; continuing without metadata.")
        slim = None
        pubdate = None
        pubyear = None
        citations_block = None
    else:
        slim = slim_datacite_record(rec)
        publication_date = slim.get("publication_date")
        created_date = slim.get("created_date")
        pubdate = get_best_dataset_date(publication_date, created_date)
        if pubdate:
            pub_dt = _to_datetime_utc(pubdate)
            pubyear = pub_dt.year if pub_dt else None
        else:
            pubyear = None
        citations_block = slim.get("citations")
        print(
            f"[JOBS] Step 2 done: metadata slimmed, pubdate={pubdate}, pubyear={pubyear}"
        )

    dataset_report["metadata"] = slim

    # --- Steps 3–6: Run independent I/O in parallel (topic, FAIR, citations, mentions) ---
    print("[JOBS] Steps 3–6: Running topic, FAIR, citations, mentions in parallel...")
    mdc_db_path = default_mdc_db_path()

    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        future_topic = executor.submit(get_primary_topic_for_doi, norm_doi)
        future_fair = executor.submit(fair_evaluation_report, norm_doi_url)
        future_mdc = executor.submit(
            find_citations_mdc_duckdb,
            doi,
            dataset_pub_date=pubdate,
            db_path=mdc_db_path,
        )
        future_oa = executor.submit(find_citations_oa, doi, dataset_pub_date=pubdate)
        future_github = executor.submit(
            find_github_mentions_for_dataset_id, doi, dataset_pub_date=pubdate
        )
        if citations_block:
            future_dc = executor.submit(
                find_citations_dc_from_citation_block,
                doi,
                citations_block,
                dataset_pub_date=pubdate,
            )
        else:
            future_dc = None

        print("[JOBS] Waiting for topic (OpenAlex)...")
        topic_result = future_topic.result()
        print("[JOBS] Topic done.")
        print("[JOBS] Waiting for FAIR (F-UJI)...")
        fair_report = future_fair.result()
        print("[JOBS] FAIR done.")
        print("[JOBS] Waiting for citations (MDC)...")
        citations_mdc = future_mdc.result()
        print("[JOBS] MDC citations done.")
        print("[JOBS] Waiting for citations (OpenAlex)...")
        citations_oa = future_oa.result()
        print("[JOBS] OpenAlex citations done.")
        print("[JOBS] Waiting for mentions (GitHub)...")
        mentions_github = future_github.result()
        print("[JOBS] GitHub mentions done.")
        if future_dc:
            print("[JOBS] Waiting for citations (DataCite block)...")
            citations_dc = future_dc.result()
            print("[JOBS] DataCite citations done.")
        else:
            citations_dc = None

    print("[JOBS] Steps 3–6 parallel work finished; merging results...")

    # Step 3: Topic
    topic_id = None
    dataset_report["topic"] = None
    if topic_result and topic_result.get("topic_score", 0.0) > 0.5:
        dataset_report["topic"] = topic_result
        topic_id = topic_result.get("topic_id")
        print(
            f"[JOBS] Step 3 done: topic_id={topic_id}, score={topic_result.get('topic_score')}"
        )
    else:
        print("[JOBS] Step 3 done: No topic or score ≤0.5.")

    # Step 4: FAIR
    dataset_report["fair"] = fair_report
    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
            print(f"[JOBS] Step 4 done: FAIR score={fair_score}")
        except Exception:
            fair_score = None
            print("[JOBS] Step 4 done: Could not parse FAIR score.")
    else:
        print("[JOBS] Step 4 done: No FAIR score in report.")

    # Step 5: Merge citations
    citations_list = []
    if citations_mdc:
        citations_list.append(citations_mdc)
    if citations_oa:
        citations_list.append(citations_oa)
    if citations_dc:
        citations_list.append(citations_dc)
    citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations
    total_cit = len(citations) if citations else 0
    print(f"[JOBS] Step 5 done: Citations merged (MDC, OA, DC), total keys={total_cit}")

    # Step 6: Mentions
    mentions_list = [mentions_github] if mentions_github else []
    mentions = merge_mentions_dicts(mentions_list)
    if mentions:
        dataset_report["mentions"] = mentions
        print(f"[JOBS] Step 6 done: Mentions merged, total={len(mentions)}")
    else:
        dataset_report["mentions"] = None
        print("[JOBS] Step 6 done: No mentions.")

    # --- Step 7: Normalization factors from topic/year ---
    print("[JOBS] Step 7: Loading normalization factors (topic_id, year)...")
    try:
        with duckdb.connect(default_norm_db_path()) as con:
            norm = get_topic_year_norm_factors(
                con,
                topic_id=topic_id,
                year=pubyear,
                table="topic_norm_factors_mock",
            )
        print("[JOBS] Step 7 done: norm loaded (FT, CTw, MTw).")
    except KeyError:
        norm = None
        print("[JOBS] Step 7 done: No normalization factors (KeyError).")

    dataset_report["normalization_factors"] = norm

    # --- Step 8: Compute dataset index time series ---
    print("[JOBS] Step 8: Computing dataset index series (Fi, FT, CTw, MTw)...")
    Fi = (float(fair_score) / 100.0) if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 0.5
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0

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
        dataset_report["dataset_index_series"] = dataset_index_series
        print(f"[JOBS] Step 8 done: Dataset index series computed (Fi={Fi:.3f}).")
    else:
        dataset_report["dataset_index_series"] = None
        print("[JOBS] Step 8 done: No dataset index series.")

    print("[JOBS] All steps complete. Dataset report ready.")
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
    print(f"[JOBS] Step 0: Building dataset report for URL: {url}")
    # --- Step 1: Validate URL ---
    print("[JOBS] Step 1: Validating URL...")
    if not isinstance(url, str) or not url.strip():
        raise ValueError("url must be a non-empty string")
    url = url.strip()

    if not url.startswith(("http://", "https://")):
        raise ValueError(
            f"Invalid URL format: {url} (must start with http:// or https://)"
        )
    if not is_url(url):
        raise ValueError(f"Invalid URL format: {url}")

    if not _is_reachable(url):
        raise ValueError(f"'{url}' does not appear to be reachable.")
    print("[JOBS] Step 1 done: URL valid and reachable.")

    norm_url = _norm_dataset_id(url)
    norm_identifier = _norm_dataset_id(identifier)

    dataset_report = {}
    dataset_report["input_url"] = url
    dataset_report["norm_url"] = norm_url
    dataset_report["input_identifier"] = identifier
    dataset_report["norm_identifier"] = norm_identifier

    # --- Step 2: Resolve pubdate (no DataCite metadata for URL path) ---
    print("[JOBS] Step 2: Resolving publication date...")
    if pubdate:
        try:
            pubdate = _norm_date_iso(pubdate)
        except ValueError as e:
            raise ValueError(f"Invalid pubdate '{pubdate}': {e}") from e
    pub_dt = _to_datetime_utc(pubdate)
    pubyear = pub_dt.year if pub_dt else None
    print(f"[JOBS] Step 2 done: pubdate={pubdate}, pubyear={pubyear}")

    dataset_report["metadata"] = None
    dataset_report["topic"] = topic_id

    # --- Steps 3–5: Run F-UJI, MDC citations, GitHub mentions in parallel ---
    print(
        "[JOBS] Steps 3–5: Running F-UJI, MDC citations, GitHub mentions in parallel..."
    )
    mdc_db_path = default_mdc_db_path()

    with ThreadPoolExecutor(max_workers=DEFAULT_MAX_WORKERS) as executor:
        future_fair = executor.submit(fair_evaluation_report, url)
        future_mdc = executor.submit(
            find_citations_mdc_duckdb,
            url,
            dataset_pub_date=pubdate,
            db_path=mdc_db_path,
        )
        future_github = executor.submit(
            find_github_mentions_for_dataset_id,
            url,
            dataset_pub_date=pubdate,
        )

        fair_report = future_fair.result()
        citations_mdc = future_mdc.result()
        mentions_github = future_github.result()

    # Step 3: FAIR
    dataset_report["fair"] = fair_report
    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
            print(f"[JOBS] Step 3 done: FAIR score={fair_score}")
        except Exception:
            fair_score = None
            print("[JOBS] Step 3 done: Could not parse FAIR score.")
    else:
        print("[JOBS] Step 3 done: No FAIR score.")

    # Step 4: Citations (MDC only)
    citations_list = [citations_mdc] if citations_mdc else []
    citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations
    print(f"[JOBS] Step 4 done: Total citations={len(citations) if citations else 0}")

    # Step 5: Mentions
    mentions_list = [mentions_github] if mentions_github else []
    mentions = merge_mentions_dicts(mentions_list) if mentions_list else None
    dataset_report["mentions"] = mentions
    print(f"[JOBS] Step 5 done: Total mentions={len(mentions) if mentions else 0}")

    # --- Step 6: Normalization factors ---
    print("[JOBS] Step 6: Loading normalization factors...")
    try:
        with duckdb.connect(default_norm_db_path()) as con:
            norm = get_topic_year_norm_factors(
                con,
                topic_id=topic_id,
                year=pubyear,
                table="topic_norm_factors_mock",
            )
        print("[JOBS] Step 6 done: Normalization factors loaded.")
    except KeyError:
        norm = None
        print("[JOBS] Step 6 done: No normalization factors (KeyError).")

    dataset_report["normalization_factors"] = norm

    # --- Step 7: Compute dataset index time series ---
    print("[JOBS] Step 7: Computing dataset index series...")
    Fi = (float(fair_score) / 100.0) if fair_score is not None else 0.0

    FT = norm["FT"] if norm else 0.5
    CTw = norm["CTw"] if norm else 1.0
    MTw = norm["MTw"] if norm else 1.0

    dataset_index_series = dataset_index_timeseries(
        Fi=Fi,
        citations=citations,
        mentions=mentions,
        pubdate=pubdate,
        FT=FT,
        CTw=CTw,
        MTw=MTw,
    )

    dataset_report["dataset_index_series"] = dataset_index_series or None
    print(f"[JOBS] Step 7 done: Dataset index series computed (Fi={Fi:.3f}).")
    print("[JOBS] All steps complete. Dataset report (from URL) ready.")
    return dataset_report
