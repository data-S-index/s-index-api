from concurrent.futures import ThreadPoolExecutor, as_completed
import os

from sindex.core.dates import is_realistic_integer_year
from sindex.utils import timed_block, timed_call
from sindex.core.http import _is_reachable, is_url
from sindex.core.ids import _norm_dataset_id, _norm_doi, _norm_doi_url, is_working_doi
from sindex.metrics.citations import merge_citations_dicts
from sindex.metrics.datasetindex import (
    dataset_index_year_timeseries,
)
from sindex.metrics.mentions import merge_mentions_dicts
from sindex.metrics.normalization import (
    get_subfield_year_norm_factors,
)
from sindex.metrics.topics import get_subfield_id_from_topic_id
from sindex.sources.datacite.discovery import get_datacite_doi_record
from sindex.sources.datacite.jobs import find_citations_dc_from_citation_block
from sindex.sources.datacite.normalize import slim_datacite_record
from sindex.sources.fuji.jobs import fair_evaluation_report
from sindex.sources.github.discovery import find_github_mentions_for_dataset_id
from sindex.sources.mdc.jobs import find_citations_mdc_duckdb
from sindex.sources.openalex.jobs import find_citations_oa, get_primary_topic_for_doi


def default_mdc_db_path():
    current_dir = os.getcwd()
    mdc_path = os.path.join(current_dir, "input", "mdc")
    return os.path.join(mdc_path, "mdc_index.duckdb")


def default_norm_db_path():
    current_dir = os.getcwd()
    db_path = os.path.join(current_dir, "input", "subfield_norm_factors.duckdb")
    return db_path


def topics_table_path():
    current_dir = os.getcwd()
    db_path = os.path.join(current_dir, "input", "openalex_topic_mapping_table.csv")
    return db_path


def dataset_index_series_from_doi(doi):
    with timed_block("Validating and normalizing DOI"):
        norm_doi = _norm_doi(doi)
        if not norm_doi:
            raise ValueError(f"Invalid DOI format: {doi}")

        norm_doi_url = _norm_doi_url(norm_doi)
        if not is_working_doi(norm_doi_url, allow_blocked=True):
            raise ValueError(f"'{norm_doi_url}' does not appear to resolve.")

    dataset_report = {}
    dataset_report["input_doi"] = doi
    dataset_report["norm_doi"] = norm_doi
    dataset_report["norm_doi_url"] = norm_doi_url

    with timed_block("Fetching metadata from DataCite"):
        rec = get_datacite_doi_record(norm_doi)

    if not rec:
        print(
            "[status] DOI not found in DataCite (e.g. manuscript DOI); continuing without metadata."
        )
        slim = None
        pubyear = None
        citations_block = None
    else:
        with timed_block("Building slim DataCite record"):
            slim = slim_datacite_record(rec)
            pubyear = slim.get("pubyear")
            citations_block = slim.get("citations")

    dataset_report["metadata"] = slim

    with timed_block("Getting OpenAlex primary topic"):
        print(f"[status] get_primary_topic_for_doi: starting for norm_doi={norm_doi!r}")
        topic_result = get_primary_topic_for_doi(norm_doi)
        print(f"[status] get_primary_topic_for_doi: finished (result={'ok' if topic_result else 'None'})")

    subfield_id = None
    dataset_report["topic"] = None

    if topic_result:
        try:
            topic_id = topic_result.get("topic_id")
            subfield_id = get_subfield_id_from_topic_id(topics_table_path(), topic_id)
            topic_result["subfield_id"] = subfield_id
            dataset_report["topic"] = topic_result
        except Exception:
            pass

    # After metadata + topic: run FAIR, citations, mentions, norm in parallel
    print(
        "[status] Running FAIR score, citations (MDC/OA/DC), mentions (GitHub), normalization (parallel)..."
    )
    mdc_path = default_mdc_db_path()
    norm_path = default_norm_db_path()

    def _task_fair():
        return ("fair", timed_call("FAIR score", fair_evaluation_report, norm_doi_url))

    def _task_citations_mdc():
        return (
            "citations_mdc",
            timed_call(
                "citations (MDC)",
                find_citations_mdc_duckdb,
                doi,
                dataset_pubyear=pubyear,
                db_path=mdc_path,
            ),
        )

    def _task_citations_oa():
        return (
            "citations_oa",
            timed_call("citations (OpenAlex)", find_citations_oa, doi, dataset_pubyear=pubyear),
        )

    def _task_citations_dc():
        if not citations_block:
            return ("citations_dc", None)
        return (
            "citations_dc",
            timed_call(
                "citations (DataCite)",
                find_citations_dc_from_citation_block,
                doi,
                citations_block,
                dataset_pubyear=pubyear,
            ),
        )

    def _task_mentions_github():
        return (
            "mentions_github",
            timed_call(
                "mentions (GitHub)",
                find_github_mentions_for_dataset_id,
                doi,
                dataset_pubyear=pubyear,
            ),
        )

    def _task_norm():
        try:
            return (
                "norm",
                timed_call(
                    "normalization factors",
                    get_subfield_year_norm_factors,
                    norm_path,
                    subfield_id=subfield_id,
                    pubyear=pubyear,
                ),
            )
        except KeyError:
            return ("norm", None)

    citations_list = []
    fair_report = None
    mentions_github = None
    norm = None

    with ThreadPoolExecutor(max_workers=6) as executor:
        futures = {
            executor.submit(_task_fair): "fair",
            executor.submit(_task_citations_mdc): "citations_mdc",
            executor.submit(_task_citations_oa): "citations_oa",
            executor.submit(_task_citations_dc): "citations_dc",
            executor.submit(_task_mentions_github): "mentions_github",
            executor.submit(_task_norm): "norm",
        }
        for future in as_completed(futures):
            name, value = future.result()
            if name == "fair":
                fair_report = value
            elif name == "citations_mdc" and value:
                citations_list.append(value)
            elif name == "citations_oa" and value:
                citations_list.append(value)
            elif name == "citations_dc" and value:
                citations_list.append(value)
            elif name == "mentions_github":
                mentions_github = value
            elif name == "norm":
                norm = value

    dataset_report["fair"] = fair_report

    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
        except Exception:
            fair_score = None

    with timed_block("Merging citations"):
        citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations

    with timed_block("Merging mentions"):
        mentions_list = [mentions_github] if mentions_github else []
        mentions = merge_mentions_dicts(mentions_list)
    if mentions:
        dataset_report["mentions"] = mentions
    else:
        dataset_report["mentions"] = None

    dataset_report["normalization_factors"] = norm

    with timed_block("Computing dataset index time series"):
        Fi = fair_score if fair_score is not None else 0.0
        FT = norm["FT"] if norm else 13.46
        CTw = norm["CTw"] if norm else 1.0
        MTw = norm["MTw"] if norm else 1.0

        dataset_index_series = dataset_index_year_timeseries(
            Fi=Fi,
            citations=citations,
            mentions=mentions,
            pubyear=pubyear,
            FT=FT,
            CTw=CTw,
            MTw=MTw,
        )

    if dataset_index_series:
        dataset_report["dataset_index_series"] = dataset_index_series
    else:
        dataset_report["dataset_index_series"] = None

    print("[status] dataset_index_series_from_doi done.")
    return dataset_report


def dataset_index_series_from_url(
    url: str,
    *,
    identifier: str | None = None,
    pubyear: int | None = None,
    subfield_id: str | None = None,
    subfield_name: str | None = None,
) -> dict:
    """
    Build a dataset_report starting from a URL (not a DOI).

    This function intentionally skips DOI-dependent sources:
      - get_datacite_doi_record
      - get_primary_topic_for_doi
      - find_citations_oa
      - find_citations_dc_from_citation_block
    """
    with timed_block("Validating URL"):
        if not isinstance(url, str) or not url.strip():
            raise ValueError("url must be a non-empty string")
        url = url.strip()

        if not url.startswith(("http://", "https://")):
            raise ValueError(
                f"Invalid URL format: {url} (must start with http:// or https://)"
            )
        if not is_url(url):
            raise ValueError(f"Invalid URL format: {url}")

    with timed_block("Checking URL reachability"):
        if not _is_reachable(url):
            raise ValueError(f"'{url}' does not appear to be reachable.")

    norm_url = _norm_dataset_id(url)
    norm_identifier = _norm_dataset_id(identifier)

    dataset_report = {}
    dataset_report["input_url"] = url
    dataset_report["norm_url"] = norm_url
    dataset_report["input_identifier"] = identifier
    dataset_report["norm_identifier"] = norm_identifier

    if not is_realistic_integer_year(pubyear):
        pubyear = None

    dataset_report["metadata"] = None

    if subfield_id:
        topic_obj = {"subfield_id": subfield_id}
        if subfield_name:
            topic_obj["subfield_name"] = subfield_name
        dataset_report["topic"] = topic_obj
    else:
        dataset_report["topic"] = None

    # Run FAIR, citations (MDC), mentions (GitHub), norm in parallel
    print(
        "[status] Running FAIR score, citations (MDC), mentions (GitHub), normalization (parallel)..."
    )
    mdc_path = default_mdc_db_path()
    norm_path = default_norm_db_path()

    def _task_fair():
        return ("fair", timed_call("FAIR score", fair_evaluation_report, url))

    def _task_citations_mdc():
        return (
            "citations_mdc",
            timed_call(
                "citations (MDC)",
                find_citations_mdc_duckdb,
                url,
                dataset_pubyear=pubyear,
                db_path=mdc_path,
            ),
        )

    def _task_mentions_github():
        return (
            "mentions_github",
            timed_call(
                "mentions (GitHub)",
                find_github_mentions_for_dataset_id,
                url,
                dataset_pubyear=pubyear,
            ),
        )

    def _task_norm():
        try:
            return (
                "norm",
                timed_call(
                    "normalization factors",
                    get_subfield_year_norm_factors,
                    norm_path,
                    subfield_id=subfield_id,
                    pubyear=pubyear,
                ),
            )
        except KeyError:
            return ("norm", None)

    fair_report = None
    citations_list: list[list[dict]] = []
    mentions_github = None
    norm = None

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = [
            executor.submit(_task_fair),
            executor.submit(_task_citations_mdc),
            executor.submit(_task_mentions_github),
            executor.submit(_task_norm),
        ]
        for future in as_completed(futures):
            name, value = future.result()
            if name == "fair":
                fair_report = value
            elif name == "citations_mdc" and value:
                citations_list.append(value)
            elif name == "mentions_github":
                mentions_github = value
            elif name == "norm":
                norm = value

    dataset_report["fair"] = fair_report

    fair_score = None
    if fair_report and "fair_score" in fair_report:
        try:
            fair_score = float(fair_report["fair_score"])
        except Exception:
            fair_score = None

    with timed_block("Merging citations"):
        citations = merge_citations_dicts(citations_list) if citations_list else None
    dataset_report["citations"] = citations

    with timed_block("Merging mentions"):
        mentions_list_url: list[list[dict]] = [mentions_github] if mentions_github else []
        mentions = merge_mentions_dicts(mentions_list_url) if mentions_list_url else None
    dataset_report["mentions"] = mentions

    dataset_report["normalization_factors"] = norm

    with timed_block("Computing dataset index time series"):
        Fi = fair_score if fair_score is not None else 0.0
        FT = norm["FT"] if norm else 13.46
        CTw = norm["CTw"] if norm else 1.0
        MTw = norm["MTw"] if norm else 1.0

        dataset_index_series = dataset_index_year_timeseries(
            Fi=Fi,
            citations=citations,
            mentions=mentions,
            pubyear=pubyear,
            FT=FT,
            CTw=CTw,
            MTw=MTw,
        )

    dataset_report["dataset_index_series"] = dataset_index_series or None
    print("[status] dataset_index_series_from_url done.")
    return dataset_report
