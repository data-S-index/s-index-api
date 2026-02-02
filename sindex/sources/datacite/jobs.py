"""DataCite harvest and citation-extraction jobs.

Harvests DataCite metadata by DOI list or date range, slims records to NDJSON,
extracts citations from citation blocks, and supports parallel/optimized batch
processing with OpenAlex pubdate lookups.
"""

from __future__ import annotations

import gzip
import multiprocessing as mp
import os
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, Iterable, List

import duckdb
import orjson
import orjson as json
import requests

from sindex.core.dates import _parse_date_strict, get_best_dataset_date
from sindex.core.http import make_session
from sindex.core.ids import _norm_doi
from sindex.core.io import _iter_json_lines
from sindex.sources.datacite.discovery import (
    get_datacite_doi_record,
    stream_datacite_records,
)

from .normalize import (
    datacite_citations_block_to_records,
    datacite_citations_block_to_records_optimized,
    slim_datacite_record,
)

# We need a global variable in the worker process to hold the shared counter
_worker_counter = None
_worker_lock = None


def harvest_datacite_doi_list_to_ndjson(
    doi_list: Iterable[str],
    output_folder: str,
    batch_size: int = 2,
):
    """
    Fetch DataCite metadata for many DOIs.

    This function is used to test our pipeline for given DOIs
    It saves Datacite records to NDJSON files in batches.
    Each NDJSON line contains exactly the JSON object returned in the DataCite
    API response under the `"data"` key, with no additional fields added.

    Args:
        doi_list: List of dois.
        batch_size: Number of metadata records to write per NDJSON output file.
        output_folder: Directory where NDJSON batch files will be written.

    Returns:
        None.
        NDJSON files are written to `output_folder`, each containing
        `batch_size` lines (except the final file, which may contain fewer).
        Each line is a standalone JSON object representing the full DataCite
        metadata record for a single DOI.

    Notes:
        - Uses a shared retry-aware `requests.Session` for performance.
        - DOIs that do not resolve in DataCite are skipped.
        - Files are named sequentially as `datacite-batch-0000.ndjson`,
          `datacite-batch-0001.ndjson`, etc.
    """
    print("[DATACITE] Step 0: Starting harvest: DOI list → NDJSON batches.")
    print("[DATACITE] Step 1: Creating session and output directory...")
    session = make_session()
    out_dir = Path(output_folder)
    out_dir.mkdir(parents=True, exist_ok=True)
    print(f"[DATACITE] Step 1 done: output_folder={out_dir}")

    batch_index = 0
    batch = []
    doi_count = 0
    skipped = 0

    for doi in doi_list:
        doi_count += 1
        print(f"[DATACITE] Step 2: Fetching DOI {doi_count}: {doi} ...")

        metadata = get_datacite_doi_record(doi, session=session)
        if metadata is None:
            print(f"[DATACITE]   No DataCite record for {doi}; skipping.")
            skipped += 1
            continue

        # Append only the metadata (the DataCite 'data' portion)
        batch.append(metadata)
        print(f"[DATACITE]   Record received; batch size={len(batch)}")

        if len(batch) == batch_size:
            fname = out_dir / f"datacite-batch-{batch_index:04d}.ndjson"
            print(
                f"[DATACITE] Step 3: Writing batch {batch_index} ({len(batch)} records) → {fname}"
            )
            with open(fname, "w", encoding="utf-8") as f:
                for obj in batch:
                    f.write(json.dumps(obj, ensure_ascii=False) + "\n")
            batch = []
            batch_index += 1
            print(f"[DATACITE] Step 3 done: batch written.")

    # Write final partial batch
    if batch:
        fname = out_dir / f"datacite-batch-{batch_index:04d}.ndjson"
        print(
            f"[DATACITE] Step 4: Writing final batch {batch_index} ({len(batch)} records) → {fname}"
        )
        with open(fname, "w", encoding="utf-8") as f:
            for obj in batch:
                f.write(json.dumps(obj, ensure_ascii=False) + "\n")
        print(
            f"[DATACITE] Step 4 done: harvest complete. Batches={batch_index + 1}, DOIs fetched={doi_count - skipped}, skipped={skipped}"
        )
    else:
        print(
            f"[DATACITE] Step 4: No remaining batch. Harvest complete. Batches={batch_index}, DOIs fetched={doi_count - skipped}, skipped={skipped}"
        )


def harvest_datacite_datasets_for_date_range_to_ndjson(
    start_date_str: str,
    end_date_str: str,
    *,
    window_days: int = 7,
    page_size: int = 1000,
    detail: bool = True,
    polite_sleep_seconds: float | None = 1.0,
    skip_empty_files: bool = False,
    save_folder: str | None = None,
    page_floor: int = 100,  # minimum fallback page size
    session: requests.Session | None = None,  # optional shared session
    timeout: tuple[int, int] = (10, 240),  # (connect, read) seconds
) -> int:
    """
    Harvest DataCite dataset records for an inclusive date range, writing one NDJSON file per date window.

    This function harvests DataCite dataset records created within [start_date, end_date] inclusive.
    We noticed that for large responses the DataCite API returns errors so we are chunking the work
    into windows of `window_days` (default 7 days as we found it to work well). It writes each window to a separate
    NDJSON file named: `datacite-<start>-<end>.ndjson`.

    The function iterates windows backwards from `end_date` toward `start_date`.

    Args:
        start_date_str: Inclusive start date (YYYY-MM-DD).
        end_date_str: Inclusive end date (YYYY-MM-DD).
        window_days: Window size in days (inclusive). Default 7.
        page_size: DataCite cursor page size (max 1000).
        polite_sleep_seconds: Optional sleep between windows to avoid hammering API.
        skip_empty_files: If True, delete output file for windows with zero records.
        save_folder: Folder where NDJSON files are written. Defaults to current directory.
        page_floor: Minimum allowed page_size when backing off after ReadTimeouts.
        session: Optional shared requests.Session; if None, a retry-configured session is created.
        timeout: (connect, read) timeout passed to DataCite requests.

    Returns:
        Total number of records written across all windows.

    Raises:
        ValueError: If dates are invalid or end_date < start_date or window_days < 1.
        requests.exceptions.RequestException: If the harvest fails after retries/backoff logic.
    """
    print("[DATACITE] Step 0: Starting date-range harvest (datasets → NDJSON).")
    if window_days < 1:
        raise ValueError("window_days must be >= 1")

    if save_folder is None:
        save_folder = "."
    os.makedirs(save_folder, exist_ok=True)
    print(f"[DATACITE] Step 1: Save folder ready: {save_folder}")

    print("[DATACITE] Step 2: Parsing date range...")
    start_date = _parse_date_strict(start_date_str)
    end_date = _parse_date_strict(end_date_str)
    if end_date < start_date:
        raise ValueError("end_date must be greater than or equal to start_date")
    print(f"[DATACITE] Step 2 done: start={start_date}, end={end_date}")

    total_records = 0
    end = end_date
    window_num = 0

    # Use provided session or create one (retry-aware)
    print("[DATACITE] Step 3: Creating/using session...")
    s = session or make_session(total_retries=6, backoff=2.0)
    print("[DATACITE] Step 3 done.")

    while True:
        window_num += 1
        # Compute window start (inclusive)
        window_start = end - timedelta(days=window_days - 1)

        # Do not cross calendar-year boundaries (to faciliate post-processing)
        year_start = date(end.year, 1, 1)
        if window_start < year_start:
            window_start = year_start

        # Clamp to requested start_date
        if window_start < start_date:
            window_start = start_date

        start_iso, end_iso = window_start.isoformat(), end.isoformat()
        out_path = os.path.join(save_folder, f"datacite-{start_iso}-{end_iso}.ndjson")
        print(
            f"[DATACITE] Step 4 (window {window_num}): Processing {start_iso} → {end_iso} → {out_path}"
        )

        # Attempt the window, on ReadTimeout shrink page_size and retry the same window
        ps = page_size
        while True:
            print(f"[DATACITE]   Fetching records (page_size={ps}, detail={detail})...")
            range_count = 0
            wrote_any = False

            try:
                with open(out_path, "w", encoding="utf-8", buffering=1024 * 1024) as f:
                    for rec in stream_datacite_records(
                        start_iso,
                        end_iso,
                        page_size=ps,
                        detail=detail,
                        session=s,
                        timeout=timeout,
                    ):
                        f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                        total_records += 1
                        range_count += 1
                        wrote_any = True

                if not wrote_any and skip_empty_files:
                    try:
                        os.remove(out_path)
                        print(f"[DATACITE]   No records; removed empty file {out_path}")
                    except OSError:
                        pass
                else:
                    print(
                        f"[DATACITE]   Window done: saved {range_count} records → {out_path}"
                    )
                break  # window succeeded

            except requests.exceptions.ReadTimeout:
                if ps > page_floor:
                    ps = max(page_floor, ps // 2)
                    print(f"  ReadTimeout; retrying with smaller page_size={ps} ...")
                    continue
                print(
                    f"  ReadTimeout persisted for {start_iso} → {end_iso} at page_size={ps}."
                )
                print(
                    f"  Resume tip: restart later with start_date_str='{start_date_str}' "
                    f"and end_date_str='{end_iso}'"
                )
                if not wrote_any and os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
                raise

            except requests.exceptions.RequestException as e:
                print(f"  Harvest failed at range {start_iso} → {end_iso}: {e}")
                print(
                    f"  Resume tip: restart later with start_date_str='{start_date_str}' "
                    f"and end_date_str='{end_iso}'"
                )
                if not wrote_any and os.path.exists(out_path):
                    try:
                        os.remove(out_path)
                    except OSError:
                        pass
                raise

        # Stop once we've reached the requested start_date
        if window_start == start_date:
            print(
                f"[DATACITE] Step 5: Harvest complete. Total records: {total_records}"
            )
            break

        # Next window ends the day before the current window starts
        print(f"[DATACITE]   Next window; sleeping {polite_sleep_seconds}s...")
        end = window_start - timedelta(days=1)
        if polite_sleep_seconds:
            time.sleep(polite_sleep_seconds)

    return total_records


def batch_slim_datacite_record_to_ndjson(
    src_folder: str,
    dst_folder: str,
    overwrite: bool = False,
    accept_gz: bool = True,
    one_line_progress: bool = True,
) -> dict:
    """
    Stream-process a directory of NDJSON(.gz) DataCite dumps and create slimmed records.

    For each NDJSON/NDJSON.GZ file in the source folder:
      - Stream (iter over lines, not loaded into memory)
      - Slim each record using `slim_datacite_record`
      - Write a matching output NDJSON(.gz) file in dst_folder

    Args:
        src_folder: Folder containing NDJSON or NDJSON.GZ source files.
        dst_folder: Folder where slimmed NDJSON files will be written.
        overwrite: If False (default), existing outputs are left untouched.
        accept_gz: If True (default), also process *.ndjson.gz files.
        one_line_progress: If True (default), progress prints as '[x/y] files completed'.

    Returns:
        A summary dict with counts:
          files_seen, records_read, records_kept, records_bad_json,
          output_dir, elapsed_sec, rate_rec_per_sec.
    """
    print("[DATACITE] Step 0: Starting batch slim: NDJSON → slim NDJSON (stream).")
    src = Path(src_folder)
    dst = Path(dst_folder)
    dst.mkdir(parents=True, exist_ok=True)
    print(f"[DATACITE] Step 1: src={src}, dst={dst}")

    patterns = ["*.ndjson"]
    if accept_gz:
        patterns.append("*.ndjson.gz")

    files = []
    for pat in patterns:
        files.extend(src.glob(pat))
    files.sort()
    num_files = len(files)
    print(f"[DATACITE] Step 2: Found {num_files} files to process.")

    total_in = total_out = total_bad = 0
    t0 = time.time()

    def _progress(i: int, n: int):
        if one_line_progress:
            print(f"\r[DATACITE] Step 3: [{i}/{n}] files completed", end="", flush=True)

    _progress(0, num_files)

    for idx, in_path in enumerate(files, 1):
        name = in_path.name
        if name.endswith(".ndjson.gz"):
            out_name = name.replace(".ndjson.gz", "-slim.ndjson.gz")
        elif name.endswith(".ndjson"):
            out_name = name.replace(".ndjson", "-slim.ndjson")
        out_path = dst / out_name

        if out_path.exists() and not overwrite:
            _progress(idx, num_files)
            continue

        if in_path.suffix == ".gz":
            out_f = gzip.open(out_path, "wt", encoding="utf-8")
        else:
            out_f = open(out_path, "wt", encoding="utf-8")

        with out_f:
            for rec, err in _iter_json_lines(in_path):
                if err:
                    total_bad += 1
                    continue

                slim = slim_datacite_record(rec)

                out_f.write(json.dumps(slim, ensure_ascii=False) + "\n")
                total_in += 1
                total_out += 1

        _progress(idx, num_files)

    if one_line_progress:
        print()

    dt = time.time() - t0
    rate = int(total_out / dt) if dt > 0 else 0
    summary = {
        "files_seen": num_files,
        "records_read": total_in,
        "records_kept": total_out,
        "records_bad_json": total_bad,
        "output_dir": str(dst.resolve()),
        "elapsed_sec": round(dt, 2),
        "rate_rec_per_sec": rate,
    }
    print(
        f"[DATACITE] Step 4: Done. files={num_files} kept={total_out:,} bad={total_bad:,} "
        f"time={dt:.1f}s rate≈{rate:,}/s → {summary['output_dir']}"
    )
    return summary


def _worker_process_file(args):
    """Process a single NDJSON(.gz) file into slim records (multiprocessing worker).

    Reads each line, slims via slim_datacite_record, writes to output.
    Used by batch_slim_datacite_record_to_ndjson_fast.

    Args:
        args: Tuple (in_path, out_path, overwrite).

    Returns:
        Tuple (records_read, records_kept, records_bad).
    """
    in_path, out_path, overwrite = args

    if out_path.exists() and not overwrite:
        return 0, 0, 0

    r = k = b = 0
    is_gz = in_path.suffix == ".gz"
    open_func = gzip.open if is_gz else open
    mode = "rb"
    out_mode = "wb"

    try:
        with open_func(in_path, mode) as f_in, open_func(out_path, out_mode) as f_out:
            for line in f_in:
                if not line.strip():
                    continue
                try:
                    rec = orjson.loads(line)
                    r += 1

                    slim = slim_datacite_record(rec)
                    f_out.write(orjson.dumps(slim) + b"\n")
                    k += 1
                except Exception:
                    b += 1
    except Exception as e:
        print(f"\n[Error] {in_path.name}: {e}")

    return r, k, b


def batch_slim_datacite_record_to_ndjson_fast(
    src_folder: str,
    dst_folder: str,
    overwrite: bool = False,
    accept_gz: bool = True,
    one_line_progress: bool = True,
    workers: int = os.cpu_count(),
) -> dict:
    """Stream-process NDJSON DataCite dumps into slim records using a process pool.

    Parallel version of batch_slim_datacite_record_to_ndjson; one file per worker.
    Returns summary dict (files_seen, records_read, records_kept, etc.).
    """
    print("[DATACITE] Step 0: Starting batch slim (parallel).")
    src, dst = Path(src_folder), Path(dst_folder)
    dst.mkdir(parents=True, exist_ok=True)
    print(f"[DATACITE] Step 1: src={src}, dst={dst}")

    # Gather NDJSON / NDJSON.GZ files
    patterns = ["*.ndjson"]
    if accept_gz:
        patterns.append("*.ndjson.gz")

    files = []
    for pat in patterns:
        files.extend(src.glob(pat))
    files.sort()
    num_files = len(files)
    if num_files == 0:
        print("[DATACITE] No files found.")
        return {}

    print(f"[DATACITE] Step 2: Found {num_files} files.")

    # Prepare task arguments
    tasks = []
    for in_path in files:
        # Naming convention: original.ndjson -> original-slim.ndjson
        suffix = ".ndjson.gz" if in_path.name.endswith(".ndjson.gz") else ".ndjson"
        out_name = in_path.name.replace(suffix, f"-slim{suffix}")
        tasks.append((in_path, dst / out_name, overwrite))
    print(f"[DATACITE] Step 3: Prepared {len(tasks)} tasks.")

    total_in = total_out = total_bad = 0
    t0 = time.time()

    # Process in parallel
    print(f"[DATACITE] Step 4: Processing {num_files} files using {workers} workers...")

    with ProcessPoolExecutor(max_workers=workers) as executor:
        future_to_index = {
            executor.submit(_worker_process_file, task): i
            for i, task in enumerate(tasks)
        }

        # Progress reporting
        for idx, future in enumerate(as_completed(future_to_index), 1):
            r, k, b = future.result()
            total_in += r
            total_out += k
            total_bad += b

            if one_line_progress:
                print(
                    f"\r[DATACITE] Step 4: [{idx}/{num_files}] files completed",
                    end="",
                    flush=True,
                )

    if one_line_progress:
        print()

    # Summary statistics
    dt = time.time() - t0
    rate = int(total_out / dt) if dt > 0 else 0
    print(f"[DATACITE] Step 5: All workers finished.")

    summary = {
        "files_seen": num_files,
        "records_read": total_in,
        "records_kept": total_out,
        "records_bad_json": total_bad,
        "output_dir": str(dst.resolve()),
        "elapsed_sec": round(dt, 2),
        "rate_rec_per_sec": rate,
    }

    print(
        f"[DATACITE] Step 6: Done. files={num_files} kept={total_out:,} bad={total_bad:,} "
        f"time={dt:.1f}s rate≈{rate:,}/s → {summary['output_dir']}"
    )
    return summary


def find_citations_dc_from_citation_block(
    target_doi: str,
    citations: Dict[str, list] | None,
    *,
    dataset_pub_date: str | None = None,
) -> List[Dict[str, object]]:
    """Build citation records from a DataCite record's citation block.

    Converts the citations dict (e.g. from slim_datacite_record) into a list
    of citation objects with dataset_id, source, citation_link, citation_weight, etc.

    Args:
        target_doi: DOI of the dataset being cited.
        citations: Citation block dict (e.g. keys like 'dois', 'references').
        dataset_pub_date: Optional publication date for weight calculation.

    Returns:
        List of citation dicts.
    """
    return datacite_citations_block_to_records(
        target_doi=target_doi,
        citations=citations,
        dataset_pub_date=dataset_pub_date,
    )


def batch_find_citations_dc_from_citation_block(
    input_folder: str, output_filepath: str
):
    """Scan NDJSON slim files, extract citation blocks, write flat citation records.

    Reads each line, gets target_doi from identifiers and citations block,
    converts to citation records with get_best_dataset_date for pub date.
    """
    input_path = Path(input_folder)

    files = list(input_path.glob("*.ndjson"))
    total_files = len(files)

    if total_files == 0:
        print(f"[DATACITE] No .ndjson files found in {input_folder}")
        return

    print(
        f"[DATACITE] Step 0: Starting citation extraction from {total_files} files → {output_filepath}"
    )
    print(f"[DATACITE] Step 1: Opening output file...")
    count_citations = 0
    with open(output_filepath, "w", encoding="utf-8") as f_out:
        for idx, file_path in enumerate(files, 1):
            print(
                f"\r[DATACITE] Step 2: Processing file {idx}/{total_files} ({file_path.name})",
                end="",
                flush=True,
            )

            with open(file_path, "r", encoding="utf-8") as f_in:
                for line in f_in:
                    if not line.strip():
                        continue

                    data = json.loads(line)

                    # Check if citations exists and is not empty/None
                    citations = data.get("citations")
                    if not citations or not any(citations.values()):
                        continue

                    # Extract target_doi
                    target_doi = None
                    for item in data.get("identifiers", []):
                        if item.get("identifier_type") == "doi":
                            target_doi = item.get("identifier")
                            break

                    if not target_doi:
                        continue

                    # Best pub date
                    publication_date = data.get("publication_date")
                    created_date = data.get("created_date")
                    best_date = get_best_dataset_date(publication_date, created_date)

                    # Process
                    citation_records = datacite_citations_block_to_records(
                        target_doi=target_doi,
                        citations=citations,
                        dataset_pub_date=best_date,
                    )

                    for record in citation_records:
                        f_out.write(json.dumps(record) + "\n")
                        count_citations += 1

                    print(
                        f"\r[DATACITE] Step 2: File {idx}/{total_files} | Citations: {count_citations:,}",
                        end="",
                        flush=True,
                    )

    print(
        f"\n[DATACITE] Step 3: Done. Saved {count_citations:,} citations to {output_filepath}"
    )


def extract_unique_dois_from_citation_blocks(input_folder: str, output_parquet: str):
    """Collect unique citation DOIs from NDJSON citation blocks and write to Parquet.

    Scans all .ndjson files in input_folder, extracts citation DOIs from each
    record's citations block, normalizes them, and writes the unique set to
    output_parquet (ZSTD-compressed Parquet).
    """
    input_path = Path(input_folder)
    files = list(input_path.glob("*.ndjson"))

    if not files:
        print(f"[DATACITE] No .ndjson files found in {input_folder}")
        return

    print(
        f"[DATACITE] Step 0: Extracting unique DOIs from {len(files)} files → {output_parquet}"
    )
    unique_dois = set()
    total_records_scanned = 0

    print(f"[DATACITE] Step 1: Scanning files in {input_path.name}...")

    for file_path in files:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue

                try:
                    item = json.loads(line)
                    total_records_scanned += 1

                    # Extract citations
                    citations = item.get("citations", {}) or {}
                    doi_list = citations.get("dois", []) or []

                    for raw_doi in doi_list:
                        normed = _norm_doi(raw_doi)
                        if normed and isinstance(normed, str):
                            clean_doi = normed.strip()
                            if clean_doi:
                                unique_dois.add(clean_doi)
                except json.JSONDecodeError:
                    continue

        print(
            f"[DATACITE]   Finished {file_path.name}. Unique DOIs so far: {len(unique_dois):,}"
        )

    print(
        f"[DATACITE] Step 1 done: Scanned all files. Total unique DOIs: {len(unique_dois):,}"
    )

    # --- SAVE TO PARQUET ---
    if unique_dois:
        print(
            f"[DATACITE] Step 2: Exporting {len(unique_dois):,} unique DOIs to Parquet..."
        )

        # Prepare for DuckDB
        doi_list_data = [(d,) for d in unique_dois]
        print(f"[DATACITE]   Prepared {len(doi_list_data):,} rows.")

        # Ensure directory exists
        os.makedirs(os.path.dirname(output_parquet), exist_ok=True)
        print(f"[DATACITE]   Output dir ready.")

        with duckdb.connect(":memory:") as temp_conn:
            temp_conn.execute("CREATE TABLE tmp_dois(doi VARCHAR)")
            temp_conn.executemany("INSERT INTO tmp_dois VALUES (?)", doi_list_data)
            print(f"[DATACITE]   Inserted into temp table.")
            temp_conn.execute(
                f"COPY tmp_dois TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION 'ZSTD')"
            )

        print(f"[DATACITE] Step 2 done: Saved to {output_parquet}")
    else:
        print("[DATACITE] Step 2: No valid DOIs found; nothing to write.")


def lookup_dates_in_oa_snapshot(db_path: str, input_parquet: str, output_parquet: str):
    """Join a Parquet DOI list with OpenAlex pubdate table and write matches to Parquet.

    Reads input_parquet (DOI column), joins on openalex_pubdate in db_path,
    groups by DOI taking MAX(pubdate), writes result to output_parquet.
    """
    start_time = time.time()
    print(
        f"[DATACITE] Step 0: Joining {input_parquet} with OpenAlex DB ({db_path}) → {output_parquet}"
    )

    # Ensure output directory exists
    print("[DATACITE] Step 1: Ensuring output directory exists...")
    os.makedirs(os.path.dirname(output_parquet), exist_ok=True)

    # Connect to your existing database
    print(f"[DATACITE] Step 2: Connecting to DB {db_path}...")
    con = duckdb.connect(db_path, read_only=True)

    try:
        # The 'magic' query:
        print(
            "[DATACITE] Step 3: Executing join query (read_parquet + openalex_pubdate)..."
        )
        # 1. read_parquet(input) acts as a virtual table
        # 2. We join it directly to the 462M row table
        # 3. We use COPY to stream results straight to the output file
        query = f"""
            COPY (
                SELECT 
                    t.doi, 
                    MAX(o.pubdate) as pubdate  -- Take the earliest date if multiple exist
                FROM read_parquet('{input_parquet}') t
                INNER JOIN openalex_pubdate o ON (t.doi = o.doi)
                GROUP BY t.doi                  -- Ensure one row per input DOI
            ) TO '{output_parquet}' (FORMAT PARQUET, COMPRESSION 'ZSTD')
        """

        con.execute(query)
        print("[DATACITE] Step 3 done: Query executed.")

        elapsed = time.time() - start_time

        # Validation
        print("[DATACITE] Step 4: Validating result count...")
        result_count = con.execute(
            f"SELECT count(*) FROM read_parquet('{output_parquet}')"
        ).fetchone()[0]
        print(
            f"[DATACITE] Step 4 done: Found {result_count:,} matches → {output_parquet} ({elapsed:.2f}s)"
        )

    except Exception as e:
        print(f"[DATACITE] Error during join: {e}")
    finally:
        con.close()


def batch_find_citations_dc_from_citation_block_optimized(
    input_folder: str,
    output_filepath: str,
    pubdate_parquet_path: str,
):
    """Extract citations from slim NDJSON using a preloaded DOI→pubdate Parquet cache.

    Uses datacite_citations_block_to_records_optimized with a date_map from
    pubdate_parquet_path for faster lookups; reports progress by line count.
    """
    # Load DOI → pubdate map from Parquet
    print(
        f"[DATACITE] Step 0: Loading pubdate cache from {Path(pubdate_parquet_path).name}..."
    )
    with duckdb.connect(":memory:") as conn:
        res = conn.execute(
            f"SELECT doi, pubdate FROM read_parquet('{pubdate_parquet_path}')"
        ).fetchall()
        date_map = dict(res)
    print(f"[DATACITE] Step 0 done: Loaded {len(date_map):,} DOI→pubdate entries.")

    input_path = Path(input_folder)
    files = list(input_path.glob("*.ndjson"))
    total_files = len(files)
    print(f"[DATACITE] Step 1: Found {total_files} NDJSON files.")

    # Count total lines for progress percentage
    print("[DATACITE] Step 2: Calculating total workload (line count)...")
    total_lines = 0
    for f in files:
        with open(f, "rb") as f_bin:
            total_lines += sum(1 for _ in f_bin)

    print(f"[DATACITE] Step 2 done: {total_lines:,} lines across {total_files} files.")

    print(f"[DATACITE] Step 3: Opening output file and processing lines...")

    count_citations = 0
    processed_lines = 0
    start_time = time.time()
    last_ui_update = 0

    with open(output_filepath, "w", encoding="utf-8") as f_out:
        for idx, file_path in enumerate(files, 1):
            with open(file_path, "r", encoding="utf-8") as f_in:
                for line in f_in:
                    processed_lines += 1

                    # --- GRANULAR PROGRESS UPDATE ---
                    # Throttle update to 10 times per second to save CPU
                    now = time.time()
                    if now - last_ui_update > 0.1:
                        pct = (processed_lines / total_lines) * 100
                        print(
                            f"\r[DATACITE] Step 3: {pct:.2f}% | Line {processed_lines:,}/{total_lines:,} | "
                            f"File {idx}/{total_files} | Citations: {count_citations:,}",
                            end="",
                            flush=True,
                        )
                        last_ui_update = now

                    line_data = line.strip()
                    if not line_data:
                        continue

                    data = json.loads(line_data)
                    citations = data.get("citations")
                    if not citations or not any(citations.values()):
                        continue

                    target_doi = next(
                        (
                            item.get("identifier")
                            for item in data.get("identifiers", [])
                            if item.get("identifier_type") == "doi"
                        ),
                        None,
                    )

                    if not target_doi:
                        continue

                    best_date = get_best_dataset_date(
                        data.get("publication_date"), data.get("created_date")
                    )

                    citation_records = datacite_citations_block_to_records_optimized(
                        target_doi=target_doi,
                        citations=citations,
                        date_map=date_map,
                        dataset_pub_date=best_date,
                    )

                    for record in citation_records:
                        f_out.write(json.dumps(record) + "\n")
                        count_citations += 1

    total_time = time.time() - start_time
    print(
        f"\n[DATACITE] Step 4: Done. Saved {count_citations:,} citations in {total_time:.2f}s → {output_filepath}"
    )


def init_worker(shared_counter, shared_lock):
    """Initialize multiprocessing worker with shared counter and lock.

    Called once per process in the pool; sets globals _worker_counter and
    _worker_lock for progress reporting in process_file_chunk.
    """
    global _worker_counter, _worker_lock
    _worker_counter = shared_counter
    _worker_lock = shared_lock


def process_file_chunk(
    file_path, start_byte, end_byte, date_map, chunk_id, output_folder
):
    """Process a byte range of an NDJSON file into citation records (parallel worker).

    Reads lines in [start_byte, end_byte), parses citation blocks, uses date_map
    for optimized pubdate lookup, writes to part_{chunk_id}.ndjson. Increments
    shared counter for progress. Returns number of citation records written.
    """
    processed_in_chunk = 0
    part_file = Path(output_folder) / f"part_{chunk_id}.ndjson"

    with open(file_path, "rb") as f, open(part_file, "wb") as f_out:
        f.seek(start_byte)
        if start_byte != 0:
            f.readline()

        while f.tell() < end_byte:
            line = f.readline()
            if not line:
                break

            # --- FIXED: Use the global lock explicitly ---
            with _worker_lock:
                _worker_counter.value += 1

            try:
                data = json.loads(line)
                citations = data.get("citations")
                if not citations or not any(citations.values()):
                    continue

                target_doi = next(
                    (
                        item.get("identifier")
                        for item in data.get("identifiers", [])
                        if item.get("identifier_type") == "doi"
                    ),
                    None,
                )
                if not target_doi:
                    continue

                best_date = get_best_dataset_date(
                    data.get("publication_date"), data.get("created_date")
                )

                citation_records = datacite_citations_block_to_records_optimized(
                    target_doi=target_doi,
                    citations=citations,
                    date_map=date_map,
                    dataset_pub_date=best_date,
                )

                for record in citation_records:
                    f_out.write(json.dumps(record) + b"\n")
                    processed_in_chunk += 1
            except Exception:
                continue

    return processed_in_chunk


def batch_find_citations_from_dc_parallel(
    input_file: str, output_filepath: str, pubdate_parquet_path: str
):
    """Extract citations from a single large NDJSON file using multiprocessing.

    Splits file by byte ranges (one chunk per CPU), runs process_file_chunk in
    parallel with shared progress counter, then merges part_*.ndjson into
    output_filepath. Uses pubdate_parquet_path for DOI→pubdate lookups.
    """
    start_time = time.time()

    # Load DOI → pubdate map
    print(
        f"[DATACITE] Step 0: Loading pubdate cache from {Path(pubdate_parquet_path).name}..."
    )
    with duckdb.connect(":memory:") as conn:
        res = conn.execute(
            f"SELECT doi, pubdate FROM read_parquet('{pubdate_parquet_path}')"
        ).fetchall()
        date_map = dict(res)
    print(f"[DATACITE] Step 0 done: {len(date_map):,} DOI→pubdate entries.")

    # Count lines for progress display
    print("[DATACITE] Step 1: Counting lines in input file...")
    with open(input_file, "rb") as f:
        total_lines = sum(1 for _ in f)
    print(f"[DATACITE] Step 1 done: {total_lines:,} lines.")

    # Build byte-range chunks for workers
    print("[DATACITE] Step 2: Building byte-range chunks...")
    file_path = Path(input_file)
    file_size = file_path.stat().st_size
    num_cpus = mp.cpu_count()
    chunk_size = file_size // num_cpus
    temp_dir = Path("./temp_parts")
    temp_dir.mkdir(exist_ok=True)

    offsets = []
    for i in range(num_cpus):
        start = i * chunk_size
        end = (i + 1) * chunk_size if i != num_cpus - 1 else file_size
        offsets.append((start, end))
    print(f"[DATACITE] Step 2 done: {num_cpus} chunks, temp_dir={temp_dir}")

    # Shared state for progress (Windows-friendly)
    print("[DATACITE] Step 3: Creating shared state (counter, lock)...")
    manager = mp.Manager()
    shared_counter = manager.Value("i", 0)
    shared_lock = manager.Lock()

    print(f"[DATACITE] Step 4: Launching {num_cpus} workers...")

    total_citations_found = 0

    with mp.Pool(
        processes=num_cpus,
        initializer=init_worker,
        initargs=(shared_counter, shared_lock),
    ) as pool:
        jobs = [
            pool.apply_async(
                process_file_chunk, (input_file, s, e, date_map, i, temp_dir)
            )
            for i, (s, e) in enumerate(offsets)
        ]

        # Monitor Loop
        while any(not j.ready() for j in jobs):
            current = shared_counter.value
            if total_lines > 0:
                pct = (current / total_lines) * 100
                print(
                    f"\r[DATACITE] Step 4: {pct:.2f}% | Processed: {current:,}/{total_lines:,} lines",
                    end="",
                    flush=True,
                )
            time.sleep(0.5)

        # Collect return values from all workers (this is the citation count)
        total_citations_found = sum(j.get() for j in jobs)
    print(
        f"\n[DATACITE] Step 4 done: All workers finished. Citations: {total_citations_found:,}"
    )

    # Merge part files into final output
    print(f"[DATACITE] Step 5: Merging {num_cpus} part files → {output_filepath}...")
    with open(output_filepath, "wb") as f_final:
        for part in sorted(temp_dir.glob("part_*.ndjson")):
            with open(part, "rb") as f_part:
                f_final.write(f_part.read())
            part.unlink()
    temp_dir.rmdir()
    print("[DATACITE] Step 5 done: Merge complete.")

    total_time = time.time() - start_time
    print(
        f"[DATACITE] Step 6: Done. Finished in {total_time:.2f}s. Citations: {total_citations_found:,}"
    )
