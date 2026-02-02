"""MDC (Medical Data Commons) index and citation jobs.

Builds a DuckDB index from MDC JSON files, looks up citations by dataset ID,
and runs batch citation extraction from slim NDJSON into output NDJSON.
"""

import json
import os
import time
from typing import Any, Dict, List

from sindex.core.dates import _norm_date_iso, get_best_dataset_date, get_realistic_date
from sindex.core.ids import _norm_dataset_id
from sindex.metrics.dedup import dedupe_citations_by_link
from sindex.metrics.weights import citation_weight

from .client import make_duckdb_conn, register_mdc_udfs
from .constants import DEFAULT_DB_PATH, DEFAULT_MDC_PATTERN
from .discovery import list_mdc_files, mdc_glob


def build_mdc_index(
    mdc_folder: str,
    *,
    pattern: str = DEFAULT_MDC_PATTERN,
    db_path: str = DEFAULT_DB_PATH,
) -> str:
    """Build a DuckDB index from MDC JSON files in a folder.

    Reads all JSON matching pattern, normalizes dataset/citation/date via UDFs,
    dedupes by (dataset_norm, citation_link), and creates mdc_index table
    with index on dataset_norm. Returns db_path.

    Args:
        mdc_folder: Directory containing MDC JSON files.
        pattern: Glob pattern for files (default from constants).
        db_path: Path to DuckDB file (created/overwritten).

    Returns:
        db_path.

    Raises:
        FileNotFoundError: If no files match in mdc_folder.
    """
    print(
        f"[MDC] Step 0: Building index from {mdc_folder} (pattern={pattern}) → {db_path}"
    )
    print("[MDC] Step 1: Listing MDC files...")
    files = list_mdc_files(mdc_folder, pattern)
    if not files:
        raise FileNotFoundError(
            f"No MDC files found in {mdc_folder} matching {pattern}"
        )
    print(f"[MDC] Step 1 done: Found {len(files)} files.")

    print("[MDC] Step 2: Connecting to DuckDB and registering UDFs...")
    con = make_duckdb_conn(db_path, read_only=False)
    register_mdc_udfs(con)
    glob_path = mdc_glob(mdc_folder, pattern)
    con.execute("DROP TABLE IF EXISTS mdc_index")
    print("[MDC] Step 2 done.")

    # Step 3: Ingest and normalize (dataset_norm, citation_link, citation_date)
    print("[MDC] Step 3: Ingesting JSON and normalizing (mdc_index)...")
    con.execute(
        f"""
        CREATE TABLE mdc_index AS
        SELECT
            norm_dataset_id(dataset) AS dataset_norm,
            norm_doi_url_or_raw(publication) AS citation_link,
            norm_date_iso_safe(publishedDate) AS citation_date
        FROM read_json_auto('{glob_path}')
        WHERE dataset IS NOT NULL
          AND publication IS NOT NULL;
    """
    )
    n_raw = con.execute("SELECT count(*) FROM mdc_index").fetchone()[0]
    print(f"[MDC] Step 3 done: Ingested {n_raw:,} rows.")

    # Step 4: Remove rows with null/empty normalized fields
    print("[MDC] Step 4: Deleting invalid rows (null/empty norm fields)...")
    con.execute(
        """
        DELETE FROM mdc_index
        WHERE dataset_norm IS NULL OR citation_link IS NULL OR citation_link = '';
    """
    )
    print("[MDC] Step 4 done.")

    # Step 5: Dedupe by (dataset_norm, citation_link); keep any citation_date
    print("[MDC] Step 5: Deduplicating (mdc_index_dedup)...")
    con.execute("DROP TABLE IF EXISTS mdc_index_dedup")
    con.execute(
        """
        CREATE TABLE mdc_index_dedup AS
        SELECT
            dataset_norm,
            citation_link,
            any_value(citation_date) AS citation_date
        FROM mdc_index
        GROUP BY dataset_norm, citation_link;
    """
    )
    con.execute("DROP TABLE mdc_index")
    con.execute("ALTER TABLE mdc_index_dedup RENAME TO mdc_index")
    n_final = con.execute("SELECT count(*) FROM mdc_index").fetchone()[0]
    print(f"[MDC] Step 5 done: Renamed to mdc_index ({n_final:,} rows).")

    # Step 6: Index for fast lookups by dataset_norm
    print("[MDC] Step 6: Creating index and ANALYZE...")
    con.execute(
        "CREATE INDEX IF NOT EXISTS idx_mdc_dataset_norm ON mdc_index(dataset_norm)"
    )
    con.execute("ANALYZE mdc_index")

    con.close()
    print(f"[MDC] Step 6 done: Index built. mdc_index ready at {db_path}")
    return db_path


def find_citations_mdc_duckdb(
    target_id: str,
    *,
    dataset_pub_date: str | None = None,
    db_path: str = DEFAULT_DB_PATH,
) -> List[Dict[str, Any]]:
    """Look up citations for a dataset ID in the MDC DuckDB index.

    Queries mdc_index by normalized target_id, attaches citation_weight from
    dataset_pub_date and citation_date, dedupes by link. Returns list of
    citation dicts (dataset_id, source, citation_link, citation_weight, etc.).

    Args:
        target_id: Dataset DOI or URL (will be normalized).
        dataset_pub_date: Optional publication date for weight calculation.
        db_path: Path to MDC DuckDB file.

    Returns:
        List of citation dicts; empty if no matches.
    """
    print(f"[MDC] Looking up citations for dataset: {target_id}")
    target_norm = _norm_dataset_id(target_id)
    if not target_norm:
        print("[MDC] Could not normalize target_id; returning no citations.")
        return []

    if dataset_pub_date:
        try:
            dataset_pub_date = _norm_date_iso(dataset_pub_date)
            dataset_pub_date = get_realistic_date(dataset_pub_date)
        except ValueError:
            dataset_pub_date = None

    out: List[Dict[str, Any]] = []

    with make_duckdb_conn(db_path, read_only=True) as con:
        rows = con.execute(
            """
            SELECT citation_link, citation_date
            FROM mdc_index
            WHERE dataset_norm = ?
            """,
            [target_norm],
        ).fetchall()

    for citation_link, citation_date_raw in rows:
        citation_date = None
        if citation_date_raw:
            try:
                norm_iso_date = _norm_date_iso(str(citation_date_raw))
                citation_date = get_realistic_date(norm_iso_date)
            except ValueError:
                citation_date = None
        rec: Dict[str, Any] = {
            "dataset_id": target_id,
            "source": ["mdc"],
            "citation_link": citation_link,
            "citation_weight": citation_weight(dataset_pub_date, citation_date),
        }
        if citation_date:
            rec["citation_date"] = citation_date

        out.append(rec)

    deduped = dedupe_citations_by_link(out)
    print(f"[MDC] Found {len(deduped):,} citations for {target_id}.")
    return deduped


def batch_find_citations_mdc_ducdb_optimized(
    slim_folder: str, out_ndjson: str, db_path: str = DEFAULT_DB_PATH
):
    """Extract MDC citations for all datasets in slim NDJSON files.

    Loads slim NDJSON from slim_folder (best_id from identifiers, dates),
    joins with mdc_index in db_path, computes citation_weight and writes
    one citation record per line to out_ndjson. Uses read_json_auto and
    batch fetch for efficiency.
    """
    start_time = time.time()

    # Step 0: Collect non-empty NDJSON files
    print(
        f"[MDC] Step 0: Batch citation extraction: {slim_folder} + {db_path} → {out_ndjson}"
    )
    all_files = [
        os.path.join(slim_folder, f)
        for f in os.listdir(slim_folder)
        if f.endswith(".ndjson")
    ]
    valid_files = [f for f in all_files if os.path.getsize(f) > 0]
    print(f"[MDC] Step 0 done: {len(valid_files)} valid (non-empty) NDJSON files.")

    if not valid_files:
        print("[MDC] No valid (non-empty) NDJSON files found.")
        return

    # Step 1: Load input into temp table and join with mdc_index
    print("[MDC] Step 1: Connecting to DuckDB, creating temp table (input_data)...")
    with make_duckdb_conn(db_path, read_only=True) as con:
        con.create_function("_norm_dataset_id", _norm_dataset_id)

        # Temp table: best_id (DOI or first identifier), publication_date, created_date
        setup_query = """
        CREATE OR REPLACE TEMP TABLE input_data AS
        SELECT 
            COALESCE(
                (SELECT x.identifier FROM unnest(identifiers) AS t(x) 
                 WHERE x.identifier_type = 'doi' LIMIT 1),
                identifiers[1].identifier
            ) AS best_id,
            publication_date,
            created_date
        FROM read_json_auto(?, 
            ignore_errors=True, 
            columns={
                'identifiers': 'STRUCT(identifier_type VARCHAR, identifier VARCHAR)[]',
                'publication_date': 'VARCHAR',
                'created_date': 'VARCHAR'
            }
        )
        WHERE best_id IS NOT NULL;
        """
        con.execute(setup_query, [valid_files])
        n_input = con.execute("SELECT count(*) FROM input_data").fetchone()[0]
        print(f"[MDC] Step 1 done: input_data created ({n_input:,} rows).")

        # Step 2: Join with mdc citations table using the normalized ID
        print("[MDC] Step 2: Joining input_data with mdc_index...")
        results = con.execute(
            """
            SELECT 
                i.best_id,
                i.publication_date,
                i.created_date,
                m.citation_link,
                m.citation_date
            FROM input_data i
            JOIN mdc_index m ON (_norm_dataset_id(i.best_id) = m.dataset_norm)
        """
        )
        print("[MDC] Step 2 done: Join executed.")

        # Step 3: Process matched citations and stream to ndjson file
        print("[MDC] Step 3: Streaming citation records to NDJSON...")
        count = 0
        with open(out_ndjson, "w", encoding="utf-8") as f_out:
            while True:
                chunk = results.fetchmany(100000)
                if not chunk:
                    break

                for row in chunk:
                    best_id, pub_d, cre_d, c_link, c_date_raw = row
                    dataset_date = get_best_dataset_date(pub_d, cre_d)

                    citation_date = None
                    if c_date_raw:
                        try:
                            norm_iso_date = _norm_date_iso(str(c_date_raw))
                            citation_date = get_realistic_date(norm_iso_date)
                        except (ValueError, TypeError):
                            citation_date = None

                    rec = {
                        "dataset_id": best_id,
                        "source": ["mdc"],
                        "citation_link": c_link,
                        "citation_weight": citation_weight(dataset_date, citation_date),
                    }

                    if citation_date:
                        rec["citation_date"] = citation_date

                    f_out.write(json.dumps(rec) + "\n")
                    count += 1

                # Progress update
                elapsed = time.time() - start_time
                print(
                    f"\r[MDC] Step 3: Citations written: {count:,} | Elapsed: {elapsed:.2f}s",
                    end="",
                )

    elapsed = time.time() - start_time
    print(
        f"\n[MDC] Step 3 done: Done. {count:,} citations → {out_ndjson} ({elapsed:.2f}s)"
    )
