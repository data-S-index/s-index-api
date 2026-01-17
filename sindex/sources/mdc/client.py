"""Client for MDC DuckDB connections."""

from typing import Optional

import duckdb

from .constants import DEFAULT_DB_PATH


def make_duckdb_conn(db_path: Optional[str] = None, read_only: bool = True):
    """Create a DuckDB connection.

    Args:
        db_path: Path to DuckDB file. If None, uses DEFAULT_DB_PATH.
        read_only: Whether to open in read-only mode.

    Returns:
        DuckDB connection object.
    """
    if db_path is None:
        db_path = DEFAULT_DB_PATH

    return duckdb.connect(db_path, read_only=read_only)


def register_mdc_udfs(con: duckdb.DuckDBPyConnection):
    """Register MDC-specific UDFs (User Defined Functions) with the connection.

    Args:
        con: DuckDB connection object.
    """
    # Register UDFs if needed
    # This is a placeholder - implement actual UDFs as needed
    # TODO: Implement actual UDF registration when needed
