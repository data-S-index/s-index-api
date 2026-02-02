"""Constants for MDC (Metadata Catalog) integration."""

import os

# Default pattern for MDC JSON files
DEFAULT_MDC_PATTERN = "*.json"


# Default DB path - file path for local development
def _get_default_db_path():
    current_dir = os.getcwd()
    mdc_path = os.path.join(current_dir, "input", "mdc")
    return os.path.join(mdc_path, "mdc_index.duckdb")


DEFAULT_DB_PATH = _get_default_db_path()
