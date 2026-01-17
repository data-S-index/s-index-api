"""Constants for F-UJI FAIR evaluation service integration.

This module defines configuration constants for connecting to and
interacting with the F-UJI FAIR evaluation service.
"""

from sindex.config.env import get_env

# Base URL for the F-UJI service (defaults to Docker service name)
FUJI_DEFAULT_BASE_URL = get_env("FUJI_BASE_URL") or "http://fuji:1071"

# API endpoint path for FAIR evaluation requests
FUJI_EVALUATE_PATH = "/fuji/api/v1/evaluate"

# Request timeout in seconds for F-UJI API calls
FUJI_TIMEOUT_SECS = 60

# Default Basic Auth credentials for F-UJI service
DEFAULT_USERNAME = "marvel"
DEFAULT_PASSWORD = "wonderwoman"
