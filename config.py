"""Configuration for the application.

This module handles environment variable configuration for the application.
It supports loading configuration from both a local .env file and system environment variables.
"""

from os import environ
from pathlib import Path
from dotenv import dotenv_values

# Check if `.env` file exists in the current directory
# This allows the app to work with or without a .env file
env_path = Path(".") / ".env"

# Flag to indicate whether a local .env file is present
LOCAL_ENV_FILE = env_path.exists()

# Load environment variables from .env file if it exists
# dotenv_values() returns a dictionary of key-value pairs from the .env file
# If .env doesn't exist, this will be an empty dictionary
config = dotenv_values(".env")


def get_env(key):
    """Return environment variable from .env or native environment.

    This function provides a unified way to access environment variables.
    It first checks the local .env file (if it exists), then falls back
    to system environment variables.

    Args:
        key (str): The environment variable key to retrieve

    Returns:
        str or None: The value of the environment variable, or None if not found
    """
    # If .env file exists, try to get value from it first
    # Otherwise, get value directly from system environment
    return config.get(key) if LOCAL_ENV_FILE else environ.get(key)
