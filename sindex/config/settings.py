"""Settings for sindex."""

from __future__ import annotations

from sindex.config.env import get_env

DEFAULT_USER_AGENT = "bvh (mailto:bvhpatel@gmail.com)"


def get_user_agent() -> str:
    return get_env("SINDEX_USER_AGENT") or DEFAULT_USER_AGENT


def get_github_token() -> str:
    token = get_env("GITHUB_TOKEN")
    if not token:
        raise EnvironmentError("GITHUB_TOKEN is not set in the environment.")
    return token
