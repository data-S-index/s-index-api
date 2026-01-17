from sindex.config.env import get_env

FUJI_DEFAULT_BASE_URL = get_env("FUJI_BASE_URL") or "http://fuji:1071"
FUJI_EVALUATE_PATH = "/fuji/api/v1/evaluate"
FUJI_TIMEOUT_SECS = 60
DEFAULT_USERNAME = "marvel"
DEFAULT_PASSWORD = "wonderwoman"
