"""OpenAlex API configuration constants."""

# Base URL for the OpenAlex REST API
OA_BASE_URL = "https://api.openalex.org"

# Request timeout in seconds for OpenAlex API calls
OA_TIMEOUT_SECS = 60

# Maximum number of results per page for paginated API requests
# OpenAlex supports up to 200 results per page
OA_PER_PAGE = 200

# User agent string to identify this client when making API requests
# Includes contact email for polite API usage
USER_AGENT_OA = "openalex-citations (mailto:bvhpatel@gmail.com)"

# Default email address to use in API requests (via mailto parameter)
# OpenAlex uses this for rate limiting and to contact users about API usage
DEFAULT_OA_EMAIL = "bvhpatel@gmail.com"
