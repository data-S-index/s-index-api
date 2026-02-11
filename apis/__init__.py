"""Initialize the API system for the backend.

This module defines the Flask-RESTX API instance and all API endpoints
for the S-Index backend service.
"""

from functools import wraps
from flask import current_app
from flask_restx import Api, Resource, reqparse
from sindex.metrics.jobs import (
    dataset_index_series_from_doi,
    dataset_index_series_from_url,
)

# Initialize the Flask-RESTX API instance
# This provides Swagger/OpenAPI documentation and request parsing
api = Api(
    title="S-Index API",
    description="The backend API system for S-Index",
    doc="/docs",
)


def apply_rate_limit(limit_string):
    """Decorator to apply rate limiting to a method.

    This decorator accesses the limiter from Flask app extensions
    and applies the rate limit decorator at runtime.

    Args:
        limit_string: Rate limit string (e.g., "500 per hour")

    Returns:
        Decorator function
    """

    def decorator(f):
        @wraps(f)
        def wrapped(*args, **kwargs):
            limiter = current_app.extensions.get("limiter")
            if limiter:
                # Apply the rate limit decorator dynamically
                # The limiter is already configured with a global key_func in app.py
                # This will raise RateLimitExceeded if the limit is exceeded
                limited_func = limiter.limit(limit_string)(f)
                return limited_func(*args, **kwargs)
            return f(*args, **kwargs)

        return wrapped

    return decorator


@api.route("/echo", endpoint="echo")
class HelloEveryNyan(Resource):
    """Test endpoint to verify the server is active and responding."""

    @api.response(200, "Success")
    @api.response(400, "Validation Error")
    def get(self):
        """Returns a simple 'Server Active' message.

        This is a health check endpoint that can be used to verify
        the API server is running and accessible.

        Returns:
            str: A simple "Server active!" message
        """
        print("[API] GET /echo - Request received")
        print("[API] GET /echo - Status: Processing")
        result = "Server active!"
        print(f"[API] GET /echo - Status: Success - Response: {result}")
        return result


@api.route("/up", endpoint="up")
class Up(Resource):
    """Health check endpoint for Kamal deployment monitoring."""

    @api.response(200, "Success")
    def get(self):
        """Returns a simple health check response.

        This endpoint is used by Kamal (deployment tool) to verify
        the service is running and healthy.

        Returns:
            str: A simple ":)" message indicating the service is up
        """
        print("[API] GET /up - Request received")
        print("[API] GET /up - Status: Processing health check")
        result = ":)"
        print(f"[API] GET /up - Status: Success - Response: {result}")
        return result


@api.route("/dataset-index-series-from-doi", endpoint="dataset_index_series_from_doi")
class DatasetIndexSeriesFromDoi(Resource):
    """Generate dataset index series from a DOI identifier.

    This endpoint processes a DOI to generate a comprehensive dataset report
    including citations, mentions, FAIR scores, and time-series index data.
    """

    # Request parser for validating and extracting query parameters
    parser = reqparse.RequestParser()
    parser.add_argument(
        "doi",
        type=str,
        required=True,
        help="DOI string (e.g., '10.13026/kpb9-mt58')",
        location="args",
    )

    @api.expect(parser)
    @api.response(200, "Success")
    @api.response(400, "Validation Error")
    @api.response(429, "Rate limit exceeded")
    @apply_rate_limit("500 per hour")
    def get(self):
        """Returns dataset report for the given DOI.

        Rate limited to 500 requests per hour globally.

        Processes the DOI through the full pipeline:
        - Validates and normalizes the DOI
        - Fetches metadata from DataCite
        - Retrieves FAIR evaluation scores
        - Collects citations from multiple sources (MDC, OpenAlex, DataCite)
        - Collects mentions from GitHub
        - Calculates normalization factors
        - Generates dataset index time series

        Returns:
            dict: Complete dataset report with citations, mentions, FAIR scores,
                  normalization factors, and dataset index series

        Raises:
            ValueError: If the DOI format is invalid or the DOI does not resolve
            Exception: If any step in the processing pipeline fails
        """
        print("[API] GET /dataset-index-series-from-doi - Request received")

        args = self.parser.parse_args()
        doi = args["doi"]
        print(
            f"[API] GET /dataset-index-series-from-doi - Status: Processing - DOI: {doi}"
        )
        try:
            result = dataset_index_series_from_doi(doi)
            print(
                f"[API] GET /dataset-index-series-from-doi - Status: Success - DOI: {doi}"
            )
            return result
        except Exception as e:
            print(
                f"[API] GET /dataset-index-series-from-doi - Status: Error - DOI: {doi} - Error: {str(e)}"
            )
            raise


@api.route("/dataset-index-series-from-url", endpoint="dataset_index_series_from_url")
class DatasetIndexSeriesFromUrl(Resource):
    """Generate dataset index series from a URL identifier.

    This endpoint processes a URL (non-DOI) to generate a dataset report.
    Unlike the DOI endpoint, this skips DOI-dependent sources like DataCite
    and OpenAlex topic discovery.
    """

    # Request parser for validating and extracting query parameters
    parser = reqparse.RequestParser()
    parser.add_argument(
        "url",
        type=str,
        required=True,
        help="URL string",
        location="args",
    )
    parser.add_argument(
        "identifier",
        type=str,
        required=False,
        help="Identifier string",
        location="args",
    )
    parser.add_argument(
        "pubyear",
        type=str,
        required=False,
        help="Publication year string",
        location="args",
    )
    parser.add_argument(
        "subfield_id",
        type=str,
        required=False,
        help="Subfield ID string",
        location="args",
    )
    parser.add_argument(
        "subfield_name",
        type=str,
        required=False,
        help="Subfield name string",
        location="args",
    )

    @api.expect(parser)
    @api.response(200, "Success")
    @api.response(400, "Validation Error")
    @api.response(429, "Rate limit exceeded")
    @apply_rate_limit("500 per hour")
    def get(self):
        """Returns dataset report for the given URL.

        Rate limited to 500 requests per hour globally.

        Processes a URL through a simplified pipeline (skips DOI-dependent sources):
        - Validates and normalizes the URL
        - Retrieves FAIR evaluation scores
        - Collects citations from MDC (DataCite and OpenAlex citations skipped)
        - Collects mentions from GitHub
        - Uses provided topic_id and pubdate if available
        - Calculates normalization factors
        - Generates dataset index time series

        Args (via query parameters):
            url: Required dataset landing page URL
            identifier: Optional dataset identifier for MDC/GitHub searches
            pubdate: Optional publication date (any reasonable format)
            topic_id: Optional OpenAlex topic ID

        Returns:
            dict: Complete dataset report with citations, mentions, FAIR scores,
                  normalization factors, and dataset index series

        Raises:
            ValueError: If the URL format is invalid or the URL is not reachable
            Exception: If any step in the processing pipeline fails
        """
        print("[API] GET /dataset-index-series-from-url - Request received")

        args = self.parser.parse_args()
        url = args["url"]
        identifier = args.get("identifier")
        pubyear = args.get("pubyear")
        subfield_id = args.get("subfield_id")
        subfield_name = args.get("subfield_name")
        print(
            f"[API] GET /dataset-index-series-from-url - Status: Processing - "
            f"URL: {url}, identifier: {identifier}, pubyear: {pubyear}, "
            f"subfield_id: {subfield_id}, subfield_name: {subfield_name}"
        )
        try:
            result = dataset_index_series_from_url(
                url,
                identifier=identifier,
                pubyear=pubyear,
                subfield_id=subfield_id,
                subfield_name=subfield_name,
            )
            print(
                f"[API] GET /dataset-index-series-from-url - Status: Success - URL: {url}"
            )
            return result
        except Exception as e:
            print(
                f"[API] GET /dataset-index-series-from-url - Status: Error - URL: {url} - Error: {str(e)}"
            )
            raise
